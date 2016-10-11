#include "trb_db_segment.hpp"
#include "trb_db_index.hpp"
#include "trb_db_store.hpp"
#include "trb_db_context.hpp"
#include <terark/db/fixed_len_store.hpp>
#include <terark/num_to_str.hpp>
#include <boost/scope_exit.hpp>
#include <terark/db/mock_db_engine.hpp>

#undef min
#undef max


namespace terark { namespace db { namespace trb {

TERARK_DB_REGISTER_SEGMENT(TrbColgroupSegment);

class MutexLockTransaction : public DbTransaction
{
    const SchemaConfig& m_sconf;
    TrbColgroupSegment *m_seg;
    DbContext          *m_ctx;
public:
    explicit
        MutexLockTransaction(TrbColgroupSegment* seg, DbContext* ctx) : m_sconf(*seg->m_schema)
    {
        m_seg = seg;
        m_ctx = ctx;
    }
    ~MutexLockTransaction()
    {
    }
    void indexSearch(size_t indexId, fstring key, valvec<llong>* recIdvec)
        override
    {
        assert(started == m_status);
        m_seg->m_indices[indexId]->searchExactAppend(key, recIdvec, m_ctx);
    }
    void indexRemove(size_t indexId, fstring key, llong recId) override
    {
        assert(started == m_status);
        m_seg->m_indices[indexId]->getWritableIndex()->remove(key, recId, m_ctx);
    }
    bool indexInsert(size_t indexId, fstring key, llong recId) override
    {
        assert(started == m_status);
        return m_seg->m_indices[indexId]->getWritableIndex()->insert(key, recId, m_ctx);
    }
    void indexUpsert(size_t indexId, fstring key, llong recId) override
    {
        assert(started == m_status);
        m_seg->m_indices[indexId]->getReadableStore()->getUpdatableStore()->update(recId, key, m_ctx);
    }
    void storeRemove(llong recId) override
    {
        assert(started == m_status);
        m_seg->remove(recId, m_ctx);
    }
    void storeUpsert(llong recId, fstring row) override
    {
        assert(started == m_status);
        m_seg->update(recId, row, m_ctx);
    }
    void storeGetRow(llong recId, valvec<byte>* row) override
    {
        assert(started == m_status);
        m_seg->getValue(recId, row, m_ctx);
    }
    void do_startTransaction() override
    {
        m_seg->m_txnMutex.lock();
    }
    bool do_commit() override
    {
        m_seg->m_txnMutex.unlock();
        return true;
    }
    void do_rollback() override
    {
        m_seg->m_txnMutex.unlock();
    }
    const std::string& strError() const override
    {
        return m_strError;
    }

    valvec<byte> m_wrtBuf;
    ColumnVec    m_cols1;
    ColumnVec    m_cols2;
    std::string  m_strError;
};

DbTransaction *TrbColgroupSegment::createTransaction(DbContext* ctx)
{
    auto txn = new MutexLockTransaction(this, ctx);
    return txn;
}

TrbColgroupSegment::TrbColgroupSegment()
{
    m_hasLockFreePointSearch = true;
}

TrbColgroupSegment::~TrbColgroupSegment()
{
}

void TrbColgroupSegment::load(PathRef path)
{
}

void TrbColgroupSegment::save(PathRef path) const
{
}

ReadableIndex *TrbColgroupSegment::openIndex(const Schema &schema, PathRef segDir) const
{
    return TrbWritableIndex::createIndex(schema, segDir / "index-" + schema.m_name);
}
ReadableIndex *TrbColgroupSegment::createIndex(const Schema &schema, PathRef segDir) const
{
    return TrbWritableIndex::createIndex(schema, segDir / "index-" + schema.m_name);
}
ReadableStore *TrbColgroupSegment::createStore(const Schema &schema, PathRef segDir) const
{
    return new TrbWritableStore(segDir / "colgroup-" + schema.m_name);
}

void TrbColgroupSegment::indexSearchExactAppend(size_t mySegIdx, size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext *ctx) const
{
    if(m_isFreezed)
    {
        ColgroupSegment::indexSearchExactAppend(mySegIdx, indexId, key, recIdvec, ctx);
    }
    else
    {
        //TODO lock ...
        ColgroupSegment::indexSearchExactAppend(mySegIdx, indexId, key, recIdvec, ctx);
    }
}

llong TrbColgroupSegment::append(fstring row, DbContext* ctx)
{
    llong ret = -1;
    m_schema->m_rowSchema->parseRow(row, &ctx->trbCols);
    size_t const colgroups_size = m_colgroups.size();
    for(size_t i = m_indices.size(); i < colgroups_size; ++i)
    {
        auto &store = m_colgroups[i];
        auto &schema = m_schema->getColgroupSchema(i);
        schema.selectParent(ctx->trbCols, &ctx->trbBuf);
        llong id = store->getAppendableStore()->append(ctx->trbBuf, ctx);
        if(ret == -1)
        {
            ret = id;
        }
        else
        {
            assert(ret == id);
            //TODO check ???
        }
    }
#if _DEBUG && !defined(NDEBUG)
    valvec<byte> check;
    for(size_t i = 0; i < m_indices.size(); ++i)
    {
        auto &store = m_colgroups[i];
        auto &schema = m_schema->getColgroupSchema(i);
        schema.selectParent(ctx->trbCols, &ctx->trbBuf);
        store->getValue(ret, &check, ctx);
        assert(check == ctx->trbBuf);
    }
#endif
    return ret;
}

void TrbColgroupSegment::update(llong id, fstring row, DbContext* ctx)
{
    m_schema->m_rowSchema->parseRow(row, &ctx->trbCols);
    size_t const colgroups_size = m_colgroups.size();
    for(size_t i = m_indices.size(); i < colgroups_size; ++i)
    {
        auto &store = m_colgroups[i];
        auto &schema = m_schema->getColgroupSchema(i);
        schema.selectParent(ctx->trbCols, &ctx->trbBuf);
        store->getUpdatableStore()->update(id, ctx->trbBuf, ctx);
    }
#if _DEBUG && !defined(NDEBUG)
    valvec<byte> check;
    for(size_t i = 0; i < m_indices.size(); ++i)
    {
        auto &store = m_colgroups[i];
        auto &schema = m_schema->getColgroupSchema(i);
        schema.selectParent(ctx->trbCols, &ctx->trbBuf);
        store->getValue(id, &check, ctx);
        assert(check == ctx->trbBuf);
    }
#endif
}

void TrbColgroupSegment::remove(llong id, DbContext* ctx)
{
    size_t const colgroups_size = m_colgroups.size();
    for(size_t i = m_indices.size(); i < colgroups_size; ++i)
    {
        auto &store = m_colgroups[i];
        store->getWritableStore()->remove(id, ctx);
    }
}

void TrbColgroupSegment::shrinkToFit()
{
    for(auto &store : m_colgroups)
    {
        auto appendable_store = store->getAppendableStore();
        if(nullptr != appendable_store)
        {
            appendable_store->shrinkToFit();
        }
    }
}

void TrbColgroupSegment::saveRecordStore(PathRef segDir) const
{
    //for(size_t colgroupId : m_schema->m_updatableColgroups)
    //{
    //    const Schema& schema = m_schema->getColgroupSchema(colgroupId);
    //    assert(schema.m_isInplaceUpdatable);
    //    assert(schema.getFixedRowLen() > 0);
    //    auto store = m_colgroups[colgroupId];
    //    assert(nullptr != store);
    //    store->save(segDir / "colgroup-" + schema.m_name);
    //}
}

void TrbColgroupSegment::loadRecordStore(PathRef segDir)
{
    //assert(m_colgroups.size() == 0);
    //m_colgroups.resize(m_schema->getColgroupNum());
    //for(size_t colgroupId : m_schema->m_updatableColgroups)
    //{
    //    const Schema& schema = m_schema->getColgroupSchema(colgroupId);
    //    assert(schema.m_isInplaceUpdatable);
    //    assert(schema.getFixedRowLen() > 0);
    //    std::unique_ptr<FixedLenStore> store(new FixedLenStore(segDir, schema));
    //    store->openStore();
    //    m_colgroups[colgroupId] = store.release();
    //}
    //size_t const colgroups_size = m_colgroups.size();
    //for(size_t i = 0; i < colgroups_size; ++i)
    //{
    //    auto &store = m_colgroups[i];
    //    if(store)
    //    {
    //        continue;
    //    }
    //    if(i < m_indices.size())
    //    {
    //        store = m_indices[i]->getReadableStore();
    //    }
    //    if(!store)
    //    {
    //        const Schema& schema = m_schema->getColgroupSchema(i);
    //        store = new TrbWritableStore(segDir / "colgroup-" + schema.m_name);
    //    }
    //}
}

llong TrbColgroupSegment::dataStorageSize() const
{
    return totalStorageSize();
}
llong TrbColgroupSegment::totalStorageSize() const
{
    llong size = 0;
    for(auto &store : m_colgroups)
    {
        size += store->dataStorageSize();
    }
    return size;
}
}}} // namespace terark::db::trb
