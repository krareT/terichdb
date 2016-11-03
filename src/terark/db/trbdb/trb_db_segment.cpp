#include "trb_db_segment.hpp"
#include "trb_db_index.hpp"
#include "trb_db_store.hpp"
#include "trb_db_context.hpp"
#include <terark/db/fixed_len_store.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/crc.hpp>
#include <boost/scope_exit.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataOutput.hpp>
#include <terark/io/DataIO.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/optional.hpp>
#include <thread>
#include <tbb/queuing_mutex.h>
#include <terark/io/MemStream.hpp>
#include <terark/util/mmap.hpp>

#undef min
#undef max


namespace terark { namespace db { namespace trbdb {

TERARK_DB_REGISTER_SEGMENT(TrbColgroupSegment, "trbdb", "trb");

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////


static byte constexpr logCheckPoint[] =
{
    'T', 'e', 'R', 'a', 'R', 'k', 'D', 'b'
};
static uint32_t constexpr logCheckPointCount = 16;
static size_t constexpr logCheckPointSize = 8192;

typedef LittleEndianDataOutput<AutoGrownMemIO> LogOutout_t;

//this should be in Transaction.cpp
//tempory place here
struct CommitPair
{
    llong recId, version;
};
DATA_IO_LOAD_SAVE_E(CommitPair, &recId&version);
typedef valvec<CommitPair> CommitVec_t;

class TrbLoggerContext : public RefCounter
{
public:
    LogOutout_t buf;
    CommitVec_t commit;
};

class TrbLogger
{
private:
    enum class LogAction : unsigned char
    {
        Reserved = 0,
        WritableUpdateRow,      //SubId, Data
        WritableRemoveRow,      //SubId, Version
        TableUpdateRow,         //SubId, RecId, Version, Data
        TableRemoveRow,         //RecId, Version
        TransactionUpdateRow,   //SubId, Version, Data
        TransactionCommitRow,   //[RecId, Version]
    };

    template<class ...args_t>
    void writeLog(DbContext *ctx, LogAction action, args_t const &...args)
    {
        if(!ctx->trbLog)
        {
            ctx->trbLog.reset(new TrbLoggerContext);
        }
        assert(dynamic_cast<TrbLoggerContext *>(ctx->trbLog.get()) != nullptr);
        auto &buffer = static_cast<TrbLoggerContext *>(ctx->trbLog.get())->buf;

        buffer.rewind();
        buffer.resize(12);
        buffer.seek(12);

        buffer << uint8_t(action);
        std::initializer_list<int>{((buffer << args), 0)...};

        if(m_logCount >= logCheckPointCount || m_logSize >= logCheckPointSize)
        {
            buffer.ensureWrite(logCheckPoint, sizeof logCheckPoint);
            m_logCount = 0;
            m_logSize = 0;
        }

        size_t size = buffer.tell();

        buffer.seek(8);
        buffer << Crc32c_update(0, buffer.buf() + 12, size - 12);
        buffer.seek(4);
        buffer << uint32_t(size);
        buffer.seek(0);
        buffer << Crc32c_update(0, buffer.buf() + 4, 8);

        ++m_logCount;
        m_logSize += size;

        lock_t l(m_mutex);
        m_fp.write(buffer.buf(), size);
        if(ctx->syncOnCommit)
        {
            m_fp.flush();
        }
    }

    typedef tbb::queuing_mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

public:
    struct Param
    {
        std::function<bool(uint32_t, ColumnVec const &, valvec<byte> &)> writableUpdateRow;
        std::function<bool(uint32_t, llong)> writableRemoveRow;
        std::function<bool(uint32_t, llong, llong, ColumnVec const &, valvec<byte> &)> tableUpdateRow;
        std::function<bool(llong, llong)> tableRemoveRow;
        std::function<bool(uint32_t, llong, ColumnVec const &, valvec<byte> &)> transactionUpdateRow;
        std::function<bool(CommitVec_t const &)> transactionCommitRow;
    };

private:
    mutex_t m_mutex;
    uint32_t m_seed;
    Param m_param;
    FileStream m_fp;
    uint32_t m_logSize; // these two fields didn't need sync ...
    size_t m_logCount;  // we don't care add check point later

public:
    TrbLogger() : m_seed(), m_logSize(), m_logCount()
    {
    }
    ~TrbLogger()
    {
        if(m_fp.isOpen())
        {
            m_fp.flush();
            m_fp.close();
        }
    }

    void initCallback(Param p)
    {
        assert(p.writableUpdateRow);
        assert(p.writableRemoveRow);
        m_param = p;
    }
    static std::string getFilePath(PathRef path, uint32_t seed)
    {
        char szBuf[64];
        snprintf(szBuf, sizeof(szBuf), "trb.%04ld.log", long(seed));
        return (path / szBuf).string();
    }
    void flush()
    {
        assert(m_fp.isOpen());
        lock_t l(m_mutex);
        m_fp.flush();
    }

    void initLog(PathRef path)
    {
        m_fp.open(getFilePath(path, m_seed).c_str(), "wb");
        assert(m_fp.isOpen());
        m_fp.disbuf();
    }
    void loadLog(PathRef path, Schema *schema)
    {
        while(true)
        {
            std::string fileName = getFilePath(path, m_seed);
            if(!boost::filesystem::exists(fileName))
            {
                break;
            }
            ++m_seed;
            if(boost::filesystem::file_size(fileName) == 0)
            {
                continue;
            }
            MmapWholeFile file(fileName);
            assert(file.base != nullptr);
            LittleEndianDataInput<MemIO> in; in.set(file.base, file.size);
            uint8_t action = 0;
            uint32_t sizeCrc = 0;
            uint32_t size = 0;
            uint32_t dataCrc = 0;
            uint32_t subId = 0;
            llong recId = 0;
            llong version = 0;
            valvec<byte> data;
            CommitVec_t commitVec;

            valvec<byte> buf;
            ColumnVec cols;
            byte const *pos;

            struct BadTrbLogException : std::logic_error
            {
                BadTrbLogException(std::string f) : std::logic_error("TrbSegment bad log : " + f)
                {
                }
            } badLog(fileName);

            try
            {
                while(true)
                {
                    pos = in.current();
                    in >> sizeCrc >> size >> dataCrc;
                    if(sizeCrc != Crc32c_update(0, in.current() - 8, 8))
                    {
                        throw badLog;
                    }
                    if(in.end() - in.current() < size)
                    {
                        break;
                    }
                    if(dataCrc != Crc32c_update(0, in.current(), size - 12))
                    {
                        throw badLog;
                    }
                    in >> action;
                    try
                    {
                        switch(LogAction(action))
                        {
                        case LogAction::WritableUpdateRow:
                            in >> subId >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.writableUpdateRow(subId, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::WritableRemoveRow:
                            in >> subId >> version;
                            if(!m_param.writableRemoveRow(subId, version))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TableUpdateRow:
                            in >> subId >> recId >> version >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.tableUpdateRow(subId, recId, version, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TableRemoveRow:
                            in >> recId >> version;
                            if(!m_param.tableRemoveRow(recId, version))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TransactionUpdateRow:
                            in >> subId >> version >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.transactionUpdateRow(subId, version, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TransactionCommitRow:
                            in >> commitVec;
                            if(!m_param.transactionCommitRow(commitVec))
                            {
                                throw badLog;
                            }
                            break;
                        default:
                            // wtf ?
                            throw badLog;
                        }
                    }
                    catch(EndOfFileException const &)
                    {
                        // verify by crc32 , but still overflow ?
                        throw badLog;
                    }
                    if(in.current() - pos != size)
                    {
                        if(false
                           || in.current() - pos + sizeof logCheckPoint != size
                           || std::memcmp(logCheckPoint, in.current(), sizeof logCheckPoint) != 0
                           )
                        {
                            throw badLog;
                        }
                        in.skip(sizeof logCheckPoint);
                    }
                }
            }
            catch(BadTrbLogException)
            {
                throw;
            }
            catch(EndOfFileException const &)
            {
            }
        }
        initLog(path);
    }
    void writableUpdateRow(DbContext *ctx, uint32_t subId, fstring data)
    {
        writeLog(ctx, LogAction::WritableUpdateRow, subId, data);
    }
    void writableRemoveRow(DbContext *ctx, uint32_t id, llong version)
    {
        writeLog(ctx, LogAction::WritableRemoveRow, id, version);
    }
    void tableUpdateRow(DbContext *ctx, uint32_t subId, llong recId, llong version, fstring data)
    {
        writeLog(ctx, LogAction::TableUpdateRow, subId, recId, version, data);
    }
    void tableRemoveRow(DbContext *ctx, llong recId, llong version)
    {
        writeLog(ctx, LogAction::TableRemoveRow, recId, version);
    }
    void transactionUpdateRow(DbContext *ctx, uint32_t subId, llong version, fstring data)
    {
        writeLog(ctx, LogAction::TransactionUpdateRow, subId, version, data);
    }
    void transactionCommitRow(DbContext *ctx, CommitVec_t const &commitVec)
    {
        writeLog(ctx, LogAction::TransactionCommitRow, commitVec);
    }
};




////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////


TrbRWRowMutex::~TrbRWRowMutex()
{
    for(auto pair : row_lock)
    {
        delete pair.second;
    }
    for(auto ptr : lock_pool)
    {
        delete ptr;
    }
}

TrbRWRowMutex::scoped_lock::scoped_lock(TrbRWRowMutex &mutex, size_t index, bool write)
    : parent(&mutex)
{
    {
        spin_lock_t::scoped_lock l(parent->g_lock);
        auto ib = parent->row_lock.emplace(uint32_t(index), nullptr);
        if(ib.second)
        {
            if(terark_unlikely(parent->lock_pool.empty()))
            {
                ib.first->second = item = new map_item;
            }
            else
            {
                ib.first->second = item = parent->lock_pool.pop_val();
            }
        }
        else
        {
            item = ib.first->second;
        }
        item->id = uint32_t(index);
        ++item->count;
    }
    lock.acquire(item->lock, write);
}

TrbRWRowMutex::scoped_lock::~scoped_lock()
{
    lock.release();
    if(--item->count == 0)
    {
        spin_lock_t::scoped_lock l(parent->g_lock);
        if(item->count == 0)
        {
            parent->row_lock.erase(item->id);
            parent->lock_pool.emplace_back(item);
        }
    }
}

bool TrbRWRowMutex::scoped_lock::upgrade()
{
    return lock.upgrade_to_writer();
}

bool TrbRWRowMutex::scoped_lock::downgrade()
{
    return lock.downgrade_to_reader();
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

class RowLockTransaction : public DbTransaction
{
    const SchemaConfig& m_sconf;
    TrbColgroupSegment *m_seg;
    DbContext          *m_ctx;
    boost::optional<TrbRWRowMutex::scoped_lock> m_lock;

public:
    explicit RowLockTransaction(TrbColgroupSegment* seg, DbContext* ctx)
        : m_sconf(*seg->m_schema)
    {
        m_seg = seg;
        m_ctx = ctx;
    }
    ~RowLockTransaction()
    {
    }
    void indexRemove(size_t indexId, fstring key) override
    {
        assert(started == m_status);
        m_seg->m_indices[indexId]->getWritableIndex()->remove(key, m_recId, m_ctx);
    }
    bool indexInsert(size_t indexId, fstring key) override
    {
        assert(started == m_status);
        return m_seg->m_indices[indexId]->getWritableIndex()->insert(key, m_recId, m_ctx);
    }
    void storeRemove() override
    {
        assert(started == m_status);
        size_t const colgroups_size = m_seg->m_colgroups.size();
        for(size_t i = m_seg->m_indices.size(); i < colgroups_size; ++i)
        {
            auto &store = m_seg->m_colgroups[i];
            store->getWritableStore()->remove(m_recId, m_ctx);
        }
        m_seg->m_logger->writableRemoveRow(m_ctx, uint32_t(m_recId), 0);
    }
    void storeUpdate(fstring row) override
    {
        assert(started == m_status);
        m_sconf.m_rowSchema->parseRow(row, &m_ctx->trbCols);
        size_t const colgroups_size = m_seg->m_colgroups.size();
        for(size_t i = m_seg->m_indices.size(); i < colgroups_size; ++i)
        {
            auto &store = m_seg->m_colgroups[i];
            auto &schema = m_sconf.getColgroupSchema(i);
            schema.selectParent(m_ctx->trbCols, &m_ctx->trbBuf);
            store->getUpdatableStore()->update(m_recId, m_ctx->trbBuf, m_ctx);
        }
        m_seg->m_logger->writableUpdateRow(m_ctx, uint32_t(m_recId), row);
    }
    void storeGetRow(valvec<byte>* row) override
    {
        assert(started == m_status);
        row->risk_set_size(0);
        m_seg->ColgroupWritableSegment::getValueAppend(m_recId, row, m_ctx);
    }
    void do_startTransaction() override
    {
        m_lock.emplace(m_seg->m_rowMutex, m_recId);
    }
    bool do_commit() override
    {
        m_lock.reset();
        return true;
    }
    void do_rollback() override
    {
        m_lock.reset();
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

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

DbTransaction *TrbColgroupSegment::createTransaction(DbContext* ctx)
{
    auto txn = new RowLockTransaction(this, ctx);
    return txn;
}

TrbColgroupSegment::TrbColgroupSegment()
{
    m_hasLockFreePointSearch = true;
    m_logger = new TrbLogger();
    TrbLogger::Param param;
    param.writableUpdateRow = [this](uint32_t subId, ColumnVec const &cols, valvec<byte> &buf)
    {
        auto colgroups_size = m_colgroups.size();
        size_t i = 0;
        for(; i < colgroups_size; ++i)
        {
            auto &store = m_colgroups[i];
            auto &schema = m_schema->getColgroupSchema(i);
            schema.selectParent(cols, &buf);
            store->getWritableStore()->update(subId, buf, nullptr);
        }
        return true;
        //TODO add fail check !!!
    };
    param.writableRemoveRow = [this](uint32_t subId, llong version)
    {
        (void)version;
        auto colgroups_size = m_colgroups.size();
        for(size_t i = 0; i < colgroups_size; ++i)
        {
            auto &store = m_colgroups[i];
            store->getWritableStore()->remove(subId, nullptr);
        }
        return true;
        //TODO add fail check !!!
    };
    m_logger->initCallback(param);
}

TrbColgroupSegment::~TrbColgroupSegment()
{
    delete m_logger;
}

void TrbColgroupSegment::load(PathRef path)
{
    assert(m_segDir == path);
    ReadableSegment::load(path);

    m_logger->loadLog(m_segDir, m_schema->m_rowSchema.get());

    size_t physicRows = this->getPhysicRows();
    for(size_t i = 0; i < m_colgroups.size(); ++i)
    {
        auto store = m_colgroups[i].get();
        assert(size_t(store->numDataRows()) == physicRows);
        if(size_t(store->numDataRows()) != physicRows)
        {
            TERARK_THROW(DbException
                         , "FATAL: "
                         "m_colgroups[%zd]->numDataRows() = %lld, physicRows = %zd"
                         , i, store->numDataRows(), physicRows
            );
        }
    }
}

void TrbColgroupSegment::save(PathRef path) const
{
    m_logger->flush();
}

void TrbColgroupSegment::initEmptySegment()
{
    size_t const indices_size = m_schema->getIndexNum();
    size_t const colgroups_size = m_schema->getColgroupNum();
    m_indices.resize(indices_size);
    m_colgroups.resize(colgroups_size);
    for(size_t i = 0; i < indices_size; ++i)
    {
        const Schema& schema = m_schema->getIndexSchema(i);
        m_indices[i] = createIndex(schema, m_segDir);
        auto store = m_indices[i]->getReadableStore();
        assert(store);
        m_colgroups[i] = store;
    }
    for(size_t i = indices_size; i < colgroups_size; ++i)
    {
        const Schema& schema = m_schema->getColgroupSchema(i);
        auto store = createStore(schema, m_segDir);
        assert(store);
        m_colgroups[i] = store;
    }

    m_logger->initLog(m_segDir);
}

ReadableIndex *TrbColgroupSegment::openIndex(const Schema &schema, PathRef) const
{
    return TrbWritableIndex::createIndex(schema);
}
ReadableIndex *TrbColgroupSegment::createIndex(const Schema &schema, PathRef) const
{
    return TrbWritableIndex::createIndex(schema);
}
ReadableStore *TrbColgroupSegment::createStore(const Schema &schema, PathRef) const
{
    if(schema.getFixedRowLen() > 0)
    {
        return new MemoryFixedLenStore(schema);
    }
    else
    {
        return new TrbWritableStore(schema);
    }
}

llong TrbColgroupSegment::append(fstring row, DbContext* ctx)
{
    assert(false);
    return llong(-1);
}

void TrbColgroupSegment::update(llong id, fstring row, DbContext* ctx)
{
    assert(false);
}

void TrbColgroupSegment::remove(llong id, DbContext* ctx)
{
    assert(false);
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
}

void TrbColgroupSegment::loadRecordStore(PathRef segDir)
{
    if(!m_colgroups.empty())
    {
        THROW_STD(invalid_argument, "m_colgroups must be empty");
    }
    // indices must be loaded first
    assert(m_indices.size() == m_schema->getIndexNum());

    size_t indices_size = m_schema->getIndexNum();
    size_t colgroups_size = m_schema->getColgroupNum();
    m_colgroups.resize(colgroups_size);
    for(size_t i = 0; i < indices_size; ++i)
    {
        assert(m_indices[i]); // index must have be loaded
        auto store = m_indices[i]->getReadableStore();
        assert(nullptr != store);
        m_colgroups[i] = store;
    }
    for(size_t i = indices_size; i < colgroups_size; ++i)
    {
        const Schema& schema = m_schema->getColgroupSchema(i);
        auto store = createStore(schema, m_segDir);
        assert(store);
        m_colgroups[i] = store;
    }
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

}}} // namespace terark::db::trbdb
