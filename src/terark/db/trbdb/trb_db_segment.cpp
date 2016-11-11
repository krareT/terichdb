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
#include <algorithm>

#undef min
#undef max


namespace terark { namespace db { namespace trbdb {

TERARK_DB_REGISTER_SEGMENT(TrbColgroupSegment, "trbdb", "trb");

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////


struct TrbLogHeader
{
    byte name[16];
    uint32_t magic;
    uint32_t ver;
    byte empty[128 - 24];

    static TrbLogHeader getDefault()
    {
        static_assert(sizeof(TrbLogHeader) == 128, "WTF ?");
        return
        {
            // name =
            {
                'T', 'E', 'R', 'A', 'R', 'K', '-', 'D', 'B', ' ', 'T', 'r', 'b', 'L', 'o', 'g'
            },
            // magic =
            0x12239275,
            // ver =
            1,
            // empty =
            {},
        };
    }
};

static TrbLogHeader const logHeader = TrbLogHeader::getDefault();
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

/**
 * |--- crc size ---|--- data size ---|--- crc data ---|--- action ---|--- param(s) ---|
 * |                |                 |                |              |                |
 * |                |    data size                  = size( action    +    param(s) )  |
 * |                |                 |    crc data = crc ( action    +    param(s) )  |
 * |    crc size = crc ( data size    +    crc data )  |              |                |
 * |                |                 |                |              |                |
 * |--- crc size ---|--- data size ---|--- crc data ---|--- action ---|--- param(s) ---|
 */
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
    void writeLog(DbContext *ctx, uint64_t seq, LogAction action, args_t const &...args)
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

        buffer << seq << uint8_t(action);
        std::initializer_list<int>{((buffer << args), 0)...};

        if(m_logCount >= logCheckPointCount || m_logSize >= logCheckPointSize)
        {
            buffer.ensureWrite(logCheckPoint, sizeof logCheckPoint);
            m_logCount = 0;
            m_logSize = 0;
        }

        size_t size = buffer.tell();

        buffer.seek(4);
        buffer << uint32_t(size);
        buffer << Crc32c_update(0, buffer.buf() + 12, size - 12);
        buffer.seek(0);
        buffer << Crc32c_update(0, buffer.buf() + 4, 8);

        ++m_logCount;
        m_logSize += size;
        m_totalLogSize += size;

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
    std::atomic<uint64_t> m_totalLogSize;   //togal log size

public:
    TrbLogger() : m_seed(), m_logSize(), m_logCount(), m_totalLogSize{0}
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

    uint64_t logSize()
    {
        return m_totalLogSize;
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
        m_fp.write(&logHeader, sizeof logHeader);
        m_totalLogSize += sizeof logHeader;
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
            struct BadTrbLogException : public std::logic_error
            {
                BadTrbLogException(std::string f) : std::logic_error("TrbSegment bad log : " + f)
                {
                }
            } badLog(fileName);

            MmapWholeFile file(fileName);
            assert(file.base != nullptr);
            m_totalLogSize += file.size;
            LittleEndianDataInput<MemIO> in; in.set(file.base, file.size);
            TrbLogHeader header;
            in.ensureRead(&header, sizeof header);
            if(false
               || std::memcmp(header.name, logHeader.name, sizeof header.name) != 0
               || header.magic != logHeader.magic
               || header.ver != logHeader.ver
               || std::find_if(header.empty,
                               header.empty + sizeof header.empty,
                               [](byte b){ return b != 0; }
               ) != header.empty + sizeof header.empty
               )
            {
                //TODO compatible old version
                throw badLog;
            }
            uint8_t action = 0;
            uint32_t sizeCrc = 0;
            uint32_t size = 0;
            uint32_t dataCrc = 0;
            uint32_t subId = 0;
            llong recId = 0;
            llong version = 0;
            valvec<byte> data;
            CommitVec_t commitVec;
            uint64_t seq = 0;

            valvec<byte> buf;
            ColumnVec cols;
            struct heap_item
            {
                uint64_t seq;
                LittleEndianDataInput<MemIO> in;
                struct comp
                {
                    bool operator()(heap_item const &left, heap_item const &right)
                    {
                        return left.seq > right.seq;
                    }
                };
            };
            valvec<heap_item> seqHeap;
            uint64_t currentSeq = 0;
            byte const *pos = in.current();

            auto proc_seq = [&]
            {
                while(!seqHeap.empty() && seqHeap.front().seq == currentSeq)
                {
                    std::pop_heap(seqHeap.begin(), seqHeap.end(), heap_item::comp());
                    auto seq_in = seqHeap.pop_val().in;
                    ++currentSeq;
                    seq_in >> action;
                    try
                    {
                        switch(LogAction(action))
                        {
                        case LogAction::WritableUpdateRow:
                            seq_in >> subId >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.writableUpdateRow(subId, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::WritableRemoveRow:
                            seq_in >> subId >> version;
                            if(!m_param.writableRemoveRow(subId, version))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TableUpdateRow:
                            seq_in >> subId >> recId >> version >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.tableUpdateRow(subId, recId, version, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TableRemoveRow:
                            seq_in >> recId >> version;
                            if(!m_param.tableRemoveRow(recId, version))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TransactionUpdateRow:
                            seq_in >> subId >> version >> data;
                            schema->parseRow(data, &cols);
                            if(!m_param.transactionUpdateRow(subId, version, cols, buf))
                            {
                                throw badLog;
                            }
                            break;
                        case LogAction::TransactionCommitRow:
                            seq_in >> commitVec;
                            if(!m_param.transactionCommitRow(commitVec))
                            {
                                throw badLog;
                            }
                            break;
                        default:
                            // verify by crc32 , but still error action ?
                            throw badLog;
                        }
                    }
                    catch(EndOfFileException const &)
                    {
                        // verify by crc32 , but still overflow ?
                        throw badLog;
                    }
                }
            };

            try
            {
                while(true)
                {
                    in >> sizeCrc >> size >> dataCrc;
                    if(Crc32c_update(0, in.current() - 8, 8) != sizeCrc)
                    {
                        throw badLog;
                    }
                    if(size < 12)
                    {
                        // verify by crc32 , but still error size ?
                        throw badLog;
                    }
                    if(in.end() - in.current() < size - 12)
                    {
                        throw EndOfFileException();
                    }
                    if(Crc32c_update(0, in.current(), size - 12) != dataCrc)
                    {
                        throw badLog;
                    }
                    in >> seq;
                    seqHeap.emplace_back(heap_item{seq, {in.current(), in.current() + size - 8}});
                    std::push_heap(seqHeap.begin(), seqHeap.end(), heap_item::comp());
                    in.skip(size - 20);
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
                    pos = in.current();
                    proc_seq();
                }
            }
            catch(BadTrbLogException const &)
            {
                throw;
            }
            catch(EndOfFileException const &)
            {
                try
                {
                    proc_seq();
                }
                catch(EndOfFileException const &)
                {
                    throw badLog;
                }
                if(!seqHeap.empty())
                {
                    fprintf(stderr,
                            "WARN: TrgSegment log incomplete , caused by unsafe shutdown . %s : %zd operation(s)\n",
                            fileName.c_str(),
                            seqHeap.size()
                    );
                }
                if(pos != in.end())
                {
                    fprintf(stderr,
                            "INFO: TrgSegment log incomplete , caused by unsafe shutdown . %s : %zd byte(s)\n",
                            fileName.c_str(),
                            in.end() - pos
                    );
                }
            }
        }
        initLog(path);
    }
    void writableUpdateRow(DbContext *ctx, uint64_t seq, uint32_t subId, fstring data)
    {
        writeLog(ctx, seq, LogAction::WritableUpdateRow, subId, data);
    }
    void writableRemoveRow(DbContext *ctx, uint64_t seq, uint32_t id, llong version)
    {
        writeLog(ctx, seq, LogAction::WritableRemoveRow, id, version);
    }
    void tableUpdateRow(DbContext *ctx, uint64_t seq, uint32_t subId, llong recId, llong version, fstring data)
    {
        writeLog(ctx, seq, LogAction::TableUpdateRow, subId, recId, version, data);
    }
    void tableRemoveRow(DbContext *ctx, uint64_t seq, llong recId, llong version)
    {
        writeLog(ctx, seq, LogAction::TableRemoveRow, recId, version);
    }
    void transactionUpdateRow(DbContext *ctx, uint64_t seq, uint32_t subId, llong version, fstring data)
    {
        writeLog(ctx, seq, LogAction::TransactionUpdateRow, seq, subId, version, data);
    }
    void transactionCommitRow(DbContext *ctx, uint64_t seq, CommitVec_t const &commitVec)
    {
        writeLog(ctx, seq, LogAction::TransactionCommitRow, seq, commitVec);
    }
};




////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

TrbRWRowMutex::map_item::map_item(uint32_t _id)
    : id(_id), count{1}
{
}

TrbRWRowMutex::TrbRWRowMutex(spin_mutex_t &mutex)
    : g_mutex(mutex)
{

}

TrbRWRowMutex::~TrbRWRowMutex()
{
    for(auto pair : row_mutex)
    {
        delete pair.second;
    }
    for(auto ptr : mutex_pool)
    {
        delete ptr;
    }
}

TrbRWRowMutex::scoped_lock::scoped_lock(TrbRWRowMutex &mutex, llong index, bool write)
    : parent(&mutex)
{
    assert(index >= 0);
    assert(index <= 0x3FFFFFFFU);
    uint32_t u32_index = uint32_t(index);
    {
        spin_mutex_t::scoped_lock l(parent->g_mutex);
        auto ib = parent->row_mutex.emplace(u32_index, nullptr);
        if(ib.second)
        {
            if(terark_unlikely(parent->mutex_pool.empty()))
            {
                ib.first->second = item = new map_item(u32_index);
                assert(item->count == 1);
                assert(item->id == u32_index);
            }
            else
            {
                ib.first->second = item = parent->mutex_pool.pop_val();
                assert(item->count == 0);
                item->id = u32_index;
                ++item->count;
            }
        }
        else
        {
            item = ib.first->second;
            assert(item->id == u32_index);
            ++item->count;
        }
    }
    lock.acquire(item->lock, write);
}

TrbRWRowMutex::scoped_lock::~scoped_lock()
{
    lock.release();
    spin_mutex_t::scoped_lock l(parent->g_mutex);
    if(--item->count == 0)
    {
        parent->row_mutex.erase(item->id);
        parent->mutex_pool.emplace_back(item);
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
    uint64_t            m_seq;
    size_t              m_seqIndex;
    boost::optional<TrbRWRowMutex::scoped_lock> m_lock;

public:
    explicit RowLockTransaction(TrbColgroupSegment* seg, DbContext* ctx)
        : m_sconf(*seg->m_schema)
        , m_seq(std::numeric_limits<uint64_t>::max())
    {
        m_seg = seg;
        m_ctx = ctx;
        m_seqIndex = m_sconf.m_uniqIndices.empty() ? 0 : m_sconf.m_uniqIndices.back();
    }
    ~RowLockTransaction()
    {
    }
    void indexRemove(size_t indexId, fstring key) override
    {
        assert(started == m_status);
        if(m_seqIndex == indexId)
        {
            assert(dynamic_cast<TrbWritableIndex *>(m_seg->m_indices[indexId]->getWritableIndex()) != nullptr);
            TrbWritableIndex *idx = static_cast<TrbWritableIndex *>(m_seg->m_indices[indexId]->getWritableIndex());
            idx->removeWithSeqId(key, m_recId, m_seq, m_ctx);
        }
        else
        {
            m_seg->m_indices[indexId]->getWritableIndex()->remove(key, m_recId, m_ctx);
        }
    }
    bool indexInsert(size_t indexId, fstring key) override
    {
        assert(started == m_status);
        if(m_seqIndex == indexId)
        {
            assert(dynamic_cast<TrbWritableIndex *>(m_seg->m_indices[indexId]->getWritableIndex()) != nullptr);
            TrbWritableIndex *idx = static_cast<TrbWritableIndex *>(m_seg->m_indices[indexId]->getWritableIndex());
            return idx->insertWithSeqId(key, m_recId, m_seq, m_ctx);
        }
        else
        {
            return m_seg->m_indices[indexId]->getWritableIndex()->insert(key, m_recId, m_ctx);
        }
    }
    void storeRemove() override
    {
        assert(started == m_status);
        assert(m_seq != std::numeric_limits<uint64_t>::max());
        size_t const colgroups_size = m_seg->m_colgroups.size();
        for(size_t i = m_seg->m_indices.size(); i < colgroups_size; ++i)
        {
            auto &store = m_seg->m_colgroups[i];
            store->getWritableStore()->remove(m_recId, m_ctx);
        }
        m_seg->m_logger->writableRemoveRow(m_ctx, m_seq, uint32_t(m_recId), 0);
    }
    void storeUpdate(fstring row) override
    {
        assert(started == m_status);
        assert(m_seq != std::numeric_limits<uint64_t>::max());
        m_sconf.m_rowSchema->parseRow(row, &m_ctx->trbCols);
        size_t const colgroups_size = m_seg->m_colgroups.size();
        for(size_t i = m_seg->m_indices.size(); i < colgroups_size; ++i)
        {
            auto &store = m_seg->m_colgroups[i];
            auto &schema = m_sconf.getColgroupSchema(i);
            schema.selectParent(m_ctx->trbCols, &m_ctx->trbBuf);
            store->getUpdatableStore()->update(m_recId, m_ctx->trbBuf, m_ctx);
        }
        m_seg->m_logger->writableUpdateRow(m_ctx, m_seq, uint32_t(m_recId), row);
    }
    void storeGetRow(valvec<byte>* row) override
    {
        assert(started == m_status);
        row->risk_set_size(0);
        m_seg->ColgroupWritableSegment::getValueAppend(m_recId, row, m_ctx);
    }
    void do_startTransaction() override
    {
        m_seq = std::numeric_limits<uint64_t>::max();
        m_lock.emplace(m_seg->m_rowMutex, m_recId, true);
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
    : m_rowMutex(m_segMutex)
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
        m_isDel.set0(subId);
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
        m_isDel.set1(subId);
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
    if(!m_isDel.empty())
    {
        m_isDel.set1(0, m_isDel.size());
    }

    m_logger->loadLog(m_segDir, m_schema->m_rowSchema.get());

    assert(!m_colgroups.empty());
    size_t storeRows = m_colgroups[0]->numDataRows();
    for(size_t i = 1; i < m_colgroups.size(); ++i)
    {
        auto store = m_colgroups[i].get();
        assert(size_t(store->numDataRows()) == storeRows);
        if(size_t(store->numDataRows()) != storeRows)
        {
            TERARK_THROW(DbException
                         , "FATAL: "
                         "m_colgroups[%zd]->numDataRows() = %lld, physicRows = %zd"
                         , i, store->numDataRows(), storeRows
            );
        }
    }
    m_delcnt = m_isDel.popcnt();
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

void TrbColgroupSegment::getValueAppend(llong id, valvec<byte>* val, DbContext *ctx) const
{
    try
    {
        if(m_isFreezed)
        {
            ColgroupWritableSegment::getValueAppend(id, val, ctx);
        }
        else
        {
            TrbRWRowMutex::scoped_lock l(m_rowMutex, id, false);
            ColgroupWritableSegment::getValueAppend(id, val, ctx);
        }
    }
    catch(TrbReadDeletedRecordException const &ex)
    {
        throw ReadDeletedRecordException(m_segDir.string(), -1, ex.id);
    }
}

void TrbColgroupSegment::selectColumns(llong recId,
                                       size_t const *colsId,
                                       size_t colsNum,
                                       valvec<byte> *colsData,
                                       DbContext *ctx
) const
{
    try
    {
        if(m_isFreezed)
        {
            ColgroupSegment::selectColumnsByPhysicId(recId, colsId, colsNum, colsData, ctx);
        }
        else
        {
            TrbRWRowMutex::scoped_lock l(m_rowMutex, recId, false);
            ColgroupSegment::selectColumnsByPhysicId(recId, colsId, colsNum, colsData, ctx);
        }
    }
    catch(TrbReadDeletedRecordException const &ex)
    {
        throw ReadDeletedRecordException(m_segDir.string(), -1, ex.id);
    }
}

void TrbColgroupSegment::selectOneColumn(llong recId,
                                         size_t columnId,
                                         valvec<byte> *colsData,
                                         DbContext *ctx
) const
{
    try
    {
        ColgroupSegment::selectOneColumnByPhysicId(recId, columnId, colsData, ctx);
    }
    catch(TrbReadDeletedRecordException const &ex)
    {
        throw ReadDeletedRecordException(m_segDir.string(), -1, ex.id);
    }
}

void TrbColgroupSegment::selectColgroups(llong id,
                                         size_t const *cgIdvec,
                                         size_t cgIdvecSize,
                                         valvec<byte> *cgDataVec,
                                         DbContext *ctx
) const
{
    try
    {
        if(m_isFreezed)
        {
            ColgroupSegment::selectColgroupsByPhysicId(id, cgIdvec, cgIdvecSize, cgDataVec, ctx);
        }
        else
        {
            TrbRWRowMutex::scoped_lock l(m_rowMutex, id, false);
            ColgroupSegment::selectColgroupsByPhysicId(id, cgIdvec, cgIdvecSize, cgDataVec, ctx);
        }
    }
    catch(TrbReadDeletedRecordException const &ex)
    {
        assert(ex.id == id);
        throw ReadDeletedRecordException(m_segDir.string(), -1, ex.id);
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

void TrbColgroupSegment::shrinkToSize(size_t size)
{
    for(auto &store : m_colgroups)
    {
        auto appendable_store = store->getAppendableStore();
        if(nullptr != appendable_store)
        {
            appendable_store->shrinkToSize(size);
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
    return m_logger->logSize();
}
llong TrbColgroupSegment::totalStorageSize() const
{
    return m_logger->logSize();
}

}}} // namespace terark::db::trbdb
