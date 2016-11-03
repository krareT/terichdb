#pragma once

#include <mutex>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/db/db_segment.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <tbb/spin_mutex.h>
#include <tbb/queuing_rw_mutex.h>
#include <atomic>
#undef min
#undef max
#include <terark/threaded_rbtree_hash.h>


namespace terark { namespace db { namespace trbdb {

class TERARK_DB_DLL RowLockTransaction;
class TrbLogger;

struct TrbRWRowMutex : boost::noncopyable
{
private:
    typedef tbb::queuing_rw_mutex rw_lock_t;
    typedef tbb::spin_mutex spin_lock_t;

    struct map_item
    {
        uint32_t id;
        std::atomic<uint32_t> count;
        rw_lock_t lock;
    };
    trb_hash_map<uint32_t, map_item *> row_lock;
    valvec<map_item *> lock_pool;
    spin_lock_t g_lock;

public:
    ~TrbRWRowMutex();

    class scoped_lock
    {
    private:
        TrbRWRowMutex *parent;
        map_item *item;
        rw_lock_t::scoped_lock lock;

    public:
        scoped_lock(TrbRWRowMutex &mutex, size_t id, bool write = true);
        ~scoped_lock();

        bool upgrade();
        bool downgrade();
    };
};

class TERARK_DB_DLL TrbColgroupSegment : public ColgroupWritableSegment {

protected:
    friend class RowLockTransaction;
    mutable TrbRWRowMutex m_rowMutex;
    mutable TrbLogger *m_logger;

public:
	class TrbDbTransaction; friend class TrbDbTransaction;
	DbTransaction* createTransaction(DbContext*) override;

    TrbColgroupSegment();
	~TrbColgroupSegment();

    void load(PathRef path) override;
    void save(PathRef path) const override;

protected:
    void initEmptySegment() override;

    ReadableIndex *openIndex(const Schema &, PathRef segDir) const override;
    ReadableIndex *createIndex(const Schema &, PathRef segDir) const override;
    ReadableStore *createStore(const Schema &, PathRef segDir) const override;

public:
    llong append(fstring, DbContext *) override;
    void remove(llong, DbContext *) override;
    void update(llong, fstring, DbContext *) override;

    void shrinkToFit(void) override;

    void saveRecordStore(PathRef segDir) const override;
    void loadRecordStore(PathRef segDir) override;

    llong dataStorageSize() const override;
    llong totalStorageSize() const override;
};

}}} // namespace terark::db::wt
