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

class TERARK_DB_DLL MutexLockTransaction;

struct TrbSegmentRWLock : boost::noncopyable
{
private:
    typedef tbb::queuing_rw_mutex rw_lock_t;
    typedef tbb::spin_mutex spin_lock_t;

    struct map_item
    {
        uint32_t id;
        std::atomic_uint32_t count;
        rw_lock_t lock;
    };
    trb_hash_map<uint32_t, map_item *> row_lock;
    valvec<map_item *> lock_pool;
    spin_lock_t g_lock;

public:
    ~TrbSegmentRWLock();

    class scoped_lock
    {
    private:
        TrbSegmentRWLock *parent;
        map_item *item;
        rw_lock_t::scoped_lock lock;

    public:
        scoped_lock(TrbSegmentRWLock &mutex, size_t id, bool write = true);
        ~scoped_lock();

        bool upgrade();
        bool downgrade();
    };
};

class TERARK_DB_DLL TrbColgroupSegment : public ColgroupWritableSegment {

protected:
    friend class MutexLockTransaction;
    mutable TrbSegmentRWLock m_lock;
    mutable FileStream m_fp;
    NativeDataOutput<OutputBuffer> m_out;

public:
	class TrbDbTransaction; friend class TrbDbTransaction;
	DbTransaction* createTransaction(DbContext*) override;

    TrbColgroupSegment();
	~TrbColgroupSegment();

    void load(PathRef path) override;
    void save(PathRef path) const override;

protected:
    static std::string fixFilePath(PathRef);
    void initIndicesColgroups();

    void initEmptySegment() override;

    ReadableIndex *openIndex(const Schema &, PathRef segDir) const override;
    ReadableIndex *createIndex(const Schema &, PathRef segDir) const override;
    ReadableStore *createStore(const Schema &, PathRef segDir) const override;

public:
    void indexSearchExactAppend(size_t mySegIdx, size_t indexId,
                                fstring key, valvec<llong>* recIdvec,
                                DbContext*) const override;

    void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

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
