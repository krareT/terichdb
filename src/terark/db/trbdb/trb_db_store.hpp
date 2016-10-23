#pragma once

#include <terark/db/db_table.hpp>
#include <terark/db/db_segment.hpp>
#include <terark/util/fstrvec.hpp>
#include <set>
#include <tbb/spin_rw_mutex.h>
#include <terark/mempool.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <terark/io/var_int.hpp>

namespace terark { namespace db { namespace trbdb {

class TrbStoreIterForward;
class TrbStoreIterBackward;

typedef tbb::spin_rw_mutex TrbStoreRWLock;

class TERARK_DB_DLL TrbWritableStore : public ReadableStore, public WritableStore {
protected:
    typedef std::size_t size_type;
    typedef terark::MemPool<4> pool_type;
    struct data_object
    {
        byte data[1];
    };
    valvec<uint32_t> m_index;
    pool_type m_data;
    ReadableSegment const *m_seg;
    mutable TrbStoreRWLock m_lock;

    fstring readItem(size_type i) const;
    void storeItem(size_type i, fstring d);
    bool removeItem(size_type i);

    friend class TrbStoreIterForward;
    friend class TrbStoreIterBackward;

public:
    TrbWritableStore(Schema const &, ReadableSegment const *);
	~TrbWritableStore();

	void save(PathRef) const override;
	void load(PathRef) override;

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  update(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;

	void shrinkToFit() override;

	AppendableStore* getAppendableStore() override;
	UpdatableStore* getUpdatableStore() override;
	WritableStore* getWritableStore() override;
};
typedef boost::intrusive_ptr<TrbWritableStore> TrbWritableStorePtr;


class TERARK_DB_DLL MemoryFixedLenStore : public ReadableStore, public WritableStore
{
protected:
    size_t m_fixlen;
    valvec<byte> m_data;

public:
    MemoryFixedLenStore(Schema const &, ReadableSegment const *);
    ~MemoryFixedLenStore();

    void save(PathRef) const override;
    void load(PathRef) override;

    llong dataStorageSize() const override;
    llong dataInflateSize() const override;
    llong numDataRows() const override;
    void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

    StoreIterator* createStoreIterForward(DbContext*) const override;
    StoreIterator* createStoreIterBackward(DbContext*) const override;

    llong append(fstring row, DbContext*) override;
    void  update(llong id, fstring row, DbContext*) override;
    void  remove(llong id, DbContext*) override;

    void shrinkToFit() override;

    AppendableStore* getAppendableStore() override;
    UpdatableStore* getUpdatableStore() override;
    WritableStore* getWritableStore() override;
};
typedef boost::intrusive_ptr<MemoryFixedLenStore> MemoryFixedLenStorePtr;

}}} // namespace terark::db::wt
