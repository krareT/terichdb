#ifndef __nark_db_data_store_hpp__
#define __nark_db_data_store_hpp__

#include <nark/util/refcount.hpp>
#include <nark/fstring.hpp>
#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include <boost/intrusive_ptr.hpp>
#include <mutex>
#include "db_conf.hpp"

namespace nark {

class NARK_DB_DLL_EXPORT Permanentable : public RefCounter {
public:
	virtual void save(fstring dir) const = 0;
	virtual void load(fstring dir) = 0;
};

typedef boost::intrusive_ptr<class ReadableStore> ReadableStorePtr;

class StoreIterator : public RefCounter {
protected:
	ReadableStorePtr m_store;
public:
	virtual ~StoreIterator();
	virtual bool increment() = 0;
	virtual void getKeyVal(llong* idKey, valvec<byte>* val) const = 0;
};
typedef boost::intrusive_ptr<StoreIterator> StoreIteratorPtr;

class NARK_DB_DLL_EXPORT ReadableStore : virtual public Permanentable {
public:
	virtual llong dataStorageSize() const = 0;
	virtual llong numDataRows() const = 0;
	virtual void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const = 0;
	virtual StoreIteratorPtr createStoreIter() const = 0;
	virtual BaseContextPtr createStoreContext() const = 0;
	virtual class WritableStore* getWritableStore();
};

class NARK_DB_DLL_EXPORT WritableStore {
public:
	virtual ~WritableStore();
	virtual llong append(fstring row, BaseContextPtr&) = 0;
	virtual void  replace(llong id, fstring row, BaseContextPtr&) = 0;
	virtual void  remove(llong id, BaseContextPtr&) = 0;
	virtual void  flush() = 0;
};
//typedef boost::intrusive_ptr<WritableStore> WritableStorePtr;
/*
class CompositeStore : public WritableStore {
	valvec<ReadableStorePtr> m_readonly;
	valvec<llong> m_rowNumVec;
	febitvec m_isDeleted;
	WritableStore* m_writable;
	std::mutex m_mutex;

public:
	bool isDeleted(llong id) const { return m_isDeleted[id]; }
	llong numDataRows() const override;

	llong insert(fstring row) override;
	llong replace(llong id, fstring row) override;
	void remove(llong id) override;

	void compact();
	virtual ReadableStore* mergeToReadonly(const valvec<ReadableStorePtr>& input) const = 0;
	virtual WritableStore* createWritable() const = 0;
};
typedef boost::intrusive_ptr<CompositeStore> CompositeStorePtr;
*/

} // namespace nark

#endif // __nark_db_data_store_hpp__
