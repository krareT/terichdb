#ifndef __nark_db_data_store_hpp__
#define __nark_db_data_store_hpp__

#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include "db_conf.hpp"
#include "db_context.hpp"

namespace nark { namespace db {

class NARK_DB_DLL Permanentable : public RefCounter {
public:
	virtual void save(fstring dir) const = 0;
	virtual void load(fstring dir) = 0;
};
typedef boost::intrusive_ptr<class Permanentable> PermanentablePtr;
typedef boost::intrusive_ptr<class ReadableStore> ReadableStorePtr;

class StoreIterator : public RefCounter {
protected:
	ReadableStorePtr m_store;
public:
	virtual ~StoreIterator();
	virtual bool increment(llong* id, valvec<byte>* val) = 0;
	virtual bool seekExact(llong  id, valvec<byte>* val) = 0;
	virtual void reset() = 0;
};
typedef boost::intrusive_ptr<StoreIterator> StoreIteratorPtr;

class NARK_DB_DLL ReadableStore : virtual public Permanentable {
public:
	virtual llong dataStorageSize() const = 0;
	virtual llong numDataRows() const = 0;
	virtual void getValueAppend(llong id, valvec<byte>* val, DbContext*) const = 0;
	virtual StoreIterator* createStoreIter(DbContext*) const = 0;
	virtual class WritableStore* getWritableStore();

	void getValue(llong id, valvec<byte>* val, DbContext* ctx) const {
		val->risk_set_size(0);
		getValueAppend(id, val, ctx);
	}
};

class NARK_DB_DLL WritableStore {
public:
	virtual ~WritableStore();
	virtual llong append(fstring row, DbContext*) = 0;
	virtual void  replace(llong id, fstring row, DbContext*) = 0;
	virtual void  remove(llong id, DbContext*) = 0;
	virtual void  clear() = 0;
	virtual void  flush() const = 0;
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

} } // namespace nark::db

#endif // __nark_db_data_store_hpp__
