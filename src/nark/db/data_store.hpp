#ifndef __nark_db_data_store_hpp__
#define __nark_db_data_store_hpp__

#include "db_conf.hpp"
#include "db_context.hpp"
#include <boost/filesystem.hpp>

namespace boost { namespace filesystem {
	inline path operator+(const path& x, nark::fstring y) {
		path z = x;
		z.concat(y.begin(), y.end());
		return z;
	}
	inline path operator+(const path& x, const char* y) {
		path z = x;
		z.concat(y, y + strlen(y));
		return z;
	}
//	class path;
}}

namespace nark { namespace db {

typedef const boost::filesystem::path& PathRef;

class NARK_DB_DLL Permanentable : public RefCounter {
	NARK_DB_NON_COPYABLE_CLASS(Permanentable);
public:
	Permanentable();
	~Permanentable();
	///@ object can hold a m_path, when path==m_path, it is a flush
	virtual void save(PathRef path) const = 0;

	virtual void load(PathRef path) = 0;
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

class NARK_DB_DLL WritableStore;
class NARK_DB_DLL ReadableIndex;
class NARK_DB_DLL ReadableStore : virtual public Permanentable {
	NARK_DB_NON_COPYABLE_CLASS(ReadableStore);
public:
	struct RegisterStoreFactory {
		RegisterStoreFactory(const char* fnameSuffix, const std::function<ReadableStore*()>& f);
	};
#define NARK_DB_REGISTER_STORE(suffix, StoreClass) \
	static ReadableStore::RegisterStoreFactory \
		regStore_##StoreClass(suffix, [](){ return new StoreClass(); });

	static ReadableStore* openStore(PathRef segDir, fstring fname);

	ReadableStore();
	~ReadableStore();
	virtual llong dataStorageSize() const = 0;
	virtual llong numDataRows() const = 0;
	virtual void getValueAppend(llong id, valvec<byte>* val, DbContext*) const = 0;
	virtual StoreIterator* createStoreIterForward(DbContext*) const = 0;
	virtual StoreIterator* createStoreIterBackward(DbContext*) const = 0;
	virtual WritableStore* getWritableStore();
	virtual ReadableIndex* getReadableIndex();

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
