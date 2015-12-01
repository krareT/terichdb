#ifndef __nark_db_data_index_hpp__
#define __nark_db_data_index_hpp__

#include "data_store.hpp"

namespace nark { namespace db {

typedef boost::intrusive_ptr<class ReadableIndex> ReadableIndexPtr;

class NARK_DB_DLL IndexIterator : public RefCounter {
public:
	virtual ~IndexIterator();
	virtual void reset(PermanentablePtr owner) = 0;
	virtual bool increment(llong* id, valvec<byte>* key) = 0;
	virtual bool decrement(llong* id, valvec<byte>* key) = 0;
	virtual bool seekExact(fstring key) = 0;
	virtual bool seekLowerBound(fstring key) = 0;
};
typedef boost::intrusive_ptr<IndexIterator> IndexIteratorPtr;

class NARK_DB_DLL ReadableIndex : virtual public Permanentable {
protected:
	bool m_isOrdered;
	bool m_isUnique;
public:
	ReadableIndex();
	virtual ~ReadableIndex();
	bool isOrdered() const { return m_isOrdered; }
	bool isUnique() const { return m_isUnique; }

	virtual IndexIterator* createIndexIter(DbContext*) const = 0;
	virtual llong numIndexRows() const = 0;
	virtual llong indexStorageSize() const = 0;
	virtual bool  exists(fstring key) const = 0;
};
typedef boost::intrusive_ptr<ReadableIndex> ReadableIndexPtr;

class NARK_DB_DLL WritableIndex : public ReadableIndex {
public:
	virtual bool remove(fstring key, llong id, DbContext*) = 0;
	virtual bool insert(fstring key, llong id, DbContext*) = 0;
	virtual bool replace(fstring key, llong id, llong newId, DbContext*) = 0;
	virtual void clear() = 0;
	virtual void flush() const = 0;
};
typedef boost::intrusive_ptr<WritableIndex> WritableIndexPtr;

class NARK_DB_DLL ReadableIndexStore :
		   public ReadableIndex, public ReadableStore {};
typedef boost::intrusive_ptr<ReadableIndexStore> ReadableIndexStorePtr;

class NARK_DB_DLL WritableIndexStore :
		   public WritableIndex, public ReadableStore {};
typedef boost::intrusive_ptr<WritableIndexStore> WritableIndexStorePtr;

/*
class CompositeIndex : public WritableIndex {
	friend class CompositeIterator;
	std::string m_myDir;
	valvec<class CompositeIterator*> m_iterSet;
	valvec<ReadableIndexPtr> m_readonly;
	const valvec<llong>* m_rowNumVec; // owned by DataStore
	const febitvec* m_isDeleted; // owned by DataStore
	WritableIndex* m_writable;
	mutable std::mutex m_mutex;

public:
	CompositeIndex(fstring dir, const valvec<llong>* rowNumVec, const febitvec* isDel);
	llong numIndexRows() const override;
	IndexIterator* createIndexIter() const override;
	llong indexStorageSize() const override;

	size_t remove(fstring key) override;
	size_t remove(fstring key, llong id) override;
	size_t insert(fstring key, llong id) override;
	size_t replace(fstring key, llong oldId, llong newId) override;

	void compact();
	virtual ReadableIndex* mergeToReadonly(const valvec<ReadableIndexPtr>& input) const = 0;
	virtual WritableIndex* createWritable() const = 0;
};
typedef boost::intrusive_ptr<CompositeIndex> CompositeIndexPtr;
*/

} } // namespace nark::db

#endif // __nark_db_data_index_hpp__
