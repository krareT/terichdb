#ifndef __nark_db_data_index_hpp__
#define __nark_db_data_index_hpp__

#include <nark/util/refcount.hpp>
#include <nark/fstring.hpp>
#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include <boost/intrusive_ptr.hpp>
#include <mutex>
#include "data_store.hpp"

namespace nark {

class IndexIterator {
protected:
	const class ReadableIndex* m_index = nullptr;
public:
	virtual ~IndexIterator();
	virtual void reset() = 0;
	virtual bool increment() = 0;
	virtual bool decrement() = 0;
	virtual bool seekExact(fstring key) = 0;
	virtual bool seekLowerBound(fstring key) = 0;
	virtual void getIndexKey(llong* id, valvec<byte>* key) const = 0;
};

class ReadableIndex : virtual public RefCounter {
protected:
	SortOrder m_sortOrder;
	bool      m_isUnique;
public:
	ReadableIndex();
	virtual ~ReadableIndex();
	SortOrder sortOrder() const { return m_sortOrder; }
	bool isUnique() const { return m_isUnique; }

	virtual IndexIterator* createIndexIter() const = 0;
	virtual BaseContext* createIndexContext() const = 0;
	virtual llong numIndexRows() const = 0;
	virtual llong indexStorageSize() const = 0;
};
typedef boost::intrusive_ptr<ReadableIndex> ReadableIndexPtr;

class WritableIndex : public ReadableIndex {
public:
	virtual size_t remove(fstring key, BaseContextPtr&) = 0;
	virtual size_t remove(fstring key, llong id, BaseContextPtr&) = 0;
	virtual size_t insert(fstring key, llong id, BaseContextPtr&) = 0;
	virtual size_t replace(fstring key, llong id, llong newId, BaseContextPtr&) = 0;
	virtual void flush() = 0;
};
typedef boost::intrusive_ptr<WritableIndex> WritableIndexPtr;

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

} // namespace nark

#endif // __nark_db_data_index_hpp__
