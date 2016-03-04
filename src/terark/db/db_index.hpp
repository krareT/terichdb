#ifndef __nark_db_db_index_hpp__
#define __nark_db_db_index_hpp__

#include "db_store.hpp"

namespace terark { namespace db {

typedef boost::intrusive_ptr<class ReadableIndex> ReadableIndexPtr;

class NARK_DB_DLL IndexIterator : public RefCounter {
public:
	virtual ~IndexIterator();
	virtual void reset() = 0;
	virtual bool increment(llong* id, valvec<byte>* key) = 0;

	///@returns: ret = compare(*retKey, key)
	/// similar with wiredtiger.cursor.search_near
	/// for all iter:
	///          ret == 0 : exact match
	/// for forward iter:
	///          ret >  0 : *retKey > key
	///          ret <  0 : key is greater than all keys, iter is eof
	/// for backward iter:
	///          ret >  0 : *retKey < key
	///          ret <  0 : key is less than all keys, iter is eof
	virtual int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) = 0;
};
typedef boost::intrusive_ptr<IndexIterator> IndexIteratorPtr;

class NARK_DB_DLL ReadableIndex : virtual public Permanentable {
	NARK_DB_NON_COPYABLE_CLASS(ReadableIndex);
protected:
	bool m_isOrdered;
	bool m_isUnique;
	bool m_isIndexKeyByteLex;
public:
	ReadableIndex();
	virtual ~ReadableIndex();
	bool isOrdered() const { return m_isOrdered; }
	bool isUnique() const { return m_isUnique; }

	///@{ ordered and unordered index
	virtual llong indexStorageSize() const = 0;

	///@returns same as recIdvec->size()
	virtual size_t searchExact(fstring key, valvec<llong>* recIdvec, DbContext*) const = 0;
	///@}

	///@{ ordered index only
	virtual void encodeIndexKey(const Schema&, valvec<byte>& key) const;
	virtual void decodeIndexKey(const Schema&, valvec<byte>& key) const;

	virtual void encodeIndexKey(const Schema&, byte* key, size_t keyLen) const;
	virtual void decodeIndexKey(const Schema&, byte* key, size_t keyLen) const;

	virtual IndexIterator* createIndexIterForward(DbContext*) const = 0;
	virtual IndexIterator* createIndexIterBackward(DbContext*) const = 0;
	///@}

	/// ReadableIndex can be a ReadableStore
	virtual class ReadableStore* getReadableStore();

	/// ReadableIndex can be a WritableIndex
	virtual class WritableIndex* getWritableIndex();
};
typedef boost::intrusive_ptr<ReadableIndex> ReadableIndexPtr;

/// both ordered and unordered index can be writable
class NARK_DB_DLL WritableIndex {
public:
	virtual bool remove(fstring key, llong id, DbContext*) = 0;
	virtual bool insert(fstring key, llong id, DbContext*) = 0;
	virtual bool replace(fstring key, llong id, llong newId, DbContext*) = 0;
	virtual void clear() = 0;
};

} } // namespace terark::db

#endif // __nark_db_db_index_hpp__
