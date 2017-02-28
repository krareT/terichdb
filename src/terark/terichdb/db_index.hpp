#ifndef __terark_db_db_index_hpp__
#define __terark_db_db_index_hpp__

#include "db_store.hpp"

namespace terark { namespace terichdb {

typedef boost::intrusive_ptr<class ReadableIndex> ReadableIndexPtr;

class TERICHDB_DLL IndexIterator : public RefCounter {
protected:
	bool m_isUniqueInSchema;
public:
	IndexIterator();
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
	virtual int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey);

	///@returns the matching prefix length, the returned {id, retKey} which
	///         retKey is the min key which has the longest matching prefix
	///         with the input key
	///         after calling this function, the caller can use increment to
	///         retrieve more data
	///         if the return value is 0, it has the same effect as reset
	virtual size_t seekMaxPrefix(fstring key, llong* id, valvec<byte>* retKey);

	inline bool isUniqueInSchema() const { return m_isUniqueInSchema; }
};
typedef boost::intrusive_ptr<IndexIterator> IndexIteratorPtr;

class TERICHDB_DLL RegexForIndex : public RefCounter {
	TERICHDB_NON_COPYABLE_CLASS(RegexForIndex);
public:
	typedef RegexForIndex* (*Factory)(fstring regex, fstring opt);
	struct TERICHDB_DLL RegisterFactory {
		RegisterFactory(std::initializer_list<fstring> names, Factory);
	};
#define REGISTER_RegexForIndex(Clazz, ...) \
	static RegexForIndex::RegisterFactory \
    s_reg##Clazz({#Clazz,##__VA_ARGS__}, \
		[](fstring regex, fstring opt) -> RegexForIndex* { \
			return new Clazz(regex, opt); \
		})

	RegexForIndex();
	virtual ~RegexForIndex();
	virtual bool matchText(fstring text) = 0;
	static
	RegexForIndex* create(fstring clazz, fstring regex, fstring opt);
};
typedef boost::intrusive_ptr<RegexForIndex> RegexForIndexPtr;

class TERICHDB_DLL ReadableIndex : virtual public Permanentable {
	TERICHDB_NON_COPYABLE_CLASS(ReadableIndex);
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
	void searchExact(fstring key, valvec<llong>* recIdvec, DbContext* ctx) const {
		recIdvec->erase_all();
		searchExactAppend(key, recIdvec, ctx);
	}
	virtual void searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const = 0;
	///@}

	virtual bool matchRegexAppend(RegexForIndex* regex, valvec<llong>* recIdvec, DbContext*) const;

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
class TERICHDB_DLL WritableIndex {
public:
	virtual ~WritableIndex();
	virtual bool remove(fstring key, llong id, DbContext*) = 0;
	virtual bool insert(fstring key, llong id, DbContext*) = 0;
	virtual bool replace(fstring key, llong id, llong newId, DbContext*) = 0;
	virtual void clear() = 0;
};

class TERICHDB_DLL EmptyIndexStore : public ReadableIndex, public ReadableStore {
public:
	EmptyIndexStore();
	EmptyIndexStore(const Schema&);
	~EmptyIndexStore();

	llong indexStorageSize() const override;

	void searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const override;

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;
	class ReadableStore* getReadableStore() override;

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	void deleteFiles() override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;
	ReadableIndex* getReadableIndex() override;

	void load(PathRef) override;
	void save(PathRef) const override;
};

} } // namespace terark::terichdb

#endif // __terark_db_db_index_hpp__
