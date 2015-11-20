#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <set>

namespace nark {

class NARK_DB_DLL MockReadonlyStore : public ReadableStore {
public:
	fstrvec m_rows;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;
};
typedef boost::intrusive_ptr<MockReadonlyStore> MockReadonlyStorePtr;

class NARK_DB_DLL MockReadonlyIndex : public ReadableIndexStore {
	friend class MockReadonlyIndexIterator;
	fstrvec          m_keys; // keys[recId] is the key
	valvec<uint32_t> m_ids;  // keys[ids[i]] <= keys[ids[i+1]]
	size_t m_fixedLen;
	SchemaPtr m_schema;
public:
	MockReadonlyIndex(SchemaPtr schema);
	~MockReadonlyIndex();

	void build(SortableStrVec& indexData);

	void save(fstring) const override;
	void load(fstring) override;

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createIndexContext() const override;
	BaseContextPtr createStoreContext() const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* key, BaseContextPtr&) const override;

	IndexIteratorPtr createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
};
typedef boost::intrusive_ptr<MockReadonlyIndex> MockReadonlyIndexPtr;

class NARK_DB_DLL MockWritableStore : public ReadableStore, public WritableStore {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	llong append(fstring row, BaseContextPtr&) override;
	void  replace(llong id, fstring row, BaseContextPtr&) override;
	void  remove(llong id, BaseContextPtr&) override;
};
typedef boost::intrusive_ptr<MockWritableStore> MockWritableStorePtr;

template<class Key>
class NARK_DB_DLL MockWritableIndex : public WritableIndex {
	class MyIndexIter; friend class MyIndexIter;
	typedef std::pair<Key, llong> kv_t;
	std::set<kv_t> m_kv;
	size_t m_keysLen = 0;
	ullong m_removeVer = 0;
public:
	void save(fstring) const override;
	void load(fstring) override;

	IndexIteratorPtr createIndexIter() const override;
	BaseContextPtr createIndexContext() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	size_t remove(fstring key, llong id, BaseContextPtr&) override;
	size_t insert(fstring key, llong id, BaseContextPtr&) override;
	size_t replace(fstring key, llong oldId, llong newId, BaseContextPtr&) override;
	void flush() override;
};

class NARK_DB_DLL MockReadonlySegment : public ReadonlySegment {
public:
	MockReadonlySegment();
	~MockReadonlySegment();
protected:
	ReadableStorePtr openPart(fstring path) const override;
	ReadableIndexStorePtr openIndex(fstring path, SchemaPtr) const override;

	ReadableIndexStorePtr buildIndex(SchemaPtr indexSchema,
									 SortableStrVec& indexData) const override;
	ReadableStorePtr buildStore(SortableStrVec& storeData) const override;
};

class NARK_DB_DLL MockWritableSegment : public PlainWritableSegment {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	MockWritableSegment();
	~MockWritableSegment();

	WritableIndexPtr createWritableIndex(SchemaPtr) const;

protected:
	void save(fstring) const override;
	void load(fstring) override;
	WritableIndexPtr openIndex(fstring, SchemaPtr) const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;
	llong totalStorageSize() const override;
	llong append(fstring row, BaseContextPtr&) override;
	void replace(llong id, fstring row, BaseContextPtr&) override;
	void remove(llong id, BaseContextPtr&) override;
	void flush() override;
};

class NARK_DB_DLL MockCompositeTable : public CompositeTable {
public:
	ReadonlySegmentPtr createReadonlySegment() const override;
	WritableSegmentPtr createWritableSegment(fstring dirBaseName) const override;
	WritableSegmentPtr openWritableSegment(fstring dirBaseName) const override;
};

} // namespace nark

#endif // __nark_db_mock_data_index_hpp__
