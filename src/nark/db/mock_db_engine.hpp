#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <nark/hash_strmap.hpp>
#include <nark/gold_hash_map.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <set>

namespace nark {

class NARK_DB_DLL_EXPORT MockReadonlyStore : public ReadableStore {
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

class NARK_DB_DLL_EXPORT MockReadonlyIndex : public ReadableStoreIndex {
	friend class MockReadonlyIndexIterator;
	SortableStrVec   m_keys; // keys[recId] is the key
	valvec<uint32_t> m_fix;  // keys[fix[i]] <= keys[fix[i+1]]
	size_t m_fixedLen;

public:
	MockReadonlyIndex();
	~MockReadonlyIndex();

	void build(const Schema& indexSchema, SortableStrVec& indexData);

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

class NARK_DB_DLL_EXPORT MockWritableStore : public ReadableStore, public WritableStore {
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

class NARK_DB_DLL_EXPORT MockWritableIndex : public WritableIndex {
	typedef std::pair<std::string, llong> kv_t;
	std::set<kv_t> m_kv;
public:
	void save(fstring) const override;
	void load(fstring) override;

	IndexIteratorPtr createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	size_t remove(fstring key, BaseContextPtr&) override;
	size_t remove(fstring key, llong id, BaseContextPtr&) override;
	size_t insert(fstring key, llong id, BaseContextPtr&) override;
	size_t replace(fstring key, llong oldId, llong newId, BaseContextPtr&) override;
	void flush() override;
};

class NARK_DB_DLL_EXPORT MockReadonlySegment : public ReadonlySegment {
public:
	MockReadonlySegment();
	~MockReadonlySegment();
protected:
	ReadableStorePtr openPart(fstring path) const override;
	ReadableStoreIndexPtr openIndex(fstring path, const Schema&) const override;

	ReadableStoreIndexPtr buildIndex(SchemaPtr indexSchema,
									 SortableStrVec& indexData) const override;
	ReadableStorePtr buildStore(SortableStrVec& storeData) const override;
};

class NARK_DB_DLL_EXPORT MockWritableSegment : public WritableSegment {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	MockWritableSegment();
	~MockWritableSegment();

protected:
	void save(fstring) const override;
	void load(fstring) override;
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

class NARK_DB_DLL_EXPORT MockCompositeTable : public CompositeTable {
public:
	ReadonlySegmentPtr createReadonlySegment() const override;
	WritableSegmentPtr createWritableSegment(fstring dirBaseName) const override;
	WritableSegmentPtr openWritableSegment(fstring dirBaseName) const override;
};

} // namespace nark

#endif // __nark_db_mock_data_index_hpp__
