#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <nark/hash_strmap.hpp>
#include <nark/gold_hash_map.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <set>

namespace nark {

class MockReadonlyStore : public ReadableStore {
public:
	fstrvec m_rows;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;
};

class MockReadonlyIndex : public ReadableStoreIndex {
	friend class MockReadonlyIndexIterator;
	friend class MockCompositeIndex;
	SortableStrVec m_keyVec;
	valvec<uint32_t> m_idToKey;
public:
	MockReadonlyIndex();
	~MockReadonlyIndex();

	StoreIteratorPtr createStoreIter() const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* key, BaseContextPtr&) const override;

	IndexIteratorPtr createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
};

class MockWritableStore : public ReadableStore, public WritableStore {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	llong append(fstring row, BaseContextPtr&) override;
	void  replace(llong id, fstring row, BaseContextPtr&) override;
	void  remove(llong id, BaseContextPtr&) override;
};

class MockWritableIndex : public WritableIndex {
	typedef std::pair<std::string, llong> kv_t;
	std::set<kv_t> m_kv;
public:
	IndexIteratorPtr createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	size_t remove(fstring key, BaseContextPtr&) override;
	size_t remove(fstring key, llong id, BaseContextPtr&) override;
	size_t insert(fstring key, llong id, BaseContextPtr&) override;
	size_t replace(fstring key, llong oldId, llong newId, BaseContextPtr&) override;
	void flush() override;
};

class MockCompositeTable : public CompositeTable {
public:
	ReadonlySegmentPtr createReadonlySegment() const override;
	WritableSegmentPtr createWritableSegment(fstring dirBaseName) const override;
	void saveReadonlySegment(ReadonlySegmentPtr, fstring dirBaseName) const override;

	ReadonlySegmentPtr openReadonlySegment(fstring dirBaseName) const override;
	WritableSegmentPtr openWritableSegment(fstring dirBaseName) const override;
};

} // namespace nark

#endif // __nark_db_mock_data_index_hpp__
