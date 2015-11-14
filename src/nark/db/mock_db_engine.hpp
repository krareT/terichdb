#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <nark/hash_strmap.hpp>
#include <nark/gold_hash_map.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <set>

namespace nark {

class MockReadableIndex : public ReadableStoreIndex {
	friend class MockReadableIndexIterator;
	friend class MockCompositeIndex;
	SortableStrVec m_keyVec;
	valvec<uint32_t> m_idToKey;
public:
	MockReadableIndex();
	~MockReadableIndex();

	StoreIterator* createStoreIter() const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* key, BaseContextPtr&) const override;

	IndexIterator* createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
};

class MockWritableIndex : public WritableIndex {
	typedef std::pair<std::string, llong> kv_t;
	std::set<kv_t> m_kv;
public:
	IndexIterator* createIndexIter() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	size_t remove(fstring key, BaseContextPtr&) override;
	size_t remove(fstring key, llong id, BaseContextPtr&) override;
	size_t insert(fstring key, llong id, BaseContextPtr&) override;
	size_t replace(fstring key, llong oldId, llong newId, BaseContextPtr&) override;
	void flush() override;
};

class MockCompositeIndex : public CompositeTable {
public:
};

} // namespace nark

#endif // __nark_db_mock_data_index_hpp__
