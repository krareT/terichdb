#pragma once

#include <nark/db/data_index.hpp>
#include <nark/fsa/nest_louds_trie.hpp>

namespace nark {
//	class Nest
} // namespace nark

namespace nark { namespace db { namespace dfadb {

class NARK_DB_DLL NestLoudsTrieStore : public ReadableStore {
public:
	NestLoudsTrieStore();
	~NestLoudsTrieStore();

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(const Schema&, SortableStrVec& strVec);
	void build_by_iter(const Schema&, StoreIterator& iter);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	std::unique_ptr<DataStore> m_store;
};

}}} // namespace nark::db::dfadb
