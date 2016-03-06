#pragma once

#include <terark/db/db_index.hpp>
#include <terark/fsa/nest_louds_trie.hpp>

namespace terark {
//	class Nest
} // namespace terark

namespace terark { namespace db { namespace dfadb {

class TERARK_DB_DLL NestLoudsTrieStore : public ReadableStore {
public:
	NestLoudsTrieStore();
	~NestLoudsTrieStore();

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(const Schema&, SortableStrVec& strVec);
	void build_by_iter(const Schema&, PathRef fpath, StoreIterator& iter,
					   const bm_uint_t* isDel, const febitvec* isPurged);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	std::unique_ptr<DataStore> m_store;
};

}}} // namespace terark::db::dfadb
