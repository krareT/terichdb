#pragma once

#include <nark/db/data_index.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/int_vector.hpp>
#include <nark/rank_select.hpp>
#include <nark/fsa/nest_trie_dawg.hpp>

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

	void build(SortableStrVec& strVec);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	std::unique_ptr<NestLoudsTrieDataStore_SE_512> m_store;
};

}}} // namespace nark::db::dfadb
