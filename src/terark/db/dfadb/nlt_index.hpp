#pragma once

#include <terark/db/db_index.hpp>
#include <terark/int_vector.hpp>
#include <terark/rank_select.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>

namespace terark {
//	class Nest
} // namespace terark

namespace terark { namespace db { namespace dfadb {

class TERARK_DB_DLL NestLoudsTrieIndex : public ReadableIndex, public ReadableStore {
public:
	explicit NestLoudsTrieIndex(const Schema& schema);
	~NestLoudsTrieIndex();

	///@{ ordered and unordered index
	llong indexStorageSize() const override;

	void searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const override;
	///@}

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;

	ReadableIndex* getReadableIndex() override;
	ReadableStore* getReadableStore() override;

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(const Schema& schema, SortableStrVec& strVec);
	void load(PathRef path) override;
	void save(PathRef path) const override;

	bool matchRegexAppend(BaseDFA* regexDFA, valvec<llong>* recIdvec, DbContext*) const;

protected:
	struct FileHeader;
	std::unique_ptr<NestLoudsTrieDAWG_SE_512> m_dfa;
	FileHeader* m_idmapBase;
	size_t      m_idmapSize;
	size_t      m_dataInflateSize;
	UintVecMin0 m_keyToId;
	UintVecMin0 m_idToKey;
	rank_select_se_512 m_recBits; // only for dupable index

	class UniqueIndexIterForward;   friend class UniqueIndexIterForward;
	class UniqueIndexIterBackward;	friend class UniqueIndexIterBackward;

	class DupableIndexIterForward;  friend class DupableIndexIterForward;
	class DupableIndexIterBackward;	friend class DupableIndexIterBackward;
};

}}} // namespace terark::db::dfadb
