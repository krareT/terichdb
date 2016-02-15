#pragma once

#include <nark/db/data_index.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/int_vector.hpp>
#include <nark/rank_select.hpp>
#include <nark/fsa/nest_trie_dawg.hpp>

namespace nark { namespace db {

class NARK_DB_DLL FixedLenStore : public ReadableStore {
public:
	FixedLenStore();
	~FixedLenStore();

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(const Schema& schema, SortableStrVec& strVec);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	valvec<byte> m_keys;
	byte_t*      m_mmapBase;
	size_t       m_mmapSize;
	size_t       m_fixedLen;
	size_t       m_rows;

	std::pair<size_t, bool> searchLowerBound(fstring binkey) const;
};

}} // namespace nark::db
