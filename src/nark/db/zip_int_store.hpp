#pragma once

#include <nark/db/data_index.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/int_vector.hpp>
#include <nark/rank_select.hpp>
#include <nark/fsa/nest_trie_dawg.hpp>

namespace nark { namespace db {

class NARK_DB_DLL ZipIntStore : public ReadableStore {
public:
	ZipIntStore();
	~ZipIntStore();

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(ColumnType intType, SortableStrVec& strVec);
	void load(fstring path) override;
	void save(fstring path) const override;

protected:
	UintVecMin0 m_dedup;
	UintVecMin0 m_index;
	byte_t*     m_mmapBase;
	size_t      m_mmapSize;
	llong       m_minValue; // may be unsigned
	ColumnType  m_intType;

	template<class Int>
	void valueAppend(size_t recIdx, valvec<byte>* res) const;

	template<class Int>
	void zipValues(const void* data, size_t size);
};

}} // namespace nark::db
