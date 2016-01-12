#pragma once

#include <nark/db/data_index.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/int_vector.hpp>
#include <nark/rank_select.hpp>
#include <nark/fsa/nest_trie_dawg.hpp>

namespace nark { namespace db {

class NARK_DB_DLL ZipIntKeyIndex : public ReadableIndex, public ReadableStore {
public:
	ZipIntKeyIndex();
	~ZipIntKeyIndex();

	///@{ ordered and unordered index
	llong indexStorageSize() const override;

	llong searchExact(fstring key, DbContext*) const override;
	///@}

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;

	ReadableIndex* getReadableIndex() override;
	ReadableStore* getReadableStore() override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(ColumnType keyType, SortableStrVec& strVec);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	UintVecMin0 m_keys;   // key   = m_keys[recId]
	UintVecMin0 m_index;  // recId = m_index.lower_bound(key)
	byte_t*     m_mmapBase;
	size_t      m_mmapSize;
	llong       m_minKey; // may be unsigned
	ColumnType  m_keyType;

	template<class Int>
	std::pair<size_t, bool> IntVecLowerBound(fstring binkey) const;
	std::pair<size_t, bool> searchLowerBound(fstring binkey) const;

	template<class Int>
	void keyAppend(size_t recIdx, valvec<byte>* res) const;

	template<class Int>
	void zipKeys(const void* data, size_t size);

	class MyIndexIterForward;  friend class MyIndexIterForward;
	class MyIndexIterBackward; friend class MyIndexIterBackward;
};

}} // namespace nark::db
