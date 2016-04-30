#pragma once

#include <terark/db/db_index.hpp>
#include <terark/int_vector.hpp>
#include <terark/rank_select.hpp>
#include <terark/util/sortable_strvec.hpp>

namespace terark { namespace db {

class TERARK_DB_DLL FixedLenKeyIndex : public ReadableIndex, public ReadableStore {
public:
	FixedLenKeyIndex();
	~FixedLenKeyIndex();

	///@{ ordered and unordered index
	llong indexStorageSize() const override;

	void searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const override;
	///@}

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;

	ReadableStore* getReadableStore() override;
	ReadableIndex* getReadableIndex() override;

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
	valvec<byte> m_keys;   // key   = m_keys[recId]
	UintVecMin0  m_index;  // recId = m_index.lower_bound(key)
	byte_t*      m_mmapBase;
	size_t       m_mmapSize;
	size_t       m_fixedLen;
	size_t       m_uniqKeys;

	size_t searchLowerBound(fstring binkey) const;
	size_t searchUpperBound(fstring binkey) const;

	class MyIndexIterForward;  friend class MyIndexIterForward;
	class MyIndexIterBackward; friend class MyIndexIterBackward;
};

}} // namespace terark::db
