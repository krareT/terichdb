#pragma once

#include <terark/db/db_index.hpp>
#include <terark/int_vector.hpp>
#include <terark/rank_select.hpp>
#include <terark/util/sortable_strvec.hpp>

namespace terark { namespace db {

class TERARK_DB_DLL ZipIntStore : public ReadableStore {
public:
	explicit ZipIntStore(const Schema& schema);
	~ZipIntStore();

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(ColumnType intType, SortableStrVec& strVec);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	UintVecMin0 m_dedup;
	UintVecMin0 m_index;
	byte_t*     m_mmapBase;
	size_t      m_mmapSize;
	llong       m_minValue; // may be unsigned
	ColumnType  m_intType;
	const Schema& m_schema;

	template<class Int>
	void valueAppend(size_t recIdx, valvec<byte>* res) const;

	template<class Int>
	void zipValues(const void* data, size_t size);
};

}} // namespace terark::db
