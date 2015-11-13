#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include <nark/util/refcount.hpp>
#include <nark/fstring.hpp>
#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include <boost/intrusive_ptr.hpp>
#include <mutex>
#include "db_conf.hpp"
#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/util/sortable_strvec.hpp>

namespace nark {

class ReadableStoreIndex : public ReadableStore, public ReadableIndex {
public:
};
typedef boost::intrusive_ptr<ReadableStoreIndex> ReadableStoreIndexPtr;

class BitVecRangeView {
	const bm_uint_t* m_bitsPtr;
	size_t m_baseIdx;
	size_t m_bitsNum;
public:
	BitVecRangeView(const febitvec& bv, size_t baseIdx, size_t bitsNum) {
		m_bitsPtr = bv.bldata();
		m_baseIdx = baseIdx;
		m_bitsNum = bitsNum;
	}
	bool operator[](size_t i) const {
		assert(i < m_bitsNum);
		return nark_bit_test(m_bitsPtr, i);
	}
	size_t size() const { return m_bitsNum; }
};

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class ReadableSegment : public ReadableStore {
public:
	~ReadableSegment();
	virtual const ReadableIndex* getReadableIndex(size_t indexId) const = 0;
	virtual llong totalStorageSize() const = 0;

	SchemaPtr     m_rowSchema;
	SchemaSetPtr  m_indexSchemaSet;
	febitvec  m_isDel;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

class ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	const ReadableIndex* getReadableIndex(size_t nth) const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* val) const override;
	StoreIterator* makeStoreIter() const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input);
	void convFrom(const ReadableSegment& input, const Schema& schema);

protected:
	virtual ReadableStoreIndexPtr
			buildIndex(SchemaPtr indexSchema, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStorePtr buildStore(SortableStrVec& storeData) const = 0;

protected:
	class MyStoreIterator;
	SchemaPtr     m_dataSchema; // full-row schema except columns in indices
	valvec<llong> m_rowNumVec;  // prallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	valvec<ReadableStoreIndexPtr> m_indices; // parallel with m_indexSchemaSet
	llong  m_dataMemSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

class WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableStore* getWritableStore() override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	valvec<WritableIndexPtr> m_indices;
protected:
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

struct ProjectWorkBuf {
	valvec<byte> buf;
	valvec<ColumnData> columns;
};

// is not a WritableStore
class CompositeTable : public ReadableStore {
public:
	CompositeTable(SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }
	void setTableDirName(fstring dir, fstring name);

	void loadTable(fstring dir, fstring name);

	virtual ReadonlySegmentPtr createReadonlySegment(fstring dirBaseName) const = 0;
	virtual WritableSegmentPtr createWritableSegment(fstring dirBaseName) const = 0;

	llong totalStorageSize() const;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* val) const override;

	llong insertRow(fstring row, bool syncIndex);
	llong replaceRow(llong id, fstring row, bool syncIndex);
	void  removeRow(llong id, bool syncIndex);

	void indexInsert(size_t indexId, fstring indexKey, llong id);
	void indexRemove(size_t indexId, fstring indexKey, llong id);
	void indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId);

	size_t columnNum() const;

	void getIndexKey(size_t indexId, const valvec<ColumnData>& columns, valvec<byte>* key) const;

	void compact();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }

protected:
	void maybeCreateNewSegment();

protected:
	std::mutex m_mutex;
	std::string m_name;
	std::string m_dir;
	valvec<llong>  m_rowNumVec;
	valvec<uint32_t>  m_deletedWrIdSet;

	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;
	SchemaPtr m_rowSchema; // full-row schema
	SchemaPtr m_nonIndexRowSchema;
	SchemaSetPtr m_indexSchemaSet;
	basic_fstrvec<unsigned> m_indexProjects;
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;
};

} // namespace nark

#endif // __nark_db_table_store_hpp__
