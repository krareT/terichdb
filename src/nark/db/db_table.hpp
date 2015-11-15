#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include <nark/util/refcount.hpp>
#include <nark/fstring.hpp>
#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include <boost/intrusive_ptr.hpp>
#include <atomic>
#include "db_conf.hpp"
#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/util/sortable_strvec.hpp>
#include <tbb/queuing_rw_mutex.h>

namespace nark {

class ReadableStoreIndex : public ReadableStore, public ReadableIndex {
public:
};
typedef boost::intrusive_ptr<ReadableStoreIndex> ReadableStoreIndexPtr;

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class ReadableSegment : public ReadableStore {
public:
	~ReadableSegment();
	virtual const ReadableIndex* getReadableIndex(size_t indexId) const = 0;
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	febitvec      m_isDel;
	SchemaPtr     m_rowSchema;
	SchemaSetPtr  m_indexSchemaSet;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

class ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	struct ReadonlyStoreContext : public BaseContext {
		valvec<byte> buf;
	};

	const ReadableIndex* getReadableIndex(size_t nth) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input);
	void convFrom(const ReadableSegment& input, const Schema& schema);

	void getValueImpl(size_t partIdx, size_t id, llong subId, valvec<byte>* val, valvec<byte>* buf) const;

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
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

class WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableStore* getWritableStore() override;
	const ReadableIndex* getReadableIndex(size_t nth) const override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	valvec<WritableIndexPtr> m_indices;
protected:
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

class TableContext : public BaseContext {
public:
	TableContext();
	~TableContext();

	valvec<byte> row1;
	valvec<byte> row2;
	valvec<byte> key1;
	valvec<byte> key2;
	valvec<ColumnData> cols1;
	valvec<ColumnData> cols2;
	valvec<BaseContextPtr> wrIndexContext;
	BaseContextPtr wrStoreContext;
	BaseContextPtr readonlyContext;
};
typedef boost::intrusive_ptr<TableContext> TableContextPtr;

// is not a WritableStore
class CompositeTable : public ReadableStore {
	class MyStoreIterator;
	friend class MyStoreIterator;
public:
	CompositeTable();

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }

	void createTable(fstring dir, fstring name,
					 SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);

	void openTable(fstring dir, fstring name);

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	virtual ReadonlySegmentPtr createReadonlySegment(fstring dirBaseName) const = 0;
	virtual WritableSegmentPtr createWritableSegment(fstring dirBaseName) const = 0;

	virtual ReadonlySegmentPtr openReadonlySegment(fstring dirBaseName) const = 0;
	virtual WritableSegmentPtr openWritableSegment(fstring dirBaseName) const = 0;

	llong totalStorageSize() const;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;

	llong insertRow(fstring row, bool syncIndex, BaseContextPtr&);
	llong replaceRow(llong id, fstring row, bool syncIndex, BaseContextPtr&);
	void  removeRow(llong id, bool syncIndex, BaseContextPtr&);

	void indexInsert(size_t indexId, fstring indexKey, llong id, BaseContextPtr&);
	void indexRemove(size_t indexId, fstring indexKey, llong id, BaseContextPtr&);
	void indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId, BaseContextPtr&);

	size_t columnNum() const;

	void getIndexKey(size_t indexId, const valvec<ColumnData>& columns, valvec<byte>* key) const;

	bool compact();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	const size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }

protected:
	void maybeCreateNewSegment(tbb::queuing_rw_mutex::scoped_lock&);
	llong insertRowImpl(fstring row, bool syncIndex,
						BaseContextPtr&, tbb::queuing_rw_mutex::scoped_lock&);

protected:
	mutable tbb::queuing_rw_mutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
	valvec<llong>  m_rowNumVec;
	valvec<uint32_t>  m_deletedWrIdSet;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;

	// constant once constructed
	std::string m_name;
	std::string m_dir;
	SchemaPtr m_rowSchema; // full-row schema
	SchemaPtr m_nonIndexRowSchema;
	SchemaSetPtr m_indexSchemaSet;
	basic_fstrvec<unsigned> m_indexProjects;
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;
};

} // namespace nark

#endif // __nark_db_table_store_hpp__
