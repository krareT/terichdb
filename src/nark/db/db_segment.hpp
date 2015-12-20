#ifndef __nark_db_segment_hpp__
#define __nark_db_segment_hpp__

#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/bitmap.hpp>

namespace nark {
	class SortableStrVec;
}

namespace nark { namespace db {

class NARK_DB_DLL SegmentSchema {
public:
	SchemaPtr     m_rowSchema;
	SchemaPtr     m_nonIndexRowSchema; // full-row schema except columns in indices
	SchemaSetPtr  m_indexSchemaSet;
	SchemaSetPtr  m_colgroupSchemaSet;

	SegmentSchema();
	~SegmentSchema();

	const Schema& getIndexSchema(size_t indexId) const {
		assert(indexId < getIndexNum());
		return *m_indexSchemaSet->m_nested.elem_at(indexId);
	}
	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
	size_t columnNum() const { return m_rowSchema->columnNum(); }

	size_t getIndexId(fstring indexColumnNames) const {
		return m_indexSchemaSet->m_nested.find_i(indexColumnNames);
	}

	void copySchema(const SegmentSchema& y);
	const SegmentSchema& segSchema() const { return *this; }

protected:
	void compileSchema();
};

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class NARK_DB_DLL ReadableSegment : public ReadableStore, public SegmentSchema {
public:
	ReadableSegment();
	~ReadableSegment();
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* openIndex(const Schema&, fstring path) const = 0;

	///@ if segDir==m_segDir, it is a flush
	virtual void loadRecordStore(fstring segDir) = 0;
	virtual void saveRecordStore(fstring segDir) const = 0;

	void openIndices(fstring dir);
	void saveIndices(fstring dir) const;
	llong totalIndexSize() const;

	void saveIsDel(fstring segDir) const;
	void loadIsDel(fstring segDir);
	void unmapIsDel();

	void deleteSegment();

	void load(fstring segDir) override;
	void save(fstring segDir) const override;

	valvec<ReadableIndexPtr> m_indices; // parallel with m_indexSchemaSet
	valvec<ReadableStorePtr> m_colgroups;
	size_t      m_delcnt;
	febitvec    m_isDel;
	byte*       m_isDelMmap = nullptr;
	std::string m_segDir;
	bool        m_tobeDel;
	bool        m_isDirty;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

// Every index is a ReadableIndexStore
//
// The <<store>> is multi-part, because the <<store>> may be
// very large, and the compressible database algo need to fit
// all uncompressed data into memory during compression, split
// the whole data into multi-part reduce the memory usage during
// compression, because just a few part of <<store>> data need
// to fit into memory at a time for compression.
//
// The <<index>> is single-part, because index is much smaller
// than the whole <<store>> data.
//
class NARK_DB_DLL ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input, DbContext* ctx);
	void convFrom(const ReadableSegment& input, DbContext* ctx);

	void getValueImpl(size_t partIdx, size_t id,
					  valvec<byte>* val, DbContext*) const;

protected:
	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* openIndex(const Schema&, fstring path) const = 0;

	// Store can use different implementation for different data
	// According to data content features
	// store could be a column store
	virtual ReadableStore* openStore(const Schema&, fstring path) const = 0;

	virtual ReadableIndex*
			buildIndex(const Schema&, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStore*
			buildStore(const Schema&, SortableStrVec& storeData)
			const = 0;

	void loadRecordStore(fstring segDir) override;
	void saveRecordStore(fstring segDir) const override;

protected:
	class MyStoreIterForward;  friend class MyStoreIterForward;
	class MyStoreIterBackward; friend class MyStoreIterBackward;
	valvec<llong> m_rowNumVec;  // parallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

// Concrete WritableSegment should not implement this class,
// should implement PlainWritableSegment or SmartWritableSegment
class NARK_DB_DLL WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableSegment();
	~WritableSegment();

	WritableStore* getWritableStore() override;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* createIndex(const Schema&, fstring path) const = 0;

	void flushSegment();

	valvec<uint32_t>  m_deletedWrIdSet;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

class NARK_DB_DLL PlainWritableSegment : public WritableSegment {
public:
protected:
};
typedef boost::intrusive_ptr<PlainWritableSegment> PlainWritableSegmentPtr;

// Every index is a WritableIndexStore
// But the <<store>> is not multi-part(such as ReadonlySegment)
class NARK_DB_DLL SmartWritableSegment : public WritableSegment {
public:
protected:
	~SmartWritableSegment();

	void getValueAppend(llong id, valvec<byte>* val, DbContext*)
	const override final;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void loadRecordStore(fstring segDir) override;
	void saveRecordStore(fstring segDir) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	ReadableStorePtr m_nonIndexStore;
	class MyStoreIterForward;  friend class MyStoreIterForward;
	class MyStoreIterBackward; friend class MyStoreIterBackward;
};
typedef boost::intrusive_ptr<SmartWritableSegment> SmartWritableSegmentPtr;

} } // namespace nark::db

#endif // __nark_db_segment_hpp__
