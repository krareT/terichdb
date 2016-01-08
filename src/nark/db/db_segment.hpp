#ifndef __nark_db_segment_hpp__
#define __nark_db_segment_hpp__

#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/bitmap.hpp>
//#include <boost/filesystem.hpp>

namespace nark {
	class SortableStrVec;
}

namespace nark { namespace db {

class NARK_DB_DLL SegmentSchema : public RefCounter {
public:
	struct Colproject {
		uint32_t colgroupId;
		uint32_t subColumnId;
	};
	SchemaPtr     m_rowSchema;
	SchemaSetPtr  m_indexSchemaSet;
	SchemaSetPtr  m_colgroupSchemaSet;
	valvec<Colproject> m_colproject; // parallel with m_rowSchema
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;

	SegmentSchema();
	~SegmentSchema();

	const Schema& getIndexSchema(size_t indexId) const {
		assert(indexId < getIndexNum());
		return *m_indexSchemaSet->m_nested.elem_at(indexId);
	}
	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
	size_t getIndexId(fstring indexColumnNames) const {
		return m_indexSchemaSet->m_nested.find_i(indexColumnNames);
	}

	const Schema& getColgroupSchema(size_t colgroupId) const {
		assert(colgroupId < getColgroupNum());
		return *m_colgroupSchemaSet->m_nested.elem_at(colgroupId);
	}
	const SchemaSet& getColgroupSchemaSet() const { return *m_colgroupSchemaSet; }
	size_t getColgroupNum() const { return m_colgroupSchemaSet->m_nested.end_i(); }
	size_t getColgroupId(fstring colgroupColumnNames) const {
		return m_colgroupSchemaSet->m_nested.find_i(colgroupColumnNames);
	}

	const Schema& getTableSchema() const { return *m_rowSchema; }
	size_t columnNum() const { return m_rowSchema->columnNum(); }

	void loadJsonString(fstring jstr);
	void loadJsonFile(fstring fname);
	void saveJsonFile(fstring fname) const;

	void loadMetaDFA(fstring fname);
	void saveMetaDFA(fstring fname) const;

protected:
	void compileSchema();
};
typedef boost::intrusive_ptr<SegmentSchema> SegmentSchemaPtr;

class NARK_DB_DLL MultiPartStore : public ReadableStore {
	class MyStoreIterForward;	friend class MyStoreIterForward;
	class MyStoreIterBackward;	friend class MyStoreIterBackward;

public:
	explicit MultiPartStore(valvec<ReadableStorePtr>& m_parts);
	~MultiPartStore();

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void load(PathRef segDir) override;
	void save(PathRef segDir) const override;

private:
	void syncRowNumVec();

//	SchemaPtr     m_schema;
	valvec<llong> m_rowNumVec;  // parallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
};

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class NARK_DB_DLL ReadableSegment : public ReadableStore {
public:
	ReadableSegment();
	~ReadableSegment();
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* openIndex(const Schema&, PathRef path) const = 0;

	///@ if segDir==m_segDir, it is a flush
	virtual void loadRecordStore(PathRef segDir) = 0;
	virtual void saveRecordStore(PathRef segDir) const = 0;

	virtual void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext*) const = 0;
	virtual void selectOneColumn(llong recId, size_t columnId,
								 valvec<byte>* colsData, DbContext*) const = 0;

	void openIndices(PathRef dir);
	void saveIndices(PathRef dir) const;
	llong totalIndexSize() const;

	void saveIsDel(PathRef segDir) const;
	void loadIsDel(PathRef segDir);
	byte*loadIsDel_aux(PathRef segDir, febitvec& isDel) const;
	void unmapIsDel();

	void deleteSegment();

	void load(PathRef segDir) override;
	void save(PathRef segDir) const override;

	SegmentSchemaPtr         m_schema;
	valvec<ReadableIndexPtr> m_indices; // parallel with m_indexSchemaSet
	size_t      m_delcnt;
	febitvec    m_isDel;
	byte*       m_isDelMmap = nullptr;
	boost::filesystem::path m_segDir;
	bool        m_tobeDel;
	bool        m_isDirty;
	mutable bool m_isBusyForRemove;
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

	void getValueImpl(size_t id, valvec<byte>* val, DbContext*) const;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

protected:
	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* openIndex(const Schema&, PathRef path) const = 0;

	virtual ReadableIndex*
			buildIndex(const Schema&, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStore*
			buildStore(const Schema&, SortableStrVec& storeData)
			const = 0;

	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;

protected:
	class MyStoreIterForward;  friend class MyStoreIterForward;
	class MyStoreIterBackward; friend class MyStoreIterBackward;
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
	valvec<ReadableStorePtr> m_colgroups; // indices + pure_colgroups
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
	virtual ReadableIndex* createIndex(const Schema&, PathRef path) const = 0;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

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

	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	class MyStoreIterForward;  friend class MyStoreIterForward;
	class MyStoreIterBackward; friend class MyStoreIterBackward;
};
typedef boost::intrusive_ptr<SmartWritableSegment> SmartWritableSegmentPtr;

} } // namespace nark::db

#endif // __nark_db_segment_hpp__
