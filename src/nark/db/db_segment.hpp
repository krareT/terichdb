#ifndef __nark_db_segment_hpp__
#define __nark_db_segment_hpp__

#include "db_conf.hpp"
#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/util/sortable_strvec.hpp>
#include <tbb/queuing_rw_mutex.h>

namespace nark { namespace db {

class NARK_DB_DLL SegmentSchema {
public:
	SchemaPtr     m_rowSchema;
	SchemaPtr     m_nonIndexRowSchema; // full-row schema except columns in indices
	SchemaSetPtr  m_indexSchemaSet;

	SegmentSchema();
	~SegmentSchema();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
	size_t columnNum() const { return m_rowSchema->columnNum(); }

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
	virtual ReadableIndex* getReadableIndex(size_t indexId) const = 0;
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	void save(fstring) const override;
	void load(fstring) override;

	void deleteSegment();

	size_t      m_delcnt;
	febitvec    m_isDel;
	byte*       m_isDelMmap = nullptr;
	std::string m_segDir;
	bool        m_tobeDel;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

class NARK_DB_DLL ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	void save(fstring) const override;
	void load(fstring) override;

	// Store can use different implementation for different data
	// According to data content features
	virtual ReadableStore* openPart(fstring path) const = 0;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndexStore* openIndex(fstring path, const Schema&) const = 0;

	ReadableIndex* getReadableIndex(size_t nth) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIter(DbContext*) const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input, DbContext* ctx);
	void convFrom(const ReadableSegment& input, DbContext* ctx);

	void getValueImpl(size_t partIdx, size_t id,
					  valvec<byte>* val, DbContext*) const;

protected:
	virtual ReadableIndexStore*
			buildIndex(const Schema& indexSchema, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStore* buildStore(SortableStrVec& storeData) const = 0;

protected:
	class MyStoreIterator;
	valvec<llong> m_rowNumVec;  // parallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	valvec<ReadableIndexStorePtr> m_indices; // parallel with m_indexSchemaSet
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
	ReadableIndex* getReadableIndex(size_t nth) const override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	// Index can use different implementation for different
	// index schema and index content features
	virtual WritableIndex* openIndex(fstring path, const Schema&) const = 0;
	virtual WritableIndex* createIndex(fstring path, const Schema&) const = 0;

	valvec<WritableIndexPtr> m_indices;
	valvec<uint32_t>  m_deletedWrIdSet;

protected:
	void openIndices(fstring dir);
	void saveIndices(fstring dir) const;
	llong totalIndexSize() const;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

class NARK_DB_DLL PlainWritableSegment : public WritableSegment {
public:
protected:
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

// Every index is a WritableIndexStore
class NARK_DB_DLL SmartWritableSegment : public WritableSegment {
public:
protected:
	~SmartWritableSegment();

	void getValueAppend(llong id, valvec<byte>* val, DbContext*)
	const override final;

	StoreIterator* createStoreIter(DbContext*) const override;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	ReadableStorePtr m_nonIndexStore;
	class MyStoreIterator; friend class MyStoreIterator;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

} } // namespace nark::db

#endif // __nark_db_segment_hpp__
