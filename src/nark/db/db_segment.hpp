#ifndef __nark_db_segment_hpp__
#define __nark_db_segment_hpp__

#include "db_conf.hpp"
#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/util/sortable_strvec.hpp>
#include <tbb/queuing_rw_mutex.h>

namespace nark {

class NARK_DB_DLL SegmentSchema {
public:
	SchemaPtr     m_rowSchema;
	SchemaPtr     m_nonIndexRowSchema; // full-row schema except columns in indices
	SchemaSetPtr  m_indexSchemaSet;

	SegmentSchema();
	~SegmentSchema();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	const size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
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
	virtual ReadableIndexPtr getReadableIndex(size_t indexId) const = 0;
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

struct ReadonlyStoreContext : public BaseContext {
	ReadonlyStoreContext();
	~ReadonlyStoreContext();
	valvec<byte> buf1;
	valvec<byte> buf2;
	valvec<byte> key1;
	valvec<byte> key2;
	valvec<size_t> offsets;
	valvec<fstring> cols1;
	valvec<fstring> cols2;
};
typedef boost::intrusive_ptr<ReadonlyStoreContext> ReadonlyStoreContextPtr;

class NARK_DB_DLL ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	void save(fstring) const override;
	void load(fstring) override;

	// Store can use different implementation for different data
	// According to data content features
	virtual ReadableStorePtr openPart(fstring path) const = 0;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndexStorePtr openIndex(fstring path, SchemaPtr) const = 0;

	ReadableIndexPtr getReadableIndex(size_t nth) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValueAppend(llong id, valvec<byte>* val, BaseContextPtr&) const override;

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input);
	void convFrom(const ReadableSegment& input, tbb::queuing_rw_mutex&);

	void getValueImpl(size_t partIdx, size_t id, llong subId,
					  valvec<byte>* val, ReadonlyStoreContext*) const;

protected:
	virtual ReadableIndexStorePtr
			buildIndex(SchemaPtr indexSchema, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStorePtr buildStore(SortableStrVec& storeData) const = 0;

protected:
	class MyStoreIterator;
	valvec<llong> m_rowNumVec;  // prallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	valvec<ReadableIndexStorePtr> m_indices; // parallel with m_indexSchemaSet
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

class NARK_DB_DLL WrSegContext : public BaseContext {
public:
	WrSegContext();
	~WrSegContext();
};

// Concrete WritableSegment should not implement this class,
// should implment PlainWritableSegment or SmartWritableSegment
class NARK_DB_DLL WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableSegment();
	~WritableSegment();

	WritableStore* getWritableStore() override;
	ReadableIndexPtr getReadableIndex(size_t nth) const override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	// Index can use different implementation for different
	// index schema and index content features
	virtual WritableIndexPtr openIndex(fstring path, SchemaPtr) const = 0;
	virtual WritableIndexPtr createIndex(fstring path, SchemaPtr) const = 0;

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

class NARK_DB_DLL SmartWrSegContext : public WrSegContext {
public:
	valvec<byte> buf;
};
// Every index is a WritableIndexStore
class NARK_DB_DLL SmartWritableSegment : public WritableSegment {
public:
protected:
	~SmartWritableSegment();

	void getValueAppend(llong id, valvec<byte>* val, BaseContextPtr&)
	const override final;

	StoreIteratorPtr createStoreIter() const override;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	ReadableStorePtr m_nonIndexStore;
	class MyStoreIterator; friend class MyStoreIterator;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

} // namespace nark

#endif // __nark_db_segment_hpp__
