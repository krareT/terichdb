#ifndef __terark_db_segment_hpp__
#define __terark_db_segment_hpp__

#include "db_index.hpp"
#include "db_store.hpp"
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>

namespace terark {
	class SortableStrVec;
}

namespace terark { namespace db {

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class TERARK_DB_DLL ReadableSegment : public ReadableStore {
public:
	ReadableSegment();
	~ReadableSegment();
	virtual class ReadonlySegment* getReadonlySegment() const;
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

	void deleteSegment();

	void load(PathRef segDir) override;
	void save(PathRef segDir) const override;

	size_t getPhysicId(size_t logicId) const;
	size_t getLogicId(size_t physicId) const;

	SchemaConfigPtr         m_schema;
	valvec<ReadableIndexPtr> m_indices; // parallel with m_indexSchemaSet
	valvec<ReadableStorePtr> m_colgroups; // indices + pure_colgroups
	size_t      m_delcnt;
	febitvec    m_isDel;
	byte*       m_isDelMmap = nullptr;
	rank_select_se m_isPurged; // just for ReadonlySegment
	byte*          m_isPurgedMmap;
	boost::filesystem::path m_segDir;
	valvec<uint32_t> m_deletionList;
	bool        m_tobeDel;
	bool        m_isDirty;
	bool        m_bookDeletion;
	bool        m_withPurgeBits;  // just for ReadonlySegment
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
class TERARK_DB_DLL ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	ReadonlySegment* getReadonlySegment() const override;

	llong dataInflateSize() const override;
	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void convFrom(class CompositeTable*, size_t segIdx);
	void purgeDeletedRecords(class CompositeTable*, size_t segIdx);

	void getValueByLogicId(size_t id, valvec<byte>* val, DbContext*) const;
	void getValueByPhysicId(size_t id, valvec<byte>* val, DbContext*) const;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

	void load(PathRef segDir) override;
	void save(PathRef segDir) const override;

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

	virtual ReadableStore*
			buildDictZipStore(const Schema&, PathRef dir, StoreIterator& inputIter,
							  const bm_uint_t* isDel, const febitvec* isPurged)
			const;

	void completeAndReload(class CompositeTable*, size_t segIdx,
						   class ReadableSegment* input);

	ReadableIndexPtr purgeIndex(size_t indexId, ReadonlySegment* input, DbContext* ctx);
	ReadableStorePtr purgeColgroup(size_t colgroupId, ReadonlySegment* input, DbContext* ctx, PathRef tmpSegDir);

	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;

	void closeFiles();
	void removePurgeBitsForCompactIdspace(PathRef segDir);
	void savePurgeBits(PathRef segDir) const;

protected:
	friend class CompositeTable;
	friend class TableIndexIter;
	class MyStoreIterForward;  friend class MyStoreIterForward;
	class MyStoreIterBackward; friend class MyStoreIterBackward;
	llong  m_dataInflateSize;
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

// Concrete WritableSegment should not implement this class,
// should implement PlainWritableSegment or SmartWritableSegment
class TERARK_DB_DLL WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableSegment();
	~WritableSegment();

	void pushIsDel(bool val);

	AppendableStore* getAppendableStore() override;
	UpdatableStore* getUpdatableStore() override;
	WritableStore* getWritableStore() override;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* createIndex(const Schema&, PathRef path) const = 0;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

	void flushSegment();

	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;

	valvec<uint32_t>  m_deletedWrIdSet;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

class TERARK_DB_DLL PlainWritableSegment : public WritableSegment {
public:
protected:
};
typedef boost::intrusive_ptr<PlainWritableSegment> PlainWritableSegmentPtr;

// Every index is a WritableIndexStore
// But the <<store>> is not multi-part(such as ReadonlySegment)
class TERARK_DB_DLL SmartWritableSegment : public WritableSegment {
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

} } // namespace terark::db

#endif // __terark_db_segment_hpp__
