#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include "data_store.hpp"
#include "data_index.hpp"
#include <tbb/queuing_rw_mutex.h>

namespace nark { namespace db {

typedef tbb::queuing_rw_mutex              MyRwMutex;
typedef tbb::queuing_rw_mutex::scoped_lock MyRwLock;

class NARK_DB_DLL ReadableSegment;
class NARK_DB_DLL ReadonlySegment;
class NARK_DB_DLL WritableSegment;
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

// is not a WritableStore
class NARK_DB_DLL CompositeTable : public ReadableStore {
	class MyStoreIterBase;	    friend class MyStoreIterBase;
	class MyStoreIterForward;	friend class MyStoreIterForward;
	class MyStoreIterBackward;	friend class MyStoreIterBackward;
public:
	CompositeTable();
	~CompositeTable();

	struct RegisterTableClass {
		RegisterTableClass(fstring clazz, const std::function<CompositeTable*()>& f);
	};
#define NARK_DB_REGISTER_TABLE_CLASS(TableClass) \
	static CompositeTable::RegisterTableClass \
		regTable_##TableClass(#TableClass, [](){ return new TableClass(); });

	static CompositeTable* createTable(fstring tableClass);

	virtual void init(PathRef dir, SchemaConfigPtr);

	void load(PathRef dir) override;
	void save(PathRef dir) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;
	virtual DbContext* createDbContext() const = 0;

	llong totalStorageSize() const;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	llong insertRow(fstring row, DbContext*);
	llong replaceRow(llong id, fstring row, DbContext*);
	bool  removeRow(llong id, DbContext*);

	const Schema& rowSchema() const { return *m_schema->m_rowSchema; }
	const Schema& getIndexSchema(size_t indexId) const {
		assert(indexId < m_schema->getIndexNum());
		return *m_schema->m_indexSchemaSet->m_nested.elem_at(indexId);
	}
	size_t getIndexId(fstring colnames) const {
		return m_schema->m_indexSchemaSet->m_nested.find_i(colnames);
	}
	size_t getIndexNum() const { return m_schema->getIndexNum(); }

	llong indexSearchExact(size_t indexId, fstring key, DbContext*) const;
	bool indexKeyExists(size_t indexId, fstring key, DbContext*) const;

	bool indexInsert(size_t indexId, fstring indexKey, llong id, DbContext*);
	bool indexRemove(size_t indexId, fstring indexKey, llong id, DbContext*);
	bool indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId, DbContext*);

	llong indexStorageSize(size_t indexId) const;

	IndexIteratorPtr createIndexIterForward(size_t indexId) const;
	IndexIteratorPtr createIndexIterForward(fstring indexCols) const;

	IndexIteratorPtr createIndexIterBackward(size_t indexId) const;
	IndexIteratorPtr createIndexIterBackward(fstring indexCols) const;

	valvec<size_t> getProjectColumns(const hash_strmap<>& colnames) const;
//	valvec<size_t> getProjectColumns(const Schema&) const;

	void selectColumns(llong id, const valvec<size_t>& cols,
					   valvec<byte>* colsData, DbContext*) const;
	void selectColumns(llong id, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const;
	void selectOneColumn(llong id, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const;

#if 0
	StoreIteratorPtr
	createProjectIterForward(const valvec<size_t>& cols, DbContext*)
	const;
	StoreIteratorPtr
	createProjectIterBackward(const valvec<size_t>& cols, DbContext*)
	const;

	StoreIteratorPtr
	createProjectIterForward(const size_t* colsId, size_t colsNum, DbContext*)
	const;
	StoreIteratorPtr
	createProjectIterBackward(const size_t* colsId, size_t colsNum, DbContext*)
	const;
#endif

	void clear();
	void flush();
	void syncFinishWriting();

	void dropTable();

	std::string toJsonStr(fstring row) const;

	size_t getSegNum () const { return m_segments.size(); }
	size_t getWritableSegNum() const;

	///@{ internal use only
	void convWritableSegmentToReadonly(size_t segIdx);
	void freezeFlushWritableSegment(size_t segIdx);
	void putToCompressionQueue(size_t segIdx);
	///@}

	static void setCompressionThreadsNum(size_t threadsNum);
	static void safeStopAndWaitForFlush();
	static void safeStopAndWaitForCompress();

protected:
	static void registerTableClass(fstring tableClass, std::function<CompositeTable*()> tableFactory);

	class MergeParam; friend class MergeParam;
	void merge(MergeParam&);

	bool maybeCreateNewSegment(MyRwLock&);
	void doCreateNewSegmentInLock();
	llong insertRowImpl(fstring row, DbContext*, MyRwLock&);
	bool insertCheckSegDup(size_t begSeg, size_t numSeg, DbContext*);
	bool insertSyncIndex(llong subId, DbContext*);
	bool replaceCheckSegDup(size_t begSeg, size_t numSeg, DbContext*);
	bool replaceSyncIndex(llong newSubId, DbContext*, MyRwLock&);

	boost::filesystem::path getMergePath(PathRef dir, size_t mergeSeq) const;
	boost::filesystem::path getSegPath(const char* type, size_t segIdx) const;
	boost::filesystem::path getSegPath2(PathRef dir, size_t mergeSeq, const char* type, size_t segIdx) const;

	virtual ReadonlySegment* createReadonlySegment(PathRef segDir) const = 0;
	virtual WritableSegment* createWritableSegment(PathRef segDir) const = 0;
	virtual WritableSegment* openWritableSegment(PathRef segDir) const = 0;

	ReadonlySegment* myCreateReadonlySegment(PathRef segDir) const;
	WritableSegment* myCreateWritableSegment(PathRef segDir) const;

//	void registerDbContext(DbContext* ctx) const;
//	void unregisterDbContext(DbContext* ctx) const;

public:
	mutable tbb::queuing_rw_mutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
protected:
//	DbContextLink* m_ctxListHead;
	valvec<llong>  m_rowNumVec;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;
	size_t m_mergeSeqNum;
	size_t m_newWrSegNum;
	size_t m_bgTaskNum;
	bool m_tobeDrop;
	bool m_isMerging;

	// constant once constructed
	boost::filesystem::path m_dir;
	SchemaConfigPtr m_schema;
	friend class TableIndexIter;
	friend class TableIndexIterBackward;
	friend class DbContext;
	friend class ReadonlySegment;
};
typedef boost::intrusive_ptr<CompositeTable> CompositeTablePtr;

/////////////////////////////////////////////////////////////////////////////

inline
StoreIteratorPtr DbContext::createTableIter() {
	assert(this != nullptr);
	return m_tab->createStoreIterForward(this);
}

inline
void DbContext::getValueAppend(llong id, valvec<byte>* val) {
	assert(this != nullptr);
	m_tab->getValueAppend(id, val, this);
}
inline
void DbContext::getValue(llong id, valvec<byte>* val) {
	assert(this != nullptr);
	m_tab->getValue(id, val, this);
}

inline
llong DbContext::insertRow(fstring row) {
	assert(this != nullptr);
	return m_tab->insertRow(row, this);
}
inline
llong DbContext::replaceRow(llong id, fstring row) {
	assert(this != nullptr);
	return m_tab->replaceRow(id, row, this);
}
inline
void  DbContext::removeRow(llong id) {
	assert(this != nullptr);
	m_tab->removeRow(id, this);
}

inline
void DbContext::indexInsert(size_t indexId, fstring indexKey, llong id) {
	assert(this != nullptr);
	m_tab->indexInsert(indexId, indexKey, id, this);
}
inline
void DbContext::indexRemove(size_t indexId, fstring indexKey, llong id) {
	assert(this != nullptr);
	m_tab->indexRemove(indexId, indexKey, id, this);
}
inline
void
DbContext::indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId) {
	assert(this != nullptr);
	m_tab->indexReplace(indexId, indexKey, oldId, newId, this);
}



} } // namespace nark::db

#endif // __nark_db_table_store_hpp__
