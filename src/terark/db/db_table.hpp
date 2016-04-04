#ifndef __terark_db_table_store_hpp__
#define __terark_db_table_store_hpp__

#include "db_store.hpp"
#include "db_index.hpp"
#include <tbb/queuing_rw_mutex.h>
//#include <tbb/spin_rw_mutex.h>

namespace terark {
	class BaseDFA; // forward declaration
} // namespace terark

namespace terark { namespace db {

typedef tbb::queuing_rw_mutex           MyRwMutex;
//typedef tbb::spin_rw_mutex              MyRwMutex;
//typedef tbb::speculative_spin_rw_mutex  MyRwMutex;

typedef MyRwMutex::scoped_lock MyRwLock;

class TERARK_DB_DLL ReadableSegment;
class TERARK_DB_DLL ReadonlySegment;
class TERARK_DB_DLL WritableSegment;
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

// is not a WritableStore
class TERARK_DB_DLL CompositeTable : public ReadableStore {
	class MyStoreIterBase;	    friend class MyStoreIterBase;
	class MyStoreIterForward;	friend class MyStoreIterForward;
	class MyStoreIterBackward;	friend class MyStoreIterBackward;
public:
	CompositeTable();
	~CompositeTable();

	struct RegisterTableClass {
		RegisterTableClass(fstring clazz, const std::function<CompositeTable*()>& f);
	};
#define TERARK_DB_REGISTER_TABLE_CLASS(TableClass) \
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
	llong dataInflateSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	bool exists(llong id) const;

	llong insertRow(fstring row, DbContext*);
	llong upsertRow(fstring row, DbContext*);
	llong updateRow(llong id, fstring row, DbContext*);
	bool  removeRow(llong id, DbContext*);

	void upsertRowMultiUniqueIndices(fstring row, valvec<llong>* resRecIdvec, DbContext*);

	void updateColumn(llong recordId, size_t columnId, fstring newColumnData, DbContext* = NULL);
	void updateColumn(llong recordId, fstring colname, fstring newColumnData, DbContext* = NULL);

	void updateColumnInteger(llong recordId, size_t columnId, const std::function<bool(llong&val)>&, DbContext* = NULL);
	void updateColumnInteger(llong recordId, fstring colname, const std::function<bool(llong&val)>&, DbContext* = NULL);
	void updateColumnDouble(llong recordId, size_t columnId, const std::function<bool(double&val)>&, DbContext* = NULL);
	void updateColumnDouble(llong recordId, fstring colname, const std::function<bool(double&val)>&, DbContext* = NULL);

	void incrementColumnValue(llong recordId, size_t columnId, llong incVal, DbContext* = NULL);
	void incrementColumnValue(llong recordId, fstring columnName, llong incVal, DbContext* = NULL);

	void incrementColumnValue(llong recordId, size_t columnId, double incVal, DbContext* = NULL);
	void incrementColumnValue(llong recordId, fstring colname, double incVal, DbContext* = NULL);

	const Schema& rowSchema() const { return *m_schema->m_rowSchema; }
	const Schema& getIndexSchema(size_t indexId) const {
		assert(indexId < m_schema->getIndexNum());
		return *m_schema->m_indexSchemaSet->m_nested.elem_at(indexId);
	}
	size_t getIndexId(fstring colnames) const {
		return m_schema->m_indexSchemaSet->m_nested.find_i(colnames);
	}
	size_t getIndexNum() const { return m_schema->getIndexNum(); }

	const Schema& getColgroupSchema(size_t cgId) const {
		assert(cgId < m_schema->getColgroupNum());
		return *m_schema->m_colgroupSchemaSet->m_nested.elem_at(cgId);
	}
	size_t getColgroupId(fstring cgName) const {
		return m_schema->m_colgroupSchemaSet->m_nested.find_i(cgName);
	}
	size_t getColgroupNum() const { return m_schema->getColgroupNum(); }

	void indexSearchExact(size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext*) const;
	bool indexKeyExists(size_t indexId, fstring key, DbContext*) const;

	void indexSearchExactNoLock(size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext*) const;
	bool indexKeyExistsNoLock(size_t indexId, fstring key, DbContext*) const;

	virtual	bool indexMatchRegex(size_t indexId, BaseDFA* regexDFA, valvec<llong>* recIdvec, DbContext*) const;
	virtual	bool indexMatchRegex(size_t indexId, fstring  regexStr, fstring regexOptions, valvec<llong>* recIdvec, DbContext*) const;

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

	void selectColgroups(llong id, const valvec<size_t>& cgIdvec,
						 valvec<valvec<byte> >* cgDataVec, DbContext*) const;
	void selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize,
						 valvec<byte>* cgDataVec, DbContext*) const;

	void selectOneColgroup(llong id, size_t cgId, valvec<byte>* cgData, DbContext*) const;

	void selectColumnsNoLock(llong id, const valvec<size_t>& cols,
					   valvec<byte>* colsData, DbContext*) const;
	void selectColumnsNoLock(llong id, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const;
	void selectOneColumnNoLock(llong id, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const;

	void selectColgroupsNoLock(llong id, const valvec<size_t>& cgIdvec,
						 valvec<valvec<byte> >* cgDataVec, DbContext*) const;
	void selectColgroupsNoLock(llong id, const size_t* cgIdvec, size_t cgIdvecSize,
						 valvec<byte>* cgDataVec, DbContext*) const;

	void selectOneColgroupNoLock(llong id, size_t cgId, valvec<byte>* cgData, DbContext*) const;

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
	void asyncPurgeDelete();

	void dropTable();

	std::string toJsonStr(fstring row) const;

	ReadableSegment* getSegmentPtr(size_t segIdx) const {
		assert(segIdx < m_segments.size());
		return m_segments[segIdx].get();
	}
	size_t findSegIdx(size_t segIdxBeg, ReadableSegment* seg) const;
	size_t getSegNum() const { return m_segments.size(); }
	size_t getWritableSegNum() const;

	size_t getSegArrayUpdateSeq() const { return this->m_segArrayUpdateSeq; }

	///@{ internal use only
	void convWritableSegmentToReadonly(size_t segIdx);
	void freezeFlushWritableSegment(size_t segIdx);
	void runPurgeDelete();
	void putToFlushQueue(size_t segIdx);
	void putToCompressionQueue(size_t segIdx);
	///@}

	static void safeStopAndWaitForFlush();
	static void safeStopAndWaitForCompress();

protected:
	static void registerTableClass(fstring tableClass, std::function<CompositeTable*()> tableFactory);

	class MergeParam; friend class MergeParam;
	void merge(MergeParam&);
	void checkRowNumVecNoLock() const;

	bool maybeCreateNewSegment(MyRwLock&);
	void doCreateNewSegmentInLock();
	llong insertRowImpl(fstring row, DbContext*, MyRwLock&);
	llong insertRowDoInsert(fstring row, DbContext*);
	bool insertCheckSegDup(size_t begSeg, size_t numSeg, DbContext*);
	bool insertSyncIndex(llong subId, DbContext*);
	bool updateCheckSegDup(size_t begSeg, size_t numSeg, DbContext*);
	bool updateSyncIndex(llong newSubId, DbContext*);
	void updateSyncMultIndex(llong newSubId, DbContext*);

	boost::filesystem::path getMergePath(PathRef dir, size_t mergeSeq) const;
	boost::filesystem::path getSegPath(const char* type, size_t segIdx) const;
	boost::filesystem::path getSegPath2(PathRef dir, size_t mergeSeq, const char* type, size_t segIdx) const;
	void removeStaleDir(PathRef dir, size_t inUseMergeSeq) const;

	virtual ReadonlySegment* createReadonlySegment(PathRef segDir) const = 0;
	virtual WritableSegment* createWritableSegment(PathRef segDir) const = 0;
	virtual WritableSegment* openWritableSegment(PathRef segDir) const = 0;

	ReadonlySegment* myCreateReadonlySegment(PathRef segDir) const;
	WritableSegment* myCreateWritableSegment(PathRef segDir) const;

	bool tryAsyncPurgeDeleteInLock(const ReadableSegment* seg);
	void asyncPurgeDeleteInLock();
	void inLockPutPurgeDeleteTaskToQueue();

//	void registerDbContext(DbContext* ctx) const;
//	void unregisterDbContext(DbContext* ctx) const;

public:
	mutable MyRwMutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
protected:
	enum class PurgeStatus : unsigned {
		none,
		pending,
		inqueue,
		purging,
	};

//	DbContextLink* m_ctxListHead;
	valvec<llong>  m_rowNumVec;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;
	size_t m_mergeSeqNum;
	size_t m_newWrSegNum;
	size_t m_bgTaskNum;
	size_t m_segArrayUpdateSeq;
	bool m_tobeDrop;
	bool m_isMerging;
	PurgeStatus m_purgeStatus;

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
StoreIteratorPtr DbContext::createTableIterForward() {
	assert(this != nullptr);
	return m_tab->createStoreIterForward(this);
}
inline
StoreIteratorPtr DbContext::createTableIterBackward() {
	assert(this != nullptr);
	return m_tab->createStoreIterBackward(this);
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
llong DbContext::upsertRow(fstring row) {
	assert(this != nullptr);
	return m_tab->upsertRow(row, this);
}
inline
llong DbContext::updateRow(llong id, fstring row) {
	assert(this != nullptr);
	return m_tab->updateRow(id, row, this);
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
inline void
DbContext::indexSearchExact(size_t indexId, fstring key, valvec<llong>* recIdvec) {
	m_tab->indexSearchExact(indexId, key, recIdvec, this);
}
inline bool
DbContext::indexKeyExists(size_t indexId, fstring key) {
	return m_tab->indexKeyExists(indexId, key, this);
}
inline void
DbContext::indexSearchExactNoLock(size_t indexId, fstring key, valvec<llong>* recIdvec) {
	m_tab->indexSearchExactNoLock(indexId, key, recIdvec, this);
}
inline bool
DbContext::indexKeyExistsNoLock(size_t indexId, fstring key) {
	return m_tab->indexKeyExistsNoLock(indexId, key, this);
}
inline bool
DbContext::indexMatchRegex(size_t indexId, BaseDFA* regexDFA, valvec<llong>* recIdvec) {
	return m_tab->indexMatchRegex(indexId, regexDFA, recIdvec, this);
}
inline bool
DbContext::indexMatchRegex(size_t indexId, fstring  regexStr, fstring regexOptions, valvec<llong>* recIdvec) {
	return m_tab->indexMatchRegex(indexId, regexStr, regexOptions, recIdvec, this);
}

inline void
DbContext::selectColumns(llong id, const valvec<size_t>& cols, valvec<byte>* colsData) {
	m_tab->selectColumns(id, cols, colsData, this);
}
inline void
DbContext::selectColumns(llong id, const size_t* colsId, size_t colsNum, valvec<byte>* colsData) {
	m_tab->selectColumns(id, colsId, colsNum, colsData, this);
}
inline void
DbContext::selectOneColumn(llong id, size_t columnId, valvec<byte>* colsData, DbContext*) {
	m_tab->selectOneColumn(id, columnId, colsData, this);
}

inline void
DbContext::selectColgroups(llong id, const valvec<size_t>& cgIdvec, valvec<valvec<byte> >* cgDataVec) {
	m_tab->selectColgroups(id, cgIdvec, cgDataVec, this);
}
inline void
DbContext::selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize, valvec<byte>* cgDataVec) {
	m_tab->selectColgroups(id, cgIdvec, cgIdvecSize, cgDataVec, this);
}
inline void
DbContext::selectOneColgroup(llong id, size_t cgId, valvec<byte>* cgData) {
	m_tab->selectOneColgroup(id, cgId, cgData, this);
}
inline void
DbContext::selectColumnsNoLock(llong id, const valvec<size_t>& cols, valvec<byte>* colsData) {
	m_tab->selectColumnsNoLock(id, cols, colsData, this);
}
inline void
DbContext::selectColumnsNoLock(llong id, const size_t* colsId, size_t colsNum, valvec<byte>* colsData) {
	m_tab->selectColumnsNoLock(id, colsId, colsNum, colsData, this);
}
inline void
DbContext::selectOneColumnNoLock(llong id, size_t columnId, valvec<byte>* colsData) {
	m_tab->selectOneColumnNoLock(id, columnId, colsData, this);
}
inline void
DbContext::selectColgroupsNoLock(llong id, const valvec<size_t>& cgIdvec, valvec<valvec<byte> >* cgDataVec) {
	m_tab->selectColgroupsNoLock(id, cgIdvec, cgDataVec, this);
}
inline void
DbContext::selectColgroupsNoLock(llong id, const size_t* cgIdvec, size_t cgIdvecSize, valvec<byte>* cgDataVec) {
	m_tab->selectColgroupsNoLock(id, cgIdvec, cgIdvecSize, cgDataVec, this);
}
inline void
DbContext::selectOneColgroupNoLock(llong id, size_t cgId, valvec<byte>* cgData) {
	m_tab->selectOneColgroupNoLock(id, cgId, cgData, this);
}


} } // namespace terark::db

#endif // __terark_db_table_store_hpp__
