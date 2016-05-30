#ifndef __terark_db_segment_hpp__
#define __terark_db_segment_hpp__

#include "db_index.hpp"
#include "db_store.hpp"
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>
#include <tbb/spin_rw_mutex.h>
#include <tbb/tbb_thread.h>

namespace terark {
	class SortableStrVec;
}

namespace terark { namespace db {

typedef tbb::spin_rw_mutex        SpinRwMutex;
typedef SpinRwMutex::scoped_lock  SpinRwLock;

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class TERARK_DB_DLL ReadableSegment : public ReadableStore {
public:
	ReadableSegment();
	~ReadableSegment();
	virtual class ReadonlySegment* getReadonlySegment() const;
	virtual class WritableSegment* getWritableSegment() const;
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* openIndex(const Schema&, PathRef path) const = 0;

	///@ if segDir==m_segDir, it is a flush
	virtual void loadRecordStore(PathRef segDir) = 0;
	virtual void saveRecordStore(PathRef segDir) const = 0;

	inline  void indexSearchExact(size_t mySegIdx, size_t indexId,
								  fstring key, valvec<llong>* recIdvec,
								  DbContext* ctx) const {
		recIdvec->erase_all();
		indexSearchExactAppend(mySegIdx, indexId, key, recIdvec, ctx);
	}

	virtual void indexSearchExactAppend(size_t mySegIdx, size_t indexId,
										fstring key, valvec<llong>* recIdvec,
										DbContext*) const = 0;

	virtual void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext*) const = 0;
	virtual void selectOneColumn(llong recId, size_t columnId,
								 valvec<byte>* colsData, DbContext*) const = 0;

	virtual void selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize,
								 valvec<byte>* cgDataVec, DbContext*) const = 0;

	void openIndices(PathRef dir);
	void saveIndices(PathRef dir) const;
	llong totalIndexSize() const;

	void saveIsDel(PathRef segDir) const;
	void loadIsDel(PathRef segDir);
	byte*loadIsDel_aux(PathRef segDir, febitvec& isDel) const;
	void closeIsDel();

	void deleteSegment();

	void load(PathRef segDir) override;
	void save(PathRef segDir) const override;

	size_t getPhysicRows() const;
	size_t getPhysicId(size_t logicId) const;
	size_t getLogicId(size_t physicId) const;

	void addtoUpdateList(size_t logicId);

	bool locked_testIsDel(size_t logicId) const {
		SpinRwLock wsLock(m_segMutex, false);
		return m_isDel[logicId];
	}

	SchemaConfigPtr         m_schema;
	valvec<ReadableIndexPtr> m_indices; // parallel with m_indexSchemaSet
	valvec<ReadableStorePtr> m_colgroups; // indices + pure_colgroups
	size_t      m_delcnt;
	febitvec    m_isDel;
	byte*       m_isDelMmap = nullptr;
	rank_select_se m_isPurged; // just for ReadonlySegment
	byte*          m_isPurgedMmap;
	boost::filesystem::path m_segDir;
	mutable SpinRwMutex m_segMutex;
	valvec<uint32_t> m_updateList; // including deletions
	febitvec    m_updateBits; // if m_updateList is too large, use updateBits
	bool        m_tobeDel;
	bool        m_isDirty;
	bool        m_isFreezed;
	bool        m_hasLockFreePointSearch;
	bool        m_bookUpdates;
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

	void indexSearchExactAppend(size_t mySegIdx, size_t indexId,
								fstring key, valvec<llong>* recIdvec,
								DbContext*) const override;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

	void selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize,
						 valvec<byte>* cgDataVec, DbContext*) const override;

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
	void syncUpdateRecordNoLock(size_t dstBaseId, size_t logicId,
								const ReadableSegment* input);

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

class DbTransaction : boost::noncopyable {
public:
	enum Status { started, committed, rollbacked } m_status;
	///@{ just for BatchWriter
	valvec<llong>   m_removeOnCommit;
	valvec<llong>   m_removeOnRollback; // the subId, must be in m_wrSeg
	// @}
	virtual void indexSearch(size_t indexId, fstring key, valvec<llong>* recIdvec) = 0;
	virtual void indexRemove(size_t indexId, fstring key, llong recId) = 0;
	virtual bool indexInsert(size_t indexId, fstring key, llong recId) = 0;
	virtual void indexUpsert(size_t indexId, fstring key, llong recId) = 0;
	virtual void storeRemove(llong recId) = 0;
	virtual void storeUpsert(llong recId, fstring row) = 0;
	virtual void storeGetRow(llong recId, valvec<byte>* row) = 0;
	void startTransaction();
	bool commit();
	void rollback();
	virtual const std::string& strError() const = 0;
	virtual ~DbTransaction();
	DbTransaction();

	virtual void do_startTransaction() = 0;
	virtual bool do_commit() = 0;
	virtual void do_rollback() = 0;
};
class TransactionGuard : boost::noncopyable {
protected:
	DbTransaction* m_txn;
public:
	explicit TransactionGuard(DbTransaction* txn) {
		assert(NULL != txn);
		txn->startTransaction();
		m_txn = txn;
	}
	~TransactionGuard() {
		assert(DbTransaction::committed == m_txn->m_status ||
			   DbTransaction::rollbacked == m_txn->m_status);
	}
	DbTransaction* getTxn() const { return m_txn; }
	void indexSearch(size_t indexId, fstring key, valvec<llong>* recIdvec) {
		m_txn->indexSearch(indexId, key, recIdvec);
	}
	void indexRemove(size_t indexId, fstring key, llong recId) {
		m_txn->indexRemove(indexId, key, recId);
	}
	bool indexInsert(size_t indexId, fstring key, llong recId) {
		return m_txn->indexInsert(indexId, key, recId);
	}
	void indexUpsert(size_t indexId, fstring key, llong recId) {
		m_txn->indexUpsert(indexId, key, recId);
	}
	void storeRemove(llong recId) {
		m_txn->storeRemove(recId);
	}
	void storeUpsert(llong recId, fstring row) {
		m_txn->storeUpsert(recId, row);
	}
	void storeGetRow(llong recId, valvec<byte>* row) {
		m_txn->storeGetRow(recId, row);
	}
	bool commit() {
		assert(DbTransaction::started == m_txn->m_status);
		return m_txn->commit();
	}
	void rollback() {
		assert(DbTransaction::started == m_txn->m_status);
		m_txn->rollback();
	}
	const std::string& strError() const { return m_txn->strError(); }
	const char* szError() const { return m_txn->strError().c_str(); }
};
class DefaultRollbackTransaction : public TransactionGuard {
public:
	explicit
	DefaultRollbackTransaction(DbTransaction* txn) : TransactionGuard(txn) {}
	~DefaultRollbackTransaction() {
		if (DbTransaction::started == m_txn->m_status) {
			m_txn->rollback();
		}
	}
};

// Concrete WritableSegment should not implement this class,
// should implement PlainWritableSegment or SmartWritableSegment
class TERARK_DB_DLL WritableSegment : public ReadableSegment, public WritableStore {
	class MyStoreIter;
public:
	virtual DbTransaction* createTransaction() = 0;

	WritableSegment();
	~WritableSegment();

	void pushIsDel(bool val);
	void popIsDel();

	WritableSegment* getWritableSegment() const override;

	llong totalStorageSize() const override;
	llong dataStorageSize() const override;
	llong dataInflateSize() const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	AppendableStore* getAppendableStore() override;
	UpdatableStore* getUpdatableStore() override;
	WritableStore* getWritableStore() override;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableIndex* createIndex(const Schema&, PathRef path) const = 0;

	virtual void getValueAppend(llong recId, valvec<byte>* val, DbContext*) const override;

	virtual llong append(fstring row, DbContext*) override;
	virtual void update(llong id, fstring row, DbContext*) override;
	virtual void remove(llong id, DbContext* ctx) override;
	virtual void shrinkToFit() override;

	void indexSearchExactAppend(size_t mySegIdx, size_t indexId,
								fstring key, valvec<llong>* recIdvec,
								DbContext*) const override;

	void getCombineAppend(llong recId, valvec<byte>* val, valvec<byte>& wrtBuf, ColumnVec& cols1, ColumnVec& cols2) const;

	void selectColumns(llong recId, const size_t* colsId, size_t colsNum,
					   valvec<byte>* colsData, DbContext*) const override;
	void selectOneColumn(llong recId, size_t columnId,
						 valvec<byte>* colsData, DbContext*) const override;

	void selectColumnsByWhole(llong recId,
							  const size_t* colsId, size_t colsNum,
							  valvec<byte>* colsData, DbContext*) const;
	void selectColumnsCombine(llong recId,
							  const size_t* colsId, size_t colsNum,
							  valvec<byte>* colsData, DbContext*) const;

	void selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize,
						 valvec<byte>* cgDataVec, DbContext*) const override;

	void flushSegment();

	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;

	void getWrtStoreData(llong subId, valvec<byte>* buf, DbContext* ctx) const;

	ReadableStorePtr  m_wrtStore;
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
