#ifndef __terark_db_db_context_hpp__
#define __terark_db_db_context_hpp__

#include "db_conf.hpp"

namespace terark {
	class BaseDFA;
}

namespace terark { namespace db {

typedef boost::intrusive_ptr<class DbTable> DbTablePtr;
typedef boost::intrusive_ptr<class StoreIterator> StoreIteratorPtr;

class TERARK_DB_DLL DbContextLink : public RefCounter {
	friend class DbTable;
protected:
	DbContextLink();
	~DbContextLink();
//	DbContextLink *m_prev, *m_next;
};
class TERARK_DB_DLL DbContext : public DbContextLink {
	friend class DbTable;
public:
	explicit DbContext(const DbTable* tab);
	~DbContext();

	void doSyncSegCtxNoLock(const DbTable* tab);
	void trySyncSegCtxNoLock(const DbTable* tab);
	void trySyncSegCtxSpeculativeLock(const DbTable* tab);
	class StoreIterator* getWrtStoreIterNoLock(size_t segIdx);
	class IndexIterator* getIndexIterNoLock(size_t segIdx, size_t indexId);

	void getWrSegWrtStoreData(const class ReadableSegment* seg, llong subId, valvec<byte>* buf);

	void debugCheckUnique(fstring row, size_t uniqIndexId);

/// @{ delegate methods
	StoreIteratorPtr createTableIterForward();
	StoreIteratorPtr createTableIterBackward();

	void getValueAppend(llong id, valvec<byte>* val);
	void getValue(llong id, valvec<byte>* val);

	llong insertRow(fstring row);
	llong upsertRow(fstring row);
	llong updateRow(llong id, fstring row);
	void  removeRow(llong id);

	void indexInsert(size_t indexId, fstring indexKey, llong id);
	void indexRemove(size_t indexId, fstring indexKey, llong id);
	void indexUpdate(size_t indexId, fstring indexKey, llong oldId, llong newId);

	void indexSearchExact(size_t indexId, fstring key, valvec<llong>* recIdvec);
	bool indexKeyExists(size_t indexId, fstring key);

	void indexSearchExactNoLock(size_t indexId, fstring key, valvec<llong>* recIdvec);
	bool indexKeyExistsNoLock(size_t indexId, fstring key);

	bool indexMatchRegex(size_t indexId, BaseDFA* regexDFA, valvec<llong>* recIdvec);
	bool indexMatchRegex(size_t indexId, fstring  regexStr, fstring regexOptions, valvec<llong>* recIdvec);

	void selectColumns(llong id, const valvec<size_t>& cols, valvec<byte>* colsData);
	void selectColumns(llong id, const size_t* colsId, size_t colsNum, valvec<byte>* colsData);
	void selectOneColumn(llong id, size_t columnId, valvec<byte>* colsData);

	void selectColgroups(llong id, const valvec<size_t>& cgIdvec, valvec<valvec<byte> >* cgDataVec);
	void selectColgroups(llong id, const size_t* cgIdvec, size_t cgIdvecSize, valvec<byte>* cgDataVec);

	void selectOneColgroup(llong id, size_t cgId, valvec<byte>* cgData);

	void selectColumnsNoLock(llong id, const valvec<size_t>& cols, valvec<byte>* colsData);
	void selectColumnsNoLock(llong id, const size_t* colsId, size_t colsNum, valvec<byte>* colsData);
	void selectOneColumnNoLock(llong id, size_t columnId, valvec<byte>* colsData);

	void selectColgroupsNoLock(llong id, const valvec<size_t>& cgIdvec, valvec<valvec<byte> >* cgDataVec);
	void selectColgroupsNoLock(llong id, const size_t* cgIdvec, size_t cgIdvecSize, valvec<byte>* cgDataVec);

	void selectOneColgroupNoLock(llong id, size_t cgId, valvec<byte>* cgData);
/// @}

	class ReadableSegment* getSegmentPtr(size_t segIdx) const;

public:
	struct SegCtx {
		class ReadableSegment* seg;
		class StoreIterator* wrtStoreIter;
		class IndexIterator* indexIter[1];
	private:
		friend class DbContext;
		~SegCtx() = delete;
		SegCtx() = delete;
		SegCtx(const SegCtx&) = delete;
		SegCtx& operator=(const SegCtx&) = delete;
		static SegCtx* create(ReadableSegment* seg, size_t indexNum);
		static void destory(SegCtx*& p, size_t indexNum);
		static void reset(SegCtx* p, size_t indexNum, ReadableSegment* seg);
	};
	DbTable* m_tab;
	class WritableSegment* m_wrSegPtr;
	std::unique_ptr<class DbTransaction> m_transaction;
	valvec<SegCtx*> m_segCtx;
	valvec<llong>   m_rowNumVec; // copy of DbTable::m_rowNumVec
	llong           m_mySnapshotVersion;
	std::string  errMsg;
	valvec<byte> buf1;
	valvec<byte> buf2;
	valvec<byte> row1;
	valvec<byte> row2;
	valvec<byte> key1;
	valvec<byte> key2;
	valvec<byte> userBuf; // TerarkDB will not use userBuf
	valvec<uint32_t> offsets;
	ColumnVec    cols1;
	ColumnVec    cols2;
	valvec<llong> exactMatchRecIdvec;
	size_t regexMatchMemLimit;
	size_t segArrayUpdateSeq;
	bool syncIndex;
	bool m_isUserDefineSnapshot;
	byte isUpsertOverwritten;
};
typedef boost::intrusive_ptr<DbContext> DbContextPtr;

} } // namespace terark::db

#endif // __terark_db_db_context_hpp__
