#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include "db_segment.hpp"
#include <tbb/queuing_rw_mutex.h>

namespace nark {
	class AutoGrownMemIO;
}

namespace nark { namespace db {

typedef tbb::queuing_rw_mutex              MyRwMutex;
typedef tbb::queuing_rw_mutex::scoped_lock MyRwLock;

// is not a WritableStore
class NARK_DB_DLL CompositeTable : public ReadableStore, public SegmentSchema {
	class MyStoreIterForward;	friend class MyStoreIterForward;
	class MyStoreIterBackward;	friend class MyStoreIterBackward;
public:
	CompositeTable();
	~CompositeTable();

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }

	void createTable(fstring dir, SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);
	void load(fstring dir) override;
	void save(fstring dir) const override;

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

	bool compact();
	void clear();
	void flush();

	void dropTable();

	std::string toJsonStr(fstring row) const;

	size_t getSegNum () const { return m_segments.size(); }

	void loadMetaJson(fstring jsonFile);
	void saveMetaJson(fstring jsonFile) const;

protected:

	void maybeCreateNewSegment(MyRwLock&);
	llong insertRowImpl(fstring row, DbContext*, MyRwLock&);
	bool insertCheckSegDup(size_t begSeg, size_t endSeg, DbContext*);
	bool insertSyncIndex(llong subId, DbContext*);
	bool replaceCheckSegDup(size_t begSeg, size_t endSeg, DbContext*);
	bool replaceSyncIndex(llong newSubId, DbContext*, MyRwLock&);

	fstring getSegPath(fstring type, size_t segIdx, AutoGrownMemIO& buf) const;
	fstring getSegPath2(fstring dir, fstring type, size_t segIdx, AutoGrownMemIO& buf) const;

#if defined(NARK_DB_ENABLE_DFA_META)
	void saveMetaDFA(fstring dir) const;
	void loadMetaDFA(fstring dir);
#endif
	virtual ReadonlySegment* createReadonlySegment(fstring segDir) const = 0;
	virtual WritableSegment* createWritableSegment(fstring segDir) const = 0;
	virtual WritableSegment* openWritableSegment(fstring segDir) const = 0;

	ReadonlySegment* myCreateReadonlySegment(fstring segDir) const;
	WritableSegment* myCreateWritableSegment(fstring segDir) const;

public:
	mutable tbb::queuing_rw_mutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
protected:
	valvec<llong>  m_rowNumVec;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;

	// constant once constructed
	std::string m_dir;
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;
	valvec<size_t> m_uniqIndices;
	valvec<size_t> m_multIndices;
	bool m_tobeDrop;
	friend class TableIndexIterUnOrdered;
	friend class TableIndexIter;
	friend class TableIndexIterBackward;
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
