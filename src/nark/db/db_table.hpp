#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include "db_segment.hpp"
#include <nark/bitmap.hpp>
#include <tbb/queuing_rw_mutex.h>
#include <nark/util/fstrvec.hpp>
#include <nark/util/sortable_strvec.hpp>

namespace nark {
	class AutoGrownMemIO;
}

namespace nark { namespace db {

// is not a WritableStore
class NARK_DB_DLL CompositeTable : public ReadableStore, public SegmentSchema {
	class MyStoreIterator;
	friend class MyStoreIterator;
public:
	CompositeTable();

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }

	void createTable(fstring dir, SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);
	void load(fstring dir) override;
	void save(fstring dir) const override;

	StoreIterator* createStoreIter(DbContext*) const override;
	virtual DbContext* createDbContext() const = 0;

	llong totalStorageSize() const;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	llong insertRow(fstring row, bool syncIndex, DbContext*);
	llong replaceRow(llong id, fstring row, bool syncIndex, DbContext*);
	void  removeRow(llong id, bool syncIndex, DbContext*);

	void indexInsert(size_t indexId, fstring indexKey, llong id, DbContext*);
	void indexRemove(size_t indexId, fstring indexKey, llong id, DbContext*);
	void indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId, DbContext*);

	IndexIteratorPtr createIndexIter(size_t indexId) const;
	IndexIteratorPtr createIndexIter(fstring indexCols) const;

	bool compact();

	std::string toJsonStr(fstring row) const;

	size_t getSegNum () const { return m_segments.size(); }

protected:

	void maybeCreateNewSegment(tbb::queuing_rw_mutex::scoped_lock&);
	llong insertRowImpl(fstring row, bool syncIndex,
						DbContext*, tbb::queuing_rw_mutex::scoped_lock&);

	fstring getSegPath(fstring type, size_t segIdx, AutoGrownMemIO& buf) const;
	fstring getSegPath2(fstring dir, fstring type, size_t segIdx, AutoGrownMemIO& buf) const;

#if defined(NARK_DB_ENABLE_DFA_META)
	void saveMetaDFA(fstring dir) const;
	void loadMetaDFA(fstring dir);
#endif
	void loadMetaJson(fstring dir);
	void saveMetaJson(fstring dir) const;

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
	friend class TableIndexIter;
};
typedef boost::intrusive_ptr<CompositeTable> CompositeTablePtr;

/////////////////////////////////////////////////////////////////////////////

inline
StoreIteratorPtr DbContext::createTableIter() {
	assert(this != nullptr);
	return m_tab->createStoreIter(this);
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
llong DbContext::insertRow(fstring row, bool syncIndex) {
	assert(this != nullptr);
	return m_tab->insertRow(row, syncIndex, this);
}
inline
llong DbContext::replaceRow(llong id, fstring row, bool syncIndex) {
	assert(this != nullptr);
	return m_tab->replaceRow(id, row, syncIndex, this);
}
inline
void  DbContext::removeRow(llong id, bool syncIndex) {
	assert(this != nullptr);
	m_tab->removeRow(id, syncIndex, this);
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
