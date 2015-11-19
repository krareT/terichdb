#ifndef __nark_db_table_store_hpp__
#define __nark_db_table_store_hpp__

#include <nark/util/refcount.hpp>
#include <nark/fstring.hpp>
#include <nark/valvec.hpp>
#include <nark/bitmap.hpp>
#include <boost/intrusive_ptr.hpp>
#include <atomic>
#include "db_conf.hpp"
#include "data_index.hpp"
#include "data_store.hpp"
#include <nark/util/sortable_strvec.hpp>
#include <tbb/queuing_rw_mutex.h>
#include "db_segment.hpp"

namespace nark {

class NARK_DB_DLL TableContext : public BaseContext {
public:
	TableContext();
	~TableContext();

	valvec<byte> row1;
	valvec<byte> row2;
	valvec<byte> key1;
	valvec<byte> key2;
	valvec<ColumnData> cols1;
	valvec<ColumnData> cols2;
	valvec<BaseContextPtr> wrIndexContext;
	BaseContextPtr wrStoreContext;
	BaseContextPtr readonlyContext;
};
typedef boost::intrusive_ptr<TableContext> TableContextPtr;

// is not a WritableStore
class NARK_DB_DLL CompositeTable : public ReadableStore {
	class MyStoreIterator;
	friend class MyStoreIterator;
public:
	CompositeTable();

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }

	void createTable(fstring dir, SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);
	void load(fstring dir) override;
	void save(fstring dir) const override;

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	llong totalStorageSize() const;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;

	llong insertRow(fstring row, bool syncIndex, BaseContextPtr&);
	llong replaceRow(llong id, fstring row, bool syncIndex, BaseContextPtr&);
	void  removeRow(llong id, bool syncIndex, BaseContextPtr&);

	void indexInsert(size_t indexId, fstring indexKey, llong id, BaseContextPtr&);
	void indexRemove(size_t indexId, fstring indexKey, llong id, BaseContextPtr&);
	void indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId, BaseContextPtr&);

	void getIndexKey(size_t indexId, const valvec<ColumnData>& row, valvec<byte>* key) const;
	bool compact();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	const size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
	size_t columnNum() const { return m_rowSchema->columnNum(); }

protected:
	void maybeCreateNewSegment(tbb::queuing_rw_mutex::scoped_lock&);
	llong insertRowImpl(fstring row, bool syncIndex,
						BaseContextPtr&, tbb::queuing_rw_mutex::scoped_lock&);

	fstring getSegPath(fstring type, size_t segIdx, class AutoGrownMemIO& buf) const;
	fstring getSegPath2(fstring dir, fstring type, size_t segIdx, class AutoGrownMemIO& buf) const;

#if defined(NARK_DB_ENABLE_DFA_META)
	void saveMetaDFA(fstring dir) const;
	void loadMetaDFA(fstring dir);
#endif
	void loadMetaJson(fstring dir);
	void saveMetaJson(fstring dir) const;

	virtual ReadonlySegmentPtr createReadonlySegment() const = 0;
	virtual WritableSegmentPtr createWritableSegment(fstring segDir) const = 0;
	virtual WritableSegmentPtr openWritableSegment(fstring segDir) const = 0;

	WritableSegmentPtr
	myCreateWritableSegment(size_t segIdx, class AutoGrownMemIO& buf) const;
	WritableSegmentPtr myCreateWritableSegment(fstring segDir) const;

protected:
	mutable tbb::queuing_rw_mutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
	valvec<llong>  m_rowNumVec;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;

	// constant once constructed
	std::string m_dir;
	SchemaPtr m_rowSchema; // full-row schema
	SchemaPtr m_nonIndexRowSchema;
	SchemaSetPtr m_indexSchemaSet;
	basic_fstrvec<unsigned> m_indexProjects;
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;
};
typedef boost::intrusive_ptr<CompositeTable> CompositeTablePtr;


} // namespace nark

#endif // __nark_db_table_store_hpp__
