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

class TableContext : public BaseContext {
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
class CompositeTable : public ReadableStore {
	class MyStoreIterator;
	friend class MyStoreIterator;
public:
	CompositeTable();

	void setMaxWritableSegSize(llong size) { m_maxWrSegSize = size; }
	void setReadonlySegBufSize(llong size) { m_readonlyDataMemSize = size; }

	void createTable(fstring dir, fstring name,
					 SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet);

	void openTable(fstring dir, fstring name);

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	virtual ReadonlySegmentPtr createReadonlySegment(fstring dirBaseName) const = 0;
	virtual WritableSegmentPtr createWritableSegment(fstring dirBaseName) const = 0;

	virtual ReadonlySegmentPtr openReadonlySegment(fstring dirBaseName) const = 0;
	virtual WritableSegmentPtr openWritableSegment(fstring dirBaseName) const = 0;

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

	size_t columnNum() const;

	void getIndexKey(size_t indexId, const valvec<ColumnData>& columns, valvec<byte>* key) const;

	bool compact();

	const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
	const Schema& getTableSchema() const { return *m_rowSchema; }
	const size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }

protected:
	void maybeCreateNewSegment(tbb::queuing_rw_mutex::scoped_lock&);
	llong insertRowImpl(fstring row, bool syncIndex,
						BaseContextPtr&, tbb::queuing_rw_mutex::scoped_lock&);

protected:
	mutable tbb::queuing_rw_mutex m_rwMutex;
	mutable size_t m_tableScanningRefCount;
	valvec<llong>  m_rowNumVec;
	valvec<uint32_t>  m_deletedWrIdSet;
	valvec<ReadableSegmentPtr> m_segments;
	WritableSegmentPtr m_wrSeg;

	// constant once constructed
	std::string m_name;
	std::string m_dir;
	SchemaPtr m_rowSchema; // full-row schema
	SchemaPtr m_nonIndexRowSchema;
	SchemaSetPtr m_indexSchemaSet;
	basic_fstrvec<unsigned> m_indexProjects;
	llong m_readonlyDataMemSize;
	llong m_maxWrSegSize;
};

} // namespace nark

#endif // __nark_db_table_store_hpp__
