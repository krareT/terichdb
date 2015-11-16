#ifndef __nark_db_segment_hpp__
#define __nark_db_segment_hpp__

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

namespace nark {

class ReadableStoreIndex : public ReadableStore, public ReadableIndex {
public:
};
typedef boost::intrusive_ptr<ReadableStoreIndex> ReadableStoreIndexPtr;

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class ReadableSegment : public ReadableStore {
public:
	~ReadableSegment();
	virtual const ReadableIndex* getReadableIndex(size_t indexId) const = 0;
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	febitvec      m_isDel;
	SchemaPtr     m_rowSchema;
	SchemaSetPtr  m_indexSchemaSet;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

class ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	struct ReadonlyStoreContext : public BaseContext {
		valvec<byte> buf;
	};

	const ReadableIndex* getReadableIndex(size_t nth) const override;

	llong dataStorageSize() const override;
	llong totalStorageSize() const override;

	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;

	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;

	void mergeFrom(const valvec<const ReadonlySegment*>& input);
	void convFrom(const ReadableSegment& input, const Schema& schema);

	void getValueImpl(size_t partIdx, size_t id, llong subId, valvec<byte>* val, valvec<byte>* buf) const;

protected:
	virtual ReadableStoreIndexPtr
			buildIndex(SchemaPtr indexSchema, SortableStrVec& indexData)
			const = 0;

	virtual ReadableStorePtr buildStore(SortableStrVec& storeData) const = 0;

protected:
	class MyStoreIterator;
	SchemaPtr     m_dataSchema; // full-row schema except columns in indices
	valvec<llong> m_rowNumVec;  // prallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	valvec<ReadableStoreIndexPtr> m_indices; // parallel with m_indexSchemaSet
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

class WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableStore* getWritableStore() override;
	const ReadableIndex* getReadableIndex(size_t nth) const override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	valvec<WritableIndexPtr> m_indices;
protected:
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

} // namespace nark

#endif // __nark_db_segment_hpp__
