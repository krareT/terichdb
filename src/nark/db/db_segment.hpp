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

class NARK_DB_DLL_EXPORT ReadableStoreIndex : public ReadableStore, public ReadableIndex {
public:
};
typedef boost::intrusive_ptr<ReadableStoreIndex> ReadableStoreIndexPtr;

// This ReadableStore is used for return full-row
// A full-row is of one table, the table has multiple indices
class NARK_DB_DLL_EXPORT ReadableSegment : public ReadableStore {
public:
	~ReadableSegment();
	virtual const ReadableIndex* getReadableIndex(size_t indexId) const = 0;
	virtual llong totalStorageSize() const = 0;
	virtual llong numDataRows() const override final;

	void save(fstring) const override;
	void load(fstring) override;

	febitvec      m_isDel;
	SchemaPtr     m_rowSchema;
	SchemaPtr     m_nonIndexRowSchema; // full-row schema except columns in indices
	SchemaSetPtr  m_indexSchemaSet;
	byte* m_isDelMmap = nullptr;
};
typedef boost::intrusive_ptr<ReadableSegment> ReadableSegmentPtr;

class NARK_DB_DLL_EXPORT ReadonlySegment : public ReadableSegment {
public:
	ReadonlySegment();
	~ReadonlySegment();

	struct ReadonlyStoreContext : public BaseContext {
		valvec<byte> buf;
	};

	void save(fstring) const override;
	void load(fstring) override;

	// Store can use different implementation for different data
	// According to data content features
	virtual ReadableStorePtr openPart(fstring path) const = 0;

	// Index can use different implementation for different
	// index schema and index content features
	virtual ReadableStoreIndexPtr openIndex(fstring path, const Schema&) const = 0;

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
	valvec<llong> m_rowNumVec;  // prallel with m_parts
	valvec<ReadableStorePtr> m_parts; // partition of row set
	valvec<ReadableStoreIndexPtr> m_indices; // parallel with m_indexSchemaSet
	llong  m_dataMemSize;
	llong  m_totalStorageSize;
	size_t m_maxPartDataSize;
};
typedef boost::intrusive_ptr<ReadonlySegment> ReadonlySegmentPtr;

class NARK_DB_DLL_EXPORT WritableSegment : public ReadableSegment, public WritableStore {
public:
	WritableStore* getWritableStore() override;
	const ReadableIndex* getReadableIndex(size_t nth) const override;

 	const WritableIndex*
	nthWritableIndex(size_t nth) const { return m_indices[nth].get(); }

	valvec<WritableIndexPtr> m_indices;
protected:
	llong totalIndexSize() const;
};
typedef boost::intrusive_ptr<WritableSegment> WritableSegmentPtr;

} // namespace nark

#endif // __nark_db_segment_hpp__
