#pragma once

#include <nark/db/db_table.hpp>
#include <nark/util/fstrvec.hpp>
#include <set>
#include <tbb/mutex.h>
#include <wiredtiger.h>

namespace nark { namespace db { namespace wt {

class NARK_DB_DLL WtWritableStore : public ReadableStore, public WritableStore {

	// brain dead wiredtiger api makes multi-thread code hard to write
	// use mutex to protect wiredtiger objects
	mutable tbb::mutex   m_wtMutex;
	mutable WT_SESSION*  m_wtSession;
	mutable WT_CURSOR*   m_wtCursor;
	mutable WT_CURSOR*   m_wtAppend;
	llong m_dataSize;

	WT_CURSOR* getReplaceCursor() const;
	WT_CURSOR* getAppendCursor() const;

public:
	WtWritableStore(WT_SESSION* session, PathRef segDir);
	~WtWritableStore();

	void save(PathRef) const override;
	void load(PathRef) override;

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  update(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;

	void clear() override;

	AppendableStore* getAppendableStore() override;
	UpdatableStore* getUpdatableStore() override;
	WritableStore* getWritableStore() override;
};
typedef boost::intrusive_ptr<WtWritableStore> WtWritableStorePtr;

}}} // namespace nark::db::wt
