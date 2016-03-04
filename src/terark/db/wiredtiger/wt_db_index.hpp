#pragma once

#include <terark/db/db_table.hpp>
#include <nark/util/fstrvec.hpp>
#include <set>
#include <wiredtiger.h>
#include <tbb/mutex.h>

namespace terark { namespace db { namespace wt {

class NARK_DB_DLL WtWritableIndex : public ReadableIndex, public WritableIndex {
	class MyIndexIterForward;  friend class MyIndexIterForward;
	class MyIndexIterBackward; friend class MyIndexIterBackward;

	// brain dead wiredtiger api makes multi-thread code hard to write
	// use mutex to protect wiredtiger objects
	mutable tbb::mutex   m_wtMutex;
	mutable WT_SESSION*  m_wtSession;
	mutable WT_CURSOR*   m_wtCursor;
	mutable WT_CURSOR*   m_wtReplace;
	llong        m_indexStorageSize;
	size_t       m_indexId;
	std::string  m_keyFmt;
	std::string  m_uri;
	const Schema*m_schema;

	void getKeyVal(WT_CURSOR* cursor, valvec<byte>* key, llong* recId) const;
	void setKeyVal(WT_CURSOR* cursor, fstring key, llong recId,
				   WT_ITEM* item, valvec<byte>* buf) const;

public:
	explicit WtWritableIndex(const Schema&, PathRef segDir, WT_SESSION* session);
	~WtWritableIndex();
	void save(PathRef) const override;
	void load(PathRef) override;

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;
	llong indexStorageSize() const override;
	bool remove(fstring key, llong id, DbContext*) override;
	bool insert(fstring key, llong id, DbContext*) override;
	bool replace(fstring key, llong oldId, llong newId, DbContext*) override;
	void clear() override;

	size_t searchExact(fstring key, valvec<llong>* recIdvec, DbContext*) const override;
	WritableIndex* getWritableIndex() override { return this; }
};
typedef boost::intrusive_ptr<WtWritableIndex> WtWritableIndexPtr;

}}} // namespace terark::db::wt

