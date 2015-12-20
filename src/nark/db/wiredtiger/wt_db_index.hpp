#pragma once

#include <nark/db/db_table.hpp>
#include <nark/util/fstrvec.hpp>
#include <set>
#include <wiredtiger.h>

namespace nark { namespace db { namespace wt {

class NARK_DB_DLL WtContext : public DbContext {
public:
	WT_CURSOR*         wtStoreCursor;
	valvec<WT_CURSOR*> wtIndexCursor;
};
class NARK_DB_DLL WtWritableIndex : public ReadableIndex, public WritableIndex {
	class MyIndexIterForward;  friend class MyIndexIterForward;
	class MyIndexIterBackward; friend class MyIndexIterBackward;
	WT_CONNECTION* m_wtConn;
	WT_SESSION*    m_wtSession;
	size_t         m_rows;
	size_t         m_indexId;
	std::string    m_uri;
	WT_CURSOR* getCursor(DbContext*, bool writable) const;
public:
	explicit WtWritableIndex(bool isUnique);
	void save(fstring) const override;
	void load(fstring) override;

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	bool remove(fstring key, llong id, DbContext*) override;
	bool insert(fstring key, llong id, DbContext*) override;
	bool replace(fstring key, llong oldId, llong newId, DbContext*) override;
//	bool exists(fstring key, DbContext*) const override; // use default
	void clear() override;

	llong searchExact(fstring key, DbContext*) const override;
	WritableIndex* getWritableIndex() override { return this; }
};
typedef boost::intrusive_ptr<WtWritableIndex> WtWritableIndexPtr;

}}} // namespace nark::db::wt

