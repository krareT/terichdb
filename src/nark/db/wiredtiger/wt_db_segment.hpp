#pragma once

#include <nark/db/db_segment.hpp>
#include <wiredtiger.h>

namespace nark { namespace db { namespace wt {

class NARK_DB_DLL WtWritableSegment : public WritableSegment {
public:
	WtWritableSegment();
	~WtWritableSegment();

	void init(PathRef segDir);

protected:
	ReadableIndex* createIndex(const Schema&, PathRef path) const override;
	ReadableIndex* openIndex(const Schema&, PathRef path) const override;

	WT_CONNECTION* m_wtConn;
	ReadableStorePtr m_rowStore;
	WritableStore*   m_wrRowStore;

public:
	llong totalStorageSize() const override;
	void loadRecordStore(PathRef segDir);
	void saveRecordStore(PathRef segDir) const;

	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  replace(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;

	void clear() override;

	WritableStore* getWritableStore() override;

	void save(PathRef path) const override;
	void load(PathRef path) override;

	size_t m_cacheSize;
};

}}} // namespace nark::db::wt
