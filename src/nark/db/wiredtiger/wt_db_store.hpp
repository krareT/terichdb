#pragma once

#include <nark/db/db_table.hpp>
#include <nark/util/fstrvec.hpp>
#include <set>

namespace nark { namespace db { namespace wt {

class NARK_DB_DLL WtWritableStore : public ReadableStore, public WritableStore {
public:
	llong m_dataSize;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  replace(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;

	void clear() override;
};
typedef boost::intrusive_ptr<WtWritableStore> WtWritableStorePtr;

}}} // namespace nark::db::wt
