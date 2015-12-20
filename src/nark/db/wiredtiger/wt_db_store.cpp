#include "wt_db_store.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db { namespace wt {

namespace fs = boost::filesystem;

//////////////////////////////////////////////////////////////////
class WtWritableStoreIterForward : public StoreIterator {
	size_t m_id;
public:
	WtWritableStoreIterForward(const WtWritableStore* store) {
		m_store.reset(const_cast<WtWritableStore*>(store));
		m_id = 0;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto store = static_cast<WtWritableStore*>(m_store.get());
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		m_id = id;
		llong id2 = -1;
		return increment(&id2, val);
	}
	void reset() override {
		m_id = 0;
	}
};
class WtWritableStoreIterBackward : public StoreIterator {
	size_t m_id;
public:
	WtWritableStoreIterBackward(const WtWritableStore* store) {
		m_store.reset(const_cast<WtWritableStore*>(store));
		m_id = store->numDataRows();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto store = static_cast<WtWritableStore*>(m_store.get());
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		m_id = id + 1;
		llong id2 = -1;
		return increment(&id2, val);
	}
	void reset() override {
		m_id = m_store->numDataRows();
	}
};

void WtWritableStore::save(fstring path1) const {
}
void WtWritableStore::load(fstring path1) {
}

llong WtWritableStore::dataStorageSize() const {
	return 0;
}

llong WtWritableStore::numDataRows() const {
	return 0;
}

void WtWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
}

StoreIterator* WtWritableStore::createStoreIterForward(DbContext*) const {
	return new WtWritableStoreIterForward(this);
}
StoreIterator* WtWritableStore::createStoreIterBackward(DbContext*) const {
	return new WtWritableStoreIterBackward(this);
}

llong WtWritableStore::append(fstring row, DbContext*) {
	return -1;
}
void WtWritableStore::replace(llong id, fstring row, DbContext*) {
	assert(id >= 0);
}
void WtWritableStore::remove(llong id, DbContext*) {
	assert(id >= 0);
}
void WtWritableStore::clear() {
}

}}} // namespace nark::db::wt
