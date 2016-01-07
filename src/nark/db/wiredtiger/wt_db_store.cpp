#include "wt_db_store.hpp"
#include "wt_db_context.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db { namespace wt {

namespace fs = boost::filesystem;

static const char g_dataStoreUri[] = "table:__DataStore__";

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

WtWritableStore::WtWritableStore(WT_CONNECTION* conn, PathRef segDir) {
	std::string strDir = segDir.string();
	int err = conn->open_session(conn, NULL, NULL, &m_wtSession);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger open session(dir=%s) = %s"
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
	err = m_wtSession->create(m_wtSession, g_dataStoreUri, "key_format=r,value_format=u");
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger create(%s, dir=%s) = %s"
			, g_dataStoreUri
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
}
WtWritableStore::~WtWritableStore() {
	m_wtSession->close(m_wtSession, NULL);
}

WT_CURSOR* WtWritableStore::getStoreCursor() const {
	if (NULL == m_wtCursor) {
		int err = m_wtSession->open_cursor(m_wtSession, g_dataStoreUri, NULL, NULL, &m_wtCursor);
		if (err) {
			auto msg = m_wtSession->strerror(m_wtSession, err);
			THROW_STD(invalid_argument, "ERROR: wiredtiger store open_cursor: %s", msg);
		}
	}
	return m_wtCursor;
}

WT_CURSOR* WtWritableStore::getStoreAppend() const {
	if (NULL == m_wtAppend) {
		int err = m_wtSession->open_cursor(m_wtSession, g_dataStoreUri, NULL, "append", &m_wtAppend);
		if (err) {
			auto msg = m_wtSession->strerror(m_wtSession, err);
			THROW_STD(invalid_argument, "ERROR: wiredtiger store append open_cursor: %s", msg);
		}
	}
	return m_wtAppend;
}

void WtWritableStore::save(PathRef path1) const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	m_wtSession->checkpoint(m_wtSession, NULL);
}

void WtWritableStore::load(PathRef path1) {
}

llong WtWritableStore::dataStorageSize() const {
	return 0;
}

llong WtWritableStore::numDataRows() const {
	return 0;
}

void WtWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx0)
const {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	FEBIRD_RT_assert(NULL != ctx, std::invalid_argument);
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getStoreCursor();
	cursor->set_key(cursor, id);
	int err = cursor->search(cursor);
	if (err) {
		THROW_STD(invalid_argument, "wiredtiger search failed: id=%lld", id);
	}
	WT_ITEM item;
	cursor->get_value(cursor, &item);
	val->append((const byte*)item.data, item.size);
	cursor->reset(cursor);
}

StoreIterator* WtWritableStore::createStoreIterForward(DbContext*) const {
	return new WtWritableStoreIterForward(this);
}
StoreIterator* WtWritableStore::createStoreIterBackward(DbContext*) const {
	return new WtWritableStoreIterBackward(this);
}

llong WtWritableStore::append(fstring row, DbContext* ctx0) {
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	FEBIRD_RT_assert(NULL != ctx, std::invalid_argument);
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getStoreAppend();
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.data = row.data();
	item.size = row.size();
	cursor->set_value(cursor, &item);
	int err = cursor->insert(cursor);
	if (err) {
		THROW_STD(invalid_argument
			, "wiredtiger append failed, err=%s, row=%s"
			, m_wtSession->strerror(m_wtSession, err)
			, ctx0->m_tab->rowSchema().toJsonStr(row).c_str()
			);
	}
	llong recno;
	cursor->get_key(cursor, &recno);
	llong recId = recno - 1;
	return recId;
}

void WtWritableStore::replace(llong id, fstring row, DbContext* ctx0) {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	FEBIRD_RT_assert(NULL != ctx, std::invalid_argument);
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getStoreCursor();
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.data = row.data();
	item.size = row.size();
	cursor->set_key(cursor, id);
	cursor->set_value(cursor, &item);
	int err = cursor->insert(cursor);
	if (err) {
		THROW_STD(invalid_argument
			, "wiredtiger replace failed, err=%s, row=%s"
			, m_wtSession->strerror(m_wtSession, err)
			, ctx0->m_tab->rowSchema().toJsonStr(row).c_str()
			);
	}
}

void WtWritableStore::remove(llong id, DbContext* ctx0) {
	assert(id >= 0);
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	FEBIRD_RT_assert(NULL != ctx, std::invalid_argument);
	tbb::mutex::scoped_lock lock(m_wtMutex);
	auto cursor = getStoreCursor();
	cursor->set_key(cursor, id);
	int err = cursor->remove(cursor);
	if (err) {
		if (WT_NOTFOUND != err) {
			THROW_STD(invalid_argument
				, "wiredtiger replace failed, err=%s"
				, m_wtSession->strerror(m_wtSession, err)
				);
		} else {
			fprintf(stderr, "WARN: WtWritableStore::remove: id=%lld not found", id);
		}
	}
}

void WtWritableStore::clear() {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	m_wtSession->truncate(m_wtSession, g_dataStoreUri, NULL, NULL, NULL);
}

WritableStore* WtWritableStore::getWritableStore() {
	return this;
}

}}} // namespace nark::db::wt
