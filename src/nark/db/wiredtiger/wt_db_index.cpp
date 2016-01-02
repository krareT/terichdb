#include "wt_db_index.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db { namespace wt {

namespace fs = boost::filesystem;

class WtWritableIndex::MyIndexIterForward : public IndexIterator {
	typedef boost::intrusive_ptr<WtWritableIndex> MockWritableIndexPtr;
	WtWritableIndexPtr m_index;
	WT_CURSOR* m_iter;
public:
	MyIndexIterForward(const WtWritableIndex* owner) {
		m_index.reset(const_cast<WtWritableIndex*>(owner));
		int err = m_index->m_wtSession->open_cursor(
			m_index->m_wtSession, m_index->m_uri.c_str(), NULL, NULL, &m_iter);
		if (err) {
			THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
		}
	}
	~MyIndexIterForward() {
		if (m_iter)
			m_iter->close(m_iter);
	}
	bool increment(llong* id, valvec<byte>* key) override {
		assert(nullptr != m_iter);
		int err = m_iter->next(m_iter);
		if (0 == err) {
			m_iter->get_key(m_iter, id);
			WT_ITEM item;
			m_iter->get_value(m_iter, &item);
			key->assign((const byte*)item.data, item.size);
			return true;
		}
		if (WT_NOTFOUND != err) {
			THROW_STD(logic_error, "cursor_next failed: %s", wiredtiger_strerror(err));
		}
		return false;
	}
	void reset() override {
		auto owner = static_cast<const WtWritableIndex*>(m_index.get());
		m_iter->reset(m_iter);
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		int cmp = 0;
		int err = m_iter->search_near(m_iter, &cmp);
		if (err == 0) {
			if (cmp < 0) {
				return -1;
			}
			WT_ITEM item;
			m_iter->get_value(m_iter, &item);
			retKey->assign((const byte*)item.data, item.size);
			m_iter->get_key(m_iter, id);
			return cmp;
		}
		THROW_STD(logic_error, "cursor_search_near failed: %s", wiredtiger_strerror(err));
	}
};

class WtWritableIndex::MyIndexIterBackward : public IndexIterator {
	typedef boost::intrusive_ptr<WtWritableIndex> MockWritableIndexPtr;
	WtWritableIndexPtr m_index;
	WT_CURSOR* m_iter;
public:
	MyIndexIterBackward(const WtWritableIndex* owner) {
		m_index.reset(const_cast<WtWritableIndex*>(owner));
		int err = m_index->m_wtSession->open_cursor(
			m_index->m_wtSession, m_index->m_uri.c_str(), NULL, NULL, &m_iter);
		if (err != 0) {
			THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
		}
	}
	~MyIndexIterBackward() {
		if (m_iter)
			m_iter->close(m_iter);
	}
	bool increment(llong* id, valvec<byte>* key) override {
		assert(nullptr != m_iter);
		int err = m_iter->prev(m_iter);
		if (0 == err) {
			m_iter->get_key(m_iter, id);
			WT_ITEM item;
			m_iter->get_value(m_iter, &item);
			key->assign((const byte*)item.data, item.size);
			return true;
		}
		if (WT_NOTFOUND != err) {
			THROW_STD(logic_error, "cursor_next failed: %s", wiredtiger_strerror(err));
		}
		return false;
	}
	void reset() override {
		auto owner = static_cast<const WtWritableIndex*>(m_index.get());
		m_iter->reset(m_iter);
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		int cmp = 0;
		int err = m_iter->search_near(m_iter, &cmp);
		if (err == 0) {
			if (cmp > 0) {
				return increment(id, retKey) ? 1 : -1;
			}
			WT_ITEM item;
			m_iter->get_value(m_iter, &item);
			retKey->assign((const byte*)item.data, item.size);
			m_iter->get_key(m_iter, id);
			return -cmp;
		}
		THROW_STD(logic_error, "cursor_search_near failed: %s", wiredtiger_strerror(err));
	}
};

WtWritableIndex::WtWritableIndex(bool isUnique) {
	this->m_isUnique = isUnique;
}


IndexIterator* WtWritableIndex::createIndexIterForward(DbContext*) const {
	return new MyIndexIterForward(this);
}


IndexIterator* WtWritableIndex::createIndexIterBackward(DbContext*) const {
	return new MyIndexIterBackward(this);
}


void WtWritableIndex::save(PathRef path1) const {
	int err = m_wtSession->checkpoint(m_wtSession, nullptr);
	if (err != 0) {
		THROW_STD(logic_error, "wt_checkpoint failed: %s", wiredtiger_strerror(err));
	}
}

void WtWritableIndex::load(PathRef path1) {
}

llong WtWritableIndex::numIndexRows() const {
	return m_rows;
}


llong WtWritableIndex::indexStorageSize() const {
	return 1024;
}

WT_CURSOR* WtWritableIndex::getCursor(DbContext* ctx0, bool writable) const {
	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
	if (nullptr == ctx) {
		THROW_STD(logic_error, "DbContext is not a WtContext");
	}
	assert(2*m_indexId + 2 <= ctx->wtIndexCursor.size());
	WT_CURSOR*& cursor = ctx->wtIndexCursor[2*m_indexId + int(writable)];
	if (!cursor) {
		int err = m_wtSession->open_cursor(m_wtSession, m_uri.c_str(), NULL, NULL, &cursor);
		if (err != 0) {
			THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
		}
	}
	return cursor;
}

bool WtWritableIndex::insert(fstring key, llong id, DbContext* ctx0) {
	WT_CURSOR* cursor = getCursor(ctx0, 0);
	WT_ITEM keyData = {0};
	keyData.data = key.data();
	keyData.size = key.size();
	cursor->set_key(cursor, id);
	cursor->set_value(cursor, &keyData);
	int err = cursor->insert(cursor);
	if (err == 0)
		return true;
	if (err == WT_DUPLICATE_KEY)
		return false;
	THROW_STD(logic_error, "wt_insert failed: %s", wiredtiger_strerror(err));
}

bool WtWritableIndex::replace(fstring key, llong oldId, llong newId, DbContext* ctx0) {
	WT_CURSOR* cursor = getCursor(ctx0, 1);
	WT_ITEM keyData = {0};
	keyData.data = key.data();
	keyData.size = key.size();
	cursor->set_key(cursor, &keyData);
	cursor->set_value(cursor, &keyData);
	int err = cursor->insert(cursor);
	if (err == 0)
		return true;
	if (err == WT_DUPLICATE_KEY)
		return false;
	THROW_STD(logic_error, "wt_insert failed: %s", wiredtiger_strerror(err));
}

llong WtWritableIndex::searchExact(fstring key, DbContext*) const {
	return -1;
}

bool WtWritableIndex::remove(fstring key, llong id, DbContext*) {
	return 0;
}

void WtWritableIndex::clear() {
}

}}} // namespace nark::db::wt
