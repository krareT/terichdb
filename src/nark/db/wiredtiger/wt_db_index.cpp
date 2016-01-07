#include "wt_db_index.hpp"
#include "wt_db_context.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/lcast.hpp>
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

static
std::string toWtSchema(const Schema& schema) {
#if 0
	std::string fmt = "key_format=";
	for (size_t i = 0; i < schema.m_columnsMeta.end_i(); ++i) {
		const ColumnMeta& colmeta = schema.m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(invalid_argument
				, "coltype=%s is not supported by wiredtiger"
				, schema.columnTypeStr(colmeta.type)
				);
		case ColumnType::Binary:
		case ColumnType::CarBin: fmt += 'u'; break;
		case ColumnType::Uint08: fmt += 'B'; break;
		case ColumnType::Sint08: fmt += 'b'; break;
		case ColumnType::Uint16: fmt += 'H'; break;
		case ColumnType::Sint16: fmt += 'h'; break;
		case ColumnType::Uint32: fmt += 'I'; break;
		case ColumnType::Sint32: fmt += 'i'; break;
		case ColumnType::Uint64: fmt += 'Q'; break;
		case ColumnType::Sint64: fmt += 'q'; break;
	//	case ColumnType::Uint128: break;
	//	case ColumnType::Sint128: break;
		case ColumnType::Float32: fmt += 'q'; break;
		case ColumnType::Float64: fmt += 'q'; break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    fmt += "16s"; break;// 16 bytes(128 bits) binary
		case ColumnType::Fixed:
			fmt += lcast(colmeta.fixedLen);
			fmt += 's';
			break;
	//	case ColumnType::VarSint:
	//	case ColumnType::VarUint:
		case ColumnType::StrZero: fmt += 'S'; break;
		case ColumnType::TwoStrZero: fmt += "SS"; break;
		}
	}
	fmt += ",value_format=r";
	return fmt;
#else
	if (schema.m_isUnique) {
		if (schema.getFixedRowLen())
			return lcast(schema.getFixedRowLen()) + "s";
		else
			return "u";
	}
	else {
		if (schema.getFixedRowLen())
			return lcast(schema.getFixedRowLen()) + "sq";
		else
			return "uq";
	}
#endif
}

void
WtWritableIndex::getKeyVal(WT_CURSOR* cursor, valvec<byte>* key, llong* recId)
const {
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	size_t oldsize = key->size();
	if (m_isUnique) {
		cursor->get_key(cursor, &item);
		cursor->get_value(cursor, recId);
	}
	else {
		cursor->get_key(cursor, &item, &recId);
	//	cursor->get_value(cursor, ...); // has no value
	}
	key->append((const byte*)item.data, item.size);
	if (m_schema->m_needEncodeToLexByteComparable) {
		m_schema->byteLexConvert(key->data() + oldsize, item.size);
	}
}

bool
WtWritableIndex::putKeyVal(WT_CURSOR* cursor, fstring key, llong recId, DbContext* ctx)
const {
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.size = key.size();
	if (m_schema->m_needEncodeToLexByteComparable) {
		ctx->buf1.assign(key);
		m_schema->byteLexConvert(ctx->buf1);
		item.data = ctx->buf1.data();
	}
	else {
		item.data = key.data();
	}
	if (m_isUnique) {
		cursor->set_key(cursor, &item);
		cursor->set_value(cursor, recId);
	}
	else {
		cursor->set_key(cursor, &item, recId);
	//	cursor->get_value(cursor, ...); // has no value
	}
	int err = cursor->insert(cursor);
	if (err == WT_DUPLICATE_KEY) {
		return false;
	}
	if (err) {
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger insert(dir=%s, uri=%s, key=%s) = %s"
			, m_wtSession->connection->get_home(m_wtSession->connection)
			, m_uri.c_str(), m_schema->toJsonStr(key).c_str()
			, wiredtiger_strerror(err)
			);
	}
	return true;
}

WtWritableIndex::WtWritableIndex(const Schema& schema, PathRef segDir, WT_SESSION* session) {
	std::string strDir = segDir.parent_path().string();
	std::string wtIndexSchema = toWtSchema(schema);
	m_uri = "table:" + schema.m_name;
	int err = session->create(session, m_uri.c_str(), wtIndexSchema.c_str());
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger create(%s, dir=%s) = %s"
			, wtIndexSchema.c_str()
			, strDir.c_str(), wiredtiger_strerror(err)
			);
	}
	this->m_wtSession = session;
	this->m_isUnique = schema.m_isUnique;
}

IndexIterator* WtWritableIndex::createIndexIterForward(DbContext*) const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	return new MyIndexIterForward(this);
}

IndexIterator* WtWritableIndex::createIndexIterBackward(DbContext*) const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	return new MyIndexIterBackward(this);
}

void WtWritableIndex::save(PathRef path1) const {
	int err = m_wtSession->checkpoint(m_wtSession, nullptr);
	if (err != 0) {
		THROW_STD(logic_error, "wt_checkpoint failed: %s", wiredtiger_strerror(err));
	}
}

void WtWritableIndex::load(PathRef path1) {
	// do nothing
}

llong WtWritableIndex::indexStorageSize() const {
	return 1024;
}

WT_CURSOR* WtWritableIndex::getCursor(DbContext* ctx0, bool writable) const {
//	WtContext* ctx = dynamic_cast<WtContext*>(ctx0);
//	FEBIRD_RT_assert(NULL != ctx, std::invalid_argument);
//	assert(2*m_indexId + 2 <= ctx->wtIndexCursor.size());
//	WT_CURSOR*& cursor = ctx->wtIndexCursor[2*m_indexId + int(writable)];
	WT_CURSOR*& cursor = m_wtCursor;
	if (!cursor) {
		int err = m_wtSession->open_cursor(m_wtSession, m_uri.c_str(), NULL, NULL, &cursor);
		if (err != 0) {
			THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
		}
	}
	return cursor;
}

bool WtWritableIndex::insert(fstring key, llong id, DbContext* ctx0) {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_CURSOR* cursor = getCursor(ctx0, 0);
	return putKeyVal(cursor, key, id, ctx0);
}

bool WtWritableIndex::replace(fstring key, llong oldId, llong newId, DbContext* ctx) {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_CURSOR* cursor = getCursor(ctx, 0);
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.size = key.size();
	if (m_schema->m_needEncodeToLexByteComparable) {
		ctx->buf1.assign(key);
		m_schema->byteLexConvert(ctx->buf1);
		item.data = ctx->buf1.data();
	}
	else {
		item.data = key.data();
	}
	if (m_isUnique) {
		cursor->set_key(cursor, &item);
		cursor->set_value(cursor, newId);
	}
	else {
		cursor->set_key(cursor, &item, oldId);
	//	cursor->get_value(cursor, ...); // has no value
		cursor->remove(cursor);
		cursor->set_key(cursor, &item, newId);
	}
	int err = cursor->insert(cursor);
	if (err == WT_DUPLICATE_KEY) {
		return false;
	}
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger insert(dir=%s, uri=%s, key=%s) = %s"
			, m_wtSession->connection->get_home(m_wtSession->connection)
			, m_uri.c_str(), m_schema->toJsonStr(key).c_str()
			, wiredtiger_strerror(err)
			);
	}
	return true;
}

llong WtWritableIndex::searchExact(fstring key, DbContext*) const {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	return -1;
}

bool WtWritableIndex::remove(fstring key, llong id, DbContext*) {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	return 0;
}

void WtWritableIndex::clear() {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	int err = m_wtSession->truncate(m_wtSession, m_uri.c_str(), NULL, NULL, NULL);
	if (err != 0) {
		THROW_STD(logic_error, "truncate failed: %s", wiredtiger_strerror(err));
	}
}

}}} // namespace nark::db::wt
