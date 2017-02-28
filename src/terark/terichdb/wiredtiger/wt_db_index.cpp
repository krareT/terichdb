#include "wt_db_index.hpp"
#include "wt_db_context.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/lcast.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace terark { namespace terichdb { namespace wt {

namespace fs = boost::filesystem;

std::atomic<size_t> g_wtIndexIterLiveCnt;
std::atomic<size_t> g_wtIndexIterCreatedCnt;

class WtWritableIndex::MyIndexIterBase : public IndexIterator {
protected:
	typedef boost::intrusive_ptr<WtWritableIndex> MockWritableIndexPtr;
	WtWritableIndexPtr m_index;
	WT_CURSOR* m_iter;
	valvec<byte> m_buf;
	MyIndexIterBase(const WtWritableIndex* owner) {
		m_isUniqueInSchema = owner->m_schema->m_isUnique;
		m_index.reset(const_cast<WtWritableIndex*>(owner));
		WT_CONNECTION* conn = m_index->m_wtSession->connection;
		WT_SESSION* session; // WT_SESSION is not thread safe
		int err = conn->open_session(conn, NULL, NULL, &session);
		if (err) {
			THROW_STD(invalid_argument
				, "FATAL: wiredtiger open session(dir=%s) = %s"
				, conn->get_home(conn), wiredtiger_strerror(err)
				);
		}
		const std::string& uri = m_index->m_uri;
		err = session->open_cursor(session, uri.c_str(), NULL, NULL, &m_iter);
		if (err) {
			session->close(session, NULL);
			THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
		}
		g_wtIndexIterLiveCnt++;
		g_wtIndexIterCreatedCnt++;
		if (getEnvBool("TerichDB_TrackBuggyObjectLife")) {
			fprintf(stderr, "DEBUG: WtWritableIndexIter live count = %zd, created = %zd\n"
				, g_wtIndexIterLiveCnt.load(), g_wtIndexIterCreatedCnt.load());
		}
	}
	~MyIndexIterBase() {
		WT_SESSION* session = m_iter->session;
		m_iter->close(m_iter);
		session->close(session, NULL);
		g_wtIndexIterLiveCnt--;
	}
	void reset() override {
		m_iter->reset(m_iter);
	}
};

class WtWritableIndex::MyIndexIterForward : public MyIndexIterBase {
public:
	MyIndexIterForward(const WtWritableIndex* o) : MyIndexIterBase(o) {}
	bool increment(llong* id, valvec<byte>* key) override {
		assert(nullptr != m_iter);
		int err = m_iter->next(m_iter);
		if (0 == err) {
			m_index->getKeyVal(m_iter, key, id);
			return true;
		}
		if (WT_NOTFOUND != err) {
			THROW_STD(logic_error, "cursor_next failed: %s", wiredtiger_strerror(err));
		}
		return false;
	}
	static fstring keyFromItem(const Schema& schema, const WT_ITEM& item) {
		if (schema.m_isUnique)
			return fstring((char*)item.data, item.size);
		else
			return fstring((char*)item.data, item.size - 9);
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		const Schema& schema = *m_index->m_schema;
		WT_CONNECTION* conn = m_iter->session->connection;
		int cmp = 0;
		WtItem item;
		m_iter->reset(m_iter);
#if 0
		while (m_iter->next(m_iter) == 0) {
			llong recId = -1;
			m_iter->get_key(m_iter, &item, &recId);
			if (item.size)
			  fprintf(stderr
				, "DEBUG: WtIndexIterForward.lowerBound: dir=%s, key=%s, recId=%lld\n"
				, conn->get_home(conn), schema.toJsonStr(item).c_str(), recId);
		}
#endif
		m_index->setKeyVal(m_iter, key, 0, &item, &m_buf);
		int err = m_iter->search_near(m_iter, &cmp);
#if 0//!defined(NDEBUG)
		fprintf(stderr
			, "DEBUG: WtIndexIterForward.lowerBound: dir=%s, key=%s, item=%s, err=%d, cmp=%d\n"
			, conn->get_home(conn)
			, schema.toJsonStr(key).c_str()
			, schema.toJsonStr(keyFromItem(schema, item)).c_str()
			, err, cmp);
#endif
		if (err == 0) {
			m_index->getKeyVal(m_iter, retKey, id);
			// This is ugly and fragile, but it works
		//	fprintf(stderr
		//		, "DEBUG: WtIndexIterForward.lowerBound: cmp=%d, key=%s, retKey=%s, recId=%lld\n"
		//		, cmp, schema.toJsonStr(key).c_str(), schema.toJsonStr(*retKey).c_str(), *id);
			if (cmp >= 0) {
				return key == fstring(*retKey) ? 0 : 1;
			}
			// when cmp < 0, it should be key > retKey, otherwise,
			// wiredtiger cursor is wrapped: overflow and seek to begin pos
			bool hasNext = increment(id, retKey);
		//	fprintf(stderr
		//		, "DEBUG: WtIndexIterForward.lowerBound: wiredtiger brain damaged, hasNext=%d, retKey=%s\n"
		//		, hasNext, schema.toJsonStr(*retKey).c_str());
			if (hasNext) {
				int cmp2 = schema.compareData(key, *retKey);
				if (cmp2 > 0)
					return -1;
				else if (cmp2 < 0)
					return +1;
				else
					return 0;
			}
			return -1;
		}
		if (WT_NOTFOUND == err) {
			return -1;
		}
		THROW_STD(logic_error, "cursor_search_near(%s, index=%s) failed: %s"
			, conn->get_home(conn), schema.m_name.c_str()
			, wiredtiger_strerror(err));
	}
	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override {
		int cmp = 0;
		WT_ITEM item;
		m_iter->reset(m_iter);
		m_index->setKeyVal(m_iter, key, INT64_MAX, &item, &m_buf);
		int err = m_iter->search_near(m_iter, &cmp);
		if (err == 0) {
			m_index->getKeyVal(m_iter, retKey, id);
			const Schema& schema = *m_index->m_schema;
			// This is ugly and fragile, but it works
		//	fprintf(stderr
		//		, "DEBUG: WtIndexIterForward.upperBound: cmp=%d, key=%s, retKey=%s, recId=%lld\n"
		//		, cmp, schema.toJsonStr(key).c_str(), schema.toJsonStr(*retKey).c_str(), *id);
			if (0 == cmp) {
				// For dupable, INT64_MAX will never be a valid record id
				// For unique,  INT64_MAX is ignored, just 'key' is searched
				assert(schema.m_isUnique);
				return increment(id, retKey) ? 1 : -1;
			}
			if (key == *retKey) {
				return increment(id, retKey) ? 1 : -1;
			}
			if (cmp > 0) {
				return 1;
			}
			// when cmp < 0, it should be key > retKey, otherwise,
			// wiredtiger cursor is wrapped: overflow and seek to begin pos
			bool hasNext = increment(id, retKey);
	//		fprintf(stderr
	//			, "DEBUG: WtIndexIterForward.upperBound: wiredtiger brain damaged, hasNext=%d, retKey=%s\n"
	//			, hasNext, schema.toJsonStr(*retKey).c_str());
			if (hasNext) {
				int cmp2 = schema.compareData(key, *retKey);
				if (cmp2 > 0)
					return -1;
				else if (cmp2 < 0)
					return +1;
				else
					return 0;
			}
			return -1;
		}
		if (WT_NOTFOUND == err) {
			return -1;
		}
		THROW_STD(logic_error, "cursor_search_near failed: %s", wiredtiger_strerror(err));
	}
};

class WtWritableIndex::MyIndexIterBackward : public MyIndexIterBase {
public:
	MyIndexIterBackward(const WtWritableIndex* o) : MyIndexIterBase(o) {}
	bool increment(llong* id, valvec<byte>* key) override {
		assert(nullptr != m_iter);
		int err = m_iter->prev(m_iter);
		if (0 == err) {
			m_index->getKeyVal(m_iter, key, id);
			return true;
		}
		if (WT_NOTFOUND != err) {
			THROW_STD(logic_error, "cursor_next failed: %s", wiredtiger_strerror(err));
		}
		return false;
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		int cmp = 0;
		WT_ITEM item;
		m_iter->reset(m_iter);
		m_index->setKeyVal(m_iter, key, INT64_MAX, &item, &m_buf);
		int err = m_iter->search_near(m_iter, &cmp);
		if (err == 0) {
			m_index->getKeyVal(m_iter, retKey, id);
			const Schema& schema = *m_index->m_schema;
			// This is ugly and fragile, but it works
		//	fprintf(stderr
		//		, "DEBUG: WtIndexIterBackward.lowerBound: cmp=%d, key=%s, retKey=%s\n"
		//		, cmp, schema.toJsonStr(key).c_str(), schema.toJsonStr(*retKey).c_str());
			int cmp2 = schema.compareData(key, *retKey);
			if (cmp < 0 && cmp2 < 0) {
				// when cmp < 0, it should be key > retKey, otherwise,
				// wiredtiger cursor is wrapped: overflow and seek to begin pos
			//	fprintf(stderr, "DEBUG: WtIndexIterBackward.lowerBound: wiredtiger cursor wrap to begin\n");
				return -1;
			}
			if (0 == cmp2)
				return 0;
			if (cmp2 > 0)
				return 1;
			if (increment(id, retKey)) {
				cmp2 = schema.compareData(key, *retKey);
				if (0 == cmp2)
					return 0;
				if (cmp2 > 0)
					return 1;
			}
			return -1;
		}
		if (WT_NOTFOUND == err) {
			return -1;
		}
		THROW_STD(logic_error, "cursor_search_near failed: %s", wiredtiger_strerror(err));
	}
	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override {
		int cmp = 0;
		WT_ITEM item;
		m_iter->reset(m_iter);
		m_index->setKeyVal(m_iter, key, 0, &item, &m_buf);
		int err = m_iter->search_near(m_iter, &cmp);
		if (err == 0) {
			m_index->getKeyVal(m_iter, retKey, id);
			const Schema& schema = *m_index->m_schema;
			// This is ugly and fragile, but it works
		//	fprintf(stderr
		//		, "DEBUG: WtIndexIterBackward.upperBound: cmp=%d, key=%s, retKey=%s\n"
		//		, cmp, schema.toJsonStr(key).c_str(), schema.toJsonStr(*retKey).c_str());
			// when cmp < 0, it should be key > retKey, otherwise,
			// wiredtiger cursor is wrapped: overflow and seek to begin pos
		//	fprintf(stderr, "DEBUG: WtIndexIterBackward.lowerBound: wiredtiger cursor wrap to begin\n");
			int cmp2 = schema.compareData(key, *retKey);
			if (cmp2 > 0)
				return +1;
			if (increment(id, retKey)) {
				cmp2 = schema.compareData(key, *retKey);
				if (cmp2 > 0)
					return +1;
			}
			return -1;
		}
		if (WT_NOTFOUND == err) {
			return -1;
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
	//	if (schema.getFixedRowLen())
	//		return "key_format=" + lcast(schema.getFixedRowLen()) + "s,value_format=q";
	//	else
			return "key_format=u,value_format=q";
	}
	else {
	//	if (schema.getFixedRowLen())
	//		return "key_format=" + lcast(schema.getFixedRowLen()) + "sq,value_format=1t";
	//	else
	//		fuck wiredtiger collator and recover on wiredtiger_open
	//		return "key_format=uq,value_format=1t,collator=terark_wt_dup_index_compare";
			return "key_format=u,value_format=1t";
	}
#endif
}

void
WtWritableIndex::getKeyVal(WT_CURSOR* cursor, valvec<byte>* key, llong* recId)
const {
	getKeyVal(*m_schema, cursor, key, recId);
}

#if defined(BOOST_BIG_ENDIAN)
	#define BigEndianValue(x) (x)
#elif defined(BOOST_LITTLE_ENDIAN)
	#define BigEndianValue(x) terark::byte_swap(x)
#else
	#error must define one of "BOOST_BIG_ENDIAN" or "BOOST_LITTLE_ENDIAN"
#endif

void WtWritableIndex::getKeyVal(const Schema& schema, WT_CURSOR* cursor,
								valvec<byte>* key, llong* recId)
{
	WtItem item;
	if (schema.m_isUnique) {
		cursor->get_key(cursor, &item);
		cursor->get_value(cursor, recId);
		key->assign((const byte*)item.data, item.size);
	}
	else {
		cursor->get_key(cursor, &item);
	//	cursor->get_key(cursor, &item, recId);
	//	cursor->get_value(cursor, ...); // has no value
		// 9 == byte('\0') + int64_t
		assert(item.size >= 9);
		assert('\0' == ((byte*)item.data)[item.size-9]);
		key->assign((const byte*)item.data, item.size-9);
		int64_t r = unaligned_load<int64_t>((byte*)item.data + item.size-8);
		*recId = BigEndianValue(r);
	}
	if (schema.m_needEncodeToLexByteComparable) {
		schema.byteLexDecode(*key);
	}
}

void WtWritableIndex::setKeyVal(WT_CURSOR* cursor, fstring key, llong recId,
								WT_ITEM* item, valvec<byte>* buf)
const {
	setKeyVal(*m_schema, cursor, key, recId, item, buf);
}
void WtWritableIndex::setKeyVal(const Schema& schema, WT_CURSOR* cursor,
								fstring key, llong recId,
								WT_ITEM* item, valvec<byte>* buf)
{
	memset(item, 0, sizeof(*item));
	item->size = key.size();
	if (schema.m_needEncodeToLexByteComparable) {
		buf->assign(key);
		schema.byteLexEncode(*buf);
		if (schema.m_isUnique) {
			cursor->set_value(cursor, recId);
		}
		else {
			cursor->set_value(cursor, 1);
			buf->push_back('\0');
			unaligned_save(buf->grow_no_init(8), BigEndianValue(recId));
			assert(buf->size() == key.size() + 9);
		}
		item->data = buf->data();
		item->size = buf->size();
	}
	else {
		if (schema.m_isUnique) {
			item->data = key.data();
			item->size = key.size();
			cursor->set_value(cursor, recId);
		}
		else {
			cursor->set_value(cursor, 1);
			buf->assign(key);
			buf->push_back('\0');
			unaligned_save(buf->grow_no_init(8), BigEndianValue(recId));
			assert(buf->size() == key.size() + 9);
			item->data = buf->data();
			item->size = buf->size();
		}
	}
	cursor->set_key(cursor, item);
}

WtWritableIndex::WtWritableIndex(const Schema& schema, WT_CONNECTION* conn) {
	WT_SESSION* session;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err) {
		THROW_STD(invalid_argument, "FATAL: wiredtiger open session(dir=%s) = %s"
			, conn->get_home(conn), wiredtiger_strerror(err)
			);
	}
	m_keyFmt = toWtSchema(schema);
	m_uri = "table:" + schema.m_name;
	std::replace(m_uri.begin(), m_uri.end(), ',', '.');
	err = session->create(session, m_uri.c_str(), m_keyFmt.c_str());
	if (err) {
		session->close(session, NULL);
		THROW_STD(invalid_argument, "FATAL: wiredtiger create(%s, dir=%s) = %s"
			, m_keyFmt.c_str()
			, conn->get_home(conn), wiredtiger_strerror(err)
			);
	}
	err = session->open_cursor(session,
			m_uri.c_str(), NULL, "overwrite=true", &m_wtReplace);
	if (err) {
		session->close(session, NULL);
		THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
	}
	err = session->open_cursor(session,
			m_uri.c_str(), NULL, "overwrite=false", &m_wtCursor);
	if (err) {
		m_wtReplace->close(m_wtReplace);
		session->close(session, NULL);
		THROW_STD(logic_error, "open_cursor failed: %s", wiredtiger_strerror(err));
	}
	this->m_wtSession = session;
	this->m_isUnique = schema.m_isUnique;
	this->m_schema = &schema;
}

WtWritableIndex::~WtWritableIndex() {
	m_wtCursor->close(m_wtCursor);
	m_wtReplace->close(m_wtReplace);
	m_wtSession->close(m_wtSession, NULL);
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
#if 0
	int err = m_wtSession->checkpoint(m_wtSession, nullptr);
	if (err != 0) {
		THROW_STD(logic_error, "wt_checkpoint failed: %s", wiredtiger_strerror(err));
	}
#endif
}

void WtWritableIndex::load(PathRef path1) {
	WT_CONNECTION* conn = m_wtSession->connection;
	boost::filesystem::path segDir = conn->get_home(conn);
	auto fpath = segDir / m_uri.substr(6); // remove beginning "table:"
	m_indexStorageSize = boost::filesystem::file_size(fpath);
}

llong WtWritableIndex::indexStorageSize() const {
	return m_indexStorageSize;
}

bool WtWritableIndex::insert(fstring key, llong id, DbContext* ctx) {
    auto buf = ctx->bufs.get();
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_ITEM item;
	setKeyVal(m_wtCursor, key, id, &item, buf.get());
	int err = m_wtCursor->insert(m_wtCursor);
	if (err == WT_DUPLICATE_KEY) {
	//	fprintf(stderr, "wiredtiger dupkey: %s\n", m_schema->toJsonStr(key).c_str());
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
	m_indexStorageSize += key.size() + sizeof(id); // estimate
	return true;
}

bool WtWritableIndex::replace(fstring key, llong oldId, llong newId, DbContext* ctx) {
    auto buf = ctx->bufs.get();
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_CURSOR* cursor = m_wtReplace;
	WT_ITEM item;
	if (!m_isUnique) {
		setKeyVal(cursor, key, oldId, &item, buf.get());
		cursor->remove(cursor);
	}
	setKeyVal(cursor, key, newId, &item, buf.get());
	int err = cursor->insert(cursor);
	if (err == WT_DUPLICATE_KEY) {
		return false;
	}
	if (err) {
		WT_CONNECTION* conn = m_wtSession->connection;
		THROW_STD(invalid_argument
			, "FATAL: wiredtiger replace(dir=%s, uri=%s, key=%s) = %s"
			, conn->get_home(conn)
			, m_uri.c_str(), m_schema->toJsonStr(key).c_str()
			, wiredtiger_strerror(err)
			);
	}
// estimate size don't change
//	m_indexStorageSize += key.size() + sizeof(id); // estimate
	return true;
}

void
WtWritableIndex::searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext* ctx)
const {
#if 1
	THROW_STD(invalid_argument, "This method should not be called");
#else
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_ITEM item;
	memset(&item, 0, sizeof(item));
	item.size = key.size();
	if (m_schema->m_needEncodeToLexByteComparable) {
		ctx->buf1.assign(key);
		m_schema->byteLexEncode(ctx->buf1);
		item.data = ctx->buf1.data();
	}
	else {
		item.data = key.data();
	}
	// for unique index, it is the exact key
	// for dupable index, it is the key part of (key,\0,id)
	m_wtCursor->set_key(m_wtCursor, &item);
	if (m_isUnique) {
		int err = m_wtCursor->search(m_wtCursor);
		if (err == WT_NOTFOUND) {
			return;
		}
		if (err) {
			WT_CONNECTION* conn = m_wtSession->connection;
			THROW_STD(invalid_argument
				, "FATAL: wiredtiger search(dir=%s, uri=%s, key=%s) = %s"
				, conn->get_home(conn)
				, m_uri.c_str(), m_schema->toJsonStr(key).c_str()
				, wiredtiger_strerror(err)
				);
		}
		llong id = -1;
		m_wtCursor->get_value(m_wtCursor, &id);
		recIdvec->push_back(id);
	}
	else {
		int cmp;
		int err = m_wtCursor->search_near(m_wtCursor, &cmp);
		if (WT_NOTFOUND == err) {
			return;
		}
		if (err) {
			WT_CONNECTION* conn = m_wtSession->connection;
			THROW_STD(invalid_argument
				, "FATAL: wiredtiger search_near(dir=%s, uri=%s, key=%s) = %s"
				, conn->get_home(conn)
				, m_uri.c_str(), m_schema->toJsonStr(key).c_str()
				, wiredtiger_strerror(err)
				);
		}
		WtItem item2;
		while (0 == err) {
			m_wtCursor->get_key(m_wtCursor, &item2);
			if (item2.size == item.size + 9 &&
				memcmp(item2.data, item.data, item2.size) == 0)
			{
				llong id = BigEndianValue(unaligned_load<int64_t>(
								(byte*)item2.data + item2.size-8));
				recIdvec->push_back(id);
				err = m_wtCursor->next(m_wtCursor);
			}
			else break;
		}
	}
	m_wtCursor->reset(m_wtCursor);
#endif
}

bool WtWritableIndex::remove(fstring key, llong id, DbContext* ctx) {
    auto buf = ctx->bufs.get();
	tbb::mutex::scoped_lock lock(m_wtMutex);
	WT_ITEM item;
	setKeyVal(m_wtCursor, key, id, &item, buf.get());
	int err = m_wtCursor->remove(m_wtCursor);
	if (WT_NOTFOUND == err) {
		fprintf(stderr
			, "WARN: wt_remove non-existing key = %s\n"
			, m_schema->toJsonStr(key).c_str()
			);
		return false;
	}
	if (err) {
		THROW_STD(logic_error, "remove failed: %s", wiredtiger_strerror(err));
	}
	m_wtCursor->reset(m_wtCursor);
	m_indexStorageSize -= key.size() + sizeof(id); // estimate
	return true;
}

void WtWritableIndex::clear() {
	tbb::mutex::scoped_lock lock(m_wtMutex);
	int err = m_wtSession->truncate(m_wtSession, m_uri.c_str(), NULL, NULL, NULL);
	if (err != 0) {
		THROW_STD(logic_error, "truncate failed: %s", wiredtiger_strerror(err));
	}
	m_indexStorageSize = 0;
}

}}} // namespace terark::terichdb::wt
