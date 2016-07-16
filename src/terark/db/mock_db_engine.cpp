#include "mock_db_engine.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <mutex>

namespace terark { namespace db {

namespace fs = boost::filesystem;

TERARK_DB_REGISTER_STORE("mock", MockReadonlyStore);

MockReadonlyStore::MockReadonlyStore(const Schema& schema)
  : m_schema(schema)
{
	m_fixedLen = schema.getFixedRowLen();
}
llong MockReadonlyStore::dataStorageSize() const {
	return m_rows.used_mem_size();
}
llong MockReadonlyStore::dataInflateSize() const {
	return m_rows.used_mem_size();
}
llong MockReadonlyStore::numDataRows() const {
	return m_rows.size();
}
void
MockReadonlyStore::getValueAppend(llong id, valvec<byte>* val, DbContext*)
const {
	assert(id >= 0);
	if (m_fixedLen) {
		assert(0 == llong(m_rows.strpool.size() % m_fixedLen));
		assert(id < llong(m_rows.strpool.size() / m_fixedLen));
		val->append(m_rows.strpool.data() + m_fixedLen * id, m_fixedLen);
	} else {
		assert(id < llong(m_rows.size()));
		val->append(m_rows[id]);
	}
}
StoreIterator* MockReadonlyStore::createStoreIterForward(DbContext*) const {
	// return nullptr indicate use default iter
	return nullptr;
}
StoreIterator* MockReadonlyStore::createStoreIterBackward(DbContext*) const {
	// return nullptr indicate use default iter
	return nullptr;
}

void MockReadonlyStore::build(const Schema& schema, SortableStrVec& data) {
	size_t fixlen = schema.getFixedRowLen();
	if (0 == fixlen) {
		if (data.str_size() >= UINT32_MAX) {
			THROW_STD(length_error,
				"keys.str_size=%lld is too large", llong(data.str_size()));
		}
		// reuse memory of keys.m_index
		auto offsets = (uint32_t*)data.m_index.data();
		size_t rows = data.m_index.size();
		for (size_t i = 0; i < rows; ++i) {
			uint32_t offset = uint32_t(data.m_index[i].offset);
			offsets[i] = offset;
		}
		offsets[rows] = data.str_size();
		BOOST_STATIC_ASSERT(sizeof(SortableStrVec::SEntry) == 4*3);
		m_rows.offsets.risk_set_data(offsets);
		m_rows.offsets.risk_set_size(rows + 1);
		m_rows.offsets.risk_set_capacity(3 * rows);
		m_rows.offsets.shrink_to_fit();
		data.m_index.risk_release_ownership();
	#if !defined(NDEBUG)
		assert(data.m_strpool.size() == m_rows.offsets.back());
		for (size_t i = 0; i < rows; ++i) {
			assert(m_rows.offsets[i] < m_rows.offsets[i+1]);
		}
	#endif
	}
	m_rows.strpool.swap((valvec<char>&)data.m_strpool);
	m_fixedLen = fixlen;
}

void MockReadonlyStore::save(PathRef path) const {
	std::string fpath = path.string() + ".mock";
	FileStream fp(fpath.c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	size_t rows = m_fixedLen ? m_rows.strpool.size() / m_fixedLen : m_rows.size();
	dio << uint64_t(m_fixedLen);
	dio << uint64_t(rows);
	dio << uint64_t(m_rows.strpool.size());
	if (0 == m_fixedLen) {
	#if !defined(NDEBUG)
		assert(m_rows.strpool.size() == m_rows.offsets.back());
		for (size_t i = 0; i < rows; ++i) {
			assert(m_rows.offsets[i] < m_rows.offsets[i+1]);
		}
	#endif
		dio.ensureWrite(m_rows.offsets.data(), m_rows.offsets.used_mem_size());
	} else {
		assert(m_rows.strpool.size() % m_fixedLen == 0);
	}
	dio.ensureWrite(m_rows.strpool.data(), m_rows.strpool.used_mem_size());
}
void MockReadonlyStore::load(PathRef fpath) {
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	uint64_t fixlen, rows, strSize;
	dio >> fixlen;
	dio >> rows;
	dio >> strSize;
	m_fixedLen = size_t(fixlen);
	m_rows.strpool.resize_no_init(size_t(strSize));
	if (0 == m_fixedLen) {
		m_rows.offsets.resize_no_init(size_t(rows + 1));
		dio.ensureRead(m_rows.offsets.data(), m_rows.offsets.used_mem_size());
	#if !defined(NDEBUG)
		assert(m_rows.strpool.size() == m_rows.offsets.back());
		for (size_t i = 0; i < rows; ++i) {
			assert(m_rows.offsets[i] < m_rows.offsets[i+1]);
		}
	#endif
	} else {
		assert(m_rows.strpool.size() % m_fixedLen == 0);
		assert(m_rows.strpool.size() / m_fixedLen == rows);
	}
	dio.ensureRead(m_rows.strpool.data(), m_rows.strpool.used_mem_size());
}

struct FixedLenKeyExtractor {
	fstring operator()(size_t x) const {
		return fstring(strpool + fixedLen * x, fixedLen);
	}
	size_t fixedLen;
	const char* strpool;
};

struct VarLenKeyExtractor {
	fstring operator()(size_t x) const {
		size_t xoff0 = offsets[x], xoff1 = offsets[x+1];
		return fstring(strpool + xoff0, xoff1 - xoff0);
	}
	const char    * strpool;
	const uint32_t* offsets;
};

class MockReadonlyIndexIterator : public IndexIterator {
	friend class MockReadonlyIndex;
	MockReadonlyIndexPtr m_index;
	size_t m_pos;
public:
	MockReadonlyIndexIterator(const MockReadonlyIndex* owner) {
		m_isUniqueInSchema = owner->m_schema->m_isUnique;
		m_index.reset(const_cast<MockReadonlyIndex*>(owner));
		m_pos = 0;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		assert(nullptr != id);
		if (terark_likely(m_pos < m_index->m_ids.size())) {
			owner->getIndexKey(id, key, m_pos);
			m_pos++;
			return true;
		}
		return false;
	}
	void reset() override {
		m_pos = 0;
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		int ret = owner->forwardLowerBound(key, &m_pos);
		if (ret >= 0) {
			assert(m_pos < owner->m_ids.size());
			owner->getIndexKey(id, retKey, m_pos);
			m_pos++;
		}
		return ret;
	}
};
class MockReadonlyIndexIterBackward : public IndexIterator {
	friend class MockReadonlyIndex;
	MockReadonlyIndexPtr m_index;
	size_t m_pos;
public:
	MockReadonlyIndexIterBackward(const MockReadonlyIndex* owner) {
		m_isUniqueInSchema = owner->m_schema->m_isUnique;
		m_index.reset(const_cast<MockReadonlyIndex*>(owner));
		m_pos = owner->m_ids.size();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		assert(nullptr != id);
		if (terark_likely(m_pos > 0)) {
			owner->getIndexKey(id, key, --m_pos);
			return true;
		}
		return false;
	}
	void reset() {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		m_pos = owner->m_ids.size();
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		int ret = owner->forwardLowerBound(key, &m_pos);
		if (ret <= 0) {
			assert(m_pos < owner->m_ids.size());
			if (ret)
				--m_pos;
			owner->getIndexKey(id, retKey, m_pos);
		}
		return -ret;
	}
};

//static
int MockReadonlyIndex::forwardLowerBound(fstring key, size_t* pLower) const {
	const uint32_t* index = m_ids.data();
	const size_t rows = m_ids.size();
	const size_t fixlen = m_fixedLen;
	const auto   cmp = m_schema->compareData_less();
	if (fixlen) {
		assert(m_keys.size() == 0);
		assert(key.size() == fixlen);
		FixedLenKeyExtractor keyEx;
		keyEx.fixedLen = fixlen;
		keyEx.strpool = m_keys.strpool.data();
		size_t lo = lower_bound_ex_0(index, rows, key, keyEx, cmp);
		*pLower = lo;
		if (lo < rows) {
			size_t jj = m_ids[lo];
			if (key == fstring(keyEx.strpool + fixlen*jj, fixlen))
				return 0;
			else
				return 1;
		}
	}
	else {
		VarLenKeyExtractor keyEx;
		keyEx.offsets = m_keys.offsets.data();
		keyEx.strpool = m_keys.strpool.data();
		size_t lo = lower_bound_ex_0(index, rows, key, keyEx, cmp);
		*pLower = lo;
		if (lo < rows) {
			size_t jj = m_ids[lo];
			if (key == m_keys[jj])
				return 0;
			else
				return 1;
		}
	}
	return -1;
}

void MockReadonlyIndex::getIndexKey(llong* id, valvec<byte>* key, size_t pos) const {
	assert(pos < m_ids.size());
	*id = m_ids[pos];
	if (key) {
		size_t fixlen = m_fixedLen;
		fstring k;
		if (fixlen) {
			k.p = m_keys.strpool.data() + fixlen * *id;
			k.n = fixlen;
		} else {
			k = m_keys[*id];
		}
		key->assign(k.udata(), k.size());
	}
}

MockReadonlyIndex::MockReadonlyIndex(const Schema& schema) {
	m_schema = &schema;
	m_fixedLen = schema.getFixedRowLen();
}

MockReadonlyIndex::~MockReadonlyIndex() {
}

StoreIterator* MockReadonlyIndex::createStoreIterForward(DbContext*) const {
	assert(!"Readonly column store did not define iterator");
	return nullptr;
}
StoreIterator* MockReadonlyIndex::createStoreIterBackward(DbContext*) const {
	assert(!"Readonly column store did not define iterator");
	return nullptr;
}

#ifdef _MSC_VER
#define qsort_r qsort_s
#endif

void
MockReadonlyIndex::build(SortableStrVec& keys) {
	const Schema* schema = m_schema;
	const byte* base = keys.m_strpool.data();
	size_t fixlen = schema->getFixedRowLen();
	if (fixlen) {
		assert(keys.m_index.size() == 0);
		assert(keys.str_size() % fixlen == 0);
		m_ids.resize_no_init(keys.str_size() / fixlen);
		for (size_t i = 0; i < m_ids.size(); ++i) m_ids[i] = i;
		std::sort(m_ids.begin(), m_ids.end(), [=](size_t x, size_t y) {
			fstring xs(base + fixlen * x, fixlen);
			fstring ys(base + fixlen * y, fixlen);
			int r = schema->compareData(xs, ys);
			if (r) return r < 0;
			else   return x < y;
		});
	}
	else {
		if (keys.str_size() >= UINT32_MAX) {
			THROW_STD(length_error,
				"keys.str_size=%lld is too large", llong(keys.str_size()));
		}
		// reuse memory of keys.m_index
		auto offsets = (uint32_t*)keys.m_index.data();
		size_t rows = keys.m_index.size();
		m_ids.resize_no_init(rows);
		for (size_t i = 0; i < rows; ++i) m_ids[i] = i;
		for (size_t i = 0; i < rows; ++i) {
			uint32_t offset = uint32_t(keys.m_index[i].offset);
			offsets[i] = offset;
		}
		offsets[rows] = keys.str_size();
		std::sort(m_ids.begin(), m_ids.end(), [=](size_t x, size_t y) {
			size_t xoff0 = offsets[x], xoff1 = offsets[x+1];
			size_t yoff0 = offsets[y], yoff1 = offsets[y+1];
			fstring xs(base + xoff0, xoff1 - xoff0);
			fstring ys(base + yoff0, yoff1 - yoff0);
			int r = schema->compareData(xs, ys);
			if (r) return r < 0;
			else   return x < y;
		});
		BOOST_STATIC_ASSERT(sizeof(SortableStrVec::SEntry) == 4*3);
		m_keys.offsets.risk_set_data(offsets);
		m_keys.offsets.risk_set_size(rows + 1);
		m_keys.offsets.risk_set_capacity(3 * rows);
		m_keys.offsets.shrink_to_fit();
		keys.m_index.risk_release_ownership();
	}
	m_keys.strpool.swap((valvec<char>&)keys.m_strpool);
	m_fixedLen = fixlen;
}

void MockReadonlyIndex::save(PathRef fpath) const {
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	size_t rows = m_ids.size();
	dio << uint64_t(m_fixedLen);
	dio << uint64_t(rows);
	dio << uint64_t(m_keys.strpool.size());
	dio.ensureWrite(m_ids.data(), m_ids.used_mem_size());
	if (m_fixedLen) {
		assert(m_keys.size() == 0);
		assert(m_keys.strpool.size() == m_fixedLen * rows);
	} else {
		assert(m_keys.size() == rows);
		dio.ensureWrite(m_keys.offsets.data(), m_keys.offsets.used_mem_size());
	}
	dio.ensureWrite(m_keys.strpool.data(), m_keys.strpool.used_mem_size());
}

void MockReadonlyIndex::load(PathRef fpath) {
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	uint64_t fixlen, rows, keylen;
	dio >> fixlen;
	dio >> rows;
	dio >> keylen;
	m_ids.resize_no_init(size_t(rows));
	dio.ensureRead(m_ids.data(), m_ids.used_mem_size());
	if (0 == fixlen) {
		m_keys.offsets.resize_no_init(size_t(rows + 1));
		dio.ensureRead(m_keys.offsets.data(), m_keys.offsets.used_mem_size());
	}
	else {
		assert(fixlen * rows == keylen);
	}
	m_keys.strpool.resize_no_init(size_t(keylen));
	dio.ensureRead(m_keys.strpool.data(), size_t(keylen));
	m_fixedLen = size_t(fixlen);
}

llong MockReadonlyIndex::numDataRows() const {
	return m_ids.size();
}
llong MockReadonlyIndex::dataStorageSize() const {
	return m_ids.used_mem_size()
		+ m_keys.offsets.used_mem_size()
		+ m_keys.strpool.used_mem_size();
}

llong MockReadonlyIndex::dataInflateSize() const {
	return m_keys.strpool.used_mem_size();
}

void
MockReadonlyIndex::getValueAppend(llong id, valvec<byte>* key, DbContext*)
const {
	assert(id < (llong)m_ids.size());
	assert(id >= 0);
	if (m_fixedLen) {
		assert(m_keys.size() == 0);
		assert(0 == llong(m_keys.strpool.size() % m_fixedLen));
		assert(m_keys.strpool.size() == m_ids.size() * m_fixedLen);
		fstring key1(m_keys.strpool.data() + m_fixedLen * id, m_fixedLen);
		key->append(key1.udata(), key1.size());
	}
	else {
		assert(m_ids.size() == m_keys.size());
		fstring key1 = m_keys[id];
		key->append(key1.udata(), key1.size());
	}
}

void
MockReadonlyIndex::searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*)
const {
	size_t lower;
	if (forwardLowerBound(key, &lower) != 0) {
		return;
	}
	size_t f = m_fixedLen;
	if (f) {
		auto sp = m_keys.strpool.data();
		size_t i = lower;
		while (i < m_ids.size() && memcmp(key.p, sp + f*m_ids[i], f) == 0) {
			recIdvec->push_back(m_ids[i]);
			i++;
		}
	}
	else {
		for (size_t i = lower; i < m_ids.size() && m_keys[m_ids[i]] == key; ++i) {
			recIdvec->push_back(m_ids[i]);
		}
	}
}

IndexIterator* MockReadonlyIndex::createIndexIterForward(DbContext*) const {
	return new MockReadonlyIndexIterator(this);
}
IndexIterator* MockReadonlyIndex::createIndexIterBackward(DbContext*) const {
	return new MockReadonlyIndexIterBackward(this);
}

llong MockReadonlyIndex::indexStorageSize() const {
	return m_ids.used_mem_size() + m_keys.offsets.used_mem_size();
}

ReadableIndex* MockReadonlyIndex::getReadableIndex() {
	return this;
}

ReadableStore* MockReadonlyIndex::getReadableStore() {
	return this;
}

//////////////////////////////////////////////////////////////////
template<class WrStore>
class MockWritableStoreIterForward : public StoreIterator {
	size_t m_id;
public:
	MockWritableStoreIterForward(const WrStore* store) {
		m_store.reset(const_cast<WrStore*>(store));
		m_id = 0;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto store = static_cast<WrStore*>(m_store.get());
		SpinRwLock lock(store->m_rwMutex, false);
		size_t rowNum = store->m_rows.size();
		while (m_id < rowNum) {
			size_t k = m_id++;
			if (!store->m_rows[k].empty()) {
				*id = k;
				*val = store->m_rows[k];
				return true;
			}
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto store = static_cast<WrStore*>(m_store.get());
		SpinRwLock lock(store->m_rwMutex, false);
		if (id < 0 || id >= llong(store->m_rows.size())) {
			THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
				, id, store->m_rows.size());
		}
		if (!store->m_rows[id].empty()) {
			*val = store->m_rows[id];
			m_id = id + 1;
			return true;
		}
		return false;
	}
	void reset() override {
		m_id = 0;
	}
};
template<class WrStore>
class MockWritableStoreIterBackward : public StoreIterator {
	size_t m_id;
public:
	MockWritableStoreIterBackward(const WrStore* store) {
		m_store.reset(const_cast<WrStore*>(store));
		m_id = store->numDataRows();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto store = static_cast<WrStore*>(m_store.get());
		SpinRwLock lock(store->m_rwMutex, false);
		while (m_id > 0) {
			size_t k = --m_id;
			if (!store->m_rows[k].empty()) {
				*id = k;
				*val = store->m_rows[k];
				return true;
			}
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto store = static_cast<WrStore*>(m_store.get());
		SpinRwLock lock(store->m_rwMutex, false);
		if (id < 0 || id >= llong(store->m_rows.size())) {
			THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
				, id, store->m_rows.size());
		}
		if (!store->m_rows[id].empty()) {
			*val = store->m_rows[id];
			m_id = id;
			return true;
		}
		return false;
	}
	void reset() override {
		m_id = m_store->numDataRows();
	}
};

MockWritableStore::MockWritableStore() {
	m_dataSize = 0;
}
MockWritableStore::~MockWritableStore() {
}

void MockWritableStore::save(PathRef fpath) const {
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_rows;
}
void MockWritableStore::load(PathRef fpath) {
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_rows;
}

llong MockWritableStore::dataStorageSize() const {
	return m_rows.used_mem_size() + m_dataSize;
}

llong MockWritableStore::dataInflateSize() const {
	return m_dataSize;
}

llong MockWritableStore::numDataRows() const {
	return m_rows.size();
}

void MockWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	SpinRwLock lock(m_rwMutex, false);
	val->append(m_rows[id]);
}

StoreIterator* MockWritableStore::createStoreIterForward(DbContext*) const {
	return new MockWritableStoreIterForward<MockWritableStore>(this);
}
StoreIterator* MockWritableStore::createStoreIterBackward(DbContext*) const {
	return new MockWritableStoreIterBackward<MockWritableStore>(this);
}

llong MockWritableStore::append(fstring row, DbContext*) {
	SpinRwLock lock(m_rwMutex, true);
	llong id = m_rows.size();
	m_rows.push_back();
	m_rows.back().assign(row);
	m_dataSize += row.size();
	return id;
}

void MockWritableStore::update(llong id, fstring row, DbContext* ctx) {
	assert(id >= 0);
	assert(id <= llong(m_rows.size()));
	if (llong(m_rows.size()) == id) {
		append(row, ctx);
		return;
	}
	SpinRwLock lock(m_rwMutex, true);
	size_t oldsize = m_rows[id].size();
	m_rows[id].assign(row);
	m_dataSize -= oldsize;
	m_dataSize += row.size();
}

void MockWritableStore::remove(llong id, DbContext*) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	SpinRwLock lock(m_rwMutex, true);
	if (m_rows.size()-1 == size_t(id)) {
		m_rows.pop_back();
	}
	else {
		m_dataSize -= m_rows[id].size();
		m_rows[id].clear();
	}
}

void MockWritableStore::shrinkToFit() {
	m_rows.shrink_to_fit();
}

AppendableStore* MockWritableStore::getAppendableStore() { return this; }
UpdatableStore* MockWritableStore::getUpdatableStore() { return this; }
WritableStore* MockWritableStore::getWritableStore() { return this; }

//////////////////////////////////////////////////////////////////

namespace {
	static void copyKey(const std::string& src, valvec<byte>* dst) {
		dst->assign((const char*)src.data(), src.size());
	}
	template<class PrimitiveKey>
	static void copyKey(const PrimitiveKey& src, valvec<byte>* dst) {
		BOOST_STATIC_ASSERT(boost::is_pod<PrimitiveKey>::value);
		dst->assign((const char*)&src, sizeof(PrimitiveKey));
	}

	template<class Primitive>
	static Primitive makeKeyImp(fstring key, Primitive*) {
		assert(key.size() == sizeof(Primitive));
		return unaligned_load<Primitive>(key.udata());
	}
	static std::string makeKeyImp(fstring key, std::string*) { return key.str(); }
	template<class Key>
	static Key makeKey(fstring key) { return makeKeyImp(key, (Key*)0); }
	template<class Primitive>
	static size_t keyHeapLen(const Primitive&) { return 0; }
	static size_t keyHeapLen(const std::string& x) { return x.size() + 1; }
}

template<class Key>
class MockWritableIndex<Key>::MyIndexIterForward : public IndexIterator {
	typedef boost::intrusive_ptr<MockWritableIndex> MockWritableIndexPtr;
	MockWritableIndexPtr m_index;
	typename std::set<kv_t>::const_iterator m_iter;
public:
	MyIndexIterForward(const MockWritableIndex* owner) {
		m_isUniqueInSchema = owner->isUnique();
		m_index.reset(const_cast<MockWritableIndex*>(owner));
		m_iter = owner->m_kv.begin();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		SpinRwLock lock(owner->m_rwMutex, false);
		if (terark_likely(owner->m_kv.end() != m_iter)) {
			*id = m_iter->second;
			copyKey(m_iter->first, key);
			++m_iter;
			return true;
		}
		return false;
	}
	void reset() override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		m_iter = owner->m_kv.begin();
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		auto kv = std::make_pair(makeKey<Key>(key), 0LL);
		SpinRwLock lock(owner->m_rwMutex, false);
		auto iter = owner->m_kv.lower_bound(kv);
		m_iter = iter;
		if (owner->m_kv.end() != iter) {
			++m_iter;
			*id = iter->second;
			copyKey(iter->first, retKey);
			if (iter->first == kv.first)
				return 0;
			else
				return 1;
		}
		return -1;
	}
};
template<class Key>
class MockWritableIndex<Key>::MyIndexIterBackward : public IndexIterator {
	typedef boost::intrusive_ptr<MockWritableIndex> MockWritableIndexPtr;
	MockWritableIndexPtr m_index;
	typename std::set<kv_t>::const_iterator m_iter;
public:
	MyIndexIterBackward(const MockWritableIndex* owner) {
		m_isUniqueInSchema = owner->isUnique();
		m_index.reset(const_cast<MockWritableIndex*>(owner));
		m_iter = owner->m_kv.end();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		SpinRwLock lock(owner->m_rwMutex, false);
		if (terark_likely(owner->m_kv.begin() != m_iter)) {
			--m_iter;
			*id = m_iter->second;
			copyKey(m_iter->first, key);
			return true;
		}
		return false;
	}
	void reset() {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		m_iter = owner->m_kv.end();
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		auto kv = std::make_pair(makeKey<Key>(key), 0LL);
		SpinRwLock lock(owner->m_rwMutex, false);
		auto iter = owner->m_kv.upper_bound(kv);
		if (owner->m_kv.begin() != iter) {
			m_iter = --iter;
			*id = iter->second;
			copyKey(iter->first, retKey);
			if (iter->first == kv.first)
				return 0;
			else
				return 1;
		}
		else {
			m_iter = iter;
			return -1;
		}
	}
};

template<class Key>
MockWritableIndex<Key>::MockWritableIndex(bool isUnique) {
	this->m_isUnique = isUnique;
	m_keysLen = 0;
}

template<class Key>
IndexIterator* MockWritableIndex<Key>::createIndexIterForward(DbContext*) const {
	return new MyIndexIterForward(this);
}

template<class Key>
IndexIterator* MockWritableIndex<Key>::createIndexIterBackward(DbContext*) const {
	return new MyIndexIterBackward(this);
}

template<class Key>
void MockWritableIndex<Key>::save(PathRef fpath) const {
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_kv;
}
template<class Key>
void MockWritableIndex<Key>::load(PathRef fpath) {
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_kv;
}

template<class Key>
llong MockWritableIndex<Key>::indexStorageSize() const {
	// std::set's rbtree node needs 4ptr space
	size_t size = m_kv.size() * (sizeof(kv_t) + 4 * sizeof(void*));
	return m_keysLen + size;
}

template<class Key>
bool MockWritableIndex<Key>::insert(fstring key, llong id, DbContext*) {
	Key k = makeKey<Key>(key);
	SpinRwLock lock(m_rwMutex, true);
	if (this->m_isUnique) {
		auto iter = m_kv.lower_bound(std::make_pair(k, 0));
		if (m_kv.end() != iter && iter->first == k && id != iter->second)
			return false;
	}
	auto ib = m_kv.insert(std::make_pair(k, id));
	if (ib.second) {
		m_keysLen += keyHeapLen(ib.first->first);
	}
	return true;
}

template<class Key>
bool MockWritableIndex<Key>::replace(fstring key, llong oldId, llong newId, DbContext*) {
	auto kx = makeKey<Key>(key);
	SpinRwLock lock(m_rwMutex, true);
	if (oldId != newId) {
		m_kv.erase(std::make_pair(kx, oldId));
	}
	auto ib = m_kv.insert(std::make_pair(kx, newId));
	return ib.second;
}

template<class Key>
void
MockWritableIndex<Key>::searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*)
const {
	auto kx = makeKey<Key>(key);
	SpinRwLock lock(m_rwMutex, false);
	auto iter = m_kv.lower_bound(std::make_pair(kx, 0LL));
	while (m_kv.end() != iter && iter->first == kx) {
		recIdvec->push_back(iter->second);
		++iter;
	}
}

template<class Key>
bool MockWritableIndex<Key>::remove(fstring key, llong id, DbContext*) {
	SpinRwLock lock(m_rwMutex, true);
	auto iter = m_kv.find(std::make_pair(makeKey<Key>(key), id));
	if (m_kv.end() != iter) {
		m_keysLen -= keyHeapLen(iter->first);
		m_kv.erase(iter);
		return 1;
	}
	return 0;
}

template<class Key>
void MockWritableIndex<Key>::clear() {
	SpinRwLock lock(m_rwMutex, true);
	m_keysLen = 0;
	m_kv.clear();
}

///////////////////////////////////////////////////////////////////////
MockReadonlySegment::MockReadonlySegment() {
}
MockReadonlySegment::~MockReadonlySegment() {
}

ReadableIndex*
MockReadonlySegment::openIndex(const Schema& schema, PathRef path) const {
	std::unique_ptr<ReadableIndex> store(ReadonlySegment::openIndex(schema, path));
	if (!store) {
		store.reset(new MockReadonlyIndex(schema));
		store->load(path);
	}
	return store.release();
}

ReadableIndex*
MockReadonlySegment::buildIndex(const Schema& schema, SortableStrVec& indexData)
const {
	std::unique_ptr<MockReadonlyIndex> index(new MockReadonlyIndex(schema));
	index->build(indexData);
	return index.release();
}

ReadableStore*
MockReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	std::unique_ptr<ReadableStore> store(ReadonlySegment::buildStore(schema, storeData));
	if (!store) {
		std::unique_ptr<MockReadonlyStore> mockStore(new MockReadonlyStore(schema));
		mockStore->build(schema, storeData);
		store = std::move(mockStore);
	}
	return store.release();
}
ReadableStore*
MockReadonlySegment::
buildDictZipStore(const Schema& schema, PathRef segDir, StoreIterator& iter,
				  const bm_uint_t* isDel, const febitvec* isPurged) const {
	valvec<byte> rec;
	std::unique_ptr<MockReadonlyStore> store(new MockReadonlyStore(schema));
	if (NULL == isPurged || isPurged->size() == 0) {
		llong recId;
		while (iter.increment(&recId, &rec)) {
			if (NULL == isDel || !terark_bit_test(isDel, recId)) {
				store->m_rows.push_back(rec);
			}
		}
	}
	else {
		assert(NULL != isDel);
		llong  physicId = 0;
		size_t logicNum = isPurged->size();
		const bm_uint_t* isPurgedptr = isPurged->bldata();
		for (size_t logicId = 0; logicId < logicNum; ++logicId) {
			if (!terark_bit_test(isPurgedptr, logicId)) {
				if (!terark_bit_test(isDel, logicId)) {
					bool hasData = iter.seekExact(physicId, &rec);
					TERARK_RT_assert(hasData, std::logic_error);
					store->m_rows.push_back(rec);
				}
				physicId++;
			}
		}
	}
	fs::path fpath = segDir / ("colgroup-" + schema.m_name);
	store->save(fpath);
	return store.release();
}

///////////////////////////////////////////////////////////////////////////
MockWritableSegment::MockWritableSegment(PathRef dir) {
	m_segDir = dir;
	m_wrtStore = new MockWritableStore();
	m_hasLockFreePointSearch = true;
}
MockWritableSegment::~MockWritableSegment() {
	if (!m_tobeDel && !m_isDel.empty())
		this->save(m_segDir);
	m_wrtStore.reset();
}

class MutexLockTransaction : public DbTransaction {
	const SchemaConfig& m_sconf;
	MockWritableSegment*m_seg;
	DbContextPtr        m_ctx;
public:
	explicit
	MutexLockTransaction(MockWritableSegment* seg) : m_sconf(*seg->m_schema) {
		m_seg = seg;
	//	m_ctx = new DbContext();
	}
	~MutexLockTransaction() {
	}
	void indexSearch(size_t indexId, fstring key, valvec<llong>* recIdvec)
	override {
		assert(started == m_status);
		auto index = m_seg->m_indices[indexId].get();
		index->searchExact(key, recIdvec, m_ctx.get());
	}
	void indexRemove(size_t indexId, fstring key, llong recId) override {
		assert(started == m_status);
		auto index = m_seg->m_indices[indexId].get();
		auto wrIndex = index->getWritableIndex();
		wrIndex->remove(key, recId, m_ctx.get());
	}
	bool indexInsert(size_t indexId, fstring key, llong recId) override {
		assert(started == m_status);
		auto index = m_seg->m_indices[indexId].get();
		auto wrIndex = index->getWritableIndex();
		return wrIndex->insert(key, recId, m_ctx.get());
	}
	void indexUpsert(size_t indexId, fstring key, llong recId) override {
		assert(started == m_status);
		auto index = m_seg->m_indices[indexId].get();
		auto wrIndex = index->getWritableIndex();
		wrIndex->insert(key, recId, m_ctx.get());
	}
	void storeRemove(llong recId) override {
		assert(started == m_status);
		auto wrtStore = m_seg->m_wrtStore->getWritableStore();
		wrtStore->remove(recId, m_ctx.get());
	}
	void storeUpsert(llong recId, fstring row) override {
		assert(started == m_status);
		auto wrtStore = m_seg->m_wrtStore->getWritableStore();
		if (m_sconf.m_updatableColgroups.empty()) {
			wrtStore->update(recId, row, m_ctx.get());
		}
		else {
			auto& sconf = m_sconf;
			auto seg = m_seg;
			sconf.m_rowSchema->parseRow(row, &m_cols1);
			SpinRwLock lock(m_seg->m_segMutex);
			for (size_t colgroupId : sconf.m_updatableColgroups) {
				auto store = seg->m_colgroups[colgroupId]->getUpdatableStore();
				assert(nullptr != store);
				const Schema& schema = sconf.getColgroupSchema(colgroupId);
				schema.selectParent(m_cols1, &m_wrtBuf);
				store->update(recId, m_wrtBuf, NULL);
			}
			sconf.m_wrtSchema->selectParent(m_cols1, &m_wrtBuf);
			wrtStore->update(recId, m_wrtBuf, m_ctx.get());
		}
	}
	void storeGetRow(llong recId, valvec<byte>* row) override {
		assert(started == m_status);
		auto seg = m_seg;		
		if (m_sconf.m_updatableColgroups.empty()) {
			seg->m_wrtStore->getValue(recId, row, m_ctx.get());
		}
		else {
			row->erase_all();
			m_cols1.erase_all();
			seg->m_wrtStore->getValue(recId, &m_wrtBuf, m_ctx.get());
			const size_t ProtectCnt = 100;
			if (seg->m_isFreezed || seg->m_isDel.unused() > ProtectCnt) {
				seg->getCombineAppend(recId, row, m_wrtBuf, m_cols1, m_cols2);
			}
			else {
				SpinRwLock  lock(seg->m_segMutex, false);
				seg->getCombineAppend(recId, row, m_wrtBuf, m_cols1, m_cols2);
			}
		}
	}
	void do_startTransaction() override {
		m_seg->m_mockTxnMutex.lock();
	}
	bool do_commit() override {
		m_seg->m_mockTxnMutex.unlock();
		return true;
	}
	void do_rollback() override {
		m_seg->m_mockTxnMutex.unlock();
	}
	const std::string& strError() const override { return m_strError; }

	valvec<byte> m_wrtBuf;
	ColumnVec    m_cols1;
	ColumnVec    m_cols2;
	std::string  m_strError;
};

DbTransaction* MockWritableSegment::createTransaction() {
	auto txn = new MutexLockTransaction(this);
	return txn;
}

ReadableIndex*
MockWritableSegment::openIndex(const Schema& schema, PathRef path) const {
	std::unique_ptr<ReadableIndex> index(createIndex(schema, path));
	index->load(path);
	return index.release();
}

ReadableIndex*
MockWritableSegment::createIndex(const Schema& schema, PathRef) const {
	if (schema.columnNum() == 1) {
		ColumnMeta cm = schema.getColumnMeta(0);
#define CASE_COL_TYPE(Enum, Type) \
		case ColumnType::Enum: return new MockWritableIndex<Type>(schema.m_isUnique);
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		switch (cm.type) {
			default: break;
			CASE_COL_TYPE(Uint08, uint8_t);
			CASE_COL_TYPE(Sint08,  int8_t);
			CASE_COL_TYPE(Uint16, uint16_t);
			CASE_COL_TYPE(Sint16,  int16_t);
			CASE_COL_TYPE(Uint32, uint32_t);
			CASE_COL_TYPE(Sint32,  int32_t);
			CASE_COL_TYPE(Uint64, uint64_t);
			CASE_COL_TYPE(Sint64,  int64_t);
			CASE_COL_TYPE(Float32, float);
			CASE_COL_TYPE(Float64, double);
		}
#undef CASE_COL_TYPE
	}
	return new MockWritableIndex<std::string>(schema.m_isUnique);
}

///////////////////////////////////////////////////////////////////////////

MockDbContext::MockDbContext(const DbTable* tab) : DbContext(tab) {
}
MockDbContext::~MockDbContext() {
}

DbContext* MockDbTable::createDbContextNoLock() const {
	return new MockDbContext(this);
}

ReadonlySegment*
MockDbTable::createReadonlySegment(PathRef dir) const {
	std::unique_ptr<MockReadonlySegment> seg(new MockReadonlySegment());
	return seg.release();
}

WritableSegment*
MockDbTable::createWritableSegment(PathRef dir) const {
	std::unique_ptr<MockWritableSegment> seg(new MockWritableSegment(dir));
	return seg.release();
}

WritableSegment*
MockDbTable::openWritableSegment(PathRef dir) const {
	auto isDelPath = dir / "IsDel";
	if (boost::filesystem::exists(isDelPath)) {
		std::unique_ptr<WritableSegment> seg(new MockWritableSegment(dir));
		seg->m_schema = this->m_schema;
		seg->load(dir);
		return seg.release();
	}
	else {
		return myCreateWritableSegment(dir);
	}
}

TERARK_DB_REGISTER_TABLE_CLASS(MockDbTable);

} } // namespace terark::db
