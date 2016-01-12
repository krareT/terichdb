#include "mock_db_engine.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/sortable_strvec.hpp>

namespace nark { namespace db {

namespace fs = boost::filesystem;

NARK_DB_REGISTER_STORE("mock", MockReadonlyStore);

llong MockReadonlyStore::dataStorageSize() const {
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
	assert(0); // should not be called
	return nullptr;
}
StoreIterator* MockReadonlyStore::createStoreIterBackward(DbContext*) const {
	assert(0); // should not be called
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

struct FixedLenKeyCompare {
	bool operator()(size_t x, fstring y) const {
		fstring xs(strpool + fixedLen * x, fixedLen);
		return schema->compareData(xs, y) < 0;
	}
	bool operator()(fstring x, size_t y) const {
		return (*this)(y, x);
	}
	size_t fixedLen;
	const char  * strpool;
	const Schema* schema;
};

struct VarLenKeyCompare {
	bool operator()(size_t x, fstring y) const {
		size_t xoff0 = offsets[x], xoff1 = offsets[x+1];
		fstring xs(strpool + xoff0, xoff1 - xoff0);
		return schema->compareData(xs, y) < 0;
	}
	bool operator()(fstring x, size_t y) const {
		return (*this)(y, x);
	}
	const char    * strpool;
	const uint32_t* offsets;
	const Schema  * schema;
};

class MockReadonlyIndexIterator : public IndexIterator {
	friend class MockReadonlyIndex;
	MockReadonlyIndexPtr m_index;
	size_t m_pos;
public:
	MockReadonlyIndexIterator(const MockReadonlyIndex* owner) {
		m_index.reset(const_cast<MockReadonlyIndex*>(owner));
		m_pos = 0;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		assert(nullptr != id);
		if (nark_likely(m_pos < m_index->m_ids.size())) {
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
		m_index.reset(const_cast<MockReadonlyIndex*>(owner));
		m_pos = owner->m_ids.size();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		assert(nullptr != id);
		if (nark_likely(m_pos > 0)) {
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
	if (fixlen) {
		assert(m_keys.size() == 0);
		assert(key.size() == fixlen);
		FixedLenKeyCompare cmp;
		cmp.fixedLen = fixlen;
		cmp.strpool = m_keys.strpool.data();
		cmp.schema = m_schema;
		size_t lo = nark::lower_bound_0(index, rows, key, cmp);
		*pLower = lo;
		if (lo < rows) {
			size_t jj = m_ids[lo];
			if (key == fstring(cmp.strpool + fixlen*jj, fixlen))
				return 0;
			else
				return 1;
		}
	}
	else {
		VarLenKeyCompare cmp;
		cmp.offsets = m_keys.offsets.data();
		cmp.strpool = m_keys.strpool.data();
		cmp.schema = m_schema;
		size_t lo = nark::lower_bound_0(index, rows, key, cmp);
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

llong MockReadonlyIndex::searchExact(fstring key, DbContext*) const {
	size_t lower;
	if (forwardLowerBound(key, &lower) >= 0) {
		return m_ids[lower];
	}
	return -1; // not found
}

IndexIterator* MockReadonlyIndex::createIndexIterForward(DbContext*) const {
	return new MockReadonlyIndexIterator(this);
}
IndexIterator* MockReadonlyIndex::createIndexIterBackward(DbContext*) const {
	return new MockReadonlyIndexIterator(this);
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
		if (m_id < store->m_rows.size()) {
			*id = m_id;
			*val = store->m_rows[m_id];
			m_id++;
			return true;
		}
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
		if (m_id > 0) {
			*id = --m_id;
			*val = store->m_rows[m_id];
			return true;
		}
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

llong MockWritableStore::numDataRows() const {
	return m_rows.size();
}

void MockWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	val->append(m_rows[id]);
}

StoreIterator* MockWritableStore::createStoreIterForward(DbContext*) const {
	return new MockWritableStoreIterForward<MockWritableStore>(this);
}
StoreIterator* MockWritableStore::createStoreIterBackward(DbContext*) const {
	return new MockWritableStoreIterBackward<MockWritableStore>(this);
}

llong MockWritableStore::append(fstring row, DbContext*) {
	llong id = m_rows.size();
	m_rows.push_back();
	m_rows.back().assign(row);
	return id;
}
void MockWritableStore::replace(llong id, fstring row, DbContext*) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	m_rows[id].assign(row);
}
void MockWritableStore::remove(llong id, DbContext*) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	m_rows[id].clear();
}
void MockWritableStore::clear() {
	m_rows.clear();
}

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
		m_index.reset(const_cast<MockWritableIndex*>(owner));
		m_iter = owner->m_kv.begin();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		if (nark_likely(owner->m_kv.end() != m_iter)) {
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
		m_index.reset(const_cast<MockWritableIndex*>(owner));
		m_iter = owner->m_kv.end();
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		if (nark_likely(owner->m_kv.begin() != m_iter)) {
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
	if (oldId != newId) {
		m_kv.erase(std::make_pair(kx, oldId));
	}
	auto ib = m_kv.insert(std::make_pair(kx, newId));
	return ib.second;
}

template<class Key>
llong MockWritableIndex<Key>::searchExact(fstring key, DbContext*) const {
	auto kx = makeKey<Key>(key);
	auto iter = m_kv.lower_bound(std::make_pair(kx, 0LL));
	if (m_kv.end() != iter && iter->first == kx) {
		return iter->second;
	}
	return -1;
}

template<class Key>
bool MockWritableIndex<Key>::remove(fstring key, llong id, DbContext*) {
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
		std::unique_ptr<MockReadonlyStore> mockStore(new MockReadonlyStore());
		mockStore->build(schema, storeData);
		store = std::move(mockStore);
	}
	return store.release();
}

///////////////////////////////////////////////////////////////////////////
MockWritableSegment::MockWritableSegment(PathRef dir) {
	m_segDir = dir;
	m_dataSize = 0;
}
MockWritableSegment::~MockWritableSegment() {
	if (!m_tobeDel && m_rows.size())
		this->save(m_segDir);
}

void MockWritableSegment::saveRecordStore(PathRef dir) const {
	fs::path fpath = dir / "rows";
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_rows;
}

void MockWritableSegment::loadRecordStore(PathRef dir) {
	fs::path fpath = dir / "rows";
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_rows;
}

ReadableIndex*
MockWritableSegment::openIndex(const Schema& schema, PathRef path) const {
	std::unique_ptr<ReadableIndex> index(createIndex(schema, path));
	index->load(path);
	return index.release();
}

llong MockWritableSegment::dataStorageSize() const {
	return m_rows.used_mem_size() + m_dataSize;
}

void
MockWritableSegment::getValueAppend(llong id, valvec<byte>* val,
									DbContext*)
const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	val->append(m_rows[id]);
}

StoreIterator* MockWritableSegment::createStoreIterForward(DbContext*) const {
	return new MockWritableStoreIterForward<MockWritableSegment>(this);
}
StoreIterator* MockWritableSegment::createStoreIterBackward(DbContext*) const {
	return new MockWritableStoreIterBackward<MockWritableSegment>(this);
}

llong MockWritableSegment::totalStorageSize() const {
	return totalIndexSize() + m_rows.used_mem_size() + m_dataSize;
}

llong MockWritableSegment::append(fstring row, DbContext*) {
	llong id = m_rows.size();
	m_rows.push_back();
	m_rows.back().assign(row);
	m_dataSize += row.size();
	return id;
}

void MockWritableSegment::replace(llong id, fstring row, DbContext*) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	size_t oldsize = m_rows[id].size();
	m_rows[id].assign(row);
	m_dataSize -= oldsize;
	m_dataSize += row.size();
}

void MockWritableSegment::remove(llong id, DbContext*) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	if (m_rows.size()-1 == size_t(id)) {
		m_rows.pop_back();
	}
	else {
		m_dataSize -= m_rows[id].size();
		m_rows[id].clear();
	}
}

void MockWritableSegment::clear() {
	m_rows.clear();
	m_dataSize = 0;
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

MockDbContext::MockDbContext(const CompositeTable* tab) : DbContext(tab) {
}
MockDbContext::~MockDbContext() {
}

DbContext* MockCompositeTable::createDbContext() const {
	return new MockDbContext(this);
}

ReadonlySegment*
MockCompositeTable::createReadonlySegment(PathRef dir) const {
	std::unique_ptr<MockReadonlySegment> seg(new MockReadonlySegment());
	return seg.release();
}

WritableSegment*
MockCompositeTable::createWritableSegment(PathRef dir) const {
	std::unique_ptr<MockWritableSegment> seg(new MockWritableSegment(dir));
	return seg.release();
}

WritableSegment*
MockCompositeTable::openWritableSegment(PathRef dir) const {
	auto isDelPath = dir / "isDel";
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

NARK_DB_REGISTER_TABLE_CLASS(MockCompositeTable);

} } // namespace nark::db
