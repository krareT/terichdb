#include "mock_db_engine.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <boost/filesystem.hpp>

namespace nark {

namespace fs = boost::filesystem;

llong MockReadonlyStore::dataStorageSize() const {
	return m_rows.used_mem_size();
}
llong MockReadonlyStore::numDataRows() const {
	return m_rows.size();
}
void
MockReadonlyStore::getValue(llong id, valvec<byte>* val, BaseContextPtr&)
const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	val->assign(m_rows[id]);
}
StoreIteratorPtr MockReadonlyStore::createStoreIter() const {
	return nullptr;
}
BaseContextPtr MockReadonlyStore::createStoreContext() const {
	return nullptr;
}

void MockReadonlyStore::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_rows;
}
void MockReadonlyStore::load(fstring path1) {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_rows;
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
	size_t m_pos = size_t(-1);
public:
	MockReadonlyIndexIterator(const MockReadonlyIndex* owner) {
		m_index.reset(const_cast<MockReadonlyIndex*>(owner));
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		assert(nullptr != id);
		if (nark_unlikely(size_t(-1) == m_pos)) {
			m_pos = 1;
			getIndexKey(id, key, owner, 0);
			return true;
		}
		if (nark_likely(m_pos < owner->m_ids.size())) {
			getIndexKey(id, key, owner, m_pos++);
			return true;
		}
		return false;
	}
	bool decrement(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		if (nark_unlikely(size_t(-1) == m_pos)) {
			m_pos = owner->m_ids.size() - 1;
			getIndexKey(id, key, owner, m_pos);
			return true;
		}
		if (nark_likely(m_pos > 0)) {
			getIndexKey(id, key, owner, --m_pos);
			return true;
		}
		return false;
	}
	void reset(PermanentablePtr p2) {
		assert(!p2 || dynamic_cast<MockReadonlyIndex*>(p2.get()));
		m_index.reset(dynamic_cast<MockReadonlyIndex*>(p2.get()));
		m_pos = size_t(-1);
	}
	bool seekExact(fstring key) override {
		size_t lo;
		if (seekLowerBound_imp(key, &lo)) {
			m_pos = lo;
			return true;
		}
		return false;
	}
	bool seekLowerBound(fstring key) override {
		return seekLowerBound_imp(key, &m_pos);
	}
	bool seekLowerBound_imp(fstring key, size_t* pLower) {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index.get());
		const uint32_t* index = owner->m_ids.data();
		const size_t rows = owner->m_ids.size();
		const size_t fixlen = owner->m_fixedLen;
		if (fixlen) {
			assert(owner->m_keys.size() == 0);
			FixedLenKeyCompare cmp;
			cmp.fixedLen = fixlen;
			cmp.strpool = owner->m_keys.strpool.data();
			cmp.schema = owner->m_schema.get();
			size_t lo = nark::lower_bound_0(index, rows, key, cmp);
			*pLower = lo;
			if (lo < rows && key == owner->m_keys[lo]) {
				return true;
			}
		}
		else {
			VarLenKeyCompare cmp;
			cmp.offsets = owner->m_keys.offsets.data();
			cmp.strpool = owner->m_keys.strpool.data();
			cmp.schema = owner->m_schema.get();
			size_t lo = nark::lower_bound_0(index, rows, key, cmp);
			*pLower = lo;
			if (lo < rows && key == owner->m_keys[lo]) {
				return true;
			}
		}
		return false;
	}

	void getIndexKey(llong* id, valvec<byte>* key,
					 const MockReadonlyIndex* owner, size_t pos) {
		assert(pos < owner->m_ids.size());
		*id = owner->m_ids[pos];
		if (key) {
			fstring k = owner->m_keys[*id];
			key->assign(k.udata(), k.size());
		}
	}
};

MockReadonlyIndex::MockReadonlyIndex(SchemaPtr schema) {
	m_schema = schema;
}

MockReadonlyIndex::~MockReadonlyIndex() {
}

StoreIteratorPtr MockReadonlyIndex::createStoreIter() const {
	assert(!"Readonly column store did not define iterator");
	return nullptr;
}

BaseContextPtr MockReadonlyIndex::createIndexContext() const {
	return nullptr;
}

BaseContextPtr MockReadonlyIndex::createStoreContext() const {
	return nullptr;
}

#ifdef _MSC_VER
#define qsort_r qsort_s
#endif

void
MockReadonlyIndex::build(SortableStrVec& keys) {
	const Schema* schema = m_schema.get();
	size_t fixlen = schema->getFixedRowLen();
	const byte* base = keys.m_strpool.data();
	if (fixlen) {
		assert(keys.m_index.size() == 0);
		m_ids.resize_no_init(keys.size() / fixlen);
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

void MockReadonlyIndex::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	size_t rows = m_ids.size();
	dio << uint64_t(m_fixedLen);
	dio << uint64_t(rows);
	dio << uint64_t(m_keys.size());
	dio.ensureWrite(m_ids.data(), m_ids.used_mem_size());
	if (m_fixedLen) {
		assert(m_keys.size() == 0);
	} else {
		assert(m_keys.size() == rows);
		dio.ensureWrite(m_keys.offsets.data(), m_keys.offsets.used_mem_size());
	}
	dio.ensureWrite(m_keys.strpool.data(), m_keys.strpool.used_mem_size());
}

void MockReadonlyIndex::load(fstring path1) {
	fs::path fpath = path1.c_str();
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

void MockReadonlyIndex::getValue(llong id, valvec<byte>* key, BaseContextPtr&) const {
	assert(m_ids.size() == m_keys.size());
	assert(id < (llong)m_ids.size());
	assert(id >= 0);
	if (m_fixedLen) {
		fstring key1(m_keys.strpool.data() + m_fixedLen * id, m_fixedLen);
		key->assign(key1.udata(), key1.size());
	}
	else {
		size_t idx = m_ids[id];
		fstring key1 = m_keys[idx];
		key->assign(key1.udata(), key1.size());
	}
}

IndexIteratorPtr MockReadonlyIndex::createIndexIter() const {
	return new MockReadonlyIndexIterator(this);
}

llong MockReadonlyIndex::numIndexRows() const {
	return m_ids.size();
}

llong MockReadonlyIndex::indexStorageSize() const {
	return m_ids.used_mem_size() + m_keys.offsets.used_mem_size();
}

//////////////////////////////////////////////////////////////////
template<class WrStore>
class MockWritableStoreIter : public StoreIterator {
	size_t m_id;
public:
	MockWritableStoreIter(const WrStore* store) {
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
};

void MockWritableStore::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_rows;
}
void MockWritableStore::load(fstring path1) {
	fs::path fpath = path1.c_str();
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

void MockWritableStore::getValue(llong id, valvec<byte>* val, BaseContextPtr&) const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	*val = m_rows[id];
}

StoreIteratorPtr MockWritableStore::createStoreIter() const {
	return new MockWritableStoreIter<MockWritableStore>(this);
}

BaseContextPtr MockWritableStore::createStoreContext() const {
	return nullptr;
}

llong MockWritableStore::append(fstring row, BaseContextPtr&) {
	llong id = m_rows.size();
	m_rows.push_back();
	m_rows.back().assign(row);
	return id;
}
void MockWritableStore::replace(llong id, fstring row, BaseContextPtr&) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	m_rows[id].assign(row);
}
void MockWritableStore::remove(llong id, BaseContextPtr&) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	m_rows[id].clear();
}

//////////////////////////////////////////////////////////////////

template<class Key>
class MockWritableIndex<Key>::MyIndexIter : public IndexIterator {
	typedef boost::intrusive_ptr<MockWritableIndex> MockWritableIndexPtr;
	MockWritableIndexPtr m_index;
	typename std::set<kv_t>::const_iterator m_iter;
	bool m_isNull;
public:
	MyIndexIter(const MockWritableIndex* owner) {
		m_index.reset(const_cast<MockWritableIndex*>(owner));
		m_isNull = true;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		if (nark_unlikely(m_isNull)) {
			m_isNull = false;
			m_iter = owner->m_kv.begin();
			if (!owner->m_kv.empty()) {
				*id = m_iter->second;
				copyKey(m_iter->first, key);
				++m_iter;
				return true;
			}
			return false;
		}
		if (nark_likely(owner->m_kv.end() != m_iter)) {
			*id = m_iter->second;
			copyKey(m_iter->first, key);
			++m_iter;
			return true;
		}
		return false;
	}
	bool decrement(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		if (nark_unlikely(m_isNull)) {
			m_isNull = false;
			m_iter = owner->m_kv.end();
			if (!owner->m_kv.empty()) {
				--m_iter;
				*id = m_iter->second;
				copyKey(m_iter->first, key);
				return true;
			}
			return false;
		}
		if (nark_likely(owner->m_kv.begin() != m_iter)) {
			--m_iter;
			*id = m_iter->second;
			copyKey(m_iter->first, key);
			return true;
		}
		return false;
	}
	void reset(PermanentablePtr p2) {
		assert(!p2 || dynamic_cast<MockWritableIndex*>(p2.get()));
		if (p2)
			m_index.reset(dynamic_cast<MockWritableIndex*>(p2.get()));
		m_isNull = true;
	}
	bool seekExact(fstring key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		auto kv = std::make_pair(makeKey(key), 0LL);
		auto iter = owner->m_kv.lower_bound(kv);
		if (owner->m_kv.end() != iter && iter->first == kv.first) {
			m_iter = iter;
			m_isNull = false;
			return true;
		}
		return false;
	}
	bool seekLowerBound(fstring key) override {
		auto owner = static_cast<const MockWritableIndex*>(m_index.get());
		auto kv = std::make_pair(makeKey(key), 0LL);
		auto iter = owner->m_kv.lower_bound(kv);
		m_iter = iter;
		m_isNull = false;
		if (owner->m_kv.end() != iter && iter->first == kv.first) {
			return true;
		}
		return false;
	}
private:
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
public:
	static Key makeKey(fstring key) { return makeKeyImp(key, (Key*)0); }
	template<class Primitive>
	static size_t keyHeapLen(const Primitive&) { return 0; }
	static size_t keyHeapLen(const std::string& x) { return x.size() + 1; }
};

template<class Key>
IndexIteratorPtr MockWritableIndex<Key>::createIndexIter() const {
	return new MyIndexIter(this);
}
template<class Key>
BaseContextPtr MockWritableIndex<Key>::createIndexContext() const {
	return nullptr;
}

template<class Key>
void MockWritableIndex<Key>::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_kv;
}
template<class Key>
void MockWritableIndex<Key>::load(fstring path1) {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_kv;
}

template<class Key>
llong MockWritableIndex<Key>::numIndexRows() const {
	return m_kv.size();
}

template<class Key>
llong MockWritableIndex<Key>::indexStorageSize() const {
	// std::set's rbtree node needs 4ptr space
	size_t size = m_kv.size() * (sizeof(kv_t) + 4 * sizeof(void*));
	return m_keysLen + size;
}

template<class Key>
size_t MockWritableIndex<Key>::insert(fstring key, llong id, BaseContextPtr&) {
	auto ib = m_kv.insert(std::make_pair(MyIndexIter::makeKey(key), id));
	if (ib.second) {
		m_keysLen += MyIndexIter::keyHeapLen(ib.first->first);
	}
	return ib.second;
}

template<class Key>
size_t MockWritableIndex<Key>::replace(fstring key, llong oldId, llong newId, BaseContextPtr&) {
	auto kx = MyIndexIter::makeKey(key);
	if (oldId != newId) {
		m_kv.erase(std::make_pair(kx, oldId));
	}
	auto ib = m_kv.insert(std::make_pair(kx, newId));
	return ib.second;
}

template<class Key>
size_t MockWritableIndex<Key>::remove(fstring key, llong id, BaseContextPtr&) {
	auto iter = m_kv.find(std::make_pair(MyIndexIter::makeKey(key), id));
	if (m_kv.end() != iter) {
		m_keysLen = MyIndexIter::keyHeapLen(iter->first);
		m_kv.erase(iter);
		return 1;
	}
	return 0;
}

template<class Key>
void MockWritableIndex<Key>::flush() {
	// do nothing
}

///////////////////////////////////////////////////////////////////////
MockReadonlySegment::MockReadonlySegment() {
}
MockReadonlySegment::~MockReadonlySegment() {
}

ReadableStorePtr
MockReadonlySegment::openPart(fstring path) const {
	// Mock just use one kind of data store
//	FileStream fp(path.c_str(), "rb");
//	fp.disbuf();
//	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	ReadableStorePtr store(new MockReadonlyStore());
	store->load(path);
	return store;
}

ReadableIndexStorePtr
MockReadonlySegment::openIndex(fstring path, SchemaPtr schema) const {
	ReadableIndexStorePtr store(new MockReadonlyIndex(schema));
	store->load(path);
	return store;
}

ReadableIndexStorePtr
MockReadonlySegment::buildIndex(SchemaPtr indexSchema,
								SortableStrVec& indexData)
const {
	std::unique_ptr<MockReadonlyIndex> index(new MockReadonlyIndex(indexSchema));
	index->build(indexData);
	return index.release();
}

ReadableStorePtr
MockReadonlySegment::buildStore(SortableStrVec& storeData) const {
	std::unique_ptr<MockReadonlyStore> store(new MockReadonlyStore());
	return store.release();
}

///////////////////////////////////////////////////////////////////////////
MockWritableSegment::MockWritableSegment() {
}
MockWritableSegment::~MockWritableSegment() {
}

void MockWritableSegment::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_rows;
}

void MockWritableSegment::load(fstring path1) {
	fs::path fpath = path1 + "/rows";
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_rows;
}

WritableIndexPtr
MockWritableSegment::openIndex(fstring path, SchemaPtr schema) const {
	WritableIndexPtr index = createWritableIndex(schema);
	index->load(path);
	return index;
}

llong MockWritableSegment::dataStorageSize() const {
	return m_rows.used_mem_size() + m_dataSize;
}

void
MockWritableSegment::getValue(llong id, valvec<byte>* val,
							  BaseContextPtr&)
const {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	*val = m_rows[id];
}

StoreIteratorPtr MockWritableSegment::createStoreIter() const {
	return StoreIteratorPtr(new MockWritableStoreIter<MockWritableSegment>(this));
}

BaseContextPtr MockWritableSegment::createStoreContext() const {
	return nullptr;
}

llong MockWritableSegment::totalStorageSize() const {
	return totalIndexSize() + m_rows.used_mem_size() + m_dataSize;
}

llong MockWritableSegment::append(fstring row, BaseContextPtr &) {
	llong id = m_rows.size();
	m_rows.push_back();
	m_rows.back().assign(row);
	m_dataSize += row.size();
	return id;
}

void MockWritableSegment::replace(llong id, fstring row, BaseContextPtr &) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	size_t oldsize = m_rows[id].size();
	m_rows[id].assign(row);
	m_dataSize -= oldsize;
	m_dataSize += row.size();
}

void MockWritableSegment::remove(llong id, BaseContextPtr &) {
	assert(id >= 0);
	assert(id < llong(m_rows.size()));
	m_dataSize -= m_rows[id].size();
	m_rows[id].clear();
}

void MockWritableSegment::flush() {
	// do nothing
}

WritableIndexPtr MockWritableSegment::createWritableIndex(SchemaPtr schema) const {
	WritableIndexPtr index;
	if (schema->columnNum() == 1) {
		ColumnMeta cm = schema->getColumnMeta(0);
#define CASE_COL_TYPE(Enum, Type) \
		case ColumnType::Enum: return new MockWritableIndex<Type>();
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		switch (cm.type) {
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
	return new MockWritableIndex<std::string>();
}

///////////////////////////////////////////////////////////////////////////

ReadonlySegmentPtr
MockCompositeTable::createReadonlySegment() const {
	return new MockReadonlySegment();
}
WritableSegmentPtr
MockCompositeTable::createWritableSegment(fstring dir) const {
	std::unique_ptr<MockWritableSegment> seg(new MockWritableSegment());
	seg->m_rowSchema = m_rowSchema;
	seg->m_indexSchemaSet = m_indexSchemaSet;
	seg->m_nonIndexRowSchema = m_nonIndexRowSchema;
	seg->m_indices.resize(m_indexSchemaSet->m_nested.end_i());
	for (size_t i = 0; i < seg->m_indices.size(); ++i) {
		SchemaPtr schema = m_indexSchemaSet->m_nested.elem_at(i);
		seg->m_indices[i] = seg->createWritableIndex(schema);
	}
	return seg.release();
}

WritableSegmentPtr
MockCompositeTable::openWritableSegment(fstring dir) const {
	WritableSegmentPtr seg(new MockWritableSegment());
	seg->load(dir);
	return seg;
}

} // namespace nark
