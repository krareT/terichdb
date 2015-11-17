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
ReadableStore::StoreIteratorPtr MockReadonlyStore::createStoreIter() const {
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

class MockReadonlyIndexIterator : public IndexIterator {
	friend class MockReadonlyIndex;
	size_t m_pos = size_t(-1);
public:
	bool increment() override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index);
		if (size_t(-1) == m_pos) {
			m_pos = 0;
		}
		if (m_pos < owner->m_keyVec.size()) {
			m_pos++;
			return true;
		}
		return false;
	}
	bool decrement() override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index);
		if (size_t(-1) == m_pos) {
			m_pos = owner->m_keyVec.size() - 1;
			return true;
		}
		if (m_pos > 0) {
			m_pos--;
			return true;
		}
		return false;
	}
	void reset() {
		m_pos = size_t(-1);
	}
	bool seekExact(fstring key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index);
		const auto& keys = owner->m_keyVec;
		size_t lo = nark::lower_bound_0<const SortableStrVec&>(keys, keys.size(), key);
		if (lo < keys.size() && key == keys[lo]) {
			m_pos = lo;
			return true;
		}
		return false;
	}
	bool seekLowerBound(fstring key) override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index);
		const auto& keys = owner->m_keyVec;
		m_pos = nark::lower_bound_0<const SortableStrVec&>(keys, keys.size(), key);
		return m_pos < keys.size() && key == keys[m_pos];
	}
	void getIndexKey(llong* id, valvec<byte>* key) const override {
		auto owner = static_cast<const MockReadonlyIndex*>(m_index);
		assert(m_pos < owner->m_keyVec.size());
		*id = owner->m_keyVec.m_index[m_pos].seq_id;
		fstring k = owner->m_keyVec[m_pos];
		key->assign(k.udata(), k.size());
	}
};

MockReadonlyIndex::MockReadonlyIndex() {
}

MockReadonlyIndex::~MockReadonlyIndex() {
}

ReadableStore::StoreIteratorPtr MockReadonlyIndex::createStoreIter() const {
	assert(!"Readonly column store did not define iterator");
	return nullptr;
}

void MockReadonlyIndex::save(fstring path1) const {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "wb");
	fp.disbuf();
	NativeDataOutput<OutputBuffer> dio; dio.attach(&fp);
	dio << m_keyVec;
	dio.ensureWrite(m_idToKey.data(), m_idToKey.used_mem_size());
}
void MockReadonlyIndex::load(fstring path1) {
	fs::path fpath = path1.c_str();
	FileStream fp(fpath.string().c_str(), "rb");
	fp.disbuf();
	NativeDataInput<InputBuffer> dio; dio.attach(&fp);
	dio >> m_keyVec;
	m_idToKey.resize_no_init(m_keyVec.size());
	dio.ensureRead(m_idToKey.data(), m_idToKey.used_mem_size());
}

llong MockReadonlyIndex::numDataRows() const {
	return m_idToKey.size();
}
llong MockReadonlyIndex::dataStorageSize() const {
	return m_idToKey.used_mem_size();
}

void MockReadonlyIndex::getValue(llong id, valvec<byte>* key, BaseContextPtr&) const {
	assert(m_idToKey.size() == m_keyVec.size());
	assert(id < (llong)m_idToKey.size());
	assert(id >= 0);
	size_t idx = m_idToKey[id];
	fstring key1 = m_keyVec[idx];
	key->assign(key1.udata(), key1.size());
}

IndexIteratorPtr MockReadonlyIndex::createIndexIter() const {
	return new MockReadonlyIndexIterator();
}

llong MockReadonlyIndex::numIndexRows() const {
	return m_keyVec.size();
}

llong MockReadonlyIndex::indexStorageSize() const {
	return m_keyVec.mem_size();
}

//////////////////////////////////////////////////////////////////
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

class MockWritableStoreIter : public ReadableStore::StoreIterator {
	ptrdiff_t m_id;
public:
	MockWritableStoreIter(const MockWritableStore* store) {
		m_store.reset(const_cast<MockWritableStore*>(store));
		m_id = -1;
	}
	bool increment() override {
		auto store = static_cast<MockWritableStore*>(m_store.get());
		m_id++;
		return m_id < ptrdiff_t(store->m_rows.size());
	}
	void getKeyVal(llong* idKey, valvec<byte>* val) const override {
		auto store = static_cast<MockWritableStore*>(m_store.get());
		assert(m_id < ptrdiff_t(store->m_rows.size()));
		*idKey = m_id;
		*val = store->m_rows[m_id];
	}
};

ReadableStore::StoreIteratorPtr MockWritableStore::createStoreIter() const {
	return new MockWritableStoreIter(this);
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

IndexIteratorPtr MockWritableIndex::createIndexIter() const {
	return new MockReadonlyIndexIterator();
}

llong MockWritableIndex::numIndexRows() const {
	return m_kv.size();
}

llong MockWritableIndex::indexStorageSize() const {
	// std::set's rbtree node needs 4ptr space
	return m_kv.size() * (sizeof(kv_t) + 4 * sizeof(void*));
}

size_t MockWritableIndex::insert(fstring key, llong id, BaseContextPtr&) {
	auto ib = m_kv.insert(std::make_pair(key.str(), id));
	return ib.second;
}
size_t MockWritableIndex::replace(fstring key, llong oldId, llong newId, BaseContextPtr&) {
	if (oldId != newId) {
		m_kv.erase(std::make_pair(key.str(), oldId));
	}
	auto ib = m_kv.insert(std::make_pair(key.str(), newId));
	return ib.second;
}
size_t MockWritableIndex::remove(fstring key, BaseContextPtr&) {
	std::string key1 = key.str();
	auto iter = m_kv.lower_bound(std::make_pair(key1, 0));
	size_t cnt = 0;
	while (iter != m_kv.end() && iter->first == key1) {
		auto next = iter; ++next;
		m_kv.erase(iter);
		iter = next;
		cnt++;
	}
	return cnt;
}
size_t MockWritableIndex::remove(fstring key, llong id, BaseContextPtr&) {
	return m_kv.erase(std::make_pair(key.str(), id));
}
void MockWritableIndex::flush() {
	// do nothing
}

ReadonlySegmentPtr
MockCompositeTable::createReadonlySegment() const {
	return nullptr;
}
WritableSegmentPtr
MockCompositeTable::createWritableSegment(fstring dirBaseName) const {
	return nullptr;
}

WritableSegmentPtr
MockCompositeTable::openWritableSegment(fstring dirBaseName) const {
	return nullptr;
}

} // namespace nark
