#include "mock_db_engine.hpp"

namespace nark {


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

ReadonlySegmentPtr MockCompositeTable::createReadonlySegment(fstring dirBaseName) const {
	return nullptr;
}
WritableSegmentPtr MockCompositeTable::createWritableSegment(fstring dirBaseName) const {
	return nullptr;
}

ReadonlySegmentPtr MockCompositeTable::openReadonlySegment(fstring dirBaseName) const {
	return nullptr;
}
WritableSegmentPtr MockCompositeTable::openWritableSegment(fstring dirBaseName) const {
	return nullptr;
}

} // namespace nark
