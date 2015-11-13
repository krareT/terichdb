#include "mock_db_engine.hpp"

namespace nark {

class MockReadableIndexIterator : public IndexIterator {
	friend class MockReadableIndex;
	size_t m_pos = size_t(-1);
public:
	bool increment() override {
		auto owner = static_cast<const MockReadableIndex*>(m_index);
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
		auto owner = static_cast<const MockReadableIndex*>(m_index);
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
		auto owner = static_cast<const MockReadableIndex*>(m_index);
		const auto& keys = owner->m_keyVec;
		size_t lo = nark::lower_bound_0<const SortableStrVec&>(keys, keys.size(), key);
		if (lo < keys.size() && key == keys[lo]) {
			m_pos = lo;
			return true;
		}
		return false;
	}
	bool seekLowerBound(fstring key) override {
		auto owner = static_cast<const MockReadableIndex*>(m_index);
		const auto& keys = owner->m_keyVec;
		m_pos = nark::lower_bound_0<const SortableStrVec&>(keys, keys.size(), key);
		return m_pos < keys.size() && key == keys[m_pos];
	}
	void getIndexKey(llong* id, valvec<byte>* key) const override {
		auto owner = static_cast<const MockReadableIndex*>(m_index);
		assert(m_pos < owner->m_keyVec.size());
		*id = owner->m_keyVec.m_index[m_pos].seq_id;
		fstring k = owner->m_keyVec[m_pos];
		key->assign(k.udata(), k.size());
	}
};

MockReadableIndex::MockReadableIndex() {
}

MockReadableIndex::~MockReadableIndex() {
}

ReadableStore::StoreIterator* MockReadableIndex::makeStoreIter() const {
	return nullptr;
}
llong MockReadableIndex::numDataRows() const {
	return m_idToKey.size();
}
llong MockReadableIndex::dataStorageSize() const {
	return m_idToKey.used_mem_size();
}

void MockReadableIndex::getValue(llong id, valvec<byte>* key) const {
	assert(m_idToKey.size() == m_keyVec.size());
	assert(id < (llong)m_idToKey.size());
	assert(id >= 0);
	size_t idx = m_idToKey[id];
	fstring key1 = m_keyVec[idx];
	key->assign(key1.udata(), key1.size());
}

IndexIterator* MockReadableIndex::makeIndexIter() const {
	return new MockReadableIndexIterator();
}

llong MockReadableIndex::numIndexRows() const {
	return m_keyVec.size();
}

llong MockReadableIndex::indexStorageSize() const {
	return m_keyVec.mem_size();
}

//////////////////////////////////////////////////////////////////

IndexIterator* MockWritableIndex::makeIndexIter() const {
	return new MockReadableIndexIterator();
}

llong MockWritableIndex::numIndexRows() const {
	return m_kv.size();
}

llong MockWritableIndex::indexStorageSize() const {
	// std::set's rbtree node needs 4ptr space
	return m_kv.size() * (sizeof(kv_t) + 4 * sizeof(void*));
}

size_t MockWritableIndex::insert(fstring key, llong id) {
	auto ib = m_kv.insert(std::make_pair(key.str(), id));
	return ib.second;
}
size_t MockWritableIndex::replace(fstring key, llong oldId, llong newId) {
	if (oldId != newId) {
		m_kv.erase(std::make_pair(key.str(), oldId));
	}
	auto ib = m_kv.insert(std::make_pair(key.str(), newId));
	return ib.second;
}
size_t MockWritableIndex::remove(fstring key) {
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
size_t MockWritableIndex::remove(fstring key, llong id) {
	return m_kv.erase(std::make_pair(key.str(), id));
}
void MockWritableIndex::flush() {
	// do nothing
}

ReadableIndex*
MockCompositeIndex::mergeToReadonly(const valvec<ReadableIndexPtr>& input)
const {
	auto merged = new MockReadableIndex();
	SortableStrVec& vec = merged->m_keyVec;
	valvec<byte> key;
	llong id;
	for (size_t i = 0; i < input.size(); ++i) {
		std::unique_ptr<IndexIterator> iter(input[i]->makeIndexIter());
		while (iter->increment()) {
			iter->getIndexKey(&id, &key);
			vec.push_back(key);
			vec.m_index.back().seq_id = id;
		}
	}
	vec.compress_strpool(1); // compress level 1
	vec.sort();
	return merged;
}

WritableIndex*
MockCompositeIndex::createWritable() const {
	return new MockWritableIndex();
}

} // namespace nark
