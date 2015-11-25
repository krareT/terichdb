#include "seq_num_index.hpp"

namespace nark {

template<class Int>
class SeqNumIndex<Int>::MyIndexIter : public IndexIterator {
	boost::intrusive_ptr<SeqNumIndex<Int> > m_index;
	Int m_curr;
public:
	MyIndexIter(const SeqNumIndex* owner) {
		m_index.reset(const_cast<SeqNumIndex*>(owner));
	}
	void reset(PermanentablePtr p2) override {
		assert(!p2 || dynamic_cast<SeqNumIndex*>(p2.get()));
		if (p2)
			m_index.reset(dynamic_cast<SeqNumIndex*>(p2.get()));
		m_curr = -1;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		if (nark_unlikely(Int(-1) == m_curr)) {
			m_curr = 1;
			getIndexKey(id, key, owner, 0);
			return true;
		}
		if (nark_likely(m_curr < owner->m_cnt)) {
			getIndexKey(id, key, owner, m_curr++);
			return true;
		}
		return false;
	}
	bool decrement(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		if (nark_unlikely(Int(-1) == m_curr)) {
			m_curr = owner->m_cnt;
			getIndexKey(id, key, owner, owner->m_cnt - 1);
			return true;
		}
		if (nark_likely(m_curr > 0)) {
			getIndexKey(id, key, owner, --m_curr);
			return true;
		}
		return false;
	}
	bool seekExact(fstring key) override {
		assert(key.size() == sizeof(Int));
		if (key.size() != sizeof(Int)) {
			THROW_STD(invalid_argument,
				"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
		}
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		Int keyId = unaligned_load<Int>(key.udata());
		keyId -= owner->m_min;
		if (keyId < 0 || keyId >= owner->m_cnt) {
			return false;
		}
		m_curr = keyId;
		return true;
	}
	bool seekLowerBound(fstring key) override {
		assert(key.size() == sizeof(Int));
		if (key.size() != sizeof(Int)) {
			THROW_STD(invalid_argument,
				"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
		}
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		Int keyId = unaligned_load<Int>(key.udata());
		keyId -= owner->m_min;
		m_curr = keyId;
		if (keyId < 0 || keyId >= owner->m_cnt) {
			return false;
		}
		return true;
	}
private:
	void getIndexKey(llong* id, valvec<byte>* key, const SeqNumIndex* owner, Int curr) const {
		Int keyId = owner->m_min + curr;
		*id = llong(curr);
		key->erase_all();
		key->assign((const byte*)&keyId, sizeof(Int));
	}
};

template<class Int>
SeqNumIndex<Int>::SeqNumIndex(Int min, Int cnt) {
	m_min = min;
	m_cnt = cnt;
}

template<class Int>
SeqNumIndex<Int>::~SeqNumIndex() { }

template<class Int>
IndexIteratorPtr
SeqNumIndex<Int>::createIndexIter() const { return nullptr; }

template<class Int>
BaseContextPtr
SeqNumIndex<Int>::createIndexContext() const { return nullptr; }

template<class Int>
llong SeqNumIndex<Int>::numIndexRows() const { return m_cnt; }

template<class Int>
llong SeqNumIndex<Int>::indexStorageSize() const { return 2 * sizeof(llong); }

template<class Int>
size_t SeqNumIndex<Int>::remove(fstring key, llong id, BaseContextPtr&) {
	// do nothing
	return 0;
}

template<class Int>
size_t SeqNumIndex<Int>::insert(fstring key, llong id, BaseContextPtr&) {
	assert(key.size() == sizeof(Int));
	assert(id >= 0);
	if (key.size() != sizeof(Int)) {
		THROW_STD(invalid_argument,
			"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
	}
	Int keyId = unaligned_load<Int>(key.udata());
	if (keyId != m_min + id) {
		THROW_STD(invalid_argument,
			"key must be consistent with id in SeqNumIndex");
	}
	if (llong(m_cnt) < id + 1) {
		llong(m_cnt) = id + 1;
	}
	return 1;
}
template<class Int>
size_t SeqNumIndex<Int>::replace(fstring key, llong id, llong newId, BaseContextPtr&) {
	assert(key.size() == sizeof(Int));
	assert(id >= 0);
	assert(id == newId);
	if (key.size() != sizeof(Int)) {
		THROW_STD(invalid_argument,
			"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
	}
	if (id != newId) {
		THROW_STD(invalid_argument,
			"replace with different id is not supported by SeqNumIndex");
	}
	Int keyId = unaligned_load<Int>(key.udata());
	if (keyId != m_min + newId) {
		THROW_STD(invalid_argument,
			"key must be consistent with id in SeqNumIndex");
	}
	return 1;
}

template<class Int>
void SeqNumIndex<Int>::flush() {}

template<class Int>
llong SeqNumIndex<Int>::dataStorageSize() const { return 2 * sizeof(llong); }
template<class Int>
llong SeqNumIndex<Int>::numDataRows() const { return m_cnt; }

template<class Int>
void SeqNumIndex<Int>::getValueAppend(llong id, valvec<byte>* val, BaseContextPtr&) const {
	Int keyAsVal = Int(this->m_min + id);
	val->append((byte*)&keyAsVal, sizeof(Int));
}

template<class Int>
StoreIteratorPtr SeqNumIndex<Int>::createStoreIter() const {
	return nullptr;
}
template<class Int>
BaseContextPtr SeqNumIndex<Int>::createStoreContext() const {
	return nullptr;
}

template class SeqNumIndex<uint32_t>;
template class SeqNumIndex<uint64_t>;

template class SeqNumIndex<int32_t>;
template class SeqNumIndex<int64_t>;

} // namespace nark
