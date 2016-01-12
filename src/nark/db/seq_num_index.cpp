#include "seq_num_index.hpp"

namespace nark { namespace db {

template<class Int>
class SeqNumIndex<Int>::MyIndexIterForward : public IndexIterator {
	boost::intrusive_ptr<SeqNumIndex<Int> > m_index;
	Int m_curr;
public:
	MyIndexIterForward(const SeqNumIndex* owner) {
		m_index.reset(const_cast<SeqNumIndex*>(owner));
		m_curr = 0;
	}
	void reset() override {
		m_curr = 0;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		if (nark_likely(m_curr < owner->m_cnt)) {
			getIndexKey(id, key, owner, m_curr++);
			return true;
		}
		return false;
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		assert(key.size() == sizeof(Int));
		if (key.size() != sizeof(Int)) {
			THROW_STD(invalid_argument,
				"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
		}
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		Int keyId = unaligned_load<Int>(key.udata());
		if (keyId >= owner->m_min + owner->m_cnt) {
			m_curr = owner->m_cnt;
			return -1;
		}
		else if (keyId < owner->m_min) {
			m_curr = 1;
			*id = 0;
			retKey->assign((const byte*)&owner->m_min, sizeof(Int));
			return 1;
		}
		else {
			keyId -= owner->m_min;
			m_curr = keyId + 1;
			*id = keyId;
			retKey->assign(key.udata(), key.size());
			return 0;
		}
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
class SeqNumIndex<Int>::MyIndexIterBackward : public IndexIterator {
	boost::intrusive_ptr<SeqNumIndex<Int> > m_index;
	Int m_curr;
public:
	MyIndexIterBackward(const SeqNumIndex* owner) {
		m_index.reset(const_cast<SeqNumIndex*>(owner));
		m_curr = owner->m_cnt;
	}
	void reset() override {
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		m_curr = owner->m_cnt;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		if (nark_likely(m_curr < owner->m_cnt)) {
			getIndexKey(id, key, owner, --m_curr);
			return true;
		}
		return false;
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		assert(key.size() == sizeof(Int));
		if (key.size() != sizeof(Int)) {
			THROW_STD(invalid_argument,
				"key.size must be sizeof(Int)=%d", int(sizeof(Int)));
		}
		auto owner = static_cast<const SeqNumIndex*>(m_index.get());
		Int keyId = unaligned_load<Int>(key.udata());
		if (keyId <= owner->m_min) {
			m_curr = 0;
			return -1;
		}
		else if (keyId > owner->m_min + owner->m_cnt) {
			m_curr = owner->m_cnt;
			*id = owner->m_cnt - 1;
			Int forwardMax = owner->m_min + owner->m_cnt - 1;
			retKey->assign((const byte*)&forwardMax, sizeof(Int));
			return 1;
		}
		else {
			keyId -= owner->m_min;
			m_curr = keyId;
			*id = keyId;
			retKey->assign(key.udata(), key.size());
			return 0;
		}
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
IndexIterator*
SeqNumIndex<Int>::createIndexIterForward(DbContext*) const { return nullptr; }
template<class Int>
IndexIterator*
SeqNumIndex<Int>::createIndexIterBackward(DbContext*) const { return nullptr; }

template<class Int>
llong SeqNumIndex<Int>::indexStorageSize() const { return 2 * sizeof(llong); }

template<class Int>
bool SeqNumIndex<Int>::remove(fstring key, llong id, DbContext*) {
	// do nothing
	return 0;
}

template<class Int>
bool SeqNumIndex<Int>::insert(fstring key, llong id, DbContext*) {
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
		m_cnt = id + 1;
	}
	return 1;
}
template<class Int>
bool SeqNumIndex<Int>::replace(fstring key, llong id, llong newId, DbContext*) {
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
void SeqNumIndex<Int>::clear() {}

template<class Int>
llong SeqNumIndex<Int>::dataStorageSize() const { return 2 * sizeof(llong); }
template<class Int>
llong SeqNumIndex<Int>::numDataRows() const { return m_cnt; }

template<class Int>
void SeqNumIndex<Int>::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	Int keyAsVal = Int(this->m_min + id);
	val->append((byte*)&keyAsVal, sizeof(Int));
}

template<class Int>
StoreIterator* SeqNumIndex<Int>::createStoreIterForward(DbContext*) const {
	return nullptr;
}
template<class Int>
StoreIterator* SeqNumIndex<Int>::createStoreIterBackward(DbContext*) const {
	return nullptr;
}

template<class Int>
WritableIndex*
SeqNumIndex<Int>::getWritableIndex() { return this; }

template<class Int>
ReadableIndex*
SeqNumIndex<Int>::getReadableIndex() { return this; }

template<class Int>
ReadableStore*
SeqNumIndex<Int>::getReadableStore() { return this; }

template class SeqNumIndex<uint32_t>;
template class SeqNumIndex<uint64_t>;

template class SeqNumIndex<int32_t>;
template class SeqNumIndex<int64_t>;

} } // namespace nark::db
