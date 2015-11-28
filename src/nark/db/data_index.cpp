#include "data_index.hpp"

namespace nark { namespace db {

ReadableIndex::ReadableIndex()
  : m_isOrdered(false)
  , m_isUnique(true)
{
}
ReadableIndex::~ReadableIndex() {
}

IndexIterator::~IndexIterator() {
}

#if 0
//
class CompositeIterator : public IndexIterator {
	friend class CompositeIndex;
public:
	std::mutex m_mutex;
	valvec<IndexIterator*> m_readonly; // IntegerIterator
	valvec<byte> m_iterKey; // for sync with iterator postion
	size_t m_idx;
	bool   m_isReset;

	CompositeIterator(const CompositeIndex* owner) {
		m_readonly.reserve(owner->m_readonly.size());
		for (auto index : owner->m_readonly) {
			m_readonly.push_back(index->createIndexIter());
		}
		m_index = owner;
		m_idx = 0;
		m_isReset = true;
	}

	void reset() override;
	bool increment() override;
	bool decrement() override;

	bool seekLowerBound(fstring key) override;
	bool seekExact(fstring key) override;
	void getIndexKey(llong* id, valvec<byte>* key) const override;
};

void CompositeIterator::reset() {
	for (auto iter : m_readonly) {
		iter->reset();
	}
	m_isReset = true;
}

bool CompositeIterator::increment() {
	auto owner = dynamic_cast<const CompositeIndex*>(m_index);
	std::lock_guard<std::mutex> lock(owner->m_mutex);
	assert(!m_readonly.empty());
	if (m_idx >= m_readonly.size())
		THROW_STD(invalid_argument
			, "Invalid state: m_idx=%ld > m_readonly.size=%ld"
			, long(m_idx), long(m_readonly.size()));

	if (m_isReset) {
		m_isReset = false;
		m_idx = 0;
	//	m_readonly[m_idx]->reset();
	}
	bool ret = m_readonly[m_idx]->increment();
	if (!ret && m_idx < m_readonly.size() - 1) {
		m_idx++;
		ret = m_readonly[m_idx]->increment();
	}
	return ret;
}

bool CompositeIterator::decrement() {
	auto owner = dynamic_cast<const CompositeIndex*>(m_index);
	std::lock_guard<std::mutex> lock(owner->m_mutex);
	assert(!m_readonly.empty());
	if (m_idx > m_readonly.size())
		THROW_STD(invalid_argument
			, "Invalid state: m_idx=%ld > m_readonly.size=%ld"
			, long(m_idx), long(m_readonly.size()));
	if (m_isReset) {
		m_isReset = false;
		m_idx = m_readonly.size() - 1;
	//	m_readonly[m_idx]->reset();
	}
	bool ret = m_readonly[m_idx]->decrement();
	if (!ret && m_idx > 0) {
		m_idx--;
		ret = m_readonly[m_idx]->decrement();
	}
	return ret;
}

bool CompositeIterator::seekLowerBound(fstring key) {
	// if return false, iter is an upper bound
	return seekExact(key); // now don't support ordered lower bound
}

bool CompositeIterator::seekExact(fstring key ) {
	auto owner = static_cast<const CompositeIndex*>(m_index);
	std::lock_guard<std::mutex> lock(owner->m_mutex);
	for (size_t i = m_readonly.size(); i > 0; ) {
		--i;
		if (m_readonly[i]->seekExact(key)) {
			m_idx = i;
			goto Found;
		}
	}
	m_isReset = true;
	return false;
Found:
	m_isReset = false;
/*
	for (size_t i = 0; i < m_idx; ++i) {
		m_readonly[i]->reset();
	}
	for (size_t i = m_idx + 1; i < m_readonly.size(); ++i) {
		m_readonly[i]->reset();
	}
*/
	return true;
}

void CompositeIterator::getIndexKey(llong* id, valvec<byte>* key) const {
	auto owner = dynamic_cast<const CompositeIndex*>(m_index);
	std::lock_guard<std::mutex> lock(owner->m_mutex);
	size_t idx = m_idx;
	if (idx >= m_readonly.size())
		THROW_STD(invalid_argument
			, "Invalid state: m_idx=%ld > m_readonly.size=%ld"
			, long(idx), long(m_readonly.size()));

	m_readonly[idx]->getIndexKey(id, key);
}

///////////////////////////////////////////////////////////////////////////////
///

CompositeIndex::
CompositeIndex(fstring dir, const valvec<llong>* rowNumVec, const febitvec* isDel) {
	m_myDir = dir.str();
	m_rowNumVec = rowNumVec;
	m_isDeleted = isDel;
	m_writable = nullptr;
}

/// @returns number of removed rows, maybe more than 1
size_t CompositeIndex::remove(fstring key) {
	std::lock_guard<std::mutex> lock(m_mutex);
	return m_writable->remove(key);
}

/// @returns number of removed rows, should be 1 or 0
size_t CompositeIndex::remove(fstring key, llong id) {
	std::lock_guard<std::mutex> lock(m_mutex);
	return m_writable->remove(key, id);
}

/// CompositeIndex::m_readonly.back() == m_writable
/// returns inserted rows, 0 if existed, 1 if not existed
size_t CompositeIndex::insert(fstring key, llong id) {
	std::lock_guard<std::mutex> lock(m_mutex);
	return m_writable->insert(key, id);
}

size_t CompositeIndex::replace(fstring key, llong oldId, llong newId) {
	std::lock_guard<std::mutex> lock(m_mutex);
	return m_writable->replace(key, oldId, newId);
}

llong CompositeIndex::numIndexRows() const {
	std::lock_guard<std::mutex> lock(m_mutex);
	llong sum = 0;
	for (auto index : m_readonly) sum += index->numIndexRows();
	return sum;
}

IndexIterator* CompositeIndex::createIndexIter() const {
	std::lock_guard<std::mutex> lock(m_mutex);
	CompositeIterator* iter = new CompositeIterator(this);
	return iter;
}

llong CompositeIndex::indexStorageSize() const {
	std::lock_guard<std::mutex> lock(m_mutex);
	llong size = 0;
	for (auto index : m_readonly) size += index->indexStorageSize();
	return size;
}

void CompositeIndex::compact() {
	valvec<ReadableIndexPtr> input;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		for (size_t i = m_readonly.size(); i > 0; ) {
			--i;
			if (dynamic_cast<WritableIndex*>(m_readonly[i].get())) {
				input.push_back(m_readonly[i]);
			}
		}
		m_writable = this->createWritable();
		for (auto iter : m_iterSet) {
			if (iter->m_idx == m_readonly.size()) { // absolute end
				iter->m_idx++; // new absolute end
			}
			iter->m_readonly.push_back(m_writable->createIndexIter());
		}
		m_readonly.emplace_back(m_writable);
	}
	ReadableIndexPtr merged = mergeToReadonly(input);
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		size_t start = m_readonly.size() - input.size() - 1;
		for (auto iter : m_iterSet) {
			for (size_t i = start; i < start + input.size(); ++i) {
				delete iter->m_readonly[i];
				iter->m_readonly[i] = nullptr;
			}
			IndexIterator* subIter = merged->createIndexIter();
			size_t& idx = iter->m_idx;
			if (idx >= start && idx < start + input.size()) {
				idx = start;
				if (!iter->m_iterKey.empty()) {
					if (merged->sortOrder() == SortOrder::UnOrdered)
						subIter->seekExact(iter->m_iterKey);
					else
						subIter->seekLowerBound(iter->m_iterKey);
				}
			}
			iter->m_readonly.erase_i(start+1, input.size()-1);
			iter->m_readonly[start] = subIter;
		}
		m_readonly.erase_i(start+1, input.size()-1);
		m_readonly[start] = merged;
	}
}

#endif

} } // namespace nark::db
