#include "data_store.hpp"

namespace nark {

void Permanentable::save(fstring) const {
	THROW_STD(invalid_argument, "This method should not be called");
}
void Permanentable::load(fstring) {
	THROW_STD(invalid_argument, "This method should not be called");
}

StoreIterator::~StoreIterator() {
}

WritableStore* ReadableStore::getWritableStore() {
	return nullptr;
}

WritableStore::~WritableStore() {
}

///////////////////////////////////////////////////////////////////////////////
/*
llong CompositeStore::numDataRows() const {
	return m_rowNumVec.back() + m_writable->numDataRows();
}

llong CompositeStore::insert(fstring row) {
	llong writableStartID = m_rowNumVec.back();
	llong id = m_writable->insert(row);
	return writableStartID + id;
}

llong CompositeStore::replace(llong id, fstring row) {
	llong writableStartID = m_rowNumVec.back();
	llong id2;
	if (id >= writableStartID) {
		id2 = m_writable->replace(id - writableStartID, row);
	}
	else {
		m_isDeleted.set1(id);
		id2 = m_writable->insert(row);
	}
	return id2;
}

void CompositeStore::remove(llong id) {
	llong writableStartID = m_rowNumVec.back();
	if (id >= writableStartID) {
		m_writable->remove(id - writableStartID);
	}
	else {
		m_isDeleted.set1(id);
	}
}

void CompositeStore::compact() {
	valvec<ReadableStorePtr> input;
	for (size_t i = m_readonly.size(); i > 0; ) {
		--i;
		if (dynamic_cast<WritableStore*>(m_readonly[i].get())) {
			input.push_back(m_readonly[i]);
		}
	}
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_writable = this->createWritable();
		m_readonly.emplace_back(m_writable);
	}
	ReadableStorePtr merged = mergeToReadonly(input);
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		size_t start = m_readonly.size() - input.size() - 1;
		m_readonly.erase_i(start+1, input.size()-1);
		m_readonly[start] = merged;
	}
}
*/

} // namespace nark
