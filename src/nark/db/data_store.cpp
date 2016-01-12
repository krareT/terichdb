#include "data_store.hpp"

namespace nark { namespace db {

Permanentable::Permanentable() {
}
Permanentable::~Permanentable() {
}
void Permanentable::save(PathRef) const {
	THROW_STD(invalid_argument, "This method should not be called");
}
void Permanentable::load(PathRef) {
	THROW_STD(invalid_argument, "This method should not be called");
}

StoreIterator::~StoreIterator() {
}

///////////////////////////////////////////////////////////////////////////////
typedef hash_strmap< std::function<ReadableStore*()>
					, fstring_func::hash_align
					, fstring_func::equal_align
					, ValueInline, SafeCopy
					>
		StoreFactory;
static	StoreFactory s_storeFactory;

ReadableStore::RegisterStoreFactory::RegisterStoreFactory
(const char* fnameSuffix, const std::function<ReadableStore*()>& f)
{
	auto ib = s_storeFactory.insert_i(fnameSuffix, f);
	assert(ib.second);
	if (!ib.second)
		THROW_STD(invalid_argument, "duplicate suffix: %s", fnameSuffix);
}

ReadableStore::ReadableStore() {
}
ReadableStore::~ReadableStore() {
}

ReadableStore* ReadableStore::openStore(PathRef segDir, fstring fname) {
	size_t sufpos = fname.size();
	while (sufpos > 0 && fname[sufpos-1] != '.') --sufpos;
	auto suffix = fname.substr(sufpos);
	size_t idx = s_storeFactory.find_i(suffix);
	if (idx < s_storeFactory.end_i()) {
		const auto& factory = s_storeFactory.val(idx);
		ReadableStore* store = factory();
		assert(NULL != store);
		if (NULL == store) {
			THROW_STD(runtime_error, "store factory should not return NULL store");
		}
		auto fpath = segDir / fname.str();
		store->load(fpath);
		return store;
	}
	else {
		THROW_STD(invalid_argument
			, "store type '%.*s' of '%s' is not registered"
			, suffix.ilen(), suffix.data()
			, (segDir / fname.str()).string().c_str()
			);
		return NULL; // avoid compiler warning
	}
}

WritableStore* ReadableStore::getWritableStore() {
	return nullptr;
}

ReadableIndex* ReadableStore::getReadableIndex() {
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

} } // namespace nark::db
