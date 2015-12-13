#include "data_index.hpp"

namespace nark { namespace db {

ReadableIndex::ReadableIndex()
  : m_isOrdered(false)
  , m_isUnique(true)
  , m_isIndexKeyByteLex(false)
{
}
ReadableIndex::~ReadableIndex() {
}

IndexIterator* ReadableIndex::createIndexIterForward(DbContext*) const {
	// ordered index must implement this method
	// unordered index is not required to implement this method
	assert(!m_isOrdered);
	return nullptr;
}
IndexIterator* ReadableIndex::createIndexIterBackward(DbContext*) const {
	// ordered index must implement this method
	// unordered index is not required to implement this method
	assert(!m_isOrdered);
	return nullptr;
}

void ReadableIndex::encodeIndexKey(const Schema& schema, valvec<byte>& key) const {
	// unordered index need not to encode index key
	assert(!m_isOrdered);

	// m_isIndexKeyByteLex is just a common encoding
	//
	// some index may use a custom encoding method, in this case,
	// it just ignore m_isIndexKeyByteLex
	//
	if (m_isIndexKeyByteLex) {
		assert(schema.m_canEncodeToLexByteComparable);
		schema.byteLexConvert(key);
	}
}

void ReadableIndex::decodeIndexKey(const Schema& schema, valvec<byte>& key) const {
	// unordered index need not to decode index key
	assert(!m_isOrdered);

	if (m_isIndexKeyByteLex) {
		assert(schema.m_canEncodeToLexByteComparable);
		schema.byteLexConvert(key);
	}
}

bool ReadableIndex::exists(fstring key, DbContext* ctx) const {
	// default implementation
	llong id = this->searchExact(key, ctx);
	return id >= 0;
}

const ReadableStore* ReadableIndex::getReadableStore() const {
	return nullptr;
}
WritableIndex* ReadableIndex::getWritableIndex() {
	return nullptr;
}

/////////////////////////////////////////////////////////////////////////////
IndexIterator::~IndexIterator() {
}

} } // namespace nark::db
