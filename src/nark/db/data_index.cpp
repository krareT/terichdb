#include "data_index.hpp"

namespace nark { namespace db {

ReadableIndex::ReadableIndex()
  : m_isOrdered(false)
  , m_isUnique(false)
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
	assert(m_isOrdered);
	return nullptr;
}

void ReadableIndex::encodeIndexKey(const Schema& schema, valvec<byte>& key) const {
	// unordered index need not to encode index key
	assert(m_isOrdered);

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

void ReadableIndex::encodeIndexKey(const Schema& schema, byte* key, size_t keyLen) const {
	// unordered index need not to encode index key
	assert(m_isOrdered);

	// m_isIndexKeyByteLex is just a common encoding
	//
	// some index may use a custom encoding method, in this case,
	// it just ignore m_isIndexKeyByteLex
	//
	if (m_isIndexKeyByteLex) {
		assert(schema.m_canEncodeToLexByteComparable);
		schema.byteLexConvert(key, keyLen);
	}
}

void ReadableIndex::decodeIndexKey(const Schema& schema, byte* key, size_t keyLen) const {
	// unordered index need not to decode index key
	assert(!m_isOrdered);

	if (m_isIndexKeyByteLex) {
		assert(schema.m_canEncodeToLexByteComparable);
		schema.byteLexConvert(key, keyLen);
	}
}

ReadableStore* ReadableIndex::getReadableStore() {
	return nullptr;
}
WritableIndex* ReadableIndex::getWritableIndex() {
	return nullptr;
}

/////////////////////////////////////////////////////////////////////////////
IndexIterator::~IndexIterator() {
}

} } // namespace nark::db
