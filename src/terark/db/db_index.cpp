#include "db_index.hpp"
#include <terark/io/FileStream.hpp>

namespace terark { namespace db {

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

WritableIndex::~WritableIndex() {
}

/////////////////////////////////////////////////////////////////////////////

IndexIterator::IndexIterator() {
	// m_isUniqueInSchema is just for a minor performance improve
	m_isUniqueInSchema = false;
}
IndexIterator::~IndexIterator() {
}

int
IndexIterator::seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) {
	int ret = seekLowerBound(key, id, retKey);
	if (ret == 0) {
		while (increment(id, retKey)) {
			if (key != *retKey)
				return 1;
		}
		return -1;
	}
	return ret;
}

/////////////////////////////////////////////////////////////////////////////
EmptyIndexStore::EmptyIndexStore() {}
EmptyIndexStore::EmptyIndexStore(const Schema&) {}
EmptyIndexStore::~EmptyIndexStore() {}

llong EmptyIndexStore::indexStorageSize() const { return 0; }

void
EmptyIndexStore::searchExactAppend(fstring, valvec<llong>* recIdvec, DbContext*) const {
}

class EmptyIndexIterator : public IndexIterator {
public:
	void reset() override {}
	bool increment(llong* id, valvec<byte>* key) override { return false; }
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) { return -1; }
	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) { return -1; }
};

IndexIterator* EmptyIndexStore::createIndexIterForward(DbContext*) const {
	return new EmptyIndexIterator();
}
IndexIterator* EmptyIndexStore::createIndexIterBackward(DbContext*) const {
	return new EmptyIndexIterator();
}
class ReadableStore* EmptyIndexStore::getReadableStore() {
	return this;
}

llong EmptyIndexStore::dataStorageSize() const { return 0; }
llong EmptyIndexStore::dataInflateSize() const { return 0; }
llong EmptyIndexStore::numDataRows() const { return 0; }
void EmptyIndexStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	THROW_STD(invalid_argument, "Invalid method call");
}
void EmptyIndexStore::deleteFiles() {}
StoreIterator* EmptyIndexStore::createStoreIterForward(DbContext*) const {
	return nullptr;
}
StoreIterator* EmptyIndexStore::createStoreIterBackward(DbContext*) const {
	return nullptr;
}
ReadableIndex* EmptyIndexStore::getReadableIndex() { return this; }

void EmptyIndexStore::load(PathRef fpath) {
	std::string strFpath = fpath.string() + ".empty";
//	FileStream fp(strFpath.c_str(), "wb");
}
void EmptyIndexStore::save(PathRef fpath) const {
	std::string strFpath = fpath.string();
	if (!fstring(strFpath).endsWith(".empty")) {
		strFpath += ".empty";
	}
	else {
		strFpath = strFpath;
	}
	FileStream fp(strFpath.c_str(), "wb");
}

TERARK_DB_REGISTER_STORE("empty", EmptyIndexStore);

} } // namespace terark::db
