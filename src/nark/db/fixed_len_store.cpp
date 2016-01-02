#include "fixed_len_store.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>

namespace nark { namespace db {

NARK_DB_REGISTER_STORE("fixlen", FixedLenStore);

FixedLenStore::FixedLenStore() {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixedLen = 0;
	m_rows = 0;
}

FixedLenStore::~FixedLenStore() {
	if (m_mmapBase) {
		m_keys.risk_release_ownership();
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

llong FixedLenStore::dataStorageSize() const {
	return m_keys.used_mem_size();
}

llong FixedLenStore::numDataRows() const {
	return m_rows;
}

void FixedLenStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	size_t idx = size_t(id);
	assert(idx < m_keys.size());
	const byte* dataPtr = m_keys.data() + m_fixedLen * idx;
	val->append(dataPtr, m_fixedLen);
}

StoreIterator* FixedLenStore::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* FixedLenStore::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

void FixedLenStore::build(const Schema& schema, SortableStrVec& strVec) {
	size_t fixlen = schema.getFixedRowLen();
	size_t rows = strVec.m_strpool.size() / fixlen;
	assert(strVec.m_index.size() == 0);
	assert(strVec.m_strpool.size() % fixlen == 0);
	m_keys.clear();
	m_keys.swap(strVec.m_strpool);
	m_rows = rows;
}

namespace {
	struct Header {
		uint32_t rows;
		uint32_t fixlen;
	};
}

void FixedLenStore::load(PathRef fpath) {
	assert(fstring(fpath.string()).endsWith(".zint"));
	m_mmapBase = (byte_t*)mmap_load(fpath.string(), &m_mmapSize);
	auto h = (const Header*)m_mmapBase;
	m_fixedLen = h->fixlen;
	m_rows     = h->rows;
	size_t keyMemSize = h->fixlen * h->rows;
	m_keys.risk_set_data((byte*)(h+1) , keyMemSize);
}

void FixedLenStore::save(PathRef path) const {
	auto fpath = path + ".fixlen";
	NativeDataOutput<FileStream> dio;
	dio.open(fpath.string().c_str(), "wb");
	Header h;
	h.rows     = uint32_t(m_rows);
	h.fixlen   = m_fixedLen;
	dio.ensureWrite(&h, sizeof(h));
	dio.ensureWrite(m_keys .data(), m_keys .used_mem_size());
}

}} // namespace nark::db
