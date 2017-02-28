#include "fixed_len_store.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/truncate_file.hpp>
#include <functional>
#include <terark/num_to_str.hpp>

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
#endif
#include <fcntl.h>

namespace terark { namespace terichdb {

TERICHDB_REGISTER_STORE("fixlen", FixedLenStore);

struct FixedLenStore::Header {
	uint64_t rows;
	uint64_t capacity;
	uint32_t fixlen;
	uint32_t padding;

	uint64_t mem_size() const { return fixlen * rows; }
	uint64_t cap_size() const { return fixlen * capacity; }

	byte_t* get_data(llong id) const {
		return (byte_t*)(this + 1) + fixlen * id;
	}
};

//std::string makeFilePath(PathRef segDir, const Schema& schema);

#define ScopeLock(WriteLock) \
	SpinRwLock lock; \
    if (m_needsLock) lock.acquire(m_mutex, WriteLock)

FixedLenStore::FixedLenStore(const Schema& schema) : m_schema(schema) {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixlen = schema.getFixedRowLen();
	m_needsLock = true;
}
FixedLenStore::FixedLenStore(PathRef segDir, const Schema& schema)
  : m_schema(schema) {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixlen = schema.getFixedRowLen();
	m_fpath = (segDir / "colgroup-" + schema.m_name + ".fixlen").string();
	m_needsLock = true;
}

FixedLenStore::~FixedLenStore() {
	if (m_mmapBase) {
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

llong FixedLenStore::dataInflateSize() const {
	ScopeLock(false);
	return NULL == m_mmapBase ? 0 : m_mmapBase->mem_size();
}

llong FixedLenStore::dataStorageSize() const {
	ScopeLock(false);
	return NULL == m_mmapBase ? 0 : m_mmapBase->mem_size();
}

llong FixedLenStore::numDataRows() const {
	ScopeLock(false);
	return NULL == m_mmapBase ? 0 : m_mmapBase->rows;
}

void FixedLenStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	assert(id < llong(m_mmapBase->rows));
	ScopeLock(false);
	const byte* dataPtr = m_mmapBase->get_data(id);
	val->append(dataPtr, m_mmapBase->fixlen);
}

StoreIterator* FixedLenStore::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* FixedLenStore::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

void FixedLenStore::build(SortableStrVec& strVec) {
	assert(m_fixlen > 0);
	assert(strVec.m_index.size() == 0);
	assert(strVec.m_strpool.size() % m_fixlen == 0);
	assert(m_fpath.size() > 0);
	size_t rows = strVec.m_strpool.size() / m_fixlen;
	Header h;
	h.rows     = rows;
	h.capacity = rows;
	h.fixlen   = m_fixlen;
	h.padding = 0;
	FileStream fp(m_fpath.c_str(), "wb");
	fp.ensureWrite(&h, sizeof(h));
	fp.ensureWrite(strVec.m_strpool.data(), strVec.m_strpool.size());
	fp.close();
	load(m_fpath);
}

void FixedLenStore::load(PathRef fpath) {
	assert(fstring(fpath.string()).endsWith(".fixlen"));
	assert(nullptr == m_mmapBase);
	const bool writable = true;
	m_mmapBase = (Header*)mmap_load(fpath.string(), &m_mmapSize, writable, m_schema.m_mmapPopulate);
	assert(m_fixlen == m_mmapBase->fixlen);
//	m_fixlen = m_mmapBase->fixlen;
	m_recordsBasePtr = m_mmapBase->get_data(0);
	m_fpath = fpath.string();
}

void FixedLenStore::openStore() {
	assert(m_fpath.size() > 0);
	assert(fstring(m_fpath).endsWith(".fixlen"));
	assert(nullptr == m_mmapBase);
	const bool writable = true;
	m_mmapBase = (Header*)mmap_load(m_fpath, &m_mmapSize, writable, m_schema.m_mmapPopulate);
	assert(m_fixlen == m_mmapBase->fixlen);
//	m_fixlen = m_mmapBase->fixlen;
	m_recordsBasePtr = m_mmapBase->get_data(0);
}

void FixedLenStore::save(PathRef path) const {
	auto fpath = path + ".fixlen";
	if (fpath == m_fpath) {
		return;
	}
	assert(nullptr != m_mmapBase);
	FileStream dio(fpath.string().c_str(), "wb");
	dio.ensureWrite(m_mmapBase, m_mmapSize);
}

WritableStore* FixedLenStore::getWritableStore() { return this; }
AppendableStore* FixedLenStore::getAppendableStore() { return this; }
UpdatableStore* FixedLenStore::getUpdatableStore() { return this; }

static ullong const ChunkBytes = TERARK_IF_DEBUG(4*1024, 1*1024*1024);

llong FixedLenStore::append(fstring row, DbContext*) {
	assert(m_fixlen > 0);
	TERARK_RT_assert(row.size() == m_fixlen, std::invalid_argument);
	ScopeLock(true);
	Header* h = m_mmapBase;
	if (nullptr == h || h->rows == h->capacity) {
		assert(m_mmapSize % ChunkBytes == 0);
		h = allocFileSize(llong(std::max(m_mmapSize, sizeof(Header)) * 1.618));
	}
	memcpy(h->get_data(h->rows), row.data(), row.size());
	return h->rows++;
}

void FixedLenStore::update(llong id, fstring row, DbContext* ctx) {
	ScopeLock(true);
	Header* h = m_mmapBase;
	assert(id >= 0);
	TERARK_RT_assert(row.size() == m_fixlen, std::invalid_argument);
	if (nullptr == h) {
		h = allocFileSize(llong(std::max(m_mmapSize, sizeof(Header)) * 1.618));
	}
	// id may greater than h->rows in concurrent insertions
	uint64_t oldRows = h->rows;
	uint64_t newRows = std::max<uint64_t>(oldRows, id+1);
	assert(uint64_t(id) <= oldRows); // assert in single thread tests
	if (newRows >= h->capacity) {
		assert(m_mmapSize % ChunkBytes == 0);
		size_t required_bytes = m_mmapSize + m_fixlen * (newRows - h->capacity);
		h = allocFileSize(llong(std::max(required_bytes, sizeof(Header)) * 1.618));
		assert(h->capacity > newRows);
	}
	memcpy(h->get_data(id), row.data(), row.size());
	h->rows = newRows;
//	if (oldRows == id)
//		h->rows = newRows; // what if mmap is in slower RAM?
}

void FixedLenStore::remove(llong id, DbContext*) {
	ScopeLock(true);
	Header* h = m_mmapBase;
	assert(id >= 0);
	assert(id < llong(h->rows));
	assert(h->rows > 0);
	if (id == llong(h->rows)-1) {
		h->rows--;
	}
}

void FixedLenStore::shrinkToFit() {
	ScopeLock(true);
	if (nullptr == m_mmapBase) {
		return;
	}
	ullong realSize = sizeof(Header) + m_mmapBase->mem_size();
	mmap_close(m_mmapBase, m_mmapSize);
	m_mmapBase = nullptr;
	truncate_file(m_fpath, realSize);
	this->openStore();
	TERARK_RT_assert(realSize == m_mmapSize, std::logic_error);
}

void FixedLenStore::shrinkToSize(size_t size)
{
    ScopeLock(true);
    if(nullptr == m_mmapBase)
    {
        return;
    }
    assert(size <= m_mmapBase->rows);
    m_mmapBase->rows = size;
}

void FixedLenStore::markFrozen() {
    ReadableStore::markFrozen();
    unneedsLock();
}

void FixedLenStore::reserveRows(size_t rows) {
	ScopeLock(true);
	allocFileSize(sizeof(Header) + m_fixlen * rows);
}

void FixedLenStore::setNumRows(size_t rows) {
	ScopeLock(true);
	assert(nullptr != m_mmapBase);
	assert(rows <= m_mmapBase->capacity);
	m_mmapBase->rows = rows;
}

FixedLenStore::Header*
FixedLenStore::allocFileSize(ullong fileSize) {
	using std::max;
	assert(fileSize > sizeof(Header));
	ullong minBytes = sizeof(Header) + m_fixlen * 1;
	ullong newBytes = max(max<ullong>(ChunkBytes, fileSize), minBytes);
	newBytes = ullong((newBytes+ChunkBytes-1)) & ~(ChunkBytes-1);
	Header* h = m_mmapBase;
	if (h) {
		mmap_close(h, m_mmapSize);
		m_mmapBase = nullptr;
	}
	truncate_file(m_fpath, newBytes);
	const bool writable = true;
	m_mmapBase = (Header*)mmap_load(m_fpath, &m_mmapSize, writable);
	m_mmapBase->capacity = (m_mmapSize - sizeof(Header)) / m_fixlen;
	if (nullptr == h) {
		h = m_mmapBase;
		h->fixlen = m_fixlen;
		h->padding = 0;
		h->rows = 0;
		assert(m_mmapSize >= sizeof(Header) + h->cap_size());
	}
	else {
		h = m_mmapBase;
		assert(m_mmapSize >= sizeof(Header) + h->cap_size());
	}
	m_recordsBasePtr = h->get_data(0);
	return h;
}

void FixedLenStore::deleteFiles() {
	mmap_close(m_mmapBase, m_mmapSize);
	m_mmapBase = nullptr;
	boost::filesystem::remove(m_fpath);
}


}} // namespace terark::terichdb
