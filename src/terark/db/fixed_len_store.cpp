#include "fixed_len_store.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/autoclose.hpp>

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
#endif
#include <fcntl.h>

namespace terark { namespace db {

TERARK_DB_REGISTER_STORE("fixlen", FixedLenStore);

struct FixedLenStore::Header {
	uint64_t rows;
	uint64_t capacity;
	uint32_t fixlen;
	uint32_t reserved;

	uint64_t mem_size() const { return fixlen * rows; }
	uint64_t cap_size() const { return fixlen * capacity; }

	byte_t* get_data(llong id) const {
		return (byte_t*)(this + 1) + fixlen * id;
	}
};

//std::string makeFilePath(PathRef segDir, const Schema& schema);

FixedLenStore::FixedLenStore() {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixlen = 0;
}
FixedLenStore::FixedLenStore(PathRef segDir, const Schema& schema) {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixlen = schema.getFixedRowLen();
	m_fpath = (segDir / "colgroup-" + schema.m_name + ".fixlen").string();
}

FixedLenStore::~FixedLenStore() {
	if (m_mmapBase) {
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

llong FixedLenStore::dataInflateSize() const {
	return m_mmapBase->mem_size();
}

llong FixedLenStore::dataStorageSize() const {
	return m_mmapBase->mem_size();
}

llong FixedLenStore::numDataRows() const {
	return m_mmapBase->rows;
}

void FixedLenStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	assert(id < llong(m_mmapBase->rows));
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
	h.reserved = 0;
	FileStream fp(m_fpath.c_str(), "wb");
	fp.ensureWrite(&h, sizeof(h));
	fp.ensureWrite(strVec.m_strpool.data(), strVec.m_strpool.size());
}

void FixedLenStore::load(PathRef fpath) {
	assert(fstring(fpath.string()).endsWith(".fixlen"));
	assert(nullptr == m_mmapBase);
	const bool writable = true;
	m_mmapBase = (Header*)mmap_load(fpath.string(), &m_mmapSize, writable);
	m_fixlen = m_mmapBase->fixlen;
}

void FixedLenStore::openStore() {
	assert(m_fpath.size() > 0);
	assert(fstring(m_fpath).endsWith(".fixlen"));
	assert(nullptr == m_mmapBase);
	const bool writable = true;
	m_mmapBase = (Header*)mmap_load(m_fpath, &m_mmapSize, writable);
	m_fixlen = m_mmapBase->fixlen;
}

void FixedLenStore::save(PathRef path) const {
	auto fpath = path + ".fixlen";
	if (fpath.string() == m_fpath) {
		return;
	}
	assert(nullptr != m_mmapBase);
	FileStream dio(fpath.string().c_str(), "wb");
	dio.ensureWrite(m_mmapBase, m_mmapSize);
}

WritableStore* FixedLenStore::getWritableStore() { return this; }
AppendableStore* FixedLenStore::getAppendableStore() { return this; }
UpdatableStore* FixedLenStore::getUpdatableStore() { return this; }

llong FixedLenStore::append(fstring row, DbContext*) {
	assert(m_fixlen > 0);
	TERARK_RT_assert(row.size() == m_fixlen, std::invalid_argument);
	ullong const ChunkBytes = TERARK_IF_DEBUG(4*1024, 1*1024*1024);
	Header* h = m_mmapBase;
	if (nullptr == h || h->rows == h->capacity) {
		assert(m_mmapSize % ChunkBytes == 0);
		ullong minBytes = sizeof(Header) + m_fixlen * 1;
		ullong newBytes = std::max({ChunkBytes, m_mmapSize, minBytes});
		newBytes = ullong((newBytes+ChunkBytes-1)*1.618) & ~(ChunkBytes-1);
		if (h) {
			mmap_close(h, m_mmapSize);
			m_mmapBase = nullptr;
		}
#ifdef _MSC_VER
	{
		Auto_close_fd fd(::_open(m_fpath.c_str(), O_CREAT|O_BINARY|O_RDWR, 0644));
		if (fd < 0) {
			THROW_STD(logic_error
				, "FATAL: ::_open(%s, O_CREAT|O_BINARY|O_RDWR) = %s"
				, m_fpath.c_str(), strerror(errno));
		}
		int err = ::_chsize_s(fd, newBytes);
		if (err) {
			THROW_STD(logic_error, "FATAL: ::_chsize_s(%s, %zd) = %s"
				, m_fpath.c_str(), newBytes, strerror(errno));
		}
	}
#else
		int err = ::truncate(m_fpath.c_str(), newBytes);
		if (err) {
			THROW_STD(logic_error, "FATAL: ::truncate(%s, %zd) = %s"
				, m_fpath.c_str(), newBytes, strerror(errno));
		}
#endif
		const bool writable = true;
		m_mmapBase = (Header*)mmap_load(m_fpath, &m_mmapSize, writable);
		m_mmapBase->capacity = (m_mmapSize - sizeof(Header)) / m_fixlen;
		if (nullptr == h) {
			h = m_mmapBase;
			h->fixlen = m_fixlen;
			h->reserved = 0;
			h->rows = 0;
			assert(m_mmapSize >= sizeof(Header) + h->cap_size());
		}
		else {
			h = m_mmapBase;
			assert(m_mmapSize >= sizeof(Header) + h->cap_size());
		}
	}
	memcpy(h->get_data(h->rows), row.data(), row.size());
	return h->rows++;
}

void FixedLenStore::update(llong id, fstring row, DbContext* ctx) {
	Header* h = m_mmapBase;
	assert(id >= 0);
	TERARK_RT_assert(row.size() == m_fixlen, std::invalid_argument);
	if (id < llong(h->rows)) {
		memcpy(h->get_data(id), row.data(), row.size());
	} else {
		TERARK_RT_assert(size_t(id) == h->rows, std::invalid_argument);
		append(row, ctx);
	}
}

byte_t* FixedLenStore::getRawDataBasePtr() {
	assert(nullptr != m_mmapBase);
	return (byte_t *)(m_mmapBase + 1);
}

void FixedLenStore::remove(llong id, DbContext*) {
	Header* h = m_mmapBase;
	assert(id >= 0);
	assert(id < llong());
	assert(h->rows > 0);
	if (id == llong(h->rows)-1) {
		h->rows--;
	}
}

void FixedLenStore::shrinkToFit() {
	if (nullptr == m_mmapBase) {
		return;
	}
	ullong realSize = sizeof(Header) + m_mmapBase->mem_size();
	mmap_close(m_mmapBase, m_mmapSize);
	m_mmapBase = nullptr;
#ifdef _MSC_VER
{
	Auto_close_fd fd(::_open(m_fpath.c_str(), O_BINARY|O_RDWR));
	if (fd < 0) {
		THROW_STD(logic_error
			, "FATAL: ::_open(%s, O_BINARY|O_RDWR) = %s"
			, m_fpath.c_str(), strerror(errno));
	}
	int err = ::_chsize_s(fd, realSize);
	if (err) {
		THROW_STD(logic_error, "FATAL: ::_chsize_s(%s, %zd) = %s"
			, m_fpath.c_str(), realSize, strerror(errno));
	}
}
#else
	int err = ::truncate(m_fpath.c_str(), realSize);
	if (err) {
		THROW_STD(logic_error, "FATAL: ::truncate(%s, %zd) = %s"
			, m_fpath.c_str(), realSize, strerror(errno));
	}
#endif
	this->openStore();
	TERARK_RT_assert(realSize == m_mmapSize, std::logic_error);
}

}} // namespace terark::db
