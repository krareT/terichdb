#include "appendonly.hpp"
#include <terark/num_to_str.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/truncate_file.hpp>

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
#endif
#include <fcntl.h>

namespace terark { namespace db {

struct RandomReadAppendonlyStore::Header {
	char      magic[64];
	uint64_t  rowsNum;
	uint64_t  rowsCap;
	uint64_t  dataLen;
	uint64_t  dataCap;
	uint08_t  offsetBits;
	uint08_t  version;
	uint08_t  padding[30];
	void init() {
		memset(this, 0, sizeof(*this));
		strcpy(magic, "terark::db::RandomReadAppendonlyStore");
		offsetBits = 64; // begin with simple
		version = 1;
	}
	uint64_t totalUsedBytes() const {
		return dataLen + (rowsNum * offsetBits + 7) / 8;
	}
	uint64_t getOffset(llong recId) const {
		return ((const uint64_t*)(this + 1))[recId];
	}
	void setOffset(llong recId, uint64_t offset) {
		((uint64_t*)(this + 1))[recId] = offset;
	}
};

AppendableStore* RandomReadAppendonlyStore::getAppendableStore() {
	return this;
}

RandomReadAppendonlyStore::RandomReadAppendonlyStore(const Schema& schema) {
	m_index = nullptr; m_indexBytes = 0;
	m_store = nullptr; m_storeBytes = 0;
}

RandomReadAppendonlyStore::~RandomReadAppendonlyStore() {
	if (m_index) {
		assert(NULL != m_store);
		mmap_close(m_index, m_indexBytes);
		mmap_close(m_store, m_storeBytes);
	}
}

llong RandomReadAppendonlyStore::dataInflateSize() const {
	return m_index->dataLen;
}
llong RandomReadAppendonlyStore::dataStorageSize() const {
	return m_index->totalUsedBytes();
}

llong RandomReadAppendonlyStore::numDataRows() const {
	return m_index->rowsNum;
}

void
RandomReadAppendonlyStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	assert(id >= 0);
	llong rows = llong(m_index->rowsNum);
	if (id >= rows) {
		THROW_STD(out_of_range, "id = %lld, rows = %lld", id, rows);
	}
	uint64_t offset0 = m_index->getOffset(id+0);
	uint64_t offset1 = m_index->getOffset(id+1);
	TERARK_RT_assert(offset0 <= offset1, std::logic_error);
	val->append(m_store + offset0, offset1 - offset0);
}

StoreIterator* RandomReadAppendonlyStore::createStoreIterForward(DbContext* ctx) const {
	return nullptr;
}
StoreIterator* RandomReadAppendonlyStore::createStoreIterBackward(DbContext* ctx) const {
	return nullptr;
}

static ullong const ChunkBytes = TERARK_IF_DEBUG(4*1024, 1*1024*1024);

llong RandomReadAppendonlyStore::append(fstring row, DbContext*) {
	Header* h = m_index;
	if (terark_unlikely(NULL == h)) {
		h = allocIndexRows(256);
		truncate_file(m_storeFile, ChunkBytes);
		m_store = (byte_t*)mmap_load(m_storeFile, &m_storeBytes);
		h->dataCap = ChunkBytes;
	}
	else {
		TERARK_RT_assert(h->rowsNum <= h->rowsCap, std::logic_error);
		if (terark_unlikely(h->rowsNum == h->rowsCap)) {
			h = allocIndexRows(ullong(h->rowsCap * 1.618));
		}
	}
	TERARK_RT_assert(h->dataLen <= h->dataCap, std::logic_error);
	if (terark_unlikely(h->dataLen + row.size() > h->dataCap)) {
		mmap_close(m_store, m_storeBytes); m_store = NULL;
		ullong require = ullong((h->dataLen + row.size()) * 1.618);
		ullong aligned = (require + ChunkBytes-1) & ~(ChunkBytes-1);
		truncate_file(m_storeFile, aligned);
		m_store = (byte_t*)mmap_load(m_storeFile, &m_storeBytes);
	}
	TERARK_RT_assert(h->getOffset(h->rowsNum) == h->dataLen, std::logic_error);
	memcpy(m_store + h->dataLen, row.data(), row.size());
	h->setOffset(h->rowsNum + 1, h->dataLen + row.size());
	h->dataLen += row.size();
	return h->rowsNum++;
}

void RandomReadAppendonlyStore::shrinkToFit() {
	m_index->dataCap = m_index->dataLen;
	m_index->rowsCap = m_index->rowsNum;
	uint64_t indexBytes = sizeof(Header) + (m_index->rowsNum * m_index->offsetBits + 7) / 8;
	uint64_t storeBytes = m_index->dataLen;
	mmap_close(m_index, m_indexBytes); m_index = NULL;
	mmap_close(m_store, m_storeBytes); m_store = NULL;
	truncate_file(m_indexFile, indexBytes);
	truncate_file(m_storeFile, storeBytes);
	m_index = (Header*)mmap_load(m_indexFile, &m_indexBytes);
	m_store = (byte_t*)mmap_load(m_storeFile, &m_storeBytes);
}

void RandomReadAppendonlyStore::deleteFiles() {
	mmap_close(m_index, m_indexBytes); m_index = NULL;
	mmap_close(m_store, m_storeBytes); m_store = NULL;
	boost::filesystem::remove(m_indexFile);
	boost::filesystem::remove(m_storeFile);
}

void RandomReadAppendonlyStore::load(PathRef fpath) {
	assert(NULL == m_index);
	assert(NULL == m_store);
	TERARK_RT_assert(m_indexFile.size() > 0, std::invalid_argument);
	std::string strFile = fpath.string();
	m_storeFile = strFile + ".ap-index";
	m_indexFile = strFile + ".ap-store";
	bool writable = true;
	m_index = (Header*)mmap_load(m_indexFile, &m_indexBytes, writable);
	m_store = (byte_t*)mmap_load(m_storeFile, &m_storeBytes, writable);
}

void RandomReadAppendonlyStore::save(PathRef path) const {
	// do nothing
	THROW_STD(invalid_argument, "Unexpected method call");
}

RandomReadAppendonlyStore::Header*
RandomReadAppendonlyStore::allocIndexRows(ullong rows) {
	using std::max;
	ullong newBytes = max<ullong>(ChunkBytes, sizeof(Header) + sizeof(uint64_t) * (rows + 1));
	newBytes = ullong((newBytes+ChunkBytes-1)) & ~(ChunkBytes-1);
	Header* h = m_index;
	if (h) {
		mmap_close(h, m_indexBytes);
		m_index = nullptr;
	}
	truncate_file(m_indexFile, newBytes);
	const bool writable = true;
	m_index = (Header*)mmap_load(m_indexFile, &m_indexBytes, writable);
	if (nullptr == h) {
		h = m_index;
		h->init();
	}
	else {
		h = m_index;
	}
	m_index->rowsCap = (m_indexBytes - sizeof(Header)) * 8 / m_index->offsetBits;
	return h;
}


/////////////////////////////////////////////////////////////////////////////

struct SeqReadAppendonlyStore::IoImpl {
	FileStream fp;
	NativeDataOutput<OutputBuffer> dio;
};

class SeqReadAppendonlyStore::MyStoreIterForward : public StoreIterator {
	llong   m_id;
	int64_t m_rows;
	int64_t m_inflateSize;
	FileStream m_fp;
	NativeDataInput<InputBuffer> m_di;
public:
	MyStoreIterForward(const SeqReadAppendonlyStore* store, fstring fname) {
		m_id = 0;
		m_store.reset(const_cast<SeqReadAppendonlyStore*>(store));
		m_fp.open(fname.c_str(), "rb");
		m_fp.disbuf();
		m_di.attach(&m_fp);
		m_di >> m_rows;
		m_di >> m_inflateSize;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		if (m_id < m_rows) {
			size_t len = m_di.load_as<var_size_t>();
			val->resize_no_init(len);
			m_di.ensureRead(val->data(), len);
			*id = m_id++;
			return true;
		}
		else {
			return false;
		}
	}
	bool seekExact(llong  id, valvec<byte>* val) override {
		THROW_STD(invalid_argument, "Unsupportted method");
	}
	void reset() override {
		m_id = 0;
		m_fp.rewind();
		m_di.resetbuf();
		m_di >> m_rows;
		m_di >> m_inflateSize;
	}
};

AppendableStore* SeqReadAppendonlyStore::getAppendableStore() {
	if (m_io)
		return this;
	else
		return NULL;
}

SeqReadAppendonlyStore::SeqReadAppendonlyStore(const Schema& schema) {
	m_fsize = -1;
	m_inflateSize = -1;
	m_rows = -1;
}

SeqReadAppendonlyStore::SeqReadAppendonlyStore(PathRef segDir, const Schema& schema) {
	auto fpath = segDir / "linear-" + schema.m_name + ".seq";
	m_fpath = fpath.string();
	if (boost::filesystem::exists(fpath)) {
		// just for read, by createStoreIterForward()
		m_fsize = -1;
		m_inflateSize = -1;
		m_rows = -1;
		this->doLoad();
	}
	else {
		m_fsize = 8;
		m_inflateSize = 0;
		m_rows = 0;
		m_io.reset(new IoImpl());
		m_io->fp.open(m_fpath.c_str(), "wb");
		m_io->fp.disbuf();
		m_io->dio.attach(&m_io->fp);
	//	m_io->dio.printf("terark::db::SeqReadAppendonlyStore\n");
		m_io->dio << int64_t(0); // rows
		m_io->dio << int64_t(0); // inflateSize
	}
}

SeqReadAppendonlyStore::~SeqReadAppendonlyStore() {
	if (m_io)
		this->shrinkToFit();
}

llong SeqReadAppendonlyStore::dataInflateSize() const {
	return m_inflateSize;
}
llong SeqReadAppendonlyStore::dataStorageSize() const {
	return m_fsize;
}

llong SeqReadAppendonlyStore::numDataRows() const {
	return m_rows;
}

void
SeqReadAppendonlyStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	THROW_STD(invalid_argument, "Unsupportted method");
}

StoreIterator* SeqReadAppendonlyStore::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, m_fpath);
}
StoreIterator* SeqReadAppendonlyStore::createStoreIterBackward(DbContext* ctx) const {
	return nullptr;
}

llong SeqReadAppendonlyStore::append(fstring row, DbContext*) {
	byte_t  buf[12];
#if TERARK_WORD_BITS == 64
	byte_t* endp = save_var_uint64(buf, uint64_t(row.size()));
#else
	byte_t* endp = save_var_uint32(buf, uint32_t(row.size()));
#endif
	m_io->dio.ensureWrite(buf, endp-buf);
	m_io->dio.ensureWrite(row.data(), row.size());
	m_fsize += endp-buf + row.size();
	m_inflateSize += row.size();
	return m_rows++;
}

void SeqReadAppendonlyStore::shrinkToFit() {
	m_io->dio.flush();
	m_io->dio.resetbuf();
	m_io->fp.rewind();
	m_io->dio << int64_t(m_rows);
	m_io->dio << int64_t(m_inflateSize);
	m_io.reset();
}

void SeqReadAppendonlyStore::deleteFiles() {
	m_io.reset();
	try {
		boost::filesystem::remove(m_fpath);
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: remove(%s) = %s\n", m_fpath.c_str(), ex.what());
		throw;
	}
}

void SeqReadAppendonlyStore::load(PathRef fpath) {
	std::string fname = fpath.filename().string();
	assert(fstring(fname).startsWith("linear-"));
	m_fpath = fpath.string() + ".seq";
	doLoad();
}

void SeqReadAppendonlyStore::doLoad() {
	m_fsize = boost::filesystem::file_size(m_fpath);
	FileStream fp(m_fpath.c_str(), "rb");
	int64_t rows, inflateSize;
	fp.ensureRead(&rows, 8);
	fp.ensureRead(&inflateSize, 8);
	m_rows = rows;
	m_inflateSize = inflateSize;
}

void SeqReadAppendonlyStore::save(PathRef path) const {
	// do nothing
	// THROW_STD(invalid_argument, "Unexpected method call");
}


} } // namespace terark::db
