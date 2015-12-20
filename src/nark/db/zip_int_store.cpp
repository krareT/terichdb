#include "zip_int_store.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>

namespace nark { namespace db {

ZipIntStore::ZipIntStore() {
	m_mmapBase = nullptr;
	m_mmapSize = 0;
}
ZipIntStore::~ZipIntStore() {
	if (m_mmapBase) {
		m_ints.risk_release_ownership();
		mmap_close(m_mmapBase, m_mmapSize);
	}
}


llong ZipIntStore::dataStorageSize() const {
	return m_ints.mem_size();
}

llong ZipIntStore::numDataRows() const {
	return m_ints.size();
}

template<class Int>
void ZipIntStore::keyAppend(size_t recIdx, valvec<byte>* key) const {
	Int iKey = Int(m_minKey + m_ints.get(recIdx));
	unaligned_save<Int>(key->grow_no_init(sizeof(Int)), iKey);
}

void ZipIntStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	size_t idx = size_t(id);
	assert(idx < m_ints.size());
	switch (m_intType) {
	default:
		THROW_STD(invalid_argument, "Bad m_intType=%s", Schema::columnTypeStr(m_intType));
	case ColumnType::Sint08: keyAppend< int8_t >(idx, val); break;
	case ColumnType::Uint08: keyAppend<uint8_t >(idx, val); break;
	case ColumnType::Sint16: keyAppend< int16_t>(idx, val); break;
	case ColumnType::Uint16: keyAppend<uint16_t>(idx, val); break;
	case ColumnType::Sint32: keyAppend< int32_t>(idx, val); break;
	case ColumnType::Uint32: keyAppend<uint32_t>(idx, val); break;
	case ColumnType::Sint64: keyAppend< int64_t>(idx, val); break;
	case ColumnType::Uint64: keyAppend<uint64_t>(idx, val); break;
	case ColumnType::VarSint: {
		byte  buf[16];
		byte* end = save_var_int64(buf, int64_t(m_minKey + m_ints.get(idx)));
		val->append(buf, end - buf);
		break; }
	case ColumnType::VarUint: {
		byte  buf[16];
		byte* end = save_var_uint64(buf, uint64_t(m_minKey + m_ints.get(idx)));
		val->append(buf, end - buf);
		break; }
	}
}

StoreIterator* ZipIntStore::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* ZipIntStore::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

template<class Int>
void ZipIntStore::zipKeys(const void* data, size_t size) {
	size_t rows = size / sizeof(Int);
	assert(size % sizeof(Int) == 0);
	const Int* keys = (Int*)data;
	m_minKey = m_ints.build_from(keys, rows);
#if !defined(NDEBUG)
	assert(m_ints.size() == rows);
	for (size_t i = 0; i < rows; ++i) {
		assert(Int(m_minKey + m_ints[i]) == keys[i]);
	}
#endif
}

void ZipIntStore::build(ColumnType keyType, SortableStrVec& strVec) {
	assert(strVec.m_index.size() == 0);
	m_intType = keyType;
	void*  data = strVec.m_strpool.data();
	size_t size = strVec.m_strpool.size();
	switch (keyType) {
	default:
		THROW_STD(invalid_argument, "Bad keyType=%s", Schema::columnTypeStr(keyType));
	case ColumnType::Sint08: zipKeys< int8_t >(data, size); break;
	case ColumnType::Uint08: zipKeys<uint8_t >(data, size); break;
	case ColumnType::Sint16: zipKeys< int16_t>(data, size); break;
	case ColumnType::Uint16: zipKeys<uint16_t>(data, size); break;
	case ColumnType::Sint32: zipKeys< int32_t>(data, size); break;
	case ColumnType::Uint32: zipKeys<uint32_t>(data, size); break;
	case ColumnType::Sint64: zipKeys< int64_t>(data, size); break;
	case ColumnType::Uint64: zipKeys<uint64_t>(data, size); break;
	case ColumnType::VarSint: {
		valvec<llong> tmp;
		const byte* pos = strVec.m_strpool.data();
		const byte* end = strVec.m_strpool.end();
		while (pos < end) {
			const byte* next = nullptr;
			llong key = load_var_int64(pos, &next);
			tmp.push_back(key);
			pos = next;
		}
		zipKeys<int64_t>(tmp.data(), tmp.used_mem_size());
		break; }
	case ColumnType::VarUint: {
		valvec<ullong> tmp;
		const byte* pos = strVec.m_strpool.data();
		const byte* end = strVec.m_strpool.end();
		while (pos < end) {
			const byte* next = nullptr;
			ullong key = load_var_uint64(pos, &next);
			tmp.push_back(key);
			pos = next;
		}
		zipKeys<uint64_t>(tmp.data(), tmp.used_mem_size());
		break; }
	}
	valvec<uint32_t> index(m_ints.size(), valvec_no_init());
	for (size_t i = 0; i < index.size(); ++i) index[i] = i;
	std::sort(index.begin(), index.end(), [&](size_t x, size_t y) {
		size_t xkey = m_ints.get(x);
		size_t ykey = m_ints.get(y);
		if (xkey < ykey) return true;
		if (xkey > ykey) return false;
		return x < y;
	});
}

namespace {
	struct ZipIntStoreHeader {
		uint32_t rows;
		uint8_t  intBits;
		uint8_t  intType;
		uint16_t padding;
		 int64_t minValue;
	};
	BOOST_STATIC_ASSERT(sizeof(ZipIntStoreHeader) == 16);
}

void ZipIntStore::load(fstring path) {
	std::string fpath = path + ".zint";
	m_mmapBase = (byte_t*)mmap_load(fpath.c_str(), &m_mmapSize);
	auto header = (ZipIntStoreHeader*)m_mmapBase;
	m_intType = ColumnType(header->intType);
	m_minKey  = header->minValue;
	m_ints.risk_set_data((byte*)(header+1), header->rows, header->intBits);
}

void ZipIntStore::save(fstring path) const {
	std::string fpath = path + ".zint";
	NativeDataOutput<FileStream> dio;
	dio.open(fpath.c_str(), "wb");
	ZipIntStoreHeader header;
	header.rows = uint32_t(numDataRows());
	header.intBits = byte(m_ints.uintbits());
	header.intType = byte(m_intType);
	header.padding = 0;
	header.minValue = int64_t(m_minKey);
	dio.ensureWrite(&header, sizeof(header));
	dio.ensureWrite(m_ints .data(), m_ints .mem_size());
}

}} // namespace nark::db
