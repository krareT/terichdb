#include "zip_int_store.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>

namespace terark { namespace db {

ZipIntStore::ZipIntStore(const Schema& schema) : m_schema(schema) {
	TERARK_RT_assert(schema.columnNum() == 1, std::invalid_argument);
	m_intType = schema.getColumnMeta(0).type;
	m_minValue = 0;
	m_mmapBase = nullptr;
	m_mmapSize = 0;
}
ZipIntStore::~ZipIntStore() {
	if (m_mmapBase) {
		m_dedup.risk_release_ownership();
		m_index.risk_release_ownership();
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

llong ZipIntStore::dataStorageSize() const {
	return m_dedup.mem_size() + m_index.mem_size();
}

llong ZipIntStore::dataInflateSize() const {
	size_t rows = m_index.size() ? m_index.size() : m_dedup.size();
	switch (m_intType) {
	default:
		THROW_STD(invalid_argument,
			"Bad m_keyType=%s", Schema::columnTypeStr(m_intType));
	case ColumnType::Sint08:
	case ColumnType::Uint08: return 1 * rows;
	case ColumnType::Sint16:
	case ColumnType::Uint16: return 2 * rows;
	case ColumnType::Sint32:
	case ColumnType::Uint32: return 4 * rows;
	case ColumnType::Sint64:
	case ColumnType::Uint64: return 8 * rows;
	}
}

llong ZipIntStore::numDataRows() const {
	return m_index.size() ? m_index.size() : m_dedup.size();
}

template<class Int>
void ZipIntStore::valueAppend(size_t recIdx, valvec<byte>* key) const {
	if (m_index.size()) {
		size_t idx = m_index.get(recIdx);
		assert(idx < m_dedup.size());
		Int iValue = Int(m_minValue + m_dedup.get(idx));
		unaligned_save<Int>(key->grow_no_init(sizeof(Int)), iValue);
	}
	else {
		Int iValue = Int(m_minValue + m_dedup.get(recIdx));
		unaligned_save<Int>(key->grow_no_init(sizeof(Int)), iValue);
	}
}

void ZipIntStore::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id < numDataRows());
	assert(id >= 0);
	size_t idx = size_t(id);
	switch (m_intType) {
	default:
		THROW_STD(invalid_argument, "Bad m_intType=%s", Schema::columnTypeStr(m_intType));
	case ColumnType::Sint08: valueAppend< int8_t >(idx, val); break;
	case ColumnType::Uint08: valueAppend<uint8_t >(idx, val); break;
	case ColumnType::Sint16: valueAppend< int16_t>(idx, val); break;
	case ColumnType::Uint16: valueAppend<uint16_t>(idx, val); break;
	case ColumnType::Sint32: valueAppend< int32_t>(idx, val); break;
	case ColumnType::Uint32: valueAppend<uint32_t>(idx, val); break;
	case ColumnType::Sint64: valueAppend< int64_t>(idx, val); break;
	case ColumnType::Uint64: valueAppend<uint64_t>(idx, val); break;
	case ColumnType::VarSint: {
		byte  buf[16];
		byte* end = save_var_int64(buf, int64_t(m_minValue + m_dedup.get(idx)));
		val->append(buf, end - buf);
		break; }
	case ColumnType::VarUint: {
		byte  buf[16];
		byte* end = save_var_uint64(buf, uint64_t(m_minValue + m_dedup.get(idx)));
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
void ZipIntStore::zipValues(const void* data, size_t size) {
	size_t rows = size / sizeof(Int);
	assert(size % sizeof(Int) == 0);
	const Int* values = (Int*)data;

	UintVecMin0 dup;
	m_minValue = dup.build_from(values, rows);

	valvec<Int> dedup(values, rows);
	std::sort(dedup.begin(), dedup.end());
	dedup.trim(std::unique(dedup.begin(), dedup.end()));

	if (dedup.size() == 1) {
		m_minValue = m_dedup.build_from(dedup);
	}

	size_t indexBits = terark_bsr_u32(dedup.size()-1) + 1;
	size_t indexSize = (indexBits      * rows         + 7) / 8;
	size_t dedupSize = (dup.uintbits() * dedup.size() + 7) / 8;
	if (dedupSize + indexSize < dup.mem_size()) {
		valvec<uint32_t> index(rows, valvec_no_init());
		for(size_t i = 0; i < rows; ++i) {
			size_t j = lower_bound_a(dedup, values[i]);
			assert(values[i] == dedup[j]);
			index[i] = j;
		}
		m_index.build_from(index);
		m_dedup.build_from(dedup);
	}
	else {
		m_dedup.swap(dup);
	}
}

void ZipIntStore::build(ColumnType keyType, SortableStrVec& strVec) {
	assert(strVec.m_index.size() == 0);
	m_intType = keyType;
	void*  data = strVec.m_strpool.data();
	size_t size = strVec.m_strpool.size();
	switch (keyType) {
	default:
		THROW_STD(invalid_argument, "Bad keyType=%s", Schema::columnTypeStr(keyType));
	case ColumnType::Sint08: zipValues< int8_t >(data, size); break;
	case ColumnType::Uint08: zipValues<uint8_t >(data, size); break;
	case ColumnType::Sint16: zipValues< int16_t>(data, size); break;
	case ColumnType::Uint16: zipValues<uint16_t>(data, size); break;
	case ColumnType::Sint32: zipValues< int32_t>(data, size); break;
	case ColumnType::Uint32: zipValues<uint32_t>(data, size); break;
	case ColumnType::Sint64: zipValues< int64_t>(data, size); break;
	case ColumnType::Uint64: zipValues<uint64_t>(data, size); break;
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
		zipValues<int64_t>(tmp.data(), tmp.used_mem_size());
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
		zipValues<uint64_t>(tmp.data(), tmp.used_mem_size());
		break; }
	}
}

namespace {
	struct ZipIntStoreHeader {
		uint32_t rows;
		uint32_t uniqNum;
		uint8_t  intBits;
		uint8_t  intType;
		uint16_t padding1;
		uint32_t padding2;
		uint64_t padding3;
		 int64_t minValue;
	};
	BOOST_STATIC_ASSERT(sizeof(ZipIntStoreHeader) == 32);
}

TERARK_DB_REGISTER_STORE("zint", ZipIntStore);

void ZipIntStore::load(PathRef fpath) {
	assert(fstring(fpath.string()).endsWith(".zint"));
	bool writable = false;
	m_mmapBase = (byte_t*)mmap_load(fpath.string(), &m_mmapSize, writable, m_schema.m_mmapPopulate);
	auto header = (const ZipIntStoreHeader*)m_mmapBase;
	size_t rows = header->rows;
	m_intType = ColumnType(header->intType);
	m_minValue  = header->minValue;
	m_dedup.risk_set_data((byte*)(header+1), header->uniqNum, header->intBits);
	if (header->uniqNum != rows) {
		assert(header->uniqNum < rows);
		size_t indexBits = terark_bsr_u32(header->uniqNum - 1) + 1;
		auto   indexData = (byte*)(header+1) + m_dedup.mem_size();
		m_index.risk_set_data(indexData, rows, indexBits);
	}
}

void ZipIntStore::save(PathRef path) const {
	auto fpath = path + ".zint";
	NativeDataOutput<FileStream> dio;
	dio.open(fpath.string().c_str(), "wb");
	ZipIntStoreHeader header;
	header.rows = uint32_t(numDataRows());
	header.uniqNum = m_dedup.size();
	header.intBits = byte(m_dedup.uintbits());
	header.intType = byte(m_intType);
	header.padding1 = 0;
	header.padding2 = 0;
	header.padding3 = 0;
	header.minValue = int64_t(m_minValue);
	dio.ensureWrite(&header, sizeof(header));
	dio.ensureWrite(m_dedup.data(), m_dedup.mem_size());
	dio.ensureWrite(m_index.data(), m_index.mem_size());
}

}} // namespace terark::db
