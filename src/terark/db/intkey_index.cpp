#include "intkey_index.hpp"
#include <terark/util/sortable_strvec.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/util/mmap.hpp>
#include <terark/num_to_str.hpp>

namespace terark { namespace db {

ZipIntKeyIndex::ZipIntKeyIndex(const Schema& schema) : m_schema(schema) {
	TERARK_RT_assert(schema.columnNum() == 1, std::invalid_argument);
	m_keyType = m_schema.getColumnType(0);
	m_minKey = 0;
	m_isOrdered = true;
	m_mmapBase = nullptr;
	m_mmapSize = 0;
}
ZipIntKeyIndex::~ZipIntKeyIndex() {
	if (m_mmapBase) {
		m_keys.risk_release_ownership();
		m_index.risk_release_ownership();
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

ReadableIndex* ZipIntKeyIndex::getReadableIndex() {
	return this;
}

ReadableStore* ZipIntKeyIndex::getReadableStore() {
	return this;
}

///@{ ordered and unordered index
llong ZipIntKeyIndex::indexStorageSize() const {
	return m_keys.mem_size() + m_index.mem_size();
}

template<class Int>
size_t ZipIntKeyIndex::IntVecLowerBound(fstring binkey) const {
	assert(binkey.size() == sizeof(Int));
	Int rawkey = unaligned_load<Int>(binkey.data());
	if (rawkey <= Int(m_minKey)) {
		return 0;
	}
	auto indexData = m_index.data();
	auto indexBits = m_index.uintbits();
	auto indexMask = m_index.uintmask();
	auto keysData = m_keys.data();
	auto keysBits = m_keys.uintbits();
	auto keysMask = m_keys.uintmask();
	ullong key = ullong(rawkey - Int(m_minKey));
	size_t i = 0, j = m_index.size();
	while (i < j) {
		size_t mid = (i + j) / 2;
		size_t hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid);
		ullong hitKey = UintVecMin0::fast_get(keysData, keysBits, keysMask, hitPos);
		if (hitKey < key)
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}

void ZipIntKeyIndex::searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const {
	std::pair<size_t, size_t> ib = searchEqualRange(key);
	for (size_t j = ib.first; j < ib.second; ++j) {
		recIdvec->push_back(m_index.get(j));
	}
}

size_t ZipIntKeyIndex::searchLowerBound(fstring key) const {
	switch (m_keyType) {
	default:
		THROW_STD(invalid_argument, "Bad m_keyType=%s", Schema::columnTypeStr(m_keyType));
	case ColumnType::Sint08 : return IntVecLowerBound< int8_t >(key); break;
	case ColumnType::Uint08 : return IntVecLowerBound<uint8_t >(key); break;
	case ColumnType::Sint16 : return IntVecLowerBound< int16_t>(key); break;
	case ColumnType::Uint16 : return IntVecLowerBound<uint16_t>(key); break;
	case ColumnType::Sint32 : return IntVecLowerBound< int32_t>(key); break;
	case ColumnType::Uint32 : return IntVecLowerBound<uint32_t>(key); break;
	case ColumnType::Sint64 : return IntVecLowerBound< int64_t>(key); break;
	case ColumnType::Uint64 : return IntVecLowerBound<uint64_t>(key); break;
	case ColumnType::VarSint: return IntVecLowerBound< int64_t>(key); break;
	case ColumnType::VarUint: return IntVecLowerBound<uint64_t>(key); break;
	}
	abort();
	return -1;
}

template<class Int>
size_t ZipIntKeyIndex::IntVecUpperBound(fstring binkey) const {
	assert(binkey.size() == sizeof(Int));
	Int rawkey = unaligned_load<Int>(binkey.data());
	if (rawkey < Int(m_minKey)) {
		return 0;
	}
	auto indexData = m_index.data();
	auto indexBits = m_index.uintbits();
	auto indexMask = m_index.uintmask();
	auto keysData = m_keys.data();
	auto keysBits = m_keys.uintbits();
	auto keysMask = m_keys.uintmask();
	size_t key = size_t(rawkey - Int(m_minKey));
	size_t i = 0, j = m_index.size();
	while (i < j) {
		size_t mid = (i + j) / 2;
		size_t hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid);
		ullong hitKey = UintVecMin0::fast_get(keysData, keysBits, keysMask, hitPos);
		if (hitKey <= key)
			i = mid + 1;
		else
			j = mid;
	}
	return i;
}

size_t ZipIntKeyIndex::searchUpperBound(fstring key) const {
	switch (m_keyType) {
	default:
		THROW_STD(invalid_argument, "Bad m_keyType=%s", Schema::columnTypeStr(m_keyType));
	case ColumnType::Sint08 : return IntVecUpperBound< int8_t >(key); break;
	case ColumnType::Uint08 : return IntVecUpperBound<uint8_t >(key); break;
	case ColumnType::Sint16 : return IntVecUpperBound< int16_t>(key); break;
	case ColumnType::Uint16 : return IntVecUpperBound<uint16_t>(key); break;
	case ColumnType::Sint32 : return IntVecUpperBound< int32_t>(key); break;
	case ColumnType::Uint32 : return IntVecUpperBound<uint32_t>(key); break;
	case ColumnType::Sint64 : return IntVecUpperBound< int64_t>(key); break;
	case ColumnType::Uint64 : return IntVecUpperBound<uint64_t>(key); break;
	case ColumnType::VarSint: return IntVecUpperBound< int64_t>(key); break;
	case ColumnType::VarUint: return IntVecUpperBound<uint64_t>(key); break;
	}
	abort();
	return -1;
}

template<class Int>
std::pair<size_t, size_t>
ZipIntKeyIndex::IntVecEqualRange(fstring binkey) const {
	assert(binkey.size() == sizeof(Int));
	Int rawkey = unaligned_load<Int>(binkey.data());
	if (rawkey < Int(m_minKey)) {
		return std::make_pair(0, false);
	}
	auto indexData = m_index.data();
	auto indexBits = m_index.uintbits();
	auto indexMask = m_index.uintmask();
	auto keysData = m_keys.data();
	auto keysBits = m_keys.uintbits();
	auto keysMask = m_keys.uintmask();
	size_t key = size_t(rawkey - Int(m_minKey));
	size_t i = 0, j = m_index.size();
	size_t mid = 0;
	while (i < j) {
		mid = (i + j) / 2;
		size_t hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid);
		size_t hitKey = UintVecMin0::fast_get(keysData, keysBits, keysMask, hitPos);
		if (hitKey < key)
			i = mid + 1;
		else if (hitKey > key)
			j = mid;
		else
			goto Found;
	}
	return std::make_pair(i, i);
Found:
	size_t lo = i, hi = mid;
	while (lo < hi) {
		size_t mid2 = (lo + hi) / 2;
		size_t hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid2);
		size_t hitKey = UintVecMin0::fast_get(keysData, keysBits, keysMask, hitPos);
		if (hitKey < key) // for lower_bound
			lo = mid2 + 1;
		else
			hi = mid2;
	}
	i = lo;
	lo = mid + 1, hi = j;
	while (lo < hi) {
		size_t mid2 = (lo + hi) / 2;
		size_t hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid2);
		size_t hitKey = UintVecMin0::fast_get(keysData, keysBits, keysMask, hitPos);
		if (hitKey <= key) // for upper_bound
			lo = mid2 + 1;
		else
			hi = mid2;
	}
	return std::make_pair(i, hi);
}

std::pair<size_t, size_t>
ZipIntKeyIndex::searchEqualRange(fstring key) const {
	switch (m_keyType) {
	default:
		THROW_STD(invalid_argument, "Bad m_keyType=%s", Schema::columnTypeStr(m_keyType));
	case ColumnType::Sint08 : return IntVecEqualRange< int8_t >(key); break;
	case ColumnType::Uint08 : return IntVecEqualRange<uint8_t >(key); break;
	case ColumnType::Sint16 : return IntVecEqualRange< int16_t>(key); break;
	case ColumnType::Uint16 : return IntVecEqualRange<uint16_t>(key); break;
	case ColumnType::Sint32 : return IntVecEqualRange< int32_t>(key); break;
	case ColumnType::Uint32 : return IntVecEqualRange<uint32_t>(key); break;
	case ColumnType::Sint64 : return IntVecEqualRange< int64_t>(key); break;
	case ColumnType::Uint64 : return IntVecEqualRange<uint64_t>(key); break;
	case ColumnType::VarSint: return IntVecEqualRange< int64_t>(key); break;
	case ColumnType::VarUint: return IntVecEqualRange<uint64_t>(key); break;
	}
	abort();
	return {};
}

///@}

llong ZipIntKeyIndex::dataStorageSize() const {
	return m_keys.mem_size() + m_index.mem_size();
}

llong ZipIntKeyIndex::dataInflateSize() const {
	switch (m_keyType) {
	default:
		THROW_STD(invalid_argument,
			"Bad m_keyType=%s", Schema::columnTypeStr(m_keyType));
	case ColumnType::Sint08:
	case ColumnType::Uint08: return 1 * m_keys.size();
	case ColumnType::Sint16: 
	case ColumnType::Uint16: return 2 * m_keys.size();
	case ColumnType::Sint32:
	case ColumnType::Uint32: return 4 * m_keys.size();
	case ColumnType::Sint64:
	case ColumnType::Uint64: return 8 * m_keys.size();
	}
}

llong ZipIntKeyIndex::numDataRows() const {
	return m_keys.size();
}

template<class Int>
void ZipIntKeyIndex::keyAppend(size_t recIdx, valvec<byte>* key) const {
	Int iKey = Int(m_minKey + m_keys.get(recIdx));
	unaligned_save<Int>(key->grow_no_init(sizeof(Int)), iKey);
}

void ZipIntKeyIndex::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	size_t idx = size_t(id);
	assert(idx < m_keys.size());
	switch (m_keyType) {
	default:
		THROW_STD(invalid_argument, "Bad m_keyType=%s", Schema::columnTypeStr(m_keyType));
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
		byte* end = save_var_int64(buf, int64_t(m_minKey + m_keys.get(idx)));
		val->append(buf, end - buf);
		break; }
	case ColumnType::VarUint: {
		byte  buf[16];
		byte* end = save_var_uint64(buf, uint64_t(m_minKey + m_keys.get(idx)));
		val->append(buf, end - buf);
		break; }
	}
}

StoreIterator* ZipIntKeyIndex::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* ZipIntKeyIndex::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

template<class Int>
void ZipIntKeyIndex::zipKeys(const void* data, size_t size) {
	size_t rows = size / sizeof(Int);
	assert(size % sizeof(Int) == 0);
	const Int* keys = (Int*)data;
	m_minKey = m_keys.build_from(keys, rows);
#if !defined(NDEBUG)
	assert(m_keys.size() == rows);
	for (size_t i = 0; i < rows; ++i) {
		assert(Int(m_minKey + m_keys[i]) == keys[i]);
	}
#endif
}

void ZipIntKeyIndex::build(ColumnType keyType, SortableStrVec& strVec) {
	assert(strVec.m_index.size() == 0);
	m_keyType = keyType;
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
	valvec<uint32_t> index(m_keys.size(), valvec_no_init());
	for (size_t i = 0; i < index.size(); ++i) index[i] = uint32_t(i);
	std::sort(index.begin(), index.end(), [&](size_t x, size_t y) {
		size_t xkey = m_keys.get(x);
		size_t ykey = m_keys.get(y);
		if (xkey < ykey) return true;
		if (xkey > ykey) return false;
		return x < y;
	});
	auto minIdx = m_index.build_from(index);
	(void)minIdx;
#if !defined(NDEBUG)
	assert(0 == minIdx);
	for(size_t i = 1; i < m_index.size(); ++i) {
		size_t xi = m_index.get(i-1);
		size_t yi = m_index.get(i-0);
		size_t xk = m_keys.get(xi);
		size_t yk = m_keys.get(yi);
		assert(xk <= yk);
	}
#endif
}

namespace {
	struct Header {
		uint32_t rows;
		uint8_t  keyBits;
		uint8_t  keyType;
		uint8_t  isUnique;
		uint8_t  padding;
		 int64_t minKey;
	};
	BOOST_STATIC_ASSERT(sizeof(Header) == 16);
}

void ZipIntKeyIndex::load(PathRef path) {
	auto fpath = path + ".zint";
	bool writable = false;
	m_mmapBase = (byte_t*)mmap_load(fpath.string(), &m_mmapSize, writable, m_schema.m_mmapPopulate);
	auto h = (const Header*)m_mmapBase;
	m_isUnique   = h->isUnique ? true : false;
	m_keyType    = ColumnType(h->keyType);
	m_minKey     = h->minKey;
	size_t indexBits = h->rows <= 1 ? 0 : terark_bsr_u64(h->rows - 1) + 1;
	m_keys .risk_set_data((byte*)(h+1)                    , h->rows, h->keyBits);
	m_index.risk_set_data((byte*)(h+1) + m_keys.mem_size(), h->rows,  indexBits);
}

void ZipIntKeyIndex::save(PathRef path) const {
	auto fpath = path + ".zint";
	NativeDataOutput<FileStream> dio;
	dio.open(fpath.string().c_str(), "wb");
	Header h;
	h.rows     = m_index.size();
	h.keyBits  = m_keys.uintbits();
	h.keyType  = uint8_t(m_keyType);
	h.isUnique = m_isUnique;
	h.padding  = 0;
	h.minKey   = m_minKey;
	dio.ensureWrite(&h, sizeof(h));
	dio.ensureWrite(m_keys .data(), m_keys .mem_size());
	dio.ensureWrite(m_index.data(), m_index.mem_size());
}

class ZipIntKeyIndex::MyIndexIterForward : public IndexIterator {
public:
	size_t m_keyIdx;
	const ZipIntKeyIndex* m_owner;

	MyIndexIterForward(const ZipIntKeyIndex* owner) {
		m_keyIdx = 0;
		m_owner = owner;
	}

	void reset() override {
		m_keyIdx = 0;
	}

	bool increment(llong* id, valvec<byte>* key) override {
		assert(nullptr != key);
		if (m_keyIdx < m_owner->m_index.size()) {
			*id = m_owner->m_index.get(m_keyIdx++);
			key->erase_all();
			m_owner->getValueAppend(*id, key, nullptr);
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		assert(nullptr != retKey);
		m_keyIdx = key.empty() ? 0 : m_owner->searchLowerBound(key);
		if (m_keyIdx < m_owner->m_index.size()) {
			*id = m_owner->m_index[m_keyIdx];
			retKey->erase_all();
			m_owner->getValueAppend(*id, retKey, nullptr);
			m_keyIdx++;
			return key == *retKey ? 0 : 1;
		}
		return -1;
	}

	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override {
		assert(nullptr != retKey);
		m_keyIdx = key.empty() ? 0 : m_owner->searchUpperBound(key);
		if (m_keyIdx < m_owner->m_index.size()) {
			*id = m_owner->m_index[m_keyIdx];
			retKey->erase_all();
			m_owner->getValueAppend(*id, retKey, nullptr);
			m_keyIdx++;
			return 1;
		}
		return -1;
	}
};

class ZipIntKeyIndex::MyIndexIterBackward : public IndexIterator {
public:
	size_t m_keyIdx;
	const ZipIntKeyIndex* m_owner;

	MyIndexIterBackward(const ZipIntKeyIndex* owner) {
		m_keyIdx = owner->m_index.size();
		m_owner = owner;
	}

	void reset() override {
		m_keyIdx = m_owner->m_index.size();
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_keyIdx > 0) {
			*id = m_owner->m_index.get(--m_keyIdx);
			if (key) {
				key->erase_all();
				m_owner->getValueAppend(*id, key, nullptr);
			}
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		if (key.empty()) {
			m_keyIdx = m_owner->m_index.size();
		} else {
			m_keyIdx = m_owner->searchUpperBound(key);
		}
		if (m_keyIdx > 0) {
			*id = m_owner->m_index[--m_keyIdx];
			retKey->erase_all();
			m_owner->getValueAppend(*id, retKey, nullptr);
			return key == *retKey ? 0 : 1;
		}
		return -1;
	}

	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override {
		if (key.empty()) {
			m_keyIdx = m_owner->m_index.size();
		} else {
			m_keyIdx = m_owner->searchLowerBound(key);
		}
		if (m_keyIdx > 0) {
			*id = m_owner->m_index[--m_keyIdx];
			retKey->erase_all();
			m_owner->getValueAppend(*id, retKey, nullptr);
			return 1;
		}
		return -1;
	}
};

IndexIterator* ZipIntKeyIndex::createIndexIterForward(DbContext*) const {
	return new MyIndexIterForward(this);
}
IndexIterator* ZipIntKeyIndex::createIndexIterBackward(DbContext*) const {
	return new MyIndexIterBackward(this);
}

}} // namespace terark::db
