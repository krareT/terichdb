#include "fixed_len_key_index.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>

namespace nark { namespace db {

FixedLenKeyIndex::FixedLenKeyIndex() {
	m_isOrdered = true;
	m_isIndexKeyByteLex = true;
	m_mmapBase = nullptr;
	m_mmapSize = 0;
	m_fixedLen = 0;
	m_uniqKeys = 0;
}

FixedLenKeyIndex::~FixedLenKeyIndex() {
	if (m_mmapBase) {
		m_keys.risk_release_ownership();
		m_index.risk_release_ownership();
		mmap_close(m_mmapBase, m_mmapSize);
	}
}

ReadableStore* FixedLenKeyIndex::getReadableStore() {
	return this;
}

ReadableIndex* FixedLenKeyIndex::getReadableIndex() {
	return this;
}

///@{ ordered and unordered index
llong FixedLenKeyIndex::indexStorageSize() const {
	return m_index.mem_size();
}

llong FixedLenKeyIndex::searchExact(fstring key, DbContext*) const {
	std::pair<size_t, bool> ib = searchLowerBound(key);
	return ib.second ? llong(ib.first) : -1LL;
}

std::pair<size_t, bool>
FixedLenKeyIndex::searchLowerBound(fstring key) const {
	assert(key.size() == m_fixedLen);
	auto indexData = m_index.data();
	auto indexBits = m_index.uintbits();
	auto indexMask = m_index.uintmask();
	auto keysData = m_keys.data();
	size_t fixlen = m_fixedLen;
	size_t hitPos = 0;
	const byte* hitKey = nullptr;
	size_t i = 0, j = m_index.size();
	while (i < j) {
		size_t mid = (i + j) / 2;
		hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, mid);
		hitKey = keysData + fixlen * hitPos;
		if (memcmp(hitKey, key.p, fixlen) < 0)
			i = mid + 1;
		else
			j = mid;
	}
	if (i < m_index.size()) {
		hitPos = UintVecMin0::fast_get(indexData, indexBits, indexMask, i);
		hitKey = keysData + fixlen * hitPos;
		return std::make_pair(i, memcmp(hitKey, key.p, fixlen) == 0);
	}
	return std::make_pair(i, false);
}

///@}

llong FixedLenKeyIndex::dataStorageSize() const {
	return m_keys.used_mem_size() + m_index.mem_size();
}

llong FixedLenKeyIndex::numDataRows() const {
	return m_keys.size();
}

void FixedLenKeyIndex::getValueAppend(llong id, valvec<byte>* val, DbContext*) const {
	assert(id >= 0);
	size_t idx = size_t(id);
	assert(idx < m_keys.size());
	const byte* dataPtr = m_keys.data() + m_fixedLen * idx;
	val->append(dataPtr, m_fixedLen);
}

StoreIterator* FixedLenKeyIndex::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* FixedLenKeyIndex::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

void FixedLenKeyIndex::build(const Schema& schema, SortableStrVec& strVec) {
	size_t fixlen = schema.getFixedRowLen();
	byte*  data = strVec.m_strpool.data();
	size_t rows = strVec.m_strpool.size() / fixlen;
	assert(strVec.m_index.size() == 0);
	assert(strVec.m_strpool.size() % fixlen == 0);
	for (size_t i = 0; i < rows; ++i) {
		schema.byteLexConvert(data + i*fixlen, fixlen);
	}
	valvec<uint32_t> index(rows, valvec_no_init());
	for (size_t i = 0; i < rows; ++i) index[i] = i;
	std::sort(index.begin(), index.end(),
		[data,fixlen](size_t x, size_t y) {
		const byte* xkey = data + fixlen * x;
		const byte* ykey = data + fixlen * y;
		int ret = memcmp(xkey, ykey, fixlen);
		if (ret)
			return ret < 0;
		else
			return x < y;
	});
	m_fixedLen = fixlen;
	m_uniqKeys = 0;
	for(size_t i = 0; i < m_keys.size(); ) {
		size_t j = i;
		int cmp = 0;
		do {
			const byte* xkey = data + fixlen * (j + 0);
			const byte* ykey = data + fixlen * (j + 1);
			cmp = memcmp(xkey, ykey, fixlen);
			++j;
		} while (0 == cmp);
		i = j;
		m_uniqKeys++;
	}
	m_isUnique = m_uniqKeys == rows;
	auto minIdx = m_index.build_from(index);
	(void)minIdx;
	assert(0 == minIdx);
	m_keys.clear();
	m_keys.swap(strVec.m_strpool);
}

namespace {
	struct Header {
		uint32_t rows;
		uint32_t uniqKeys;
		uint32_t fixlen;
		uint32_t padding;
	};
}

void FixedLenKeyIndex::load(PathRef path) {
	auto fpath = path + ".fixlen";
	m_mmapBase = (byte_t*)mmap_load(fpath.string(), &m_mmapSize);
	auto h = (const Header*)m_mmapBase;
	m_isUnique = h->uniqKeys == h->rows;
	m_uniqKeys = h->uniqKeys;
	m_fixedLen = h->fixlen;
	size_t rbits = nark_bsr_u32(h->rows) + 1;
	size_t keyMemSize = h->fixlen * h->rows;
	m_keys .risk_set_data((byte*)(h+1) , keyMemSize);
	keyMemSize = (keyMemSize + 15) & ~15;
	m_index.risk_set_data((byte*)(h+1) + keyMemSize, h->rows, rbits);
}

void FixedLenKeyIndex::save(PathRef path) const {
	auto fpath = path + ".fixlen";
	NativeDataOutput<FileStream> dio;
	dio.open(fpath.string().c_str(), "wb");
	Header h;
	h.rows     = uint32_t(m_index.size());
	h.uniqKeys = m_uniqKeys;
	h.fixlen   = m_fixedLen;
	h.padding  = 0;
	dio.ensureWrite(&h, sizeof(h));
	byte zero[16];
	memset(zero, 0, sizeof(zero));
	dio.ensureWrite(m_keys .data(), m_keys .used_mem_size());
	if (m_keys.used_mem_size() % 16 != 0) {
		dio.ensureWrite(zero, 16 - m_keys.used_mem_size() % 16);
	}
	dio.ensureWrite(m_index.data(), m_index.mem_size());
}

class FixedLenKeyIndex::MyIndexIterForward : public IndexIterator {
public:
	size_t m_keyIdx;
	const FixedLenKeyIndex* m_owner;

	MyIndexIterForward(const FixedLenKeyIndex* owner) {
		m_keyIdx = 0;
		m_owner = owner;
	}

	void reset() override {
		m_keyIdx = 0;
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_keyIdx < m_owner->m_index.size()) {
			*id = m_owner->m_index.get(m_keyIdx++);
			if (key) {
				key->erase_all();
				m_owner->getValueAppend(*id, key, nullptr);
			}
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		std::pair<size_t, bool> ib;
		if (key.empty())
			ib = std::make_pair(0, false);
		else
			ib = m_owner->searchLowerBound(key);
		m_keyIdx = ib.first;
		if (ib.first < m_owner->m_index.size()) {
			*id = m_owner->m_index.get(ib.first);
			if (retKey) {
				retKey->erase_all();
				m_owner->getValueAppend(*id, retKey, nullptr);
			}
			m_keyIdx++;
			return ib.second ? 0 : 1;
		}
		return -1;
	}
};

class FixedLenKeyIndex::MyIndexIterBackward : public IndexIterator {
public:
	size_t m_keyIdx;
	const FixedLenKeyIndex* m_owner;

	MyIndexIterBackward(const FixedLenKeyIndex* owner) {
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
		std::pair<size_t, bool> ib;
		if (key.empty())
			ib = std::make_pair(m_owner->m_index.size(), false);
		else
			ib = m_owner->searchLowerBound(key);
		m_keyIdx = ib.first;
		if (ib.first > 0 || ib.second) {
			if (!ib.second)
				--m_keyIdx; // backward next is backward-greater than key
			*id = m_owner->m_index[m_keyIdx];
			if (retKey) {
				retKey->erase_all();
				m_owner->getValueAppend(*id, retKey, nullptr);
			}
			return ib.second ? 0 : 1;
		}
		return -1;
	}
};

IndexIterator* FixedLenKeyIndex::createIndexIterForward(DbContext*) const {
	return new MyIndexIterForward(this);
}
IndexIterator* FixedLenKeyIndex::createIndexIterBackward(DbContext*) const {
	return new MyIndexIterBackward(this);
}

}} // namespace nark::db
