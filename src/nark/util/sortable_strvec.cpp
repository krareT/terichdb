#include "sortable_strvec.hpp"
#include <nark/radix_sort.hpp>
#include <nark/gold_hash_map.hpp>

// memcpy on gcc-4.9+ linux fails on some corner case
// so use memmove, it is always ok
// #define memcpyForward memcpy
#define memcpyForward memmove

namespace nark {

size_t SortableStrVec::sync_real_str_size() {
	size_t len = 0;
	for (auto& x : m_index) len += x.length;
	m_real_str_size = len;
	return len;
}

fstring SortableStrVec::operator[](size_t idx) const {
	assert(idx < m_index.size());
	size_t offset = m_index[idx].offset;
	size_t length = m_index[idx].length;
	return fstring(m_strpool.data() + offset, length);
}

void SortableStrVec::swap(SortableStrVec& y) {
	m_index.swap(y.m_index);
	m_strpool.swap(y.m_strpool);
}

void SortableStrVec::push_back(fstring str) {
	assert(str.size() < MAX_STR_LEN);
	SEntry tmp;
	tmp.offset = m_strpool.size();
	tmp.length = uint32_t(str.size());
	tmp.seq_id = uint32_t(m_index.size());
	m_index.push_back(tmp);
	m_strpool.append(str);
}

void SortableStrVec::pop_back() {
	assert(m_index.size() >= 1);
	m_strpool.resize_no_init(m_index.back().offset);
	m_index.pop_back();
}

void SortableStrVec::back_append(fstring str) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + str.size() < MAX_STR_LEN);
	m_index.back().length += str.size();
	m_strpool.append(str);
}

void SortableStrVec::back_shrink(size_t nShrink) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length >= nShrink);
	m_index.back().length -= nShrink;
	m_strpool.resize_no_init(m_strpool.size() - nShrink);
}

void SortableStrVec::back_grow_no_init(size_t nGrow) {
	assert(m_index.size() >= 1);
	assert(m_index.back().length + nGrow < MAX_STR_LEN);
	m_strpool.resize_no_init(m_strpool.size() + nGrow);
	m_index.back().length += nGrow;
}

void SortableStrVec::reverse_keys() {
	byte* base = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		assert(length < MAX_STR_LEN);
		std::reverse(base + offset, base + offset + length);
	}
}

void SortableStrVec::sort() {
	const byte* pool = m_strpool.data();
	double avgLen = double(m_strpool.size()+1) / double(m_index.size() + 1);
	double minRadixSortStrLen = UINT32_MAX; // disable radix sort by default
	if (const char* env = getenv("SortableStrVec_minRadixSortStrLen")) {
		minRadixSortStrLen = atof(env);
	}
	if (avgLen < minRadixSortStrLen) {
		std::sort(m_index.begin(), m_index.end(),
		[pool](const SEntry& x, const SEntry& y) {
			fstring sx(pool + x.offset, x.length);
			fstring sy(pool + y.offset, y.length);
			return sx < sy;
		});
	} else { // use radix sort
		auto getChar = [pool](const SEntry& x,size_t i){return pool[x.offset+i];};
		auto getSize = [](const SEntry& x) { return x.length; };
		radix_sort_tpl(m_index.data(), m_index.size(), getChar, getSize);
	}
}

void SortableStrVec::clear() {
	m_strpool.clear();
	m_index.clear();
}

struct CompareBy_offset {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.offset < y.offset;
	}
};
struct CompareBy_seq_id {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.seq_id < y.seq_id;
	}
};
struct CompareBy_lex_lenDesc {
	const byte* base;
	CompareBy_lex_lenDesc(const byte* base1) : base(base1) {}
	template<class T>
	bool operator()(const T& x, const T& y) const {
		size_t xlen = x.length, ylen = y.length;
		int ret = memcmp(base + x.offset, base + y.offset, std::min(xlen, ylen));
		if (ret) return ret < 0;
		else     return xlen > ylen; // lenDesc
	}
};
struct CompareBy_pos_lenDesc {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		size_t xpos = x.offset, ypos = y.offset;
		if (xpos != ypos) return xpos < ypos;
		return x.length > y.length; // lenDesc
	}
};

void SortableStrVec::sort_by_offset() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_offset());
}
void SortableStrVec::sort_by_seq_id() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_seq_id());
}

void SortableStrVec::build_subkeys(valvec<SEntry>& subkeys) {
	byte* base = m_strpool.data();
	// to reuse m_strpool, must sort by offset
	std::sort(subkeys.begin(), subkeys.end(), CompareBy_offset());
	size_t offset = 0;
	for(size_t i = 0; i < subkeys.size(); ++i) {
		SEntry s = subkeys[i];
		memcpyForward(base + offset, base + s.offset, s.length);
		subkeys[i].offset = uint32_t(offset);
		offset += s.length;
	}
	assert(offset <= m_strpool.size());
	m_strpool.risk_set_size(offset);
	m_strpool.shrink_to_fit();
	m_index.swap(subkeys);
	m_index.shrink_to_fit();
	subkeys.clear();
}

void SortableStrVec::compact() {
	byte* base = m_strpool.data();
	// to reuse m_strpool, must sort by offset
	std::sort(m_index.begin(), m_index.end(), CompareBy_offset());
	size_t offset = 0;
	for(size_t i = 0; i < m_index.size(); ++i) {
		SEntry s = m_index[i];
		memcpyForward(base + offset, base + s.offset, s.length);
		m_index[i].offset = offset;
		offset += s.length;
	}
	assert(offset <= m_strpool.size());
	m_strpool.risk_set_size(offset);
	m_strpool.shrink_to_fit();
	m_index.shrink_to_fit();
}

// only load first Int, if len is too small, set other bytes in Int as zero
template<class Uint, size_t Len>
inline Uint binaryToIntHigh(const unsigned char* bin) {
	Uint val = 0;
	memcpy(&val, bin, sizeof(Uint) < Len ? sizeof(Uint) : Len);
#if defined(BOOST_LITTLE_ENDIAN)
	return val << (sizeof(Uint) - Len) * 8;
#else
	return val;
#endif
}

void SortableStrVec::compress_strpool(int compressLevel) {
	if (m_index.size() < 1)
		return;

	if (compressLevel < 1)
		return;
	compress_strpool_level_1();

	// 2 or 3
	if (compressLevel == 2)
		compress_strpool_level_2();
	else if (compressLevel >= 3)
		compress_strpool_level_3();
}

void SortableStrVec::compress_strpool_level_1() {
	std::sort(m_index.begin(), m_index.end(), CompareBy_lex_lenDesc(m_strpool.data()));
	valvec<byte> strpool(m_strpool.size() + 3, valvec_reserve());
	for (size_t i = 0; i < m_index.size(); ++i) {
		strpool.append((*this)[i]);
	}
	size_t offset = 0;
	for (size_t i = 0; i < m_index.size(); ++i) {
		m_index[i].offset = offset;
		offset += m_index[i].length;
	}
	offset = m_index[0].endpos();
	for (size_t i = 1; i < m_index.size(); ++i) { // dedup loop
		size_t curlen = m_index[i].length;
		if (m_index[i-1].length >= curlen &&
				memcmp(&strpool[m_index[i].offset],
					   &strpool[m_index[i-1].offset], curlen) == 0) {
			m_index[i].offset = m_index[i-1].offset;
		} else {
			memcpyForward(&strpool[offset], &strpool[m_index[i].offset], curlen);
			m_index[i].offset = offset;
			offset += curlen;
		}
	}
	assert(offset <= strpool.size());
	if (const char* env = getenv("SortableStrVec_statCompressLevel1")) {
		int showIt = atoi(env);
		if (showIt) {
			long oldsize = strpool.size(), newsize = offset;
			fprintf(stderr, "SortableStrVec::compress1: oldsize=%ld newsize=%ld\n", oldsize, newsize);
		}
	}
	strpool.risk_set_size(offset + 3);
	strpool.fill(offset, 3, 0);
	strpool.shrink_to_fit();
	strpool.risk_set_size(offset);
	m_strpool.swap(strpool);
}

// dup strings never overlap
void SortableStrVec::compress_strpool_level_2() {
	uint32_t nBucket = (uint32_t)__hsm_stl_next_prime(m_strpool.size());
	AutoFree<uint32_t> bucket(nBucket, UINT32_MAX);
	AutoFree<uint32_t> hhlink(m_strpool.size(), UINT32_MAX);
	size_t zippedSize = 0;
	size_t prevOldPos = size_t(-1);
	byte* pool = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		assert(m_index[i].length >= 3);
		size_t len = m_index[i].length;
		size_t pos = m_index[i].offset;
		if (pos == prevOldPos) {
			assert(i > 0);
			assert(len <= m_index[i-1].length); // compress_level_1 ensures this
			m_index[i].offset = m_index[i-1].offset;
			continue;
		}
		prevOldPos = pos;
		if (len <= 3) {
			size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit;
				assert(hit + 3 <= zippedSize);
				if (memcmp(pool + hit, pool + pos, 3) == 0) {
					m_index[i].offset = hit;
					zippedSize = std::max(zippedSize, hit + 3);
					goto Found3;
				}
				pHit = &hhlink[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			*pHit = zippedSize; // append to hash list end, zippedSize is new pos
		} else {
			for (size_t j = 0; j < len - 2; ++j) { // never overlap
				size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos+j) % nBucket;
				uint32_t* pHit = &bucket[hashMod];
				while (UINT32_MAX != *pHit) {
					size_t hit = *pHit;
					if (memcmp(pool + hit, pool + pos, 3) == 0)
						goto Found3_n;
					pHit = &hhlink[hit];
				}
				*pHit = zippedSize + j;
			Found3_n:;
			}
		}
		m_index[i].offset = zippedSize;
		memcpyForward(pool + zippedSize, pool + pos, len);
		zippedSize += len;
	Found3:;
	}
	fprintf(stderr, "zipLen3: oldlen=%d newlen=%d\n", (int)m_strpool.size(), (int)zippedSize);
	std::fill_n(bucket.p, nBucket, UINT32_MAX);
	std::fill_n(hhlink.p, zippedSize, UINT32_MAX);
	m_strpool.risk_set_size(zippedSize);

	// may be a very long string preceding many short strings which are
	// all reference to a substring of the long string
	size_t prevMaxLen = 0;          // the long string's len
	size_t prevNewPos = size_t(-1); // the long string's new pos
	prevOldPos = m_strpool.size();  // the long string's old pos
	zippedSize = 0;
	std::sort(m_index.begin(), m_index.end(), CompareBy_pos_lenDesc());
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t const len = m_index[i].length;
		size_t const pos = m_index[i].offset;
		assert(len >= 3);
		if (pos >= prevOldPos && pos + len <= prevOldPos + prevMaxLen) {
			assert(i > 0);
			m_index[i].offset = prevNewPos + (pos - prevOldPos);
			continue;
		}
		if (len == 3) {
			m_index[i].offset = zippedSize;
			memcpyForward(pool + zippedSize, pool + pos, 3);
			zippedSize += 3;
			continue;
		}
		assert(len >= 4);
		size_t const maxZipLen = 8; // long string is slow and unlikely to be zipped
		size_t const prefixLen = std::min(len, maxZipLen);
		if (len <= maxZipLen) {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
				assert(hit + 4 <= zippedSize);
				if (hit + len <= zippedSize &&
						memcmp(pool + hit, pool + pos, len) == 0) {
					m_index[i].offset = hit;
					goto Found4;
				}
				pHit = &hhlink[hit];
			}
			// Not found, insert it
			*pHit = zippedSize; // append to hash list end
		}
		for(size_t j = (len <= maxZipLen) ? 1 : 0; j < len - 3; ++j) {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos+j) % nBucket;
			uint32_t* pHit = &bucket[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit;
				if (memcmp(pool + hit, pool + pos+j, prefixLen) == 0)
					goto Found_prefixLen;
				pHit = &hhlink[hit];
			}
			// Not found, insert it
			*pHit = zippedSize + j; // append to hash list end
		Found_prefixLen:;
		}
		prevMaxLen = len;
		prevOldPos = pos;
		prevNewPos = zippedSize;
		m_index[i].offset = zippedSize;
		memcpyForward(pool + zippedSize, pool + pos, len);
		zippedSize += len;
	Found4:;
	}
	m_strpool.resize_no_init(zippedSize);
}

// dup strings maybe overlap
void SortableStrVec::compress_strpool_level_3() {
	uint32_t nBucket = (uint32_t)__hsm_stl_next_prime(m_strpool.size());
	AutoFree<uint32_t> bucket3(nBucket, UINT32_MAX);
	AutoFree<uint32_t> bucket4(nBucket, UINT32_MAX);
	AutoFree<uint32_t> hhlink3(m_strpool.size(), UINT32_MAX);
	AutoFree<uint32_t> hhlink4(m_strpool.size(), UINT32_MAX);
	size_t zippedSize = 0;
	size_t prevOldPos = size_t(-1);
	byte* pool = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t len = m_index[i].length;
		size_t pos = m_index[i].offset;
		if (pos == prevOldPos) {
			assert(i > 0);
			m_index[i].offset = m_index[i-1].offset;
			continue;
		}
		prevOldPos = pos;
		if (len < 4) {
			assert(3 == len);
			size_t hashMod = binaryToIntHigh<uint32_t, 3>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket3[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
			// range[zippedSize-minStrLen, zippedSize) would never be hitted
				assert(hit + 3 < zippedSize);
				if (memcmp(pool + hit, pool + pos, 3) == 0) {
					m_index[i].offset = hit;
					zippedSize = std::max(zippedSize, hit + 3);
					goto Found;
				}
				pHit = &hhlink3[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			memmove(pool + zippedSize, pool + pos, 3);
			pos = m_index[i].offset = zippedSize;
			*pHit = pos; // append to hash list end
		}
		else {
			size_t hashMod = unaligned_load<uint32_t>(pool + pos) % nBucket;
			uint32_t* pHit = &bucket4[hashMod];
			while (UINT32_MAX != *pHit) {
				size_t hit = *pHit; // hit may overlap with 'i'
			// range[zippedSize-minStrLen, zippedSize) would never be hitted
				assert(hit + 3 < zippedSize);
				if (hit + len <= zippedSize) {
					if (memcmp(pool + hit, pool + pos, len) == 0) {
						m_index[i].offset = hit;
						zippedSize = std::max(zippedSize, hit + len);
						goto Found;
					}
				} else { // this would be a very rare case and hard to handle
					// and the compression rate gain would be very little
				}
				pHit = &hhlink4[hit];
			}
		// Not found, insert it
			assert(zippedSize <= pos);
			memmove(pool + zippedSize, pool + pos, len);
			pos = m_index[i].offset = zippedSize;
			*pHit = pos; // append to hash list end

	// just insert once
	#define HASH_INSERT_HEAD_3(beg, end) \
			for (size_t k = beg; k < end; ++k) { \
				hashMod = binaryToIntHigh<uint32_t, 3>(pool + k) % nBucket; \
				size_t hit = bucket3[hashMod]; \
				while (UINT32_MAX != hit) { \
					if (memcmp(pool + hit, pool + pos, 3) == 0) \
						goto BOOST_PP_CAT(Found_, __LINE__); \
					hit = hhlink3[hit]; \
				} \
				hhlink3[k] = bucket3[hashMod]; \
				bucket3[hashMod] = uint32_t(k); \
				BOOST_PP_CAT(Found_, __LINE__):; \
			}
	// insert multiple, LEN would be 4 or 8, Uint would be uint32_t or uint64_t
	#define HASH_INSERT_HEAD_4(beg, end) \
			for (size_t k = beg; k < end; ++k) { \
				hashMod = unaligned_load<uint32_t>(pool + k) % nBucket; \
				hhlink4[k] = bucket4[hashMod]; \
				bucket4[hashMod] = uint32_t(k); \
			}
			HASH_INSERT_HEAD_3(pos + 0, pos + len - 2);
			HASH_INSERT_HEAD_4(pos + 1, pos + len - 3);
			if (i) {
				HASH_INSERT_HEAD_3(pos - 2, pos);
				HASH_INSERT_HEAD_4(pos - 3, pos);
			}
		}
		zippedSize += len;
	Found:;
	}
	m_strpool.resize_no_init(zippedSize);

	bool printHistogram = false;
	if (const char* env = getenv("SortableStrVec_printHistogram")) {
		printHistogram = atoi(env) ? 1 : 0;
	}
	if (printHistogram) {
		gold_hash_map<int, int> cntHist, lenHist;
		for(size_t i = 0; i < m_index.size(); ++i) {
			size_t len = m_index[i].length;
			size_t pos = m_index[i].offset;
			for(size_t j = 0; j < len; ++j) {
				size_t hashMod = unaligned_load<uint32_t>(pool + pos + j) % nBucket;
				uint32_t hit = bucket4[hashMod];
				int listLen = 0;
				while (UINT32_MAX != hit) {
					listLen++;
					hit = hhlink4[hit];
				}
				cntHist[listLen]++;
			}
			lenHist[len]++;
		}
		typedef std::pair<int,int> ii;
		auto cmpKeyDest = [](ii x, ii y) { return x.first > y.first; };
		cntHist.sort(cmpKeyDest);
		lenHist.sort(cmpKeyDest);
		for (size_t i = cntHist.beg_i(); i < cntHist.end_i(); ++i) {
			int key = cntHist.key(i), val = cntHist.val(i);
			fprintf(stderr, "histogram: listLen=%d freq=%d\n", key, val);
		}
		for (size_t i = lenHist.beg_i(); i < lenHist.end_i(); ++i) {
			int key = lenHist.key(i), val = lenHist.val(i);
			fprintf(stderr, "histogram: strLen=%d freq=%d\n", key, val);
		}
	}
}

void SortableStrVec::make_ascending_offset() {
	valvec<byte> tmp(m_strpool.size(), valvec_reserve());
	const byte* oldptr = m_strpool.data();
	for(size_t i = 0; i < m_index.size(); ++i) {
		size_t offset = m_index[i].offset;
		size_t length = m_index[i].length;
		m_index[i].offset = tmp.size();
		tmp.append(oldptr + offset, length);
	}
	m_strpool.swap(tmp);
}

size_t SortableStrVec::lower_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset < offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::upper_bound_by_offset(size_t offset) const {
	SEntry const* a = m_index.data();
	size_t lo = 0, hi = m_index.size();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (a[mid].offset <= offset)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::lower_bound_at_pos(size_t lo, size_t hi, size_t pos) const {
	assert(lo < hi);
	assert(hi <= m_index.size());
	assert(pos < m_index[lo].length);
	SEntry const* a = m_index.data();
	byte   const* s = m_strpool.data();
	byte   const ch = s[a[lo].offset + pos];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (s[a[mid].offset + pos] < ch)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos) const {
	assert(lo < hi);
	assert(hi <= m_index.size());
	assert(pos < m_index[lo].length);
	SEntry const* a = m_index.data();
	byte   const* s = m_strpool.data();
	byte   const ch = s[a[lo].offset + pos];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (s[a[mid].offset + pos] <= ch)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

size_t SortableStrVec::lower_bound(fstring key) const {
	return lower_bound_0<const SortableStrVec&>(*this, m_index.size(), key);
}

size_t SortableStrVec::upper_bound(fstring key) const {
	return upper_bound_0<const SortableStrVec&>(*this, m_index.size(), key);
}

/////////////////////////////////////////////////////////////////////

DataStore::DataStore() {
	m_numRecords = 0;
}
DataStore::~DataStore() {
}

void DataStore::risk_swap(DataStore& y) {
	std::swap(m_numRecords, y.m_numRecords);
}

void DataStore::get_record(size_t recID, valvec<byte_t>* recData) const {
	recData->erase_all();
	get_record_append(recID, recData);
}

valvec<byte_t> DataStore::get_record(size_t recID) const {
	valvec<byte_t> data;
	get_record(recID, &data);
	return data;
}

} // namespace nark

