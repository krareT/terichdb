#include "bitmap.hpp"
#include <nark/util/autofree.hpp>
#include <nark/util/throw.hpp>
#include <algorithm>
#include <stdexcept>

namespace nark {

void febitvec::push_back_slow_path(bool val) {
	assert(m_size % WordBits == 0);
	resize_no_init(m_size + 1);
	m_words[m_size / WordBits] = val ? 1 : 0;
}

febitvec::febitvec(size_t bits, bool val) {
	risk_release_ownership();
	resize(bits, val);
}

febitvec::febitvec(size_t bits, bool val, bool padding) {
	risk_release_ownership();
	resize_no_init(bits);
	size_t nWords = num_words();
	std::fill_n(m_words, nWords, bm_uint_t(val ? -1 : 0));
	if (bits != m_capacity && padding != val) {
		assert(bits > 0);
		if (padding)
			m_words[nWords-1] = ~((bm_uint_t(-1) >> (m_capacity-bits)) - 1);
		else
			m_words[nWords-1] =  ((bm_uint_t(-1) >> (m_capacity-bits)) - 1);
	}
}

febitvec::febitvec(size_t bits, valvec_no_init) {
	risk_release_ownership();
	resize_no_init(bits);
}

febitvec::febitvec(size_t bits, valvec_reserve) {
	risk_release_ownership();
	reserve(bits);
}

febitvec::febitvec(const febitvec& y) {
	size_t nWords = ((y.m_size + AllocUnitBits-1) & ~(AllocUnitBits-1))
				  / WordBits;
	m_words = AutoFree<bm_uint_t>(nWords, y.bldata()).release();
	m_size = y.m_size;
	m_capacity = WordBits * nWords;
}

febitvec& febitvec::operator=(const febitvec& y) {
	clear();
	new(this)febitvec(y);
	return *this;
}

#if defined(HSM_HAS_MOVE)
febitvec::febitvec(febitvec&& y) {
	m_words = y.m_words;
	m_size = y.m_size;
	m_capacity = y.m_capacity;
	y.risk_release_ownership();
}
febitvec& febitvec::operator=(febitvec&& y) {
	if (m_words)
		::free(m_words);
	m_words = y.m_words;
	m_size = y.m_size;
	m_capacity = y.m_capacity;
	y.risk_release_ownership();
	return *this;
}
#endif


febitvec::febitvec(const febitvec& y, size_t beg, size_t len) {
	assert(beg <= y.m_size);
	assert(beg + len <= y.m_size);
	if (beg + len > y.m_size) {
		THROW_STD(out_of_range
			, "beg = %zd, len = %zd, beg+len = %zd, y.size = %zd"
			, beg, len, beg+len, y.m_size);
	}
	if (0 == len) {
		new(this)febitvec();
		return;
	}
	m_size = len;
	size_t begBlock = beg / WordBits;
	size_t endBlock = (beg + len + WordBits - 1) / WordBits;
	size_t numBlock = endBlock - begBlock;
	m_capacity = (WordBits*numBlock + AllocUnitBits-1) & ~(AllocUnitBits-1);
	size_t capBlock = m_capacity / WordBits;
	AutoFree<bm_uint_t> bits(numBlock, capBlock, y.m_words + begBlock);
	size_t rightShift = beg % WordBits;
	if (rightShift) {
		size_t leftShift = WordBits - rightShift;
		for (size_t i = 0; i < numBlock-1; ++i) {
			bits.p[i] = (bits.p[i] >> rightShift) | (bits.p[i+1] << leftShift);
		}
		bits.p[numBlock-1] >>= rightShift;
	}
	m_words = bits.release();
}

febitvec::~febitvec() {
	if (m_words)
		::free(m_words);
}

void febitvec::append(const febitvec& y) {
//	resize_no_init(m_size + y.size());
// TODO: improve performance
	for (size_t i = 0, n = y.m_size; i < n; ++i)
		this->push_back(y[i]);
}

void febitvec::append(const febitvec& y, size_t beg, size_t len) {
// TODO: improve performance
	assert(beg + len <= y.m_size);
	for (size_t i = beg, end = beg + len; i < end; ++i)
		this->push_back(y[i]);
}

void febitvec::assign(const febitvec& y) {
	*this = y;
}

void febitvec::risk_memcpy(const febitvec& y) {
	assert(m_size == y.m_size);
	size_t numWords = (m_size + WordBits-1) / WordBits;
	memcpy(m_words, y.m_words, sizeof(bm_uint_t) * numWords);
}

void febitvec::copy(size_t destBeg, const febitvec& y) {
	// TODO:
	THROW_STD(invalid_argument, "Not implemeted");
}

void febitvec::copy(size_t destBeg, const febitvec& y, size_t srcBeg, size_t len) {
	// TODO:
	THROW_STD(invalid_argument, "Not implemeted");
}

void febitvec::clear() {
	if (m_words)
		::free(m_words);
	risk_release_ownership();
}
void febitvec::erase_all() {
	m_size = 0;
}
void febitvec::fill(bool val) {
	std::fill_n(m_words, num_words(), bm_uint_t(val ? -1 : 0));
}

void febitvec::grow(size_t cnt, bool val) {
	resize(m_size + cnt, val);
}

void febitvec::reserve(size_t newcap) {
	newcap = (newcap + AllocUnitBits-1) & ~(AllocUnitBits-1);
	if (newcap > m_capacity) {
		bm_uint_t* q = (bm_uint_t*)realloc(m_words, newcap/8);
		if (NULL == q)
			throw std::bad_alloc();
		m_words = q;
		m_capacity = newcap;
	}
}

void febitvec::resize(size_t newsize, bool val) {
	size_t oldsize = m_size;
	resize_no_init(newsize);
	if (oldsize < newsize)
		bits_range_set(m_words, oldsize, newsize, val);
}

void febitvec::resize_no_init(size_t newsize) {
	if (m_capacity < newsize) {
		size_t newcap = std::max(m_capacity, size_t(AllocUnitBits));
		while (newcap < newsize) newcap *= 2;
		bm_uint_t* q = (bm_uint_t*)realloc(m_words, newcap/8);
		if (NULL == q)
			throw std::bad_alloc();
		m_words = q;
		m_capacity = newcap;
	}
	m_size = newsize;
}

void febitvec::resize_fill(size_t newsize, bool val) {
	resize_no_init(newsize);
	fill(val);
}

febitvec& febitvec::operator-=(const febitvec& y) {
	ptrdiff_t nBlocks = std::min(num_words(), y.num_words());
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	for (ptrdiff_t i = 0; i < nBlocks; ++i) xdata[i] &= ~ydata[i];
	return *this;
}
febitvec& febitvec::operator^=(const febitvec& y) {
	ptrdiff_t nBlocks = std::min(num_words(), y.num_words());
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	for (ptrdiff_t i = 0; i < nBlocks; ++i) xdata[i] ^= ydata[i];
	return *this;
}
febitvec& febitvec::operator&=(const febitvec& y) {
	ptrdiff_t nBlocks = std::min(num_words(), y.num_words());
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	for (ptrdiff_t i = 0; i < nBlocks; ++i) xdata[i] &= ydata[i];
	return *this;
}
febitvec& febitvec::operator|=(const febitvec& y) {
	ptrdiff_t nBlocks = std::min(num_words(), y.num_words());
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	for (ptrdiff_t i = 0; i < nBlocks; ++i) xdata[i] |= ydata[i];
	return *this;
}

void febitvec::block_or(const febitvec& y, size_t yblstart, size_t blcnt) {
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	size_t yblfinish = yblstart + blcnt;
	for (size_t i = yblstart; i < yblfinish; ++i) xdata[i] |= ydata[i];
}

void febitvec::block_and(const febitvec& y, size_t yblstart, size_t blcnt) {
	bm_uint_t* xdata = m_words;
	bm_uint_t const* ydata = y.m_words;
	size_t yblfinish = yblstart + blcnt;
	for (size_t i = yblstart; i < yblfinish; ++i) xdata[i] &= ydata[i];
}

bool febitvec::isall0() const {
	assert(m_size > 0);
	size_t n = m_size / WordBits;
	for (size_t i = 0; i < n; ++i) {
		if (m_words[i])
			return false;
	}
	if (m_size % WordBits == 0) {
		return true;
	} else {
		bm_uint_t allone = (bm_uint_t(1) << (m_size%WordBits)) - 1;
		return (m_words[n] & allone) == 0;
	}
}
bool febitvec::isall1() const {
	assert(m_size > 0);
	size_t n = m_size / WordBits;
	for (size_t i = 0; i < n; ++i) {
		if (bm_uint_t(-1) != m_words[i])
			return false;
	}
	if (m_size % WordBits == 0) {
		return true;
	} else {
		bm_uint_t allone = (bm_uint_t(1) << (m_size%WordBits)) - 1;
		return (m_words[n] & allone) == allone;
	}
}
size_t febitvec::popcnt() const {
	if (0 == m_size)
		return 0;
	size_t pc = 0;
	size_t n = m_size / WordBits;
	for (size_t i = 0; i < n; ++i)
		pc += fast_popcount(m_words[i]);
	if (m_size % WordBits)
		pc += fast_popcount_trail(m_words[n], m_size % WordBits);
	return pc;
}
size_t febitvec::popcnt(size_t blstart, size_t blcnt) const {
	assert(blstart <= num_words());
	assert(blcnt <= num_words());
	assert(blstart + blcnt <= num_words());
	size_t pc = 0;
	for (ptrdiff_t i = blstart, n = blstart + blcnt; i < n; ++i)
		pc += fast_popcount(m_words[i]);
	return pc;
}

size_t febitvec::one_seq_len(size_t bitpos) const {
	assert(bitpos < m_size);
	const bm_uint_t* bits = m_words;
	size_t j, sum;
	if (bitpos%WordBits != 0) {
		bm_uint_t x = bits[bitpos/WordBits];
		if ( !(x & (bm_uint_t(1) << bitpos%WordBits)) ) return 0;
		bm_uint_t y = ~(x >> bitpos%WordBits);
		size_t ctz = fast_ctz(y);
		if (ctz < WordBits - bitpos%WordBits) {
			// last zero bit after bitpos is in x
			return ctz;
		}
		assert(ctz == WordBits - bitpos%WordBits);
		j = bitpos/WordBits + 1;
		sum = ctz;
	} else {
		j = bitpos/WordBits;
		sum = 0;
	}
	size_t len = num_words();
	for (; j < len; ++j) {
		bm_uint_t y = ~bits[j];
		if (0 == y) {
			sum += WordBits;
		} else {
			sum += fast_ctz(y);
			break;
		}
	}
	return sum;
}

size_t febitvec::zero_seq_len(size_t bitpos) const {
	assert(bitpos < m_size);
	const bm_uint_t* bits = m_words;
	size_t j, sum;
	if (bitpos%WordBits != 0) {
		bm_uint_t x = bits[bitpos/WordBits];
		if (x & (bm_uint_t(1) << bitpos%WordBits)) return 0;
		bm_uint_t y = (x >> bitpos%WordBits);
		if (y) {
			return fast_ctz(y);
		}
		j = bitpos/WordBits + 1;
		sum = WordBits - bitpos%WordBits;
	} else {
		j = bitpos/WordBits;
		sum = 0;
	}
	size_t len = num_words();
	for (; j < len; ++j) {
		bm_uint_t x = bits[j];
		if (0 == x) {
			sum += WordBits;
		} else {
			sum += fast_ctz(x);
			break;
		}
	}
	return sum;
}

size_t febitvec::one_seq_revlen(size_t endpos) const {
	assert(endpos <= m_size);
	const bm_uint_t* bits = m_words;
	size_t sum = 0;
	if (endpos%WordBits != 0) {
		bm_uint_t x = bits[(endpos-1)/WordBits];
		if ( !(x & (bm_uint_t(1) << (endpos-1)%WordBits)) ) return 0;
		bm_uint_t y = ~(x << (WordBits - endpos%WordBits));
		// on core-i7 which don't support `lzcnt`
		// the `lzcnt` performs same as `bsr`.
		//
		// For reference the opcodes are:
		// BSR: 0xBD
		// LZCNT: 0xBD (Same as BSR but has a prefix of 0xF3)
		// Detail: https://software.intel.com/en-us/forums/topic/293866
		size_t clz  = fast_clz(y);
		assert(clz <= endpos%WordBits);
		assert(clz >= 1);
		if (clz < endpos%WordBits) {
			return clz; // just it
		}
		sum = clz;
	}
	for(size_t j = endpos/WordBits; j > 0; ) {
		j--;
		bm_uint_t y = ~bits[j];
		if (0 == y) {
			sum += WordBits;
		} else {
			sum += fast_clz(y);
			break;
		}
	}
	return sum;
}

size_t febitvec::zero_seq_revlen(size_t endpos) const {
	assert(endpos <= m_size);
	const bm_uint_t* bits = m_words;
	size_t sum = 0;
	if (endpos%WordBits != 0) {
		bm_uint_t x = bits[(endpos-1)/WordBits];
		if (x & (bm_uint_t(1) << (endpos-1)%WordBits)) return 0;
		bm_uint_t y = x << (WordBits - endpos%WordBits);
		if (y) {
			return fast_clz(y);
		}
		sum = endpos%WordBits;
	}
	for(size_t j = endpos/WordBits; j > 0; ) {
		j--;
		bm_uint_t y = bits[j];
		if (0 == y) {
			sum += WordBits;
		} else {
			sum += fast_clz(y);
			break;
		}
	}
	return sum;
}


void febitvec::set0(size_t i, size_t nbits) {
	assert(i < m_size);
	assert(i + nbits <= m_capacity);
	bits_range_set0(m_words, i, i + nbits);
}

void febitvec::set1(size_t i, size_t nbits) {
	assert(i < m_size);
	assert(i + nbits <= m_capacity);
	bits_range_set1(m_words, i, i + nbits);
}

void febitvec::set(size_t i, size_t nbits, bool val) {
	assert(i < m_size);
	assert(i + nbits <= m_capacity);
	bits_range_set(m_words, i, i + nbits, val);
}

void febitvec::risk_release_ownership() {
	m_words = NULL;
	m_size = 0;
	m_capacity = 0;
}

void febitvec::shrink_to_fit() {
	assert(m_size <= m_capacity);
	if (NULL == m_words) {
		assert(0 == m_size);
		assert(0 == m_capacity);
		return;
	}
	assert(m_capacity > 0);
	assert(m_capacity % AllocUnitBits == 0);
	if (0 == m_size) {
		::free(m_words);
		m_words = NULL;
		m_capacity = 0;
	}
	size_t ceiled_bits = (m_size + AllocUnitBits-1) & ~(AllocUnitBits-1);
	m_words = (bm_uint_t*)realloc(m_words, ceiled_bits/8);
	assert(NULL != m_words);
	if (NULL == m_words) {
		// realloc is required to return non-null on shrink
		abort();
	}
	m_capacity = ceiled_bits;
}

} // namespace nark
