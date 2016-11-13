#include "rank_select.hpp"
#include "rank_select_inline.hpp"
#include <terark/util/throw.hpp>

namespace terark {

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

rank_select::rank_select() {
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
}
rank_select::rank_select(size_t n, bool val) : febitvec(n, val) {
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
}

rank_select::rank_select(size_t n, valvec_no_init) {
	febitvec::resize_no_init(n);
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
}
rank_select::rank_select(size_t n, valvec_reserve) {
	febitvec::reserve(n);
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
}
rank_select::rank_select(const rank_select& y)
	: febitvec() // call default cons, do not forward copy-cons
{
	assert(this != &y);
	assert(y.m_capacity % WordBits == 0);
	reserve(y.m_capacity);
	m_size = y.m_size;
	STDEXT_copy_n(y.m_words, y.m_capacity/WordBits, m_words);
	if (y.m_rank_cache) {
		m_rank_cache = (uint32_t*)(m_words + num_words());
		m_max_rank0 = y.m_max_rank0;
		m_max_rank1 = y.m_max_rank1;
	}
	else {
		m_rank_cache = NULL;
		m_max_rank0 = m_max_rank1 = 0;
	}
}
rank_select& rank_select::operator=(const rank_select& y) {
	if (this != &y) {
		this->clear();
		new(this)rank_select(y);
	}
	return *this;
}

#if defined(HSM_HAS_MOVE)
rank_select::rank_select(rank_select&& y) noexcept {
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
}
rank_select&
rank_select::operator=(rank_select&& y) noexcept {
	if (m_words)
		::free(m_words);
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
	return *this;
}
#endif

rank_select::~rank_select() {
}

void rank_select::clear() {
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	febitvec::clear();
}

void rank_select::risk_release_ownership() {
	// cache is built after mmap, with malloc
	// do nullize m_rank_cache
	m_rank_cache = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	febitvec::risk_release_ownership();
}

/// @param length is count in bytes
void rank_select::risk_mmap_from(unsigned char* base, size_t length) {
	// one 32-bit-rank-sum per line
	// an extra rank-sum(32), and bits-number(32)
	assert(NULL == m_words);
	size_t bits = (size_t)((uint32_t*)(base + length))[-1];
	size_t ceiled_bits = (bits + LineBits - 1) & ~(LineBits - 1);
	m_words = (bm_uint_t*)base;
	m_size = bits;
	m_capacity = length * 8;
	m_rank_cache = (uint32_t*)(m_words + ceiled_bits / LineBits);
	m_max_rank1 = m_rank_cache[ceiled_bits / LineBits];
	m_max_rank0 = m_size - m_max_rank1;
}

void rank_select::shrink_to_fit() {
	assert(NULL == m_rank_cache);
	assert(0 == m_max_rank0);
	assert(0 == m_max_rank1);
	febitvec::shrink_to_fit();
}

void rank_select::swap(rank_select& y) {
	febitvec::swap(y);
	std::swap(m_rank_cache, y.m_rank_cache);
	std::swap(m_max_rank0 , y.m_max_rank0);
	std::swap(m_max_rank1 , y.m_max_rank1);
}

/// params are just place holder
void rank_select::build_cache(bool speed_select0, bool speed_select1) {
	assert(NULL == m_rank_cache);
	if (NULL == m_words) return;
	size_t ceiled_bits = (m_size + LineBits-1) & ~(LineBits-1);
	size_t const nlines = ceiled_bits / LineBits;
	reserve(ceiled_bits + 32 * (nlines + 2));
	bits_range_set0(m_words, m_size, ceiled_bits);
	const bm_uint_t* bits = this->m_words;
	uint32_t* cache = (uint32_t*)(bits + ceiled_bits/WordBits);
	size_t Rank1 = 0;
	for(size_t i = 0; i < nlines; ++i) {
		cache[i] = uint32_t(Rank1);
		for (size_t j = 0; j < LineWords; ++j)
			Rank1 += fast_popcount(bits[i*LineWords + j]);
	}
	cache[nlines + 0] = uint32_t(Rank1);
	cache[nlines + 1] = uint32_t(m_size);
	m_rank_cache = cache;
	m_max_rank1 = Rank1;
	m_max_rank0 = m_size - Rank1;
}

size_t rank_select::mem_size() const {
	return m_capacity / 8;
}

size_t rank_select::rank1(size_t bitpos) const {
	assert(bitpos < this->size());
	size_t line_wordpos = (bitpos & ~(LineBits - 1)) / WordBits;
	size_t line_word_idxupp = bitpos / WordBits;
	size_t rank = m_rank_cache[bitpos / LineBits];
	const bm_uint_t* bits = this->bldata();
	for (size_t i = line_wordpos; i < line_word_idxupp; ++i)
		rank += fast_popcount(bits[i]);
	if (bitpos % WordBits != 0)
		rank += fast_popcount_trail(bits[line_word_idxupp], bitpos % WordBits);
	return rank;
}

#define GUARD_MAX_RANK(B, rank) \
	assert(rank < m_max_rank##B);

size_t rank_select::select1(size_t rank) const {
	GUARD_MAX_RANK(1, rank);
	const size_t  nLines = (m_size + LineBits - 1) / LineBits;
	const uint32_t* rank_cache = m_rank_cache;
	size_t lo = 0, hi = nLines;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (rank_cache[mid] <= rank) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(rank < rank_cache[lo]);

	const bm_uint_t* bits = m_words;
	size_t hit = rank_cache[lo - 1];
	bits += LineWords * (lo - 1);
	for(size_t i = 0; i < LineWords; ++i) {
		bm_uint_t bm = bits[i];
		size_t upper = hit + fast_popcount(bm);
		if (rank < upper)
			return LineBits * (lo - 1) +
				i * WordBits + UintSelect1(bm, rank - hit);
		hit = upper;
	}
	assert(0);
	return size_t(-1);
}

size_t rank_select::select0(size_t rank) const {
	GUARD_MAX_RANK(0, rank);
	const size_t  nLines = (m_size + LineBits - 1) / LineBits;
	const uint32_t* rank_cache = m_rank_cache;
	size_t lo = 0, hi = nLines;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t bitpos = LineBits * mid;
		size_t hit = bitpos - rank_cache[mid];
		if (hit <= rank) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(rank < LineBits * lo - rank_cache[lo]);
	const bm_uint_t* bits = m_words;
	size_t hit = LineBits * (lo - 1) - rank_cache[lo - 1];
	bits += LineWords * (lo - 1);
	for(size_t i = 0; i < LineWords; ++i) {
		bm_uint_t bm = ~bits[i];
		size_t upper = hit + fast_popcount(bm);
		if (rank < upper)
			return LineBits * (lo - 1) +
				i * WordBits + UintSelect1(bm, rank - hit);
		hit = upper;
	}
	assert(0);
	return size_t(-1);
//	THROW_STD(runtime_error, "Unexpected, maybe a bug");
}

/////////////////////////////////////////////////////////////////////////////
inline rank_select_se::RankCache::RankCache(uint32_t l1) {
	lev1 = l1;
	memset(lev2, 0, sizeof(lev2));
}

void rank_select_se::nullize_cache() {
	m_rank_cache = NULL;
	m_sel0_cache = NULL;
	m_sel1_cache = NULL; // now select1 is not accelerated
	m_max_rank0 = 0;
	m_max_rank1 = 0;
}

rank_select_se::rank_select_se() {
	nullize_cache();
}

rank_select_se::rank_select_se(size_t n, bool val) : febitvec(n, val) {
	nullize_cache();
}

rank_select_se::rank_select_se(size_t n, valvec_no_init) {
	febitvec::resize_no_init(n);
	nullize_cache();
}

rank_select_se::rank_select_se(size_t n, valvec_reserve) {
	febitvec::reserve(n);
	nullize_cache();
}

rank_select_se::rank_select_se(const rank_select_se& y)
	: febitvec() // call default cons, don't call copy-cons
{
	assert(this != &y);
	assert(y.m_capacity % WordBits == 0);
	nullize_cache();
	this->reserve(y.m_capacity);
	this->m_size = y.m_size;
	STDEXT_copy_n(y.m_words, y.m_capacity/WordBits, this->m_words);
	if (y.m_rank_cache) {
		assert(m_size % WordBits == 0);
		m_rank_cache = (RankCache*)this->m_words
					 + (y.m_rank_cache - (RankCache*)y.m_words);
	}
	if (y.m_sel0_cache) {
		assert(m_rank_cache);
		m_sel0_cache = (uint32_t*)this->m_words
					 + (y.m_sel0_cache - (uint32_t*)y.m_words);
	}
	if (y.m_sel1_cache) {
		assert(m_rank_cache);
		m_sel1_cache = (uint32_t*)this->m_words
					 + (y.m_sel1_cache - (uint32_t*)y.m_words);
	}
	m_max_rank0 = y.m_max_rank0;
	m_max_rank1 = y.m_max_rank1;
}

rank_select_se&
rank_select_se::operator=(const rank_select_se& y) {
	if (this != &y) {
		this->clear();
		new(this)rank_select_se(y);
	}
	return *this;
}

#if defined(HSM_HAS_MOVE)
rank_select_se::rank_select_se(rank_select_se&& y) noexcept {
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
}
rank_select_se&
rank_select_se::operator=(rank_select_se&& y) noexcept {
	if (m_words)
		::free(m_words);
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
	return *this;
}
#endif

rank_select_se::~rank_select_se() {
}

void rank_select_se::clear() {
	nullize_cache();
	febitvec::clear();
}

void rank_select_se::risk_release_ownership() {
	nullize_cache();
	febitvec::risk_release_ownership();
}

void rank_select_se::risk_mmap_from(unsigned char* base, size_t length) {
	assert(NULL == m_words);
	assert(NULL == m_rank_cache);
	assert(length % sizeof(uint64_t) == 0);
	uint64_t flags = ((uint64_t*)(base + length))[-1];
	m_size = size_t(flags >> 8);
	size_t ceiled_bits = (m_size + LineBits - 1) & ~size_t(LineBits - 1);
	size_t nlines = ceiled_bits / LineBits;
	m_words = (bm_uint_t*)base;
	m_capacity = length * 8;
	m_rank_cache = (RankCache*)(m_words + ceiled_bits / WordBits);
	m_max_rank1 = m_rank_cache[nlines].lev1;
	m_max_rank0 = m_size - m_max_rank1;
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	uint32_t* select_index = (uint32_t*)(m_rank_cache + nlines + 1);
	if (flags & (1 << 0))
		m_sel0_cache = select_index, select_index += select0_slots + 1;
	if (flags & (1 << 1))
		m_sel1_cache = select_index;
}

void rank_select_se::shrink_to_fit() {
	assert(NULL == m_rank_cache);
	assert(NULL == m_sel0_cache);
	assert(NULL == m_sel1_cache);
	assert(0 == m_max_rank0);
	assert(0 == m_max_rank1);
	febitvec::shrink_to_fit();
}

void rank_select_se::swap(rank_select_se& y) {
	febitvec::swap(y);
	std::swap(m_rank_cache, y.m_rank_cache);
	std::swap(m_sel0_cache, y.m_sel0_cache);
	std::swap(m_sel1_cache, y.m_sel1_cache);
	std::swap(m_max_rank0, y.m_max_rank0);
	std::swap(m_max_rank1, y.m_max_rank1);
}

void rank_select_se::build_cache(bool speed_select0, bool speed_select1) {
	if (NULL == m_words) return;
	shrink_to_fit();
	size_t ceiled_bits = (m_size + LineBits-1) & ~(LineBits-1);
	size_t nlines = ceiled_bits / LineBits;
	reserve(ceiled_bits + (nlines + 1) * sizeof(RankCache) * 8);
	bits_range_set0(m_words, m_size, ceiled_bits);
	RankCache* rank_cache = (RankCache*)(m_words + ceiled_bits/WordBits);
	uint64_t* pBit64 = (uint64_t*)this->bldata();
	size_t Rank1 = 0;
	for(size_t i = 0; i < nlines; ++i) {
		size_t r = 0;
		rank_cache[i].lev1 = (uint32_t)(Rank1);
		for(size_t j = 0; j < 4; ++j) {
			rank_cache[i].lev2[j] = (uint8_t)r;
			r += fast_popcount(pBit64[i*4 + j]);
		}
		Rank1 += r;
	}
	rank_cache[nlines] = RankCache((uint32_t)Rank1);
	m_max_rank0 = m_size - Rank1;
	m_max_rank1 = Rank1;
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	size_t select1_slots = (m_max_rank1 + LineBits - 1) / LineBits;
	size_t u32_slots = ( (speed_select0 ? select0_slots + 1 : 0)
					   + (speed_select1 ? select1_slots + 1 : 0)
					   + 1
					   ) & ~size_t(1);
	size_t flag_as_u32_slots = 2;
	reserve(m_capacity + 32 * (u32_slots + flag_as_u32_slots));
	rank_cache = m_rank_cache = (RankCache*)(m_words + ceiled_bits/WordBits);

	uint32_t* select_index = (uint32_t*)(rank_cache + nlines + 1);
	if (speed_select0) {
		uint32_t* sel0_cache = select_index;
		sel0_cache[0] = 0;
		for (size_t j = 1; j < select0_slots; ++j) {
			size_t k = sel0_cache[j - 1];
			while (k * LineBits - rank_cache[k].lev1 < LineBits * j) ++k;
			sel0_cache[j] = k;
		}
		sel0_cache[select0_slots] = nlines;
		m_sel0_cache = sel0_cache;
		select_index += select0_slots + 1;
	}
	if (speed_select1) {
		uint32_t* sel1_cache = select_index;
		sel1_cache[0] = 0;
		for (size_t j = 1; j < select1_slots; ++j) {
			size_t k = sel1_cache[j - 1];
			while (rank_cache[k].lev1 < LineBits * j) ++k;
			sel1_cache[j] = k;
		}
		sel1_cache[select1_slots] = nlines;
		m_sel1_cache = sel1_cache;
	}
	uint64_t flags
		= (uint64_t(m_size       ) << 8)
		| (uint64_t(speed_select1) << 1)
		| (uint64_t(speed_select0) << 0)
		;
	((uint64_t*)(m_words + m_capacity/WordBits))[-1] = flags;
}

size_t rank_select_se::mem_size() const {
	return m_capacity / 8;
}

size_t rank_select_se::select0(size_t Rank0) const {
	GUARD_MAX_RANK(0, Rank0);
	size_t lo, hi;
	if (m_sel0_cache) { // get the very small [lo, hi) range
		lo = m_sel0_cache[Rank0 / LineBits];
		hi = m_sel0_cache[Rank0 / LineBits + 1];
	//	assert(lo < hi);
	}
	else { // global range
		lo = 0;
		hi = (m_size + LineBits + 1) / LineBits;
	}
	const RankCache* rank_cache = m_rank_cache;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - rank_cache[mid].lev1;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - rank_cache[lo].lev1);
	size_t hit = LineBits * lo - rank_cache[lo].lev1;
	const bm_uint_t* bm_words = this->bldata();
	size_t line_bitpos = (lo-1) * LineBits;
	RankCache rc = rank_cache[lo-1];
	hit = LineBits * (lo-1) - rc.lev1;
	const uint64_t* pBit64 = (const uint64_t*)(bm_words + LineWords * (lo-1));

	if (Rank0 < hit + 64*2 - rc.lev2[2]) {
		if (Rank0 < hit + 64*1 - rc.lev2[1]) { // rc.lev2[0] is always 0
			return line_bitpos + UintSelect1(~pBit64[0], Rank0 - hit);
		}
		return line_bitpos + 64*1 +
			UintSelect1(~pBit64[1], Rank0 - (hit + 64*1 - rc.lev2[1]));
	}
	if (Rank0 < hit + 64*3 - rc.lev2[3]) {
		return line_bitpos + 64*2 +
			UintSelect1(~pBit64[2], Rank0 - (hit + 64*2 - rc.lev2[2]));
	}
   	else {
		return line_bitpos + 64 * 3 +
			UintSelect1(~pBit64[3], Rank0 - (hit + 64*3 - rc.lev2[3]));
	}
}

size_t rank_select_se::select1(size_t Rank1) const {
	GUARD_MAX_RANK(1, Rank1);
	size_t lo, hi;
	if (m_sel1_cache) { // get the very small [lo, hi) range
		lo = m_sel1_cache[Rank1 / LineBits];
		hi = m_sel1_cache[Rank1 / LineBits + 1];
	//	assert(lo < hi);
	}
	else {
		lo = 0;
		hi = (m_size + LineBits + 1) / LineBits;
	}
	const RankCache* rank_cache = m_rank_cache;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = rank_cache[mid].lev1;
		if (mid_val <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank1 < rank_cache[lo].lev1);
	size_t hit = rank_cache[lo].lev1;
	const bm_uint_t* bm_words = this->bldata();
	size_t line_bitpos = (lo-1) * LineBits;
	RankCache rc = rank_cache[lo-1];
	hit = rc.lev1;
	const uint64_t* pBit64 = (const uint64_t*)(bm_words + LineWords * (lo-1));

	if (Rank1 < hit + rc.lev2[2]) {
		if (Rank1 < hit + rc.lev2[1]) { // rc.lev2[0] is always 0
			return line_bitpos + UintSelect1(pBit64[0], Rank1 - hit);
		}
		return line_bitpos + 64*1 +
			 UintSelect1(pBit64[1], Rank1 - (hit + rc.lev2[1]));
	}
	if (Rank1 < hit + rc.lev2[3]) {
		return line_bitpos + 64*2 +
			 UintSelect1(pBit64[2], Rank1 - (hit + rc.lev2[2]));
	}
   	else {
		return line_bitpos + 64*3 +
			 UintSelect1(pBit64[3], Rank1 - (hit + rc.lev2[3]));
	}
}

/////////////////////////////////////////////////////////////////////////////

inline rank_select_se_512::RankCache512::RankCache512(uint32_t l1) {
	BOOST_STATIC_ASSERT(sizeof(RankCache512) == 12);
	base = l1;
	rela = 0;
}

void rank_select_se_512::nullize_cache() {
	m_rank_cache = NULL;
	m_sel0_cache = NULL;
	m_sel1_cache = NULL; // now select1 is not accelerated
	m_max_rank0 = 0;
	m_max_rank1 = 0;
}

rank_select_se_512::rank_select_se_512() {
	nullize_cache();
}

rank_select_se_512::rank_select_se_512(size_t n, bool val) : febitvec(n, val) {
	nullize_cache();
}

rank_select_se_512::rank_select_se_512(size_t n, valvec_no_init) {
	febitvec::resize_no_init(n);
	nullize_cache();
}

rank_select_se_512::rank_select_se_512(size_t n, valvec_reserve) {
	febitvec::reserve(n);
	nullize_cache();
}

rank_select_se_512::rank_select_se_512(const rank_select_se_512& y)
	: febitvec() // call default cons, don't call copy-cons
{
	assert(this != &y);
	assert(y.m_capacity % WordBits == 0);
	nullize_cache();
	this->reserve(y.m_capacity);
	this->m_size = y.m_size;
	STDEXT_copy_n(y.m_words, y.m_capacity/WordBits, this->m_words);
	if (y.m_rank_cache) {
		assert(m_size % WordBits == 0);
		m_rank_cache = (RankCache512*)this->m_words
					 + (y.m_rank_cache - (RankCache512*)y.m_words);
	}
	if (y.m_sel0_cache) {
		assert(m_rank_cache);
		m_sel0_cache = (uint32_t*)this->m_words
					 + (y.m_sel0_cache - (uint32_t*)y.m_words);
	}
	if (y.m_sel1_cache) {
		assert(m_rank_cache);
		m_sel1_cache = (uint32_t*)this->m_words
					 + (y.m_sel1_cache - (uint32_t*)y.m_words);
	}
	m_max_rank0 = y.m_max_rank0;
	m_max_rank1 = y.m_max_rank1;
}

rank_select_se_512&
rank_select_se_512::operator=(const rank_select_se_512& y) {
	if (this != &y) {
		this->clear();
		new(this)rank_select_se_512(y);
	}
	return *this;
}

#if defined(HSM_HAS_MOVE)
rank_select_se_512::rank_select_se_512(rank_select_se_512&& y) noexcept {
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
}
rank_select_se_512&
rank_select_se_512::operator=(rank_select_se_512&& y) noexcept {
	if (m_words)
		::free(m_words);
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
	return *this;
}
#endif

rank_select_se_512::~rank_select_se_512() {
}

void rank_select_se_512::clear() {
	nullize_cache();
	febitvec::clear();
}

void rank_select_se_512::risk_release_ownership() {
	nullize_cache();
	febitvec::risk_release_ownership();
}

void rank_select_se_512::risk_mmap_from(unsigned char* base, size_t length) {
	assert(NULL == m_words);
	assert(NULL == m_rank_cache);
	assert(length % sizeof(uint64_t) == 0);
	uint64_t flags = ((uint64_t*)(base + length))[-1];
	m_size = size_t(flags >> 8);
	size_t ceiled_bits = (m_size + LineBits - 1) & ~size_t(LineBits - 1);
	size_t nlines = ceiled_bits / LineBits;
	m_words = (bm_uint_t*)base;
	m_capacity = length * 8;
	m_rank_cache = (RankCache512*)(m_words + ceiled_bits / WordBits);
	m_max_rank1 = m_rank_cache[nlines].base;
	m_max_rank0 = m_size - m_max_rank1;
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	uint32_t* select_index = (uint32_t*)(m_rank_cache + nlines + 1);
	if (flags & (1 << 0))
		m_sel0_cache = select_index, select_index += select0_slots + 1;
	if (flags & (1 << 1))
		m_sel1_cache = select_index;
}

void rank_select_se_512::shrink_to_fit() {
	assert(NULL == m_rank_cache);
	assert(NULL == m_sel0_cache);
	assert(NULL == m_sel1_cache);
	assert(0 == m_max_rank0);
	assert(0 == m_max_rank1);
	febitvec::shrink_to_fit();
}

void rank_select_se_512::swap(rank_select_se_512& y) {
	febitvec::swap(y);
	std::swap(m_rank_cache, y.m_rank_cache);
	std::swap(m_sel0_cache, y.m_sel0_cache);
	std::swap(m_sel1_cache, y.m_sel1_cache);
	std::swap(m_max_rank0, y.m_max_rank0);
	std::swap(m_max_rank1, y.m_max_rank1);
}

void rank_select_se_512::build_cache(bool speed_select0, bool speed_select1) {
	if (NULL == m_words) return;
	shrink_to_fit();
	size_t ceiled_bits = (m_size + LineBits-1) & ~(LineBits-1);
	size_t nlines = ceiled_bits / LineBits;
	reserve(ceiled_bits + (nlines + 1) * sizeof(RankCache512) * 8);
	bits_range_set0(m_words, m_size, ceiled_bits);
	RankCache512* rank_cache = (RankCache512*)(m_words + ceiled_bits/WordBits);
	uint64_t* pBit64 = (uint64_t*)m_words;
	size_t Rank1 = 0;
	for(size_t i = 0; i < nlines; ++i) {
		size_t r = 0;
		uint64_t rela = 0;
		BOOST_STATIC_ASSERT(LineBits/64 == 8);
		for(size_t j = 0; j < (LineBits/64); ++j) {
			r += fast_popcount(pBit64[i*(LineBits/64) + j]);
			rela |= uint64_t(r) << (j*9); // last 'r' will not be in 'rela'
		//	printf("i = %zd, j = %zd, r = %zd\n", i, j, r);
		}
		rela &= uint64_t(-1) >> 1; // set unused bit as zero
		rank_cache[i].base = uint32_t(Rank1);
		rank_cache[i].rela = rela;
		Rank1 += r;
	}
	rank_cache[nlines] = RankCache512(uint32_t(Rank1));
	m_max_rank0 = m_size - Rank1;
	m_max_rank1 = Rank1;
//	printf("size = %zd, nlines = %zd\n", m_size, nlines);
//	printf("max_rank1 = %zd, max_rank0 = %zd\n", m_max_rank1, m_max_rank0);
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	size_t select1_slots = (m_max_rank1 + LineBits - 1) / LineBits;
	size_t u32_slots = ( (speed_select0 ? select0_slots + 1 : 0)
					   + (speed_select1 ? select1_slots + 1 : 0)
					   + 1
					   ) & ~size_t(1);
	size_t flag_as_u32_slots = 2;
	reserve(m_capacity + 32 * (u32_slots + flag_as_u32_slots));
	rank_cache = m_rank_cache = (RankCache512*)(m_words + ceiled_bits/WordBits);
	{
		char* start  = (char*)(rank_cache + nlines + 1);
		char* finish = (char*)(m_words + m_capacity/WordBits);
		std::fill(start, finish, 0);
	}

	uint32_t* select_index = (uint32_t*)(rank_cache + nlines + 1);
	if (speed_select0) {
		uint32_t* sel0_cache = select_index;
		sel0_cache[0] = 0;
		for(size_t j = 1; j < select0_slots; ++j) {
			size_t k = sel0_cache[j - 1];
			while (k * LineBits - rank_cache[k].base < LineBits * j) ++k;
			sel0_cache[j] = k;
		}
		sel0_cache[select0_slots] = nlines;
		m_sel0_cache = sel0_cache;
		select_index += select0_slots + 1;
	}
	if (speed_select1) {
		uint32_t* sel1_cache = select_index;
		sel1_cache[0] = 0;
		for(size_t j = 1; j < select1_slots; ++j) {
			size_t k = sel1_cache[j - 1];
			while (rank_cache[k].base < LineBits * j) ++k;
			sel1_cache[j] = k;
		}
		sel1_cache[select1_slots] = nlines;
		m_sel1_cache = sel1_cache;
	}
	uint64_t flags
		= (uint64_t(m_size       ) << 8)
		| (uint64_t(speed_select1) << 1)
		| (uint64_t(speed_select0) << 0)
		;
	((uint64_t*)(m_words + m_capacity/WordBits))[-1] = flags;

//	for (size_t i = 0; i < nlines; ++i) {
//		RankCache512 rc = m_rank_cache[i];
//		printf("RankCache[%zd] = %u %llX\n", i, rc.base, (long long)rc.rela);
//	}
}

size_t rank_select_se_512::mem_size() const {
	return m_capacity / 8;
}

size_t rank_select_se_512::select0(size_t Rank0) const {
	GUARD_MAX_RANK(0, Rank0);
	size_t lo, hi;
	if (m_sel0_cache) { // get the very small [lo, hi) range
		lo = m_sel0_cache[Rank0 / LineBits];
		hi = m_sel0_cache[Rank0 / LineBits + 1];
	//	assert(lo < hi);
	}
	else { // global range
		lo = 0;
		hi = (m_size + LineBits + 1) / LineBits;
	}
	const RankCache512* rank_cache = m_rank_cache;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - rank_cache[mid].base;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - rank_cache[lo].base);
	size_t hit = LineBits * lo - rank_cache[lo].base;
	const bm_uint_t* bm_words = this->bldata();
	size_t line_bitpos = (lo-1) * LineBits;
	uint64_t rcRela = rank_cache[lo-1].rela;
	hit = LineBits * (lo-1) - rank_cache[lo-1].base;
	const uint64_t* pBit64 = (const uint64_t*)(bm_words + LineWords * (lo-1));

#define select0_nth64(n) line_bitpos + 64*n + \
	UintSelect1(~pBit64[n], Rank0 - (hit + 64*n - rank512(rcRela, n)))

	if (Rank0 < hit + 64*4 - rank512(rcRela, 4)) {
		if (Rank0 < hit + 64*2 - rank512(rcRela, 2))
			if (Rank0 < hit + 64*1 - rank512(rcRela, 1))
				return line_bitpos + UintSelect1(~pBit64[0], Rank0 - hit);
			else
				return select0_nth64(1);
		else
			if (Rank0 < hit + 64*3 - rank512(rcRela, 3))
				return select0_nth64(2);
			else
				return select0_nth64(3);
	} else {
		if (Rank0 < hit + 64*6 - rank512(rcRela, 6))
			if (Rank0 < hit + 64*5 - rank512(rcRela, 5))
				return select0_nth64(4);
			else
				return select0_nth64(5);
		else
			if (Rank0 < hit + 64*7 - rank512(rcRela, 7))
				return select0_nth64(6);
			else
				return select0_nth64(7);
	}
#undef select0_nth64
}

size_t rank_select_se_512::select1(size_t Rank1) const {
	GUARD_MAX_RANK(1, Rank1);
	size_t lo, hi;
	if (m_sel1_cache) { // get the very small [lo, hi) range
		lo = m_sel1_cache[Rank1 / LineBits];
		hi = m_sel1_cache[Rank1 / LineBits + 1];
	//	assert(lo < hi);
	}
	else {
		lo = 0;
		hi = (m_size + LineBits + 1) / LineBits;
	}
	const RankCache512* rank_cache = m_rank_cache;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = rank_cache[mid].base;
		if (mid_val <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank1 < rank_cache[lo].base);
	size_t hit = rank_cache[lo].base;
	const bm_uint_t* bm_words = this->bldata();
	size_t line_bitpos = (lo-1) * LineBits;
	uint64_t rcRela = rank_cache[lo-1].rela;
	hit = rank_cache[lo-1].base;
	const uint64_t* pBit64 = (const uint64_t*)(bm_words + LineWords * (lo-1));

#define select1_nth64(n) line_bitpos + 64*n + \
	 UintSelect1(pBit64[n], Rank1 - (hit + rank512(rcRela, n)))

	if (Rank1 < hit + rank512(rcRela, 4)) {
		if (Rank1 < hit + rank512(rcRela, 2))
			if (Rank1 < hit + rank512(rcRela, 1))
				return line_bitpos + UintSelect1(pBit64[0], Rank1 - hit);
			else
				return select1_nth64(1);
		else
			if (Rank1 < hit + rank512(rcRela, 3))
				return select1_nth64(2);
			else
				return select1_nth64(3);
	} else {
		if (Rank1 < hit + rank512(rcRela, 6))
			if (Rank1 < hit + rank512(rcRela, 5))
				return select1_nth64(4);
			else
				return select1_nth64(5);
		else
			if (Rank1 < hit + rank512(rcRela, 7))
				return select1_nth64(6);
			else
				return select1_nth64(7);
	}
#undef select1_nth64
}


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

inline rank_select_il::Line::Line(bool val) {
	rlev1 = 0;
	memset(rlev2, 0, sizeof(rlev2));
	if (val)
		memset(words, -1, sizeof(words));
	else
		memset(words, 0, sizeof(words));
}

void rank_select_il::push_back_slow_path(bool val) {
	m_lines.emplace_back(false);
	this->set(m_size++, val);
}

rank_select_il::rank_select_il() {
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = 0;
}

rank_select_il::rank_select_il(size_t bits, bool val) {
	m_lines.resize_fill(BitsToLines(bits), Line(val));
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, bool val, bool padding) {
	m_lines.resize_fill(BitsToLines(bits), Line(val));
	if (padding != val)
		set(bits, m_lines.size()*LineBits - bits, padding);
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, valvec_no_init) {
	m_lines.resize_no_init(BitsToLines(bits));
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, valvec_reserve) {
	m_lines.reserve(BitsToLines(bits));
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = 0;
}

rank_select_il::rank_select_il(const rank_select_il& y) = default;
rank_select_il&
rank_select_il::operator=(const rank_select_il& y) = default;

#if defined(HSM_HAS_MOVE)
rank_select_il::rank_select_il(rank_select_il&& y) noexcept {
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
}
rank_select_il&
rank_select_il::operator=(rank_select_il&& y) noexcept {
	m_lines.clear();
	memcpy(this, &y, sizeof(*this));
	y.risk_release_ownership();
	return *this;
}
#endif

rank_select_il::~rank_select_il() {
	this->clear();
}

void rank_select_il::clear() {
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_lines.clear();
	m_size = 0;
}

void rank_select_il::fill(bool val) {
	m_lines.fill(Line(val));
}

void rank_select_il::resize(size_t newsize, bool val) {
	m_lines.resize(BitsToLines(newsize), Line(val));
	m_size = newsize;
}

void rank_select_il::resize_no_init(size_t newsize) {
	m_lines.resize_no_init(BitsToLines(newsize));
	m_size = newsize;
}

void rank_select_il::resize_fill(size_t newsize, bool val) {
	m_lines.resize_fill(BitsToLines(newsize), Line(val));
	m_size = newsize;
}

void rank_select_il::shrink_to_fit() {
	assert(NULL == m_fast_select0);
	assert(NULL == m_fast_select1);
	assert(0 == m_max_rank0);
	assert(0 == m_max_rank1);
	m_lines.shrink_to_fit();
}

void rank_select_il::swap(rank_select_il& y) {
	m_lines.swap(y.m_lines);
	std::swap(m_fast_select0, y.m_fast_select0);
	std::swap(m_fast_select1, y.m_fast_select1);
	std::swap(m_max_rank0, y.m_max_rank0);
	std::swap(m_max_rank1, y.m_max_rank1);
	std::swap(m_size, y.m_size);
}

void rank_select_il::set(size_t i, size_t nbits, bool val) {
	if (val)
		set1(i, nbits);
	else
		set0(i, nbits);
}

void rank_select_il::set0(size_t i, size_t nbits) {
	for (size_t j = 0; j < nbits; ++j) set0(i + j);
}

void rank_select_il::set1(size_t i, size_t nbits) {
	for (size_t j = 0; j < nbits; ++j) set1(i + j);
}

bool rank_select_il::isall0() const {
	for (size_t i = 0, n = m_lines.size(); i < n; ++i)
		for (size_t j = 0; j < LineWords; ++i)
			if (m_lines[i].words[j])
				return false;
	return true;
}

bool rank_select_il::isall1() const {
	for (size_t i = 0, n = m_lines.size(); i < n; ++i)
		for (size_t j = 0; j < LineWords; ++i)
			if (bm_uint_t(-1) != m_lines[i].words[j])
				return false;
	return true;
}

size_t rank_select_il::popcnt() const {
	return this->popcnt(0, m_lines.size());
}

size_t rank_select_il::popcnt(size_t startline, size_t lines) const {
	assert(startline <= m_lines.size());
	assert(lines <= m_lines.size());
	assert(startline + lines <= m_lines.size());
	assert(m_max_rank0 + m_max_rank1 == LineBits*m_lines.size());
	const Line* plines = m_lines.data();
	assert(plines[startline + lines].rlev1 >=plines[startline].rlev1);
	return plines[startline + lines].rlev1 - plines[startline].rlev1;
}

size_t rank_select_il::one_seq_len(size_t bitpos) const {
	assert(bitpos < this->size());
	size_t j = bitpos / LineBits, k, sum;
	if (bitpos % WordBits != 0) {
		bm_uint_t x = m_lines[j].words[bitpos%LineBits / WordBits];
		if (!(x & (bm_uint_t(1) << bitpos%WordBits))) return 0;
		bm_uint_t y = ~(x >> bitpos%WordBits);
		size_t ctz = fast_ctz(y);
		if (ctz < WordBits - bitpos%WordBits) {
			// last zero bit after bitpos is in x
			return ctz;
		}
		assert(ctz == WordBits - bitpos%WordBits);
		k = bitpos % LineBits / WordBits + 1;
		sum = ctz;
	}
	else {
		k = bitpos % LineBits / WordBits;
		sum = 0;
	}
	size_t len = m_lines.size();
	for (; j < len; ++j) {
		for (; k < LineWords; ++k) {
			bm_uint_t y = ~m_lines[j].words[k];
			if (0 == y)
				sum += WordBits;
			else
				return sum + fast_ctz(y);
		}
		k = 0;
	}
	return sum;
}

size_t rank_select_il::zero_seq_len(size_t bitpos) const {
	assert(bitpos < this->size());
	size_t j = bitpos / LineBits, k, sum;
	if (bitpos % WordBits != 0) {
		bm_uint_t x = m_lines[j].words[bitpos%LineBits / WordBits];
		if (x & (bm_uint_t(1) << bitpos%WordBits)) return 0;
		bm_uint_t y = x >> bitpos%WordBits;
		if (y) {
			// last zero bit after bitpos is in x
			return fast_ctz(y);
		}
		k = bitpos % LineBits / WordBits + 1;
		sum = WordBits - bitpos % WordBits;
	}
	else {
		k = bitpos % LineBits / WordBits;
		sum = 0;
	}
	size_t len = m_lines.size();
	for (; j < len; ++j) {
		for (; k < LineWords; ++k) {
			bm_uint_t y = m_lines[j].words[k];
			if (0 == y)
				sum += WordBits;
			else
				return sum + fast_ctz(y);
		}
		k = 0;
	}
	return sum;
}

size_t rank_select_il::one_seq_revlen(size_t endpos) const {
	THROW_STD(invalid_argument, "Not Implemented");
	return 0;
}

size_t rank_select_il::zero_seq_revlen(size_t endpos) const {
	THROW_STD(invalid_argument, "Not Implemented");
	return 0;
}

size_t rank_select_il::select0(size_t Rank0) const {
	GUARD_MAX_RANK(0, Rank0);
	size_t lo, hi;
	if (m_fast_select0) { // get the very small [lo, hi) range
		lo = m_fast_select0[Rank0 / LineBits];
		hi = m_fast_select0[Rank0 / LineBits + 1];
	//	assert(lo < hi);
	}
	else { // global range
		lo = 0;
		hi = m_lines.size();
	}
	const Line* lines = m_lines.data();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - lines[mid].rlev1;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - lines[lo].rlev1);
	size_t hit = LineBits * lo - lines[lo].rlev1;
	const Line& xx = lines[lo - 1];
	hit = LineBits * (lo - 1) - xx.rlev1;
	size_t index = (lo-1) * LineBits; // base bit index
	if (Rank0 < hit + 64*2 - xx.rlev2[2]) {
		if (Rank0 < hit + 64*1 - xx.rlev2[1]) { // xx.rlev2[0] is always 0
			return index + 64*0 + UintSelect1(~xx.bit64[0], Rank0 - hit);
		}
		return index + 64*1 + UintSelect1(
				~xx.bit64[1], Rank0 - (hit + 64*1 - xx.rlev2[1]));
	}
	if (Rank0 < hit + 64*3 - xx.rlev2[3]) {
		return index + 64*2 + UintSelect1(
				~xx.bit64[2], Rank0 - (hit + 64*2 - xx.rlev2[2]));
	}
   	else {
		return index + 64*3 + UintSelect1(
				~xx.bit64[3], Rank0 - (hit + 64*3 - xx.rlev2[3]));
	}
}

size_t rank_select_il::select1(size_t Rank1) const {
	GUARD_MAX_RANK(1, Rank1);
	size_t lo, hi;
	if (m_fast_select1) { // get the very small [lo, hi) range
		lo = m_fast_select1[Rank1 / LineBits];
		hi = m_fast_select1[Rank1 / LineBits + 1];
	//	assert(lo < hi);
	}
	else { // global range
		lo = 0;
		hi = m_lines.size();
	}
	const Line* lines = m_lines.data();
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (lines[mid].rlev1 <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	size_t hit = lines[lo].rlev1;
	assert(Rank1 < hit);
	const Line& xx = lines[lo - 1];
	hit = xx.rlev1;
	assert(Rank1 >= hit);
	size_t index = (lo-1) * LineBits; // base bit index
	if (Rank1 < hit + xx.rlev2[2]) {
		if (Rank1 < hit + xx.rlev2[1]) { // xx.rlev2[0] is always 0
			return index + UintSelect1(xx.bit64[0], Rank1 - hit);
		}
		return index + 64*1 + UintSelect1(
				 xx.bit64[1], Rank1 - (hit + xx.rlev2[1]));
	}
	if (Rank1 < hit + xx.rlev2[3]) {
		return index + 64*2 + UintSelect1(
				 xx.bit64[2], Rank1 - (hit + xx.rlev2[2]));
	}
   	else {
		return index + 64*3 + UintSelect1(
				 xx.bit64[3], Rank1 - (hit + xx.rlev2[3]));
	}
}

void rank_select_il::risk_mmap_from(unsigned char* base, size_t length) {
	assert(length % sizeof(Line) == 0);
	uint64_t flags = ((uint64_t*)(base + length))[-1];
	m_size = size_t(flags >> 8);
	size_t nlines = (m_size + LineBits - 1) / LineBits;
	m_lines.risk_set_data((Line*)base, nlines);
	m_lines.risk_set_size(nlines);
	m_lines.risk_set_capacity(length / sizeof(Line));
	m_max_rank1 = m_lines.end()->rlev1;
	m_max_rank0 = m_size - m_max_rank1;
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	uint32_t* select_index = (uint32_t*)(m_lines.end() + 1);
	if (flags & (1 << 0)) // select0 flag == (1 << 0)
		m_fast_select0 = select_index, select_index += select0_slots + 1;
	if (flags & (1 << 1)) // select1 flag == (1 << 1)
		m_fast_select1 = select_index;
}

void rank_select_il::risk_release_ownership() {
	m_fast_select0 = m_fast_select1 = NULL;
	m_max_rank0 = m_max_rank1 = 0;
	m_size = 0;
	m_lines.risk_release_ownership();
}

void rank_select_il::build_cache(bool speed_select0, bool speed_select1) {
	assert(NULL == m_fast_select0);
	assert(NULL == m_fast_select1);
	assert(0 == m_max_rank0);
	assert(0 == m_max_rank1);
	if (m_size % LineBits) {
		bits_range_set0(m_lines.back().words, m_size % LineBits, LineBits);
	}
	shrink_to_fit();
	Line* lines = m_lines.data();
	size_t Rank1 = 0;
	for(size_t i = 0; i < m_lines.size(); ++i) {
		size_t inc = 0;
		lines[i].rlev1 = (uint32_t)(Rank1);
		for (size_t j = 0; j < 4; ++j) {
			lines[i].rlev2[j] = (uint8_t)inc;
			inc += fast_popcount(lines[i].bit64[j]);
		}
		Rank1 += inc;
	}
	m_max_rank0 = m_size - Rank1;
	m_max_rank1 = Rank1;
	size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
	size_t select1_slots = (m_max_rank1 + LineBits - 1) / LineBits;
	size_t u32_slots = ( (speed_select0 ? select0_slots + 1 : 0)
					   + (speed_select1 ? select1_slots + 1 : 0) );
	size_t flag_as_u32_slots = 2;
	m_lines.reserve(m_lines.size() + 1 +
		(u32_slots + flag_as_u32_slots + sizeof(Line)/sizeof(uint32_t) - 1)
			/ (sizeof(Line)/sizeof(uint32_t))
	);
	lines = m_lines.data();
	memset(&lines[m_lines.size()], 0, sizeof(Line));
	lines[m_lines.size()].rlev1 = (uint32_t)Rank1;

	uint32_t* select_index = (uint32_t*)(m_lines.end() + 1);
	if (speed_select0) {
		m_fast_select0 = select_index;
		m_fast_select0[0] = 0;
		for(size_t j = 1; j < select0_slots; ++j) {
			size_t k = m_fast_select0[j - 1];
			while (k * LineBits - lines[k].rlev1 < LineBits * j) ++k;
			m_fast_select0[j] = k;
		}
		m_fast_select0[select0_slots] = m_lines.size();
		select_index += select0_slots + 1;
	}
	if (speed_select1) {
		m_fast_select1 = select_index;
		m_fast_select1[0] = 0;
		for(size_t j = 1; j < select1_slots; ++j) {
			size_t k = m_fast_select1[j - 1];
			while (lines[k].rlev1 < LineBits * j) ++k;
			m_fast_select1[j] = k;
		}
		m_fast_select1[select1_slots] = m_lines.size();
	}
	uint64_t flags
		= (uint64_t(m_size)         << 8)
		| (uint64_t(speed_select1 ) << 1)
		| (uint64_t(speed_select0 ) << 0)
		;
	((uint64_t*)(m_lines.data() + m_lines.capacity()))[-1] = flags;
}

} // namespace terark

