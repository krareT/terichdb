#include "rank_select_simple.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

rank_select_simple::rank_select_simple() {
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
}
rank_select_simple::rank_select_simple(size_t n, bool val) : febitvec(n, val) {
    rank_select_check_overflow(m_size, > , rank_select_simple);
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
}

rank_select_simple::rank_select_simple(size_t n, valvec_no_init) {
    rank_select_check_overflow(m_size, > , rank_select_simple);
    febitvec::resize_no_init(n);
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
}
rank_select_simple::rank_select_simple(size_t n, valvec_reserve) {
    rank_select_check_overflow(m_size, > , rank_select_simple);
    febitvec::reserve(n);
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
}
rank_select_simple::rank_select_simple(const rank_select_simple& y)
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
rank_select_simple& rank_select_simple::operator=(const rank_select_simple& y) {
    if (this != &y) {
        this->clear();
        new(this)rank_select_simple(y);
    }
    return *this;
}

#if defined(HSM_HAS_MOVE)
rank_select_simple::rank_select_simple(rank_select_simple&& y) noexcept {
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
}
rank_select_simple&
rank_select_simple::operator=(rank_select_simple&& y) noexcept {
    if (m_words)
        ::free(m_words);
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
    return *this;
}
#endif

rank_select_simple::~rank_select_simple() {
}

void rank_select_simple::clear() {
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    febitvec::clear();
}

void rank_select_simple::risk_release_ownership() {
    // cache is built after mmap, with malloc
    // do nullize m_rank_cache
    m_rank_cache = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    febitvec::risk_release_ownership();
}

/// @param length is count in bytes
void rank_select_simple::risk_mmap_from(unsigned char* base, size_t length) {
    // one 32-bit-rank-sum per line
    // an extra rank-sum(32), and bits-number(32)
    assert(NULL == m_words);
    size_t bits = (size_t)((uint32_t*)(base + length))[-1];
    size_t ceiled_bits = (bits + LineBits - 1) & ~(LineBits - 1);
    m_words = (bm_uint_t*)base;
    m_size = bits;
    m_capacity = length * 8;
    m_rank_cache = (uint32_t*)(m_words + ceiled_bits / WordBits);
    m_max_rank1 = m_rank_cache[ceiled_bits / LineBits];
    m_max_rank0 = m_size - m_max_rank1;
}

void rank_select_simple::shrink_to_fit() {
    assert(NULL == m_rank_cache);
    assert(0 == m_max_rank0);
    assert(0 == m_max_rank1);
    febitvec::shrink_to_fit();
}

void rank_select_simple::swap(rank_select_simple& y) {
    febitvec::swap(y);
    std::swap(m_rank_cache, y.m_rank_cache);
    std::swap(m_max_rank0 , y.m_max_rank0);
    std::swap(m_max_rank1 , y.m_max_rank1);
}

/// params are just place holder
void rank_select_simple::build_cache(bool speed_select0, bool speed_select1) {
    rank_select_check_overflow(m_size, > , rank_select_simple);
    assert(NULL == m_rank_cache);
    if (NULL == m_words) return;
    size_t ceiled_bits = (m_size + LineBits-1) & ~(LineBits-1);
    size_t const nlines = ceiled_bits / LineBits;
    shrink_to_fit();
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
    ((uint32_t*)((char*)m_words + m_capacity / 8))[-1] = uint32_t(m_size);
    m_rank_cache = cache;
    m_max_rank1 = Rank1;
    m_max_rank0 = m_size - Rank1;
}

size_t rank_select_simple::mem_size() const {
    return m_capacity / 8;
}

size_t rank_select_simple::rank1(size_t bitpos) const {
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

size_t rank_select_simple::select1(size_t rank) const {
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

size_t rank_select_simple::select0(size_t rank) const {
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
//    THROW_STD(runtime_error, "Unexpected, maybe a bug");
}

} // namespace terark