#include "rank_select_se_256.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

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
    rank_select_check_overflow(n, > , rank_select_se_256);
    nullize_cache();
}

rank_select_se::rank_select_se(size_t n, valvec_no_init) {
    rank_select_check_overflow(n, > , rank_select_se_256);
    febitvec::resize_no_init(n);
    nullize_cache();
}

rank_select_se::rank_select_se(size_t n, valvec_reserve) {
    rank_select_check_overflow(n, > , rank_select_se_256);
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
    rank_select_check_overflow(m_size, > , rank_select_se_256);
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
        //assert(lo < hi);
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
    const bm_uint_t* bm_words = this->bldata();
    size_t line_bitpos = (lo-1) * LineBits;
    RankCache rc = rank_cache[lo-1];
    size_t hit = LineBits * (lo-1) - rc.lev1;
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
        //assert(lo < hi);
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
    const bm_uint_t* bm_words = this->bldata();
    size_t line_bitpos = (lo-1) * LineBits;
    RankCache rc = rank_cache[lo-1];
    size_t hit = rc.lev1;
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

} // namespace terark

