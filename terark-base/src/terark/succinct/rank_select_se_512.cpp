#include "rank_select_se_512.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::nullize_cache() {
    m_rank_cache = NULL;
    m_sel0_cache = NULL;
    m_sel1_cache = NULL; // now select1 is not accelerated
    m_max_rank0 = 0;
    m_max_rank1 = 0;
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl() {
    nullize_cache();
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl(size_t n, bool val) : febitvec(n, val) {
    if (sizeof(rank_cache_base_t) == 4)
        rank_select_check_overflow(m_size, > , rank_select_se_512);
    nullize_cache();
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl(size_t n, valvec_no_init) {
    if (sizeof(rank_cache_base_t) == 4)
        rank_select_check_overflow(m_size, > , rank_select_se_512);
    febitvec::resize_no_init(n);
    nullize_cache();
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl(size_t n, valvec_reserve) {
    if (sizeof(rank_cache_base_t) == 4)
        rank_select_check_overflow(m_size, > , rank_select_se_512);
    febitvec::reserve(n);
    nullize_cache();
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl(const rank_select_se_512_tpl& y)
    : febitvec() // call default cons, don't call copy-cons
{
    assert(this != &y);
    assert(y.m_capacity % WordBits == 0);
    nullize_cache();
    this->reserve(y.m_capacity);
    this->m_size = y.m_size;
    STDEXT_copy_n(y.m_words, y.m_capacity/WordBits, this->m_words);
    if (y.m_rank_cache) {
        m_rank_cache = (RankCache512*)this->m_words
                     + (y.m_rank_cache - (RankCache512*)y.m_words);
    }
    if (y.m_sel0_cache) {
        assert(m_rank_cache);
        m_sel0_cache = (index_t*)this->m_words
                     + (y.m_sel0_cache - (index_t*)y.m_words);
    }
    if (y.m_sel1_cache) {
        assert(m_rank_cache);
        m_sel1_cache = (index_t*)this->m_words
                     + (y.m_sel1_cache - (index_t*)y.m_words);
    }
    m_max_rank0 = y.m_max_rank0;
    m_max_rank1 = y.m_max_rank1;
}

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>&
rank_select_se_512_tpl<rank_cache_base_t>::operator=(const rank_select_se_512_tpl& y) {
    if (this != &y) {
        this->clear();
        new(this)rank_select_se_512_tpl(y);
    }
    return *this;
}

#if defined(HSM_HAS_MOVE)
template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::rank_select_se_512_tpl(rank_select_se_512_tpl&& y) noexcept {
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
}
template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>&
rank_select_se_512_tpl<rank_cache_base_t>::operator=(rank_select_se_512_tpl&& y) noexcept {
    if (m_words)
        ::free(m_words);
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
    return *this;
}
#endif

template<class rank_cache_base_t>
rank_select_se_512_tpl<rank_cache_base_t>::~rank_select_se_512_tpl() {
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::clear() {
    nullize_cache();
    febitvec::clear();
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::risk_release_ownership() {
    nullize_cache();
    febitvec::risk_release_ownership();
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::risk_mmap_from(unsigned char* base, size_t length) {
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
    index_t* select_index = (index_t*)(m_rank_cache + nlines + 1);
    if (flags & (1 << 0))
        m_sel0_cache = select_index, select_index += select0_slots + 1;
    if (flags & (1 << 1))
        m_sel1_cache = select_index;
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::shrink_to_fit() {
    assert(NULL == m_rank_cache);
    assert(NULL == m_sel0_cache);
    assert(NULL == m_sel1_cache);
    assert(0 == m_max_rank0);
    assert(0 == m_max_rank1);
    febitvec::shrink_to_fit();
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::swap(rank_select_se_512_tpl& y) {
    febitvec::swap(y);
    std::swap(m_rank_cache, y.m_rank_cache);
    std::swap(m_sel0_cache, y.m_sel0_cache);
    std::swap(m_sel1_cache, y.m_sel1_cache);
    std::swap(m_max_rank0, y.m_max_rank0);
    std::swap(m_max_rank1, y.m_max_rank1);
}

template<class rank_cache_base_t>
void rank_select_se_512_tpl<rank_cache_base_t>::build_cache(bool speed_select0, bool speed_select1) {
    if (sizeof(rank_cache_base_t) == 4)
        rank_select_check_overflow(m_size, > , rank_select_se_512);
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
        //    printf("i = %zd, j = %zd, r = %zd\n", i, j, r);
        }
        rela &= uint64_t(-1) >> 1; // set unused bit as zero
        rank_cache[i].base = index_t(Rank1);
        rank_cache[i].rela = rela;
        Rank1 += r;
    }
    rank_cache[nlines] = RankCache512(index_t(Rank1));
    m_max_rank0 = m_size - Rank1;
    m_max_rank1 = Rank1;
//  printf("size = %zd, nlines = %zd\n", m_size, nlines);
//  printf("max_rank1 = %zd, max_rank0 = %zd\n", m_max_rank1, m_max_rank0);
    size_t select0_slots = (m_max_rank0 + LineBits - 1) / LineBits;
    size_t select1_slots = (m_max_rank1 + LineBits - 1) / LineBits;
    size_t u32_slots = (1
        + (speed_select0 ? select0_slots + 1 : 0) * (sizeof(index_t) / sizeof(uint32_t))
        + (speed_select1 ? select1_slots + 1 : 0) * (sizeof(index_t) / sizeof(uint32_t))
        ) & ~size_t(1);
    size_t flag_as_u32_slots = 2;
    reserve(m_capacity + 32 * (u32_slots + flag_as_u32_slots));
    rank_cache = m_rank_cache = (RankCache512*)(m_words + ceiled_bits/WordBits);
    {
        char* start  = (char*)(rank_cache + nlines + 1);
        char* finish = (char*)(m_words + m_capacity/WordBits);
        std::fill(start, finish, 0);
    }

    index_t* select_index = (index_t*)(rank_cache + nlines + 1);
    if (speed_select0) {
        index_t* sel0_cache = select_index;
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
        index_t* sel1_cache = select_index;
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

//    for (size_t i = 0; i < nlines; ++i) {
//        RankCache512 rc = m_rank_cache[i];
//        printf("RankCache[%zd] = %u %llX\n", i, rc.base, (long long)rc.rela);
//    }
}

template<class rank_cache_base_t>
size_t rank_select_se_512_tpl<rank_cache_base_t>::mem_size() const {
    return m_capacity / 8;
}

template<class rank_cache_base_t>
size_t rank_select_se_512_tpl<rank_cache_base_t>::select0(size_t Rank0) const {
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
    size_t hit = LineBits * (lo-1) - rank_cache[lo-1].base;
    const bm_uint_t* bm_words = this->bldata();
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rank_cache[lo-1].rela;
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

template<class rank_cache_base_t>
size_t rank_select_se_512_tpl<rank_cache_base_t>::select1(size_t Rank1) const {
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
    size_t hit = rank_cache[lo-1].base;
    const bm_uint_t* bm_words = this->bldata();
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rank_cache[lo-1].rela;
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

template class TERARK_DLL_EXPORT rank_select_se_512_tpl<uint32_t>;
template class TERARK_DLL_EXPORT rank_select_se_512_tpl<uint64_t>;

} // namespace terark

