#include "rank_select_mixed_se_512.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

rank_select_mixed_se_512::rank_select_mixed_se_512() {
    m_words = NULL;
    m_capacity = 0;
    m_size[0] = m_size[1] = 0;
    nullize_cache();
}

rank_select_mixed_se_512::rank_select_mixed_se_512(size_t n, bool val0, bool val1) {
    rank_select_check_overflow(n, > , rank_select_mixed_se_512);
    m_capacity = ((n + LineBits - 1) & ~(LineBits - 1)) * 2;
    m_size[0] = m_size[1] = n;
    m_words = (bm_uint_t*)malloc(m_capacity / 8);
    if (NULL == m_words)
        throw std::bad_alloc();
    get<0>().set(0, n, val0);
    get<1>().set(0, n, val1);
    nullize_cache();
}

rank_select_mixed_se_512::rank_select_mixed_se_512(size_t n, valvec_no_init) {
    rank_select_check_overflow(n, > , rank_select_mixed_se_512);
    m_capacity = ((n + LineBits - 1) & ~(LineBits - 1)) * 2;
    m_size[0] = m_size[1] = n;
    m_words = (bm_uint_t*)malloc(m_capacity / 8);
    if (NULL == m_words)
        throw std::bad_alloc();
    nullize_cache();
}

rank_select_mixed_se_512::rank_select_mixed_se_512(size_t n, valvec_reserve) {
    rank_select_check_overflow(n, > , rank_select_mixed_se_512);
    m_capacity = ((n + LineBits - 1) & ~(LineBits - 1)) * 2;
    m_size[0] = m_size[1] = 0;
    m_words = (bm_uint_t*)malloc(m_capacity / 8);
    if (NULL == m_words)
        throw std::bad_alloc();
    nullize_cache();
}

rank_select_mixed_se_512::rank_select_mixed_se_512(const rank_select_mixed_se_512& y)
    : rank_select_mixed_se_512(y.m_capacity, valvec_reserve())
{
    assert(this != &y);
    assert(y.m_capacity % WordBits == 0);
    assert(y.m_size[0] == y.m_size[1]);
    m_size[0] = y.m_size[0];
    m_size[1] = y.m_size[1];
    STDEXT_copy_n(y.m_words, y.m_capacity / WordBits, this->m_words);
    if (y.m_rank_cache) {
        m_rank_cache = (RankCacheMixed*)this->m_words + (y.m_rank_cache - (RankCacheMixed*)y.m_words);
    }
    if (y.m_sel0_cache[0]) {
        assert(m_rank_cache);
        m_sel0_cache[0] = (uint32_t*)this->m_words + (y.m_sel0_cache[0] - (uint32_t*)y.m_words);
    }
    if (y.m_sel0_cache[1]) {
        assert(m_rank_cache);
        m_sel0_cache[1] = (uint32_t*)this->m_words + (y.m_sel0_cache[1] - (uint32_t*)y.m_words);
    }
    if (y.m_sel1_cache[0]) {
        assert(m_rank_cache);
        m_sel1_cache[0] = (uint32_t*)this->m_words + (y.m_sel1_cache[0] - (uint32_t*)y.m_words);
    }
    if (y.m_sel1_cache[1]) {
        assert(m_rank_cache);
        m_sel1_cache[1] = (uint32_t*)this->m_words + (y.m_sel1_cache[1] - (uint32_t*)y.m_words);
    }
    m_max_rank0[0] = y.m_max_rank0[0];
    m_max_rank0[1] = y.m_max_rank0[1];
    m_max_rank1[0] = y.m_max_rank1[0];
    m_max_rank1[1] = y.m_max_rank1[1];
}

rank_select_mixed_se_512& rank_select_mixed_se_512::operator=(const rank_select_mixed_se_512& y) {
    if (this != &y) {
        this->clear();
        new(this)rank_select_mixed_se_512(y);
    }
    return *this;
}

#if defined(HSM_HAS_MOVE)

rank_select_mixed_se_512::rank_select_mixed_se_512(rank_select_mixed_se_512&& y) noexcept {
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
}

rank_select_mixed_se_512& rank_select_mixed_se_512::operator=(rank_select_mixed_se_512&& y) noexcept {
    if (m_words)
        ::free(m_words);
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
    return *this;
}

#endif

rank_select_mixed_se_512::~rank_select_mixed_se_512() {
    if (m_words)
        ::free(m_words);
}

void rank_select_mixed_se_512::clear() {
    if (m_words)
        ::free(m_words);
    risk_release_ownership();
}

void rank_select_mixed_se_512::risk_release_ownership() {
    nullize_cache();
    m_words = nullptr;
    m_capacity = 0;
    m_size[0] = m_size[1] = 0;
}

void rank_select_mixed_se_512::risk_mmap_from(unsigned char* base, size_t length) {
    assert(NULL == m_words);
    assert(NULL == m_rank_cache);
    assert(length % sizeof(uint64_t) == 0);
    m_flags = ((uint64_t*)(base + length))[-1];
    m_size[0] = ((uint32_t*)(base + length))[-4];
    m_size[1] = ((uint32_t*)(base + length))[-3];
    size_t ceiled_bits = (std::max(m_size[0], m_size[1]) + LineBits - 1) & ~size_t(LineBits - 1);
    size_t nlines = ceiled_bits / LineBits;
    ceiled_bits *= 2;
    m_words = (bm_uint_t*)base;
    m_capacity = length * 8;
    m_rank_cache = (RankCacheMixed*)(m_words + ceiled_bits / WordBits);
    uint32_t* select_index = (uint32_t*)(m_rank_cache + nlines + 1);
    auto load_d0 = [&] {
        if (m_flags & (1 << 1)) {
            m_max_rank1[0] = m_rank_cache[nlines].base[0];
            m_max_rank0[0] = m_size[0] - m_max_rank1[0];
            size_t select0_slots_d0 = (m_max_rank0[0] + LineBits - 1) / LineBits;
            size_t select1_slots_d0 = (m_max_rank1[0] + LineBits - 1) / LineBits;
            if (m_flags & (1 << 2))
                m_sel0_cache[0] = select_index, select_index += select0_slots_d0 + 1;
            if (m_flags & (1 << 3))
                m_sel1_cache[0] = select_index, select_index += select1_slots_d0 + 1;
        }
    };
    auto load_d1 = [&] {
        if (m_flags & (1 << 4)) {
            m_max_rank1[1] = m_rank_cache[nlines].base[1];
            m_max_rank0[1] = m_size[1] - m_max_rank1[1];
            size_t select0_slots_d1 = (m_max_rank0[1] + LineBits - 1) / LineBits;
            size_t select1_slots_d1 = (m_max_rank1[1] + LineBits - 1) / LineBits;
            if (m_flags & (1 << 5))
                m_sel0_cache[1] = select_index, select_index += select0_slots_d1 + 1;
            if (m_flags & (1 << 6))
                m_sel1_cache[1] = select_index, select_index += select1_slots_d1 + 1;
        }
    };
    if ((m_flags & (1 << 0)) == 0) {
        load_d0();
        load_d1();
    }
    else {
        load_d1();
        load_d0();
    }
}

void rank_select_mixed_se_512::shrink_to_fit() {
    assert(NULL == m_rank_cache);
    assert(NULL == m_sel0_cache[0]);
    assert(NULL == m_sel0_cache[1]);
    assert(NULL == m_sel1_cache[0]);
    assert(NULL == m_sel1_cache[1]);
    assert(0 == m_max_rank0[0]);
    assert(0 == m_max_rank0[1]);
    assert(0 == m_max_rank1[0]);
    assert(0 == m_max_rank1[1]);
    size_t size = std::max(m_size[0], m_size[1]);
    size_t new_bytes = ((size + LineBits - 1) & ~(LineBits - 1)) * 2 / 8;
    auto new_words = (bm_uint_t*)realloc(m_words, new_bytes);
    if (NULL == new_words)
        throw std::bad_alloc();
    m_words = new_words;
    m_capacity = new_bytes * 8;
}

void rank_select_mixed_se_512::swap(rank_select_mixed_se_512& y) {
    std::swap(m_words, y.m_words);
    std::swap(m_size, y.m_size);
    std::swap(m_capacity, y.m_capacity);
    std::swap(m_rank_cache, y.m_rank_cache);
    std::swap(m_sel0_cache, y.m_sel0_cache);
    std::swap(m_sel1_cache, y.m_sel1_cache);
    std::swap(m_max_rank0, y.m_max_rank0);
    std::swap(m_max_rank1, y.m_max_rank1);
}

const void* rank_select_mixed_se_512::data() const {
    return m_words;
}

size_t rank_select_mixed_se_512::mem_size() const {
    return m_capacity / 8;
}

void rank_select_mixed_se_512::grow() {
    assert(std::max(m_size[0], m_size[1]) * 2 == m_capacity);
    assert((m_flags & (1 << 1)) == 0);
    assert((m_flags & (1 << 4)) == 0);
	// size_t(WordBits) prevent debug link error
    m_capacity = std::max(m_capacity, size_t(WordBits));
    auto new_words = (bm_uint_t*)realloc(m_words, m_capacity / 8 * 2);
    if (NULL == new_words)
        throw std::bad_alloc();
    m_words = new_words;
    m_capacity *= 2;
}

void rank_select_mixed_se_512::reserve(size_t capacity) {
    assert(capacity == ((capacity + WordBits - 1) & ~(WordBits - 1)));
    if (capacity <= m_capacity)
        return;
    auto new_words = (bm_uint_t*)realloc(m_words, capacity / 8);
    if (NULL == new_words)
        throw std::bad_alloc();
    m_words = new_words;
    m_capacity = capacity;
}

void rank_select_mixed_se_512::nullize_cache() {
    m_flags = 0;
    m_rank_cache = NULL;
    m_sel0_cache[0] = NULL;
    m_sel1_cache[0] = NULL;
    m_sel0_cache[1] = NULL;
    m_sel1_cache[1] = NULL;
    m_max_rank0[0] = 0;
    m_max_rank0[1] = 0;
    m_max_rank1[0] = 0;
    m_max_rank1[1] = 0;
}

template<size_t dimensions>
void rank_select_mixed_se_512::bits_range_set0_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_words[j * 2 + dimensions] &= ~(bm_uint_t(-1) << i % UintBits)
            | (bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits)
            m_words[j++ * 2 + dimensions] &= ~(bm_uint_t(-1) << i % UintBits);
        while (j < k / UintBits)
            m_words[j++ * 2 + dimensions] = 0;
        if (k % UintBits)
            m_words[j * 2 + dimensions] &= bm_uint_t(-1) << k % UintBits;
    }
}

template void TERARK_DLL_EXPORT rank_select_mixed_se_512::bits_range_set0_dx<0>(size_t i, size_t k);
template void TERARK_DLL_EXPORT rank_select_mixed_se_512::bits_range_set0_dx<1>(size_t i, size_t k);

template<size_t dimensions>
void rank_select_mixed_se_512::bits_range_set1_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_words[j * 2 + dimensions] |= (bm_uint_t(-1) << i % UintBits)
            & ~(bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits)
            m_words[j++ * 2 + dimensions] |= (bm_uint_t(-1) << i % UintBits);
        while (j < k / UintBits)
            m_words[j++ * 2 + dimensions] = bm_uint_t(-1);
        if (k % UintBits)
            m_words[j * 2 + dimensions] |= ~(bm_uint_t(-1) << k % UintBits);
    }
}

template void TERARK_DLL_EXPORT rank_select_mixed_se_512::bits_range_set1_dx<0>(size_t i, size_t k);
template void TERARK_DLL_EXPORT rank_select_mixed_se_512::bits_range_set1_dx<1>(size_t i, size_t k);

template<size_t dimensions>
void rank_select_mixed_se_512::build_cache_dx(bool speed_select0, bool speed_select1) {
    rank_select_check_overflow(m_size[dimensions], > , rank_select_mixed_se_512);
    if (NULL == m_words) return;
    size_t constexpr flag_x_offset = dimensions == 0 ? 1 : 4;
    size_t constexpr flag_y_offset = dimensions == 0 ? 4 : 1;
    size_t constexpr dimensions_y = 1 - dimensions;
    assert((m_flags & (1 << flag_x_offset)) == 0);
    size_t ceiled_bits = (std::max(m_size[0], m_size[1]) + LineBits-1) & ~(LineBits-1);
    size_t lines = ceiled_bits / LineBits;
    ceiled_bits *= 2;
    size_t flag_as_u32_slots = 4;
    size_t u32_slots_used;
    if (m_flags & (1 << flag_y_offset)) {
        if (dimensions == 0)
            m_flags |= (1 << 0);
        size_t select0_slots_dy = (m_max_rank0[dimensions_y] + LineBits - 1) / LineBits;
        size_t select1_slots_dy = (m_max_rank1[dimensions_y] + LineBits - 1) / LineBits;
        u32_slots_used = ((m_flags & (1 << (flag_y_offset + 1))) ? select0_slots_dy + 1 : 0)
                       + ((m_flags & (1 << (flag_y_offset + 2))) ? select1_slots_dy + 1 : 0)
                       ;
    }
    else {
        shrink_to_fit();
        u32_slots_used = 0;
        reserve(ceiled_bits + (lines + 1) * sizeof(RankCacheMixed) * 8);
    }
    bits_range_set0_dx<dimensions>(m_size[dimensions], ceiled_bits / 2);
    m_flags |= (1 << flag_x_offset);
    m_flags |= (uint64_t(!!speed_select1) << (flag_x_offset + 1));
    m_flags |= (uint64_t(!!speed_select0) << (flag_x_offset + 2));

    RankCacheMixed* rank_cache = (RankCacheMixed*)(m_words + ceiled_bits / WordBits);
    uint64_t* pBit64 = (uint64_t*)m_words;
    size_t Rank1 = 0;
    for(size_t i = 0; i < lines; ++i) {
        size_t r = 0;
        uint64_t rela = 0;
        BOOST_STATIC_ASSERT(LineBits / 64 == 8);
        for(size_t j = 0; j < (LineBits / 64); ++j) {
            r += fast_popcount(pBit64[(i * (LineBits / 64) + j) * 2 + dimensions]);
            rela |= uint64_t(r) << (j * 9);
        }
        rela &= uint64_t(-1) >> 1; // set unused bit as zero
        rank_cache[i].base[dimensions] = uint32_t(Rank1);
        rank_cache[i].rela[dimensions] = rela;
        Rank1 += r;
    }
    rank_cache[lines].base[dimensions] = uint32_t(Rank1);
    rank_cache[lines].rela[dimensions] = 0;
    m_max_rank0[dimensions] = m_size[dimensions] - Rank1;
    m_max_rank1[dimensions] = Rank1;
    size_t select0_slots_dx = (m_max_rank0[dimensions] + LineBits - 1) / LineBits;
    size_t select1_slots_dx = (m_max_rank1[dimensions] + LineBits - 1) / LineBits;
    size_t u32_slots = (speed_select0 ? select0_slots_dx + 1 : 0)
                     + (speed_select1 ? select1_slots_dx + 1 : 0)
                     ;
    reserve((0
             + ceiled_bits
             + (lines + 1) * sizeof(RankCacheMixed) * 8
             + u32_slots * 32
             + u32_slots_used * 32
             + flag_as_u32_slots * 32
             + WordBits - 1
             ) & ~(WordBits - 1));
    rank_cache = m_rank_cache = (RankCacheMixed*)(m_words + ceiled_bits / WordBits);
    {
        char* start  = (char*)(rank_cache + lines + 1) + u32_slots_used * 4;
        char* finish = (char*)(m_words + m_capacity / WordBits);
        memset(start, 0, finish - start);
    }
    uint32_t* select_index = (uint32_t*)(rank_cache + lines + 1) + u32_slots_used;
    if (speed_select0) {
        uint32_t* sel0_cache = select_index;
        sel0_cache[dimensions] = 0;
        for(size_t j = 1; j < select0_slots_dx; ++j) {
            size_t k = sel0_cache[j - 1];
            while (k * LineBits - rank_cache[k].base[dimensions] < LineBits * j) ++k;
            sel0_cache[j] = k;
        }
        sel0_cache[select0_slots_dx] = lines;
        m_sel0_cache[dimensions] = sel0_cache;
        select_index += select0_slots_dx + 1;
    }
    if (speed_select1) {
        uint32_t* sel1_cache = select_index;
        sel1_cache[dimensions] = 0;
        for(size_t j = 1; j < select1_slots_dx; ++j) {
            size_t k = sel1_cache[j - 1];
            while (rank_cache[k].base[dimensions] < LineBits * j) ++k;
            sel1_cache[j] = k;
        }
        sel1_cache[select1_slots_dx] = lines;
        m_sel1_cache[dimensions] = sel1_cache;
    }
    ((uint64_t*)(m_words + m_capacity / WordBits))[-1] = m_flags;
    ((uint32_t*)(m_words + m_capacity / WordBits))[-4] = uint32_t(m_size[0]);
    ((uint32_t*)(m_words + m_capacity / WordBits))[-3] = uint32_t(m_size[1]);
}

template void TERARK_DLL_EXPORT rank_select_mixed_se_512::build_cache_dx<0>(bool speed_select0, bool speed_select1);
template void TERARK_DLL_EXPORT rank_select_mixed_se_512::build_cache_dx<1>(bool speed_select0, bool speed_select1);

template<size_t dimensions>
size_t rank_select_mixed_se_512::one_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / WordBits * 2 + dimensions, sum;
    size_t mod = bitpos % WordBits;
    if (mod != 0) {
        bm_uint_t x = m_words[j];
        if (!(x & (bm_uint_t(1) << mod))) return 0;
        bm_uint_t y = ~(x >> mod);
        size_t ctz = fast_ctz(y);
        if (ctz < WordBits - mod) {
            return ctz;
        }
        assert(ctz == WordBits - mod);
        j += 2;
        sum = ctz;
    }
    else {
        sum = 0;
    }
    size_t len = (m_size[dimensions] + WordBits - 1) / WordBits * 2;
    for (; j < len; j += 2) {
        bm_uint_t y = ~m_words[j];
        if (0 == y)
            sum += WordBits;
        else
            return sum + fast_ctz(y);
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::one_seq_len_dx<0>(size_t bitpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::one_seq_len_dx<1>(size_t bitpos) const;

template<size_t dimensions>
size_t rank_select_mixed_se_512::zero_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / WordBits * 2 + dimensions, sum;
    size_t mod = bitpos % WordBits;
    if (mod != 0) {
        bm_uint_t x = m_words[j];
        if (x & (bm_uint_t(1) << mod)) return 0;
        bm_uint_t y = x >> mod;
        if (y) {
            return fast_ctz(y);
        }
        j += 2;
        sum = WordBits - mod;
    }
    else {
        sum = 0;
    }
    size_t len = (m_size[dimensions] + WordBits - 1) / WordBits * 2;
    for (; j < len; j += 2) {
        bm_uint_t y = m_words[j];
        if (0 == y)
            sum += WordBits;
        else
            return sum + fast_ctz(y);
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::zero_seq_len_dx<0>(size_t bitpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::zero_seq_len_dx<1>(size_t bitpos) const;

template<size_t dimensions>
size_t rank_select_mixed_se_512::one_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, sum;
    if (endpos%WordBits != 0) {
        j = (endpos-1) / WordBits * 2 + dimensions;
        bm_uint_t x = m_words[j];
		if ( !(x & (bm_uint_t(1) << (endpos-1)%WordBits)) ) return 0;
		bm_uint_t y = ~(x << (WordBits - endpos%WordBits));
        size_t clz = fast_clz(y);
        assert(clz <= endpos%WordBits);
        assert(clz >= 1);
        if (clz < endpos%WordBits) {
            return clz;
        }
        sum = clz;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos-1) / WordBits * 2 + dimensions + 2;
        sum = 0;
    }
    while (j >= 2) {
        j -= 2;
        bm_uint_t y = ~m_words[j];
        if (0 == y)
            sum += WordBits;
        else
            return sum + fast_clz(y);
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::one_seq_revlen_dx<0>(size_t endpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::one_seq_revlen_dx<1>(size_t endpos) const;

template<size_t dimensions>
size_t rank_select_mixed_se_512::zero_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, sum;
    if (endpos%WordBits != 0) {
        j = (endpos-1) / WordBits * 2 + dimensions;
        bm_uint_t x = m_words[j];
		if (x & (bm_uint_t(1) << (endpos-1)%WordBits)) return 0;
		bm_uint_t y = x << (WordBits - endpos%WordBits);
        if (y) {
            return fast_clz(y);
        }
        sum = endpos%WordBits;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos-1) / WordBits * 2 + dimensions + 2;
        sum = 0;
    }
    while (j >= 2) {
        j -= 2;
        bm_uint_t y = m_words[j];
        if (0 == y)
            sum += WordBits;
        else
            return sum + fast_clz(y);
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::zero_seq_revlen_dx<0>(size_t endpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::zero_seq_revlen_dx<1>(size_t endpos) const;

template<size_t dimensions>
size_t rank_select_mixed_se_512::select0_dx(size_t Rank0) const {
    assert(m_flags & (1 << (dimensions == 0 ? 1 : 4)));
    GUARD_MAX_RANK(0[dimensions], Rank0);
    size_t lo, hi;
    if (m_sel0_cache[dimensions]) { // get the very small [lo, hi) range
        lo = m_sel0_cache[dimensions][Rank0 / LineBits];
        hi = m_sel0_cache[dimensions][Rank0 / LineBits + 1];
        //assert(lo < hi);
    }
    else {
        lo = 0;
        hi = (m_size[dimensions] + LineBits + 1) / LineBits;
    }
    const RankCacheMixed* rank_cache = m_rank_cache;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - rank_cache[mid].base[dimensions];
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - rank_cache[lo].base[dimensions]);
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rank_cache[lo-1].rela[dimensions];
    size_t hit = LineBits * (lo-1) - rank_cache[lo-1].base[dimensions];
    const uint64_t* pBit64 = (const uint64_t*)(m_words + LineWords * (lo-1) * 2);

#define select0_nth64(n) line_bitpos + 64*n + \
    UintSelect1(~pBit64[n*2+dimensions], Rank0 - (hit + 64*n - rank512(rcRela, n)))

    if (Rank0 < hit + 64*4 - rank512(rcRela, 4)) {
        if (Rank0 < hit + 64*2 - rank512(rcRela, 2))
            if (Rank0 < hit + 64*1 - rank512(rcRela, 1))
                return line_bitpos + UintSelect1(~pBit64[dimensions], Rank0 - hit);
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

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::select0_dx<0>(size_t Rank0) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::select0_dx<1>(size_t Rank0) const;

template<size_t dimensions>
size_t rank_select_mixed_se_512::select1_dx(size_t Rank1) const {
    assert(m_flags & (1 << (dimensions == 0 ? 1 : 4)));
    GUARD_MAX_RANK(1[dimensions], Rank1);
    size_t lo, hi;
    if (m_sel1_cache[dimensions]) { // get the very small [lo, hi) range
        lo = m_sel1_cache[dimensions][Rank1 / LineBits];
        hi = m_sel1_cache[dimensions][Rank1 / LineBits + 1];
        //assert(lo < hi);
    }
    else {
        lo = 0;
        hi = (m_size[dimensions] + LineBits + 1) / LineBits;
    }
    const RankCacheMixed* rank_cache = m_rank_cache;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = rank_cache[mid].base[dimensions];
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < rank_cache[lo].base[dimensions]);
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rank_cache[lo-1].rela[dimensions];
    size_t hit = rank_cache[lo-1].base[dimensions];
    const uint64_t* pBit64 = (const uint64_t*)(m_words + LineWords * (lo-1) * 2);

#define select1_nth64(n) line_bitpos + 64*n + \
     UintSelect1(pBit64[n*2+dimensions], Rank1 - (hit + rank512(rcRela, n)))

    if (Rank1 < hit + rank512(rcRela, 4)) {
        if (Rank1 < hit + rank512(rcRela, 2))
            if (Rank1 < hit + rank512(rcRela, 1))
                return line_bitpos + UintSelect1(pBit64[dimensions], Rank1 - hit);
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

template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::select1_dx<0>(size_t Rank1) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_se_512::select1_dx<1>(size_t Rank1) const;

template class TERARK_DLL_EXPORT rank_select_mixed_dimensions<rank_select_mixed_se_512, 0>;
template class TERARK_DLL_EXPORT rank_select_mixed_dimensions<rank_select_mixed_se_512, 1>;

} // namespace terark

