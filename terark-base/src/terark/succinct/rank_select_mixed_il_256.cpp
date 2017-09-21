#include "rank_select_mixed_il_256.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

rank_select_mixed_il_256::rank_select_mixed_il_256() {
    m_lines = NULL;
    m_capacity = 0;
    m_size[0] = m_size[1] = 0;
    nullize_cache();
}

rank_select_mixed_il_256::rank_select_mixed_il_256(size_t n, bool val0, bool val1) {
    rank_select_check_overflow(n, > , rank_select_mixed_il_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    m_size[0] = m_size[1] = n;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    get<0>().set(0, n, val0);
    get<1>().set(0, n, val1);
    nullize_cache();
}

rank_select_mixed_il_256::rank_select_mixed_il_256(size_t n, valvec_no_init) {
    rank_select_check_overflow(n, > , rank_select_mixed_il_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    m_size[0] = m_size[1] = n;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    nullize_cache();
}

rank_select_mixed_il_256::rank_select_mixed_il_256(size_t n, valvec_reserve) {
    rank_select_check_overflow(n, > , rank_select_mixed_il_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    m_size[0] = m_size[1] = 0;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    nullize_cache();
}

rank_select_mixed_il_256::rank_select_mixed_il_256(const rank_select_mixed_il_256& y)
    : rank_select_mixed_il_256(y.m_capacity, valvec_reserve())
{
    assert(this != &y);
    assert(y.m_capacity % sizeof(bm_uint_t) == 0);
    assert(y.m_size[0] == y.m_size[1]);
    m_size[0] = y.m_size[0];
    m_size[1] = y.m_size[1];
    memcpy(this->m_lines, y.m_lines, y.m_capacity);
    if (y.m_sel0_cache[0]) {
        m_sel0_cache[0] = (uint32_t*)this->m_lines + (y.m_sel0_cache[0] - (uint32_t*)y.m_lines);
    }
    if (y.m_sel0_cache[1]) {
        m_sel0_cache[1] = (uint32_t*)this->m_lines + (y.m_sel0_cache[1] - (uint32_t*)y.m_lines);
    }
    if (y.m_sel1_cache[0]) {
        m_sel1_cache[0] = (uint32_t*)this->m_lines + (y.m_sel1_cache[0] - (uint32_t*)y.m_lines);
    }
    if (y.m_sel1_cache[1]) {
        m_sel1_cache[1] = (uint32_t*)this->m_lines + (y.m_sel1_cache[1] - (uint32_t*)y.m_lines);
    }
    m_max_rank0[0] = y.m_max_rank0[0];
    m_max_rank0[1] = y.m_max_rank0[1];
    m_max_rank1[0] = y.m_max_rank1[0];
    m_max_rank1[1] = y.m_max_rank1[1];
}

rank_select_mixed_il_256& rank_select_mixed_il_256::operator=(const rank_select_mixed_il_256& y) {
    if (this != &y) {
        this->clear();
        new(this)rank_select_mixed_il_256(y);
    }
    return *this;
}

#if defined(HSM_HAS_MOVE)

rank_select_mixed_il_256::rank_select_mixed_il_256(rank_select_mixed_il_256&& y) noexcept {
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
}

rank_select_mixed_il_256& rank_select_mixed_il_256::operator=(rank_select_mixed_il_256&& y) noexcept {
    if (m_lines)
        ::free(m_lines);
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
    return *this;
}

#endif

rank_select_mixed_il_256::~rank_select_mixed_il_256() {
    if (m_lines)
        ::free(m_lines);
}

void rank_select_mixed_il_256::clear() {
    if (m_lines)
        ::free(m_lines);
    risk_release_ownership();
}

void rank_select_mixed_il_256::risk_release_ownership() {
    nullize_cache();
    m_lines = nullptr;
    m_capacity = 0;
    m_size[0] = m_size[1] = 0;
}

void rank_select_mixed_il_256::risk_mmap_from(unsigned char* base, size_t length) {
    assert(NULL == m_lines);
    assert(length % sizeof(uint64_t) == 0);
    m_flags = ((uint64_t*)(base + length))[-1];
    m_size[0] = ((uint32_t*)(base + length))[-4];
    m_size[1] = ((uint32_t*)(base + length))[-3];
    size_t ceiled_bits = (std::max(m_size[0], m_size[1]) + LineBits - 1) & ~size_t(LineBits - 1);
    size_t nlines = ceiled_bits / LineBits;
    ceiled_bits *= 2;
    m_lines = (RankCacheMixed*)base;
    m_capacity = length;
    uint32_t* select_index = (uint32_t*)(m_lines + nlines + 1);
    auto load_d0 = [&] {
        if (m_flags & (1 << 1)) {
            m_max_rank1[0] = m_lines[nlines].mixed[0].base;
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
            m_max_rank1[1] = m_lines[nlines].mixed[1].base;
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

void rank_select_mixed_il_256::shrink_to_fit() {
    assert(NULL == m_sel0_cache[0]);
    assert(NULL == m_sel0_cache[1]);
    assert(NULL == m_sel1_cache[0]);
    assert(NULL == m_sel1_cache[1]);
    assert(0 == m_max_rank0[0]);
    assert(0 == m_max_rank0[1]);
    assert(0 == m_max_rank1[0]);
    assert(0 == m_max_rank1[1]);
    size_t size = std::max(m_size[0], m_size[1]);
    size_t new_bytes = ((size + LineBits - 1) & ~(LineBits - 1)) / LineBits * sizeof(RankCacheMixed);
    auto new_lines = (RankCacheMixed*)realloc(m_lines, new_bytes);
    if (NULL == new_lines)
        throw std::bad_alloc();
    m_lines = new_lines;
    m_capacity = new_bytes;
}

void rank_select_mixed_il_256::swap(rank_select_mixed_il_256& y) {
    std::swap(m_lines, y.m_lines);
    std::swap(m_size, y.m_size);
    std::swap(m_capacity, y.m_capacity);
    std::swap(m_sel0_cache, y.m_sel0_cache);
    std::swap(m_sel1_cache, y.m_sel1_cache);
    std::swap(m_max_rank0, y.m_max_rank0);
    std::swap(m_max_rank1, y.m_max_rank1);
}

const void* rank_select_mixed_il_256::data() const {
    return m_lines;
}

size_t rank_select_mixed_il_256::mem_size() const {
    return m_capacity;
}

void rank_select_mixed_il_256::grow() {
    assert(std::max(m_size[0], m_size[1]) == m_capacity / sizeof(RankCacheMixed) * LineBits);
    assert((m_flags & (1 << 1)) == 0);
    assert((m_flags & (1 << 4)) == 0);
    m_capacity = std::max(m_capacity, sizeof(RankCacheMixed));
    auto new_lines = (RankCacheMixed*)realloc(m_lines, m_capacity * 2);
    if (NULL == new_lines)
        throw std::bad_alloc();
    m_lines = new_lines;
    m_capacity *= 2;
}

void rank_select_mixed_il_256::reserve_bytes(size_t capacity) {
    assert(capacity == ((capacity + sizeof(bm_uint_t) - 1) & ~(sizeof(bm_uint_t) - 1)));
    if (capacity <= m_capacity)
        return;
    auto new_lines = (RankCacheMixed*)realloc(m_lines, capacity);
    if (NULL == new_lines)
        throw std::bad_alloc();
    m_lines = new_lines;
    m_capacity = capacity;
}

void rank_select_mixed_il_256::reserve(size_t capacity) {
    assert(capacity == ((capacity + LineBits - 1) & ~(LineBits - 1)));
    reserve_bytes(capacity / LineBits * sizeof(RankCacheMixed));
}

void rank_select_mixed_il_256::nullize_cache() {
    m_flags = 0;
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
void rank_select_mixed_il_256::bits_range_set0_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] &= ~(bm_uint_t(-1) << i % UintBits)
            | (bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits) {
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] &= ~(bm_uint_t(-1) << i % UintBits);
            ++j;
        }
        while (j < k / UintBits) {
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] = 0;
            ++j;
        }
        if (k % UintBits)
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] &= bm_uint_t(-1) << k % UintBits;
    }
}

template void TERARK_DLL_EXPORT rank_select_mixed_il_256::bits_range_set0_dx<0>(size_t i, size_t k);
template void TERARK_DLL_EXPORT rank_select_mixed_il_256::bits_range_set0_dx<1>(size_t i, size_t k);

template<size_t dimensions>
void rank_select_mixed_il_256::bits_range_set1_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] |= (bm_uint_t(-1) << i % UintBits)
            & ~(bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits) {
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] |= (bm_uint_t(-1) << i % UintBits);
            ++j;
        }
        while (j < k / UintBits) {
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] = bm_uint_t(-1);
            ++j;
        }
        if (k % UintBits)
            m_lines[j / LineWords].mixed[dimensions].words[j % LineWords] |= ~(bm_uint_t(-1) << k % UintBits);
    }
}

template void TERARK_DLL_EXPORT rank_select_mixed_il_256::bits_range_set1_dx<0>(size_t i, size_t k);
template void TERARK_DLL_EXPORT rank_select_mixed_il_256::bits_range_set1_dx<1>(size_t i, size_t k);

template<size_t dimensions>
void rank_select_mixed_il_256::build_cache_dx(bool speed_select0, bool speed_select1) {
    rank_select_check_overflow(m_size[dimensions], > , rank_select_mixed_il_256);
    if (NULL == m_lines) return;
    size_t constexpr flag_x_offset = dimensions == 0 ? 1 : 4;
    size_t constexpr flag_y_offset = dimensions == 0 ? 4 : 1;
    size_t constexpr dimensions_y = 1 - dimensions;
    assert((m_flags & (1 << flag_x_offset)) == 0);
    size_t ceiled_bits = (std::max(m_size[0], m_size[1]) + LineBits-1) & ~(LineBits-1);
    size_t lines = ceiled_bits / LineBits;
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
        reserve_bytes((lines + 1) * sizeof(RankCacheMixed));
    }
    bits_range_set0_dx<dimensions>(m_size[dimensions], ceiled_bits);
    m_flags |= (1 << flag_x_offset);
    m_flags |= (uint64_t(!!speed_select1) << (flag_x_offset + 1));
    m_flags |= (uint64_t(!!speed_select0) << (flag_x_offset + 2));

    size_t Rank1 = 0;
    for(size_t i = 0; i < lines; ++i) {
        size_t inc = 0;
        m_lines[i].mixed[dimensions].base = (uint32_t)(Rank1);
        for (size_t j = 0; j < 4; ++j) {
            m_lines[i].mixed[dimensions].rlev[j] = (uint8_t)inc;
            inc += fast_popcount(m_lines[i].mixed[dimensions].bit64[j]);
        }
        Rank1 += inc;
    }
    m_lines[lines].mixed[dimensions].base = uint32_t(Rank1);
    for (size_t j = 0; j < 4; ++j)
        m_lines[lines].mixed[dimensions].rlev[j] = 0;
    m_max_rank0[dimensions] = m_size[dimensions] - Rank1;
    m_max_rank1[dimensions] = Rank1;
    size_t select0_slots_dx = (m_max_rank0[dimensions] + LineBits - 1) / LineBits;
    size_t select1_slots_dx = (m_max_rank1[dimensions] + LineBits - 1) / LineBits;
    size_t u32_slots = (speed_select0 ? select0_slots_dx + 1 : 0)
                     + (speed_select1 ? select1_slots_dx + 1 : 0)
                     ;
    reserve_bytes((0
                   + (lines + 1) * sizeof(RankCacheMixed)
                   + u32_slots * 4
                   + u32_slots_used * 4
                   + flag_as_u32_slots * 4
                   + sizeof(bm_uint_t) - 1
                   ) & ~(sizeof(bm_uint_t) - 1));
    {
        char* start  = (char*)(m_lines + lines + 1) + u32_slots_used * 4;
        char* finish = (char*)m_lines + m_capacity;
        memset(start, 0, finish - start);
    }
    uint32_t* select_index = (uint32_t*)(m_lines + lines + 1) + u32_slots_used;
    if (speed_select0) {
        uint32_t* sel0_cache = select_index;
        sel0_cache[dimensions] = 0;
        for(size_t j = 1; j < select0_slots_dx; ++j) {
            size_t k = sel0_cache[j - 1];
            while (k * LineBits - m_lines[k].mixed[dimensions].base < LineBits * j) ++k;
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
            while (m_lines[k].mixed[dimensions].base < LineBits * j) ++k;
            sel1_cache[j] = k;
        }
        sel1_cache[select1_slots_dx] = lines;
        m_sel1_cache[dimensions] = sel1_cache;
    }
    ((uint64_t*)((char*)m_lines + m_capacity))[-1] = m_flags;
    ((uint32_t*)((char*)m_lines + m_capacity))[-4] = uint32_t(m_size[0]);
    ((uint32_t*)((char*)m_lines + m_capacity))[-3] = uint32_t(m_size[1]);
}

template void TERARK_DLL_EXPORT rank_select_mixed_il_256::build_cache_dx<0>(bool speed_select0, bool speed_select1);
template void TERARK_DLL_EXPORT rank_select_mixed_il_256::build_cache_dx<1>(bool speed_select0, bool speed_select1);

template<size_t dimensions>
size_t rank_select_mixed_il_256::one_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / LineBits, k, sum;
    if (bitpos % WordBits != 0) {
        bm_uint_t x = m_lines[j].mixed[dimensions].words[bitpos % LineBits / WordBits];
        if (!(x & (bm_uint_t(1) << bitpos % WordBits))) return 0;
        bm_uint_t y = ~(x >> bitpos % WordBits);
        size_t ctz = fast_ctz(y);
        if (ctz < WordBits - bitpos % WordBits) {
            return ctz;
        }
        assert(ctz == WordBits - bitpos % WordBits);
        k = bitpos % LineBits / WordBits + 1;
        sum = ctz;
    }
    else {
        k = bitpos % LineBits / WordBits;
        sum = 0;
    }
    size_t len = BitsToLines(m_size[dimensions]) + 1;
    for (; j < len; ++j) {
        for (; k < LineWords; ++k) {
            bm_uint_t y = ~m_lines[j].mixed[dimensions].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_ctz(y);
        }
        k = 0;
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::one_seq_len_dx<0>(size_t bitpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::one_seq_len_dx<1>(size_t bitpos) const;

template<size_t dimensions>
size_t rank_select_mixed_il_256::zero_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / LineBits, k, sum;
    if (bitpos % WordBits != 0) {
        bm_uint_t x = m_lines[j].mixed[dimensions].words[bitpos % LineBits / WordBits];
        if (x & (bm_uint_t(1) << bitpos % WordBits)) return 0;
        bm_uint_t y = x >> bitpos % WordBits;
        if (y) {
            return fast_ctz(y);
        }
        k = bitpos % LineBits / WordBits + 1;
        sum = WordBits - bitpos % WordBits;
    }
    else {
        k = bitpos % LineBits / WordBits;
        sum = 0;
    }
    size_t len = BitsToLines(m_size[dimensions]) + 1;
    for (; j < len; ++j) {
        for (; k < LineWords; ++k) {
            bm_uint_t y = m_lines[j].mixed[dimensions].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_ctz(y);
        }
        k = 0;
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::zero_seq_len_dx<0>(size_t bitpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::zero_seq_len_dx<1>(size_t bitpos) const;

template<size_t dimensions>
size_t rank_select_mixed_il_256::one_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos - 1) / LineBits;
        bm_uint_t x = m_lines[j].mixed[dimensions].words[(endpos-1)%LineBits / WordBits];
        if (!(x & (bm_uint_t(1) << (endpos-1)%WordBits))) return 0;
        bm_uint_t y = ~(x << (WordBits - endpos%WordBits));
        size_t clz = fast_clz(y);
        assert(clz <= endpos%WordBits);
        assert(clz >= 1);
        if (clz < endpos%WordBits) {
            return clz;
        }
        k = (endpos-1) % LineBits / WordBits - 1;
        sum = clz;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos - 1) / LineBits;
        k = (endpos - 1) % LineBits / WordBits;
        sum = 0;
    }
    for (; j != size_t(-1); --j) {
        for (; k != size_t(-1); --k) {
            bm_uint_t y = ~m_lines[j].mixed[dimensions].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::one_seq_revlen_dx<0>(size_t endpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::one_seq_revlen_dx<1>(size_t endpos) const;

template<size_t dimensions>
size_t rank_select_mixed_il_256::zero_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos - 1) / LineBits;
        bm_uint_t x = m_lines[j].mixed[dimensions].words[(endpos-1)%LineBits / WordBits];
		if (x & (bm_uint_t(1) << (endpos-1)%WordBits)) return 0;
		bm_uint_t y = x << (WordBits - endpos%WordBits);
		if (y) {
			return fast_clz(y);
		}
        k = (endpos-1) % LineBits / WordBits - 1;
        sum = endpos%WordBits;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos-1) / LineBits;
        k = (endpos-1) % LineBits / WordBits;
        sum = 0;
    }
    for (; j != size_t(-1); --j) {
        for (; k != size_t(-1); --k) {
            bm_uint_t y = m_lines[j].mixed[dimensions].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::zero_seq_revlen_dx<0>(size_t endpos) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::zero_seq_revlen_dx<1>(size_t ebdpos) const;

template<size_t dimensions>
size_t rank_select_mixed_il_256::select0_dx(size_t Rank0) const {
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
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1].mixed[dimensions];
    size_t hit = LineBits * (lo - 1) - xx.base;
    size_t index = (lo-1) * LineBits; // base bit index

    if (Rank0 < hit + 64*2 - xx.rlev[2]) {
        if (Rank0 < hit + 64*1 - xx.rlev[1]) { // xx.rlev[0] is always 0
            return index + 64*0 + UintSelect1(~xx.bit64[0], Rank0 - hit);
        }
        return index + 64*1 + UintSelect1(
                ~xx.bit64[1], Rank0 - (hit + 64*1 - xx.rlev[1]));
    }
    if (Rank0 < hit + 64*3 - xx.rlev[3]) {
        return index + 64*2 + UintSelect1(
                ~xx.bit64[2], Rank0 - (hit + 64*2 - xx.rlev[2]));
    }
       else {
        return index + 64*3 + UintSelect1(
                ~xx.bit64[3], Rank0 - (hit + 64*3 - xx.rlev[3]));
    }
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::select0_dx<0>(size_t Rank0) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::select0_dx<1>(size_t Rank0) const;

template<size_t dimensions>
size_t rank_select_mixed_il_256::select1_dx(size_t Rank1) const {
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
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1].mixed[dimensions];
    size_t hit = xx.base;
    assert(Rank1 >= hit);
    size_t index = (lo-1) * LineBits; // base bit index
    if (Rank1 < hit + xx.rlev[2]) {
        if (Rank1 < hit + xx.rlev[1]) { // xx.rlev[0] is always 0
            return index + UintSelect1(xx.bit64[0], Rank1 - hit);
        }
        return index + 64*1 + UintSelect1(
                 xx.bit64[1], Rank1 - (hit + xx.rlev[1]));
    }
    if (Rank1 < hit + xx.rlev[3]) {
        return index + 64*2 + UintSelect1(
                 xx.bit64[2], Rank1 - (hit + xx.rlev[2]));
    }
       else {
        return index + 64*3 + UintSelect1(
                 xx.bit64[3], Rank1 - (hit + xx.rlev[3]));
    }
}

template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::select1_dx<0>(size_t Rank1) const;
template size_t TERARK_DLL_EXPORT rank_select_mixed_il_256::select1_dx<1>(size_t Rank1) const;

template class TERARK_DLL_EXPORT rank_select_mixed_dimensions<rank_select_mixed_il_256, 0>;
template class TERARK_DLL_EXPORT rank_select_mixed_dimensions<rank_select_mixed_il_256, 1>;

} // namespace terark

