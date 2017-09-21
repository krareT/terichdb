#include "rank_select_il_256.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

inline rank_select_il::Line::Line(bool val) {
    rlev1 = 0;
    memset(rlev2, 0, sizeof(rlev2));
    if (val)
        memset(words, -1, sizeof(words));
    else
        memset(words, 0, sizeof(words));
}

void rank_select_il::push_back_slow_path(bool val) {
    rank_select_check_overflow(m_size, >= , rank_select_il_256);
    m_lines.emplace_back(false);
    this->set(m_size++, val);
}

rank_select_il::rank_select_il() {
    m_fast_select0 = m_fast_select1 = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    m_size = 0;
}

rank_select_il::rank_select_il(size_t bits, bool val) {
    rank_select_check_overflow(bits, > , rank_select_il_256);
    m_lines.resize_fill(BitsToLines(bits), Line(val));
    m_fast_select0 = m_fast_select1 = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, bool val, bool padding) {
    rank_select_check_overflow(bits, > , rank_select_il_256);
    m_lines.resize_fill(BitsToLines(bits), Line(val));
    if (padding != val)
        set(bits, m_lines.size()*LineBits - bits, padding);
    m_fast_select0 = m_fast_select1 = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, valvec_no_init) {
    rank_select_check_overflow(bits, > , rank_select_il_256);
    m_lines.resize_no_init(BitsToLines(bits));
    m_fast_select0 = m_fast_select1 = NULL;
    m_max_rank0 = m_max_rank1 = 0;
    m_size = bits;
}

rank_select_il::rank_select_il(size_t bits, valvec_reserve) {
    rank_select_check_overflow(bits, > , rank_select_il_256);
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
    rank_select_check_overflow(newsize, > , rank_select_il_256);
    m_lines.resize(BitsToLines(newsize), Line(val));
    m_size = newsize;
}

void rank_select_il::resize_no_init(size_t newsize) {
    rank_select_check_overflow(newsize, > , rank_select_il_256);
    m_lines.resize_no_init(BitsToLines(newsize));
    m_size = newsize;
}

void rank_select_il::resize_fill(size_t newsize, bool val) {
    rank_select_check_overflow(newsize, > , rank_select_il_256);
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
    assert(endpos <= this->size());
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos-1) / LineBits;
        bm_uint_t x = m_lines[j].words[(endpos-1)%LineBits / WordBits];
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
        j = (endpos-1) / LineBits;
        k = (endpos-1) % LineBits / WordBits;
        sum = 0;
    }
    for (; j != size_t(-1); --j) {
        for (; k != size_t(-1); --k) {
            bm_uint_t y = ~m_lines[j].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

size_t rank_select_il::zero_seq_revlen(size_t endpos) const {
    assert(endpos <= this->size());
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos-1) / LineBits;
        bm_uint_t x = m_lines[j].words[(endpos-1)%LineBits / WordBits];
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
            bm_uint_t y = m_lines[j].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

size_t rank_select_il::select0(size_t Rank0) const {
    GUARD_MAX_RANK(0, Rank0);
    size_t lo, hi;
    if (m_fast_select0) { // get the very small [lo, hi) range
        lo = m_fast_select0[Rank0 / LineBits];
        hi = m_fast_select0[Rank0 / LineBits + 1];
        //assert(lo < hi);
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
    const Line& xx = lines[lo - 1];
    size_t hit = LineBits * (lo - 1) - xx.rlev1;
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
        //assert(lo < hi);
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
    assert(Rank1 < lines[lo].rlev1);
    const Line& xx = lines[lo - 1];
    size_t hit = xx.rlev1;
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
    rank_select_check_overflow(m_size, > , rank_select_il_256);
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

