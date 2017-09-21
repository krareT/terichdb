#ifndef __terark_rank_select_se_256_hpp__
#define __terark_rank_select_se_256_hpp__

#include "rank_select_basic.hpp"

namespace terark {

// rank   use 2-level cache, time is O(1), 2 memory access
// select use 1-level cache, time is O(1+loglog(n))
// rank_select_se, "_se" means "separated"
// rank index is separated from bits
class TERARK_DLL_EXPORT rank_select_se
    : public RankSelectConstants<256>, public febitvec {
public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_se();
    rank_select_se(size_t n, bool val = false);
    rank_select_se(size_t n, valvec_no_init);
    rank_select_se(size_t n, valvec_reserve);
    rank_select_se(const rank_select_se&);
    rank_select_se& operator=(const rank_select_se&);
#if defined(HSM_HAS_MOVE)
    rank_select_se(rank_select_se&& y) noexcept;
    rank_select_se& operator=(rank_select_se&& y) noexcept;
#endif
    ~rank_select_se();
    void clear();
    void risk_release_ownership();
    void risk_mmap_from(unsigned char* base, size_t length);
    void shrink_to_fit();

    void swap(rank_select_se&);
    void build_cache(bool speed_select0, bool speed_select1);
    size_t mem_size() const;
    inline size_t rank0(size_t bitpos) const;
    inline size_t rank1(size_t bitpos) const;
    size_t select0(size_t id) const;
    size_t select1(size_t id) const;
    size_t max_rank1() const { return m_max_rank1; }
    size_t max_rank0() const { return m_max_rank0; }
protected:
    void nullize_cache();
    struct RankCache {
        uint32_t  lev1;
        uint8_t   lev2[4]; // use uint64 for rank-select word
        explicit RankCache(uint32_t l1);
        operator size_t() const { return lev1; }
    };
    RankCache* m_rank_cache;
    uint32_t*  m_sel0_cache;
    uint32_t*  m_sel1_cache;
    size_t     m_max_rank0;
    size_t     m_max_rank1;
public:
    const RankCache* get_rank_cache() const { return m_rank_cache; }
    const uint32_t* get_sel0_cache() const { return m_sel0_cache; }
    const uint32_t* get_sel1_cache() const { return m_sel1_cache; }

    static inline size_t fast_rank0(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos);
    static inline size_t fast_rank1(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos);
    static inline size_t fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const RankCache* rankCache, size_t id);
    static inline size_t fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const RankCache* rankCache, size_t id);

    size_t excess1(size_t bp) const { return 2*rank1(bp) - bp; }
    static size_t fast_excess1(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos)
        { return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos; }
};

inline size_t rank_select_se::
rank0(size_t bitpos) const {
    assert(bitpos < m_size);
    return bitpos - rank1(bitpos);
}

inline size_t rank_select_se::
rank1(size_t bitpos) const {
    assert(bitpos < m_size);
    RankCache rc = m_rank_cache[bitpos / LineBits];
    return rc.lev1 + rc.lev2[(bitpos / 64) % 4] +
        fast_popcount_trail(
            ((const uint64_t*)this->m_words)[bitpos / 64], bitpos % 64);
}

inline size_t rank_select_se::
fast_rank0(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos) {
    return bitpos - fast_rank1(bits, rankCache, bitpos);
}

inline size_t rank_select_se::
fast_rank1(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos) {
    RankCache rc = rankCache[bitpos / LineBits];
    return rc.lev1 + rc.lev2[(bitpos / 64) % 4] +
        fast_popcount_trail(
            ((const uint64_t*)bits)[bitpos / 64], bitpos % 64);
}

inline size_t rank_select_se::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const RankCache* rankCache, size_t Rank0) {
    size_t lo = sel0[Rank0 / LineBits];
    size_t hi = sel0[Rank0 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - rankCache[mid].lev1;
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - rankCache[lo].lev1);
    size_t line_bitpos = (lo-1) * LineBits;
    RankCache rc = rankCache[lo-1];
    size_t hit = LineBits * (lo-1) - rc.lev1;
    const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

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

inline size_t rank_select_se::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const RankCache* rankCache, size_t Rank1) {
    size_t lo = sel1[Rank1 / LineBits];
    size_t hi = sel1[Rank1 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = rankCache[mid].lev1;
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < rankCache[lo].lev1);
    size_t line_bitpos = (lo-1) * LineBits;
    RankCache rc = rankCache[lo-1];
    size_t hit = rc.lev1;
    const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

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

typedef rank_select_se rank_select_se_256;
typedef rank_select_se rank_select_se_256_32;

} // namespace terark

#endif // __terark_rank_select_se_256_hpp__

