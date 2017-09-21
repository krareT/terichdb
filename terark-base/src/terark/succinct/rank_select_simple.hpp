#ifndef __terark_rank_select_simple_hpp__
#define __terark_rank_select_simple_hpp__

#include "rank_select_basic.hpp"

namespace terark {

class TERARK_DLL_EXPORT rank_select_simple
    : public RankSelectConstants<256>, public febitvec {
    uint32_t* m_rank_cache;
    size_t    m_max_rank0;
    size_t    m_max_rank1;
public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_simple();
    rank_select_simple(size_t n, bool val = false);
    rank_select_simple(size_t n, valvec_no_init);
    rank_select_simple(size_t n, valvec_reserve);
    rank_select_simple(const rank_select_simple&);
    rank_select_simple& operator=(const rank_select_simple&);
#if defined(HSM_HAS_MOVE)
    rank_select_simple(rank_select_simple&& y) noexcept;
    rank_select_simple& operator=(rank_select_simple&& y) noexcept;
#endif
    ~rank_select_simple();
    void clear();
    void risk_release_ownership();
    void risk_mmap_from(unsigned char* base, size_t length);
    void shrink_to_fit();

    void swap(rank_select_simple&);
    void build_cache(bool speed_select0, bool speed_select1);
    size_t mem_size() const;
    size_t rank1(size_t bitpos) const;
    size_t rank0(size_t bitpos) const { return bitpos - rank1(bitpos); }
    size_t select1(size_t id) const;
    size_t select0(size_t id) const;
    size_t max_rank1() const { return m_max_rank1; }
    size_t max_rank0() const { return m_max_rank0; }

    const uint32_t* get_rank_cache() const { return m_rank_cache; }
    const uint32_t* get_sel0_cache() const { return NULL; }
    const uint32_t* get_sel1_cache() const { return NULL; }
    static inline size_t fast_rank0(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos);
    static inline size_t fast_rank1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos);
    static inline size_t fast_select0(const bm_uint_t* bits, const uint32_t* sel0Cache, const uint32_t* rankCache, size_t id);
    static inline size_t fast_select1(const bm_uint_t* bits, const uint32_t* sel1Cache, const uint32_t* rankCache, size_t id);

    size_t excess1(size_t bp) const { return 2*rank1(bp) - bp; }
    static size_t fast_excess1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos)
        { return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos; }
};

inline size_t rank_select_simple::
fast_rank0(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
    return bitpos - fast_rank1(bits, rankCache, bitpos);
}

inline size_t rank_select_simple::
fast_rank1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
    size_t line_wordpos = (bitpos & ~(LineBits - 1)) / WordBits;
    size_t line_word_idxupp = bitpos / WordBits;
    size_t rank = rankCache[bitpos / LineBits];
    for (size_t i = line_wordpos; i < line_word_idxupp; ++i)
        rank += fast_popcount(bits[i]);
    if (bitpos % WordBits != 0)
        rank += fast_popcount_trail(bits[line_word_idxupp], bitpos % WordBits);
    return rank;
}

inline size_t rank_select_simple::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const uint32_t* rankCache, size_t rank) {
    THROW_STD(invalid_argument, "not supported");
}

inline size_t rank_select_simple::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const uint32_t* rankCache, size_t rank) {
    THROW_STD(invalid_argument, "not supported");
}

} // namespace terark

#endif // __terark_rank_select_simple_hpp__

