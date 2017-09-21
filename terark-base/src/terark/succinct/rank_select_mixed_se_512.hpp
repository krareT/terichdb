#ifndef __terark_rank_select_mixed_se_512_hpp__
#define __terark_rank_select_mixed_se_512_hpp__

#include "rank_select_basic.hpp"
#include "rank_select_mixed_basic.hpp"

namespace terark {

class TERARK_DLL_EXPORT rank_select_mixed_se_512 : public RankSelectConstants<512> {
public:
    typedef boost::mpl::true_ is_mixed;
    typedef uint32_t index_t;
    rank_select_mixed_se_512();
    rank_select_mixed_se_512(size_t n, bool val0 = false, bool val1 = false);
    rank_select_mixed_se_512(size_t n, valvec_no_init);
    rank_select_mixed_se_512(size_t n, valvec_reserve);
    rank_select_mixed_se_512(const rank_select_mixed_se_512&);
    rank_select_mixed_se_512& operator=(const rank_select_mixed_se_512&);
#if defined(HSM_HAS_MOVE)
    rank_select_mixed_se_512(rank_select_mixed_se_512&& y) noexcept;
    rank_select_mixed_se_512& operator=(rank_select_mixed_se_512&& y) noexcept;
#endif

    ~rank_select_mixed_se_512();
    void clear();
    void risk_release_ownership();
    void risk_mmap_from(unsigned char* base, size_t length);
    void shrink_to_fit();

    void swap(rank_select_mixed_se_512&);
    const void* data() const;
    size_t mem_size() const;
    
protected:
    struct RankCacheMixed {
        uint32_t base[2];
        union {
            uint64_t rela[2];
            struct {
                uint64_t _1 : 9;
                uint64_t _2 : 9;
                uint64_t _3 : 9;
                uint64_t _4 : 9;
                uint64_t _5 : 9;
                uint64_t _6 : 9;
                uint64_t _7 : 9;
                uint64_t _A : 1;
            } rela_debug[2];
        };
        template<size_t dimensions> size_t get_base() const { return base[dimensions]; }
    };
    typedef bm_uint_t bldata_t;
    
    static size_t fix_resize_size(size_t bits) {
        rank_select_check_overflow(bits, > , rank_select_mixed_se_512);
        return ((bits + WordBits - 1) & ~(WordBits - 1)) * 2;
    }
    void grow();
    void reserve(size_t bits_capacity);
    void nullize_cache();
    const bldata_t* bldata() const { return m_words; }

    template<size_t dimensions> void bits_range_set0_dx(size_t i, size_t k);
    template<size_t dimensions> void bits_range_set1_dx(size_t i, size_t k);
    
    template<size_t dimensions>
    void set_word_dx(size_t word_idx, bm_uint_t bits) {
        assert(word_idx < num_words_dx<dimensions>());
        m_words[word_idx * 2 + dimensions] = bits;
    }
    template<size_t dimensions>
    bm_uint_t get_word_dx(size_t word_idx) const {
        assert(word_idx < num_words_dx<dimensions>());
        return m_words[word_idx * 2 + dimensions];
    }
    template<size_t dimensions>
    size_t num_words_dx() const { return (m_size[dimensions] + WordBits - 1) / WordBits; }
    
    template<size_t dimensions>
    void push_back_dx(bool val) {
        rank_select_check_overflow(m_size[dimensions], >= , rank_select_mixed_se_512);
        assert(m_size[dimensions] * 2 <= m_capacity);
        if (terark_unlikely(m_size[dimensions] * 2 == m_capacity))
            grow();
        size_t i = m_size[dimensions]++;
        val ? set1_dx<dimensions>(i) : set0_dx<dimensions>(i);
    }
    template<size_t dimensions>
    bool is0_dx(size_t i) const {
        assert(i < m_size[dimensions]);
        return !terark_bit_test(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
    }
    template<size_t dimensions>
    bool is1_dx(size_t i) const {
        assert(i < m_size[dimensions]);
        return terark_bit_test(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
    }
    template<size_t dimensions>
    void set0_dx(size_t i) {
        assert(i < m_size[dimensions]);
        terark_bit_set0(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
    }
    template<size_t dimensions>
    void set1_dx(size_t i) {
        assert(i < m_size[dimensions]);
        terark_bit_set1(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
    }
    template<size_t dimensions> void build_cache_dx(bool speed_select0, bool speed_select1);
    template<size_t dimensions> size_t one_seq_len_dx(size_t bitpos) const;
    template<size_t dimensions> size_t zero_seq_len_dx(size_t bitpos) const;
    template<size_t dimensions> size_t one_seq_revlen_dx(size_t endpos) const;
    template<size_t dimensions> size_t zero_seq_revlen_dx(size_t endpos) const;
    template<size_t dimensions> inline size_t rank0_dx(size_t bitpos) const;
    template<size_t dimensions> inline size_t rank1_dx(size_t bitpos) const;
    template<size_t dimensions> size_t select0_dx(size_t id) const;
    template<size_t dimensions> size_t select1_dx(size_t id) const;

public:
    template<size_t dimensions>
    rank_select_mixed_dimensions<rank_select_mixed_se_512, dimensions>& get() {
        static_assert(dimensions < 2, "dimensions must less than 2 !");
        return *reinterpret_cast<rank_select_mixed_dimensions<rank_select_mixed_se_512, dimensions>*>(this);
    }
    rank_select_mixed_dimensions<rank_select_mixed_se_512, 0>& first (){ return get<0>(); }
    rank_select_mixed_dimensions<rank_select_mixed_se_512, 1>& second(){ return get<1>(); }
    rank_select_mixed_dimensions<rank_select_mixed_se_512, 0>& left  (){ return get<0>(); }
    rank_select_mixed_dimensions<rank_select_mixed_se_512, 1>& right (){ return get<1>(); }

protected:
    bm_uint_t* m_words;
    size_t m_size[2];
    size_t m_capacity;  // bits
    union {
        uint64_t m_flags;
        struct {
            uint64_t is_first_load_d1  : 1;
            uint64_t has_d0_rank_cache : 1;
            uint64_t has_d0_sel0_cache : 1;
            uint64_t has_d0_sel1_cache : 1;
            uint64_t has_d1_rank_cache : 1;
            uint64_t has_d1_sel0_cache : 1;
            uint64_t has_d1_sel1_cache : 1;
        } m_flags_debug;
    };
    RankCacheMixed* m_rank_cache;
    uint32_t*  m_sel0_cache[2];
    uint32_t*  m_sel1_cache[2];
    size_t     m_max_rank0[2];
    size_t     m_max_rank1[2];
    
    const RankCacheMixed* get_rank_cache_base() const { return m_rank_cache; }
public:
    template<size_t dimensions>
    static inline bool fast_is0_dx(const bm_uint_t* bits, size_t i);
    template<size_t dimensions>
    static inline bool fast_is1_dx(const bm_uint_t* bits, size_t i);

    template<size_t dimensions>
    static inline size_t fast_rank0_dx(const bm_uint_t* bits, const RankCacheMixed* rankCache, size_t bitpos);
    template<size_t dimensions>
    static inline size_t fast_rank1_dx(const bm_uint_t* bits, const RankCacheMixed* rankCache, size_t bitpos);
    template<size_t dimensions>
    static inline size_t fast_select0_dx(const bm_uint_t* bits, const uint32_t* sel0, const RankCacheMixed* rankCache, size_t id);
    template<size_t dimensions>
    static inline size_t fast_select1_dx(const bm_uint_t* bits, const uint32_t* sel1, const RankCacheMixed* rankCache, size_t id);
};

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
rank0_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    return bitpos - rank1_dx<dimensions>(bitpos);
}

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
rank1_dx(size_t bitpos) const {
    assert(m_flags & (1 << (dimensions == 0 ? 1 : 4)));
    assert(bitpos < m_size[dimensions]);
    const RankCacheMixed& rc = m_rank_cache[bitpos / 512];
    const uint64_t* pu64 = (const uint64_t*)m_words;
    size_t k = bitpos % 512 / 64;
    size_t tail = fast_popcount_trail(pu64[bitpos / 64 * 2 + dimensions], bitpos % 64);
    return rc.base[dimensions] + tail + TERARK_GET_BITS_64(rc.rela[dimensions], k, 9);
}

template<size_t dimensions>
inline bool rank_select_mixed_se_512::
fast_is0_dx(const bldata_t* m_words, size_t i) {
    return !terark_bit_test(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
}

template<size_t dimensions>
inline bool rank_select_mixed_se_512::
fast_is1_dx(const bldata_t* m_words, size_t i) {
    return terark_bit_test(m_words + (i / WordBits * 2 + dimensions), i % WordBits);
}

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
fast_rank0_dx(const bm_uint_t* bits, const RankCacheMixed* rankCache, size_t bitpos) {
    return bitpos - fast_rank1_dx<dimensions>(bits, rankCache, bitpos);
}

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
fast_rank1_dx(const bm_uint_t* bits, const RankCacheMixed* rankCache, size_t bitpos) {
    const RankCacheMixed& rc = rankCache[bitpos / 512];
    const uint64_t* pu64 = (const uint64_t*)bits;
    size_t k = bitpos % 512 / 64;
    size_t tail = fast_popcount_trail(pu64[bitpos / 64 * 2 + dimensions], bitpos % 64);
    return rc.base[dimensions] + tail + TERARK_GET_BITS_64(rc.rela[dimensions], k, 9);
}

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
fast_select0_dx(const bm_uint_t* bits, const uint32_t* sel0, const RankCacheMixed* rankCache, size_t Rank0) {
    size_t lo, hi;
    lo = sel0[Rank0 / LineBits];
    hi = sel0[Rank0 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - rankCache[mid].base[dimensions];
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - rankCache[lo].base[dimensions]);
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rankCache[lo-1].rela[dimensions];
    size_t hit = LineBits * (lo-1) - rankCache[lo-1].base[dimensions];
    const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1) * 2);

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

template<size_t dimensions>
inline size_t rank_select_mixed_se_512::
fast_select1_dx(const bm_uint_t* bits, const uint32_t* sel1, const RankCacheMixed* rankCache, size_t Rank1) {
    size_t lo, hi;
    lo = sel1[Rank1 / LineBits];
    hi = sel1[Rank1 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = rankCache[mid].base[dimensions];
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < rankCache[lo].base[dimensions]);
    size_t line_bitpos = (lo-1) * LineBits;
    uint64_t rcRela = rankCache[lo-1].rela[dimensions];
    size_t hit = rankCache[lo-1].base[dimensions];
    const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1) * 2);

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

typedef rank_select_mixed_dimensions<rank_select_mixed_se_512, 0> rank_select_mixed_se_512_0;
typedef rank_select_mixed_dimensions<rank_select_mixed_se_512, 1> rank_select_mixed_se_512_1;

} // namespace terark

#endif // __terark_rank_select_mixed_se_512_hpp__

