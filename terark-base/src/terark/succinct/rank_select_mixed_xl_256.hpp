#ifndef __terark_rank_select_mixed_xl_256_hpp__
#define __terark_rank_select_mixed_xl_256_hpp__

#include "rank_select_basic.hpp"
#include "rank_select_mixed_basic.hpp"

namespace terark {

class TERARK_DLL_EXPORT rank_select_mixed_xl_256 : public RankSelectConstants<256> {
public:
    typedef boost::mpl::true_ is_mixed;
    typedef uint32_t index_t;
    rank_select_mixed_xl_256();
    rank_select_mixed_xl_256(size_t n, bool val0 = false, bool val1 = false);
    rank_select_mixed_xl_256(size_t n, valvec_no_init);
    rank_select_mixed_xl_256(size_t n, valvec_reserve);
    rank_select_mixed_xl_256(const rank_select_mixed_xl_256&);
    rank_select_mixed_xl_256& operator=(const rank_select_mixed_xl_256&);
#if defined(HSM_HAS_MOVE)
    rank_select_mixed_xl_256(rank_select_mixed_xl_256&& y) noexcept;
    rank_select_mixed_xl_256& operator=(rank_select_mixed_xl_256&& y) noexcept;
#endif

    ~rank_select_mixed_xl_256();
    void clear();
    void risk_release_ownership();
    void risk_mmap_from(unsigned char* base, size_t length);
    void shrink_to_fit();

    void swap(rank_select_mixed_xl_256&);
    const void* data() const;
    size_t mem_size() const;
    
protected:
    struct RankCacheMixed {
        union {
            uint64_t  bit64[8];
            bm_uint_t words[LineWords * 2];
        };
        struct {
            uint32_t      base;
            unsigned char rlev[4];
        } mixed[2];
        template<size_t dimensions> size_t get_base() const { return mixed[dimensions].base; }
    };
    typedef RankCacheMixed bldata_t;
    
    static size_t fix_resize_size(size_t bits) {
        rank_select_check_overflow(bits, > , rank_select_mixed_xl_256);
        return (bits + LineBits - 1) & ~(LineBits - 1);
    }
    void grow();
    void reserve_bytes(size_t bytes_capacity);
    void reserve(size_t bits_capacity);
    void nullize_cache();
    const RankCacheMixed* bldata() const { return m_lines; }

    template<size_t dimensions> void bits_range_set0_dx(size_t i, size_t k);
    template<size_t dimensions> void bits_range_set1_dx(size_t i, size_t k);
    
    template<size_t dimensions>
    void set_word_dx(size_t word_idx, bm_uint_t bits) {
        assert(word_idx < num_words_dx<dimensions>());
        m_lines[word_idx / LineWords].words[word_idx % LineWords * 2 + dimensions] = bits;
    }
    template<size_t dimensions>
    bm_uint_t get_word_dx(size_t word_idx) const {
        assert(word_idx < num_words_dx<dimensions>());
        return m_lines[word_idx / LineWords].words[word_idx % LineWords * 2 + dimensions];
    }
    template<size_t dimensions>
    size_t num_words_dx() const { return (m_size[dimensions] + WordBits - 1) / WordBits; }
    
    template<size_t dimensions>
    void push_back_dx(bool val) {
        rank_select_check_overflow(m_size[dimensions], >= , rank_select_mixed_xl_256);
        assert(m_size[dimensions] <= m_capacity / sizeof(RankCacheMixed) * LineBits);
        if (terark_unlikely(m_size[dimensions] == m_capacity / sizeof(RankCacheMixed) * LineBits))
            grow();
        size_t i = m_size[dimensions]++;
        val ? set1_dx<dimensions>(i) : set0_dx<dimensions>(i);
    }
    template<size_t dimensions>
    bool is0_dx(size_t i) const {
        assert(i < m_size[dimensions]);
        return !terark_bit_test(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
    }
    template<size_t dimensions>
    bool is1_dx(size_t i) const {
        assert(i < m_size[dimensions]);
        return terark_bit_test(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
    }
    template<size_t dimensions>
    void set0_dx(size_t i) {
        assert(i < m_size[dimensions]);
        terark_bit_set0(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
    }
    template<size_t dimensions>
    void set1_dx(size_t i) {
        assert(i < m_size[dimensions]);
        terark_bit_set1(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
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
    rank_select_mixed_dimensions<rank_select_mixed_xl_256, dimensions>& get() {
        static_assert(dimensions < 2, "dimensions must less than 2 !");
        return *reinterpret_cast<rank_select_mixed_dimensions<rank_select_mixed_xl_256, dimensions>*>(this);
    }
    rank_select_mixed_dimensions<rank_select_mixed_xl_256, 0>& first (){ return get<0>(); }
    rank_select_mixed_dimensions<rank_select_mixed_xl_256, 1>& second(){ return get<1>(); }
    rank_select_mixed_dimensions<rank_select_mixed_xl_256, 0>& left  (){ return get<0>(); }
    rank_select_mixed_dimensions<rank_select_mixed_xl_256, 1>& right (){ return get<1>(); }

protected:
    RankCacheMixed* m_lines;
    size_t m_size[2];
    size_t m_capacity;  // bytes;
    union
    {
        uint64_t m_flags;
        struct
        {
            uint64_t is_first_load_d1  : 1;
            uint64_t has_d0_rank_cache : 1;
            uint64_t has_d0_sel0_cache : 1;
            uint64_t has_d0_sel1_cache : 1;
            uint64_t has_d1_rank_cache : 1;
            uint64_t has_d1_sel0_cache : 1;
            uint64_t has_d1_sel1_cache : 1;
        } m_flags_debug;
    };
    uint32_t*  m_sel0_cache[2];
    uint32_t*  m_sel1_cache[2];
    size_t     m_max_rank0[2];
    size_t     m_max_rank1[2];
    
    const RankCacheMixed* get_rank_cache_base() const { return m_lines; }
public:
    template<size_t dimensions>
    static inline bool fast_is0_dx(const bldata_t* bits, size_t i);
    template<size_t dimensions>
    static inline bool fast_is1_dx(const bldata_t* bits, size_t i);

    template<size_t dimensions>
    static inline size_t fast_rank0_dx(const bldata_t* bits, const RankCacheMixed* rankCache, size_t bitpos);
    template<size_t dimensions>
    static inline size_t fast_rank1_dx(const bldata_t* bits, const RankCacheMixed* rankCache, size_t bitpos);
    template<size_t dimensions>
    static inline size_t fast_select0_dx(const bldata_t* bits, const uint32_t* sel0, const RankCacheMixed* rankCache, size_t id);
    template<size_t dimensions>
    static inline size_t fast_select1_dx(const bldata_t* bits, const uint32_t* sel1, const RankCacheMixed* rankCache, size_t id);
};

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
rank0_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    return bitpos - rank1_dx<dimensions>(bitpos);
}

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
rank1_dx(size_t bitpos) const {
    assert(m_flags & (1 << (dimensions == 0 ? 1 : 4)));
    assert(bitpos < m_size[dimensions]);
    const auto& line = m_lines[bitpos / LineBits];
    return line.mixed[dimensions].base + line.mixed[dimensions].rlev[bitpos % LineBits / 64]
        + fast_popcount_trail(line.bit64[bitpos % LineBits / 64 * 2 + dimensions], bitpos % 64);
}

template<size_t dimensions>
inline bool rank_select_mixed_xl_256::
fast_is0_dx(const bldata_t* m_lines, size_t i) {
    return !terark_bit_test(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
}

template<size_t dimensions>
inline bool rank_select_mixed_xl_256::
fast_is1_dx(const bldata_t* m_lines, size_t i) {
    return terark_bit_test(&m_lines[i / LineBits].words[i % LineBits / WordBits * 2 + dimensions], i % WordBits);
}

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
fast_rank0_dx(const bldata_t* m_lines, const RankCacheMixed* rankCache, size_t bitpos) {
    return bitpos - fast_rank1_dx<dimensions>(m_lines, rankCache, bitpos);
}

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
fast_rank1_dx(const bldata_t* m_lines, const RankCacheMixed*, size_t bitpos) {
    const auto& line = m_lines[bitpos / LineBits];
    return line.mixed[dimensions].base + line.mixed[dimensions].rlev[bitpos % LineBits / 64]
        + fast_popcount_trail(line.bit64[bitpos % LineBits / 64 * 2 + dimensions], bitpos % 64);
}

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
fast_select0_dx(const bldata_t* m_lines, const uint32_t* sel0, const RankCacheMixed*, size_t Rank0) {
    size_t lo, hi;
    lo = sel0[Rank0 / LineBits];
    hi = sel0[Rank0 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1];
    size_t hit = LineBits * (lo - 1) - xx.mixed[dimensions].base;
    size_t index = (lo-1) * LineBits; // base bit index

    if (Rank0 < hit + 64*2 - xx.mixed[dimensions].rlev[2]) {
        if (Rank0 < hit + 64*1 - xx.mixed[dimensions].rlev[1]) { // xx.rlev[0] is always 0
            return index + 64*0 + UintSelect1(~xx.bit64[dimensions], Rank0 - hit);
        }
        return index + 64*1 + UintSelect1(
                ~xx.bit64[2 + dimensions], Rank0 - (hit + 64*1 - xx.mixed[dimensions].rlev[1]));
    }
    if (Rank0 < hit + 64*3 - xx.mixed[dimensions].rlev[3]) {
        return index + 64*2 + UintSelect1(
                ~xx.bit64[4 + dimensions], Rank0 - (hit + 64*2 - xx.mixed[dimensions].rlev[2]));
    }
       else {
        return index + 64*3 + UintSelect1(
                ~xx.bit64[6 + dimensions], Rank0 - (hit + 64*3 - xx.mixed[dimensions].rlev[3]));
    }
}

template<size_t dimensions>
inline size_t rank_select_mixed_xl_256::
fast_select1_dx(const bldata_t* m_lines, const uint32_t* sel1, const RankCacheMixed*, size_t Rank1) {
    size_t lo, hi;
    lo = sel1[Rank1 / LineBits];
    hi = sel1[Rank1 / LineBits + 1];
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1];
    size_t hit = xx.mixed[dimensions].base;
    assert(Rank1 >= hit);
    size_t index = (lo-1) * LineBits; // base bit index
    if (Rank1 < hit + xx.mixed[dimensions].rlev[2]) {
        if (Rank1 < hit + xx.mixed[dimensions].rlev[1]) { // xx.rlev[0] is always 0
            return index + UintSelect1(xx.bit64[dimensions], Rank1 - hit);
        }
        return index + 64*1 + UintSelect1(
                 xx.bit64[2 + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[1]));
    }
    if (Rank1 < hit + xx.mixed[dimensions].rlev[3]) {
        return index + 64*2 + UintSelect1(
                 xx.bit64[4 + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[2]));
    }
       else {
        return index + 64*3 + UintSelect1(
                 xx.bit64[6 + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[3]));
    }
}

typedef rank_select_mixed_dimensions<rank_select_mixed_xl_256, 0> rank_select_mixed_xl_256_0;
typedef rank_select_mixed_dimensions<rank_select_mixed_xl_256, 1> rank_select_mixed_xl_256_1;

} // namespace terark

#endif // __terark_rank_select_mixed_xl_256_hpp__

