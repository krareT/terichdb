#ifndef __terark_rank_select_il_256_hpp__
#define __terark_rank_select_il_256_hpp__

#include "rank_select_basic.hpp"

namespace terark {

/// index and bits are stored InterLeaved
///
class TERARK_DLL_EXPORT rank_select_il : public RankSelectConstants<256> {
protected:
    struct Line {
        uint32_t      rlev1;
        unsigned char rlev2[4];
        union {
            uint64_t  bit64[4];
            bm_uint_t words[LineWords];
        };
        explicit Line(bool val);
        size_t popcnt() const
          { return rlev2[LineWords-1] + fast_popcount(bit64[3]); }
    };
    valvec<Line> m_lines;
    uint32_t* m_fast_select0; // use memory of m_lines.free_mem_size()
    uint32_t* m_fast_select1; // use memory of m_lines.free_mem_size()
    size_t    m_max_rank0;
    size_t    m_max_rank1; // m_max_rank0+m_max_rank1==LineBits*m_lines.size()
    size_t    m_size;

    void push_back_slow_path(bool val);

public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_il();
    explicit
    rank_select_il(size_t bits, bool val = false);
    rank_select_il(size_t bits, bool val, bool padding);
    rank_select_il(size_t bits, valvec_no_init);
    rank_select_il(size_t bits, valvec_reserve);
    rank_select_il(const rank_select_il& y);
    rank_select_il& operator=(const rank_select_il& y);
#if defined(HSM_HAS_MOVE)
    rank_select_il(rank_select_il&& y) noexcept;
    rank_select_il& operator=(rank_select_il&& y) noexcept;
#endif
    ~rank_select_il();

    void clear();
    void erase_all() { m_lines.erase_all(); }
    void fill(bool val);
    void resize(size_t newsize, bool val = false);
    void resize_no_init(size_t newsize);
    void resize_fill(size_t newsize, bool val = false);
    void shrink_to_fit();

    size_t mem_size() const { return sizeof(Line) * m_lines.capacity(); }
    size_t num_bits() const { return m_lines.size() * LineBits; }
    size_t size() const { return m_size; }
    bool empty() const { return m_lines.empty(); }
    void swap(rank_select_il& y);

          void* data()       { return m_lines.data(); }
    const void* data() const { return m_lines.data(); }

    void push_back(bool val) {
        rank_select_check_overflow(m_size, >= , rank_select_il_256);
        if (terark_likely(m_size < m_lines.size() * LineBits))
            this->set(m_size++, val);
        else
            push_back_slow_path(val);
    }

    bool back() const { assert(m_size > 0); return is1(m_size-1); }
    void pop_back() { assert(m_size > 0); --m_size; }

    bool operator[](size_t i) const { // alias of 'is1'
        assert(i < m_size);
        return terark_bit_test(m_lines[i/LineBits].words, i%LineBits);
    }
    bool is1(size_t i) const {
        assert(i < m_size);
        return terark_bit_test(m_lines[i/LineBits].words, i%LineBits);
    }
    bool is0(size_t i) const {
        assert(i < m_size);
        return !terark_bit_test(m_lines[i/LineBits].words, i%LineBits);
    }
    void set(size_t i, bool val) {
        assert(i < m_size);
        val ? set1(i) : set0(i);
    }
    void set0(size_t i) {
        assert(i < m_size);
        terark_bit_set0(m_lines[i/LineBits].words, i%LineBits);
    }
    void set1(size_t i) {
        assert(i < m_size);
        terark_bit_set1(m_lines[i/LineBits].words, i%LineBits);
    }
    void set0(size_t first, size_t num);
    void set1(size_t first, size_t num);
    void set (size_t first, size_t num, bool val);

    void set_word(size_t word_idx, bm_uint_t bits) {
        m_lines[word_idx/LineWords]
         .words[word_idx%LineWords] = bits;
    }
    bm_uint_t get_word(size_t word_idx) const {
        return m_lines[word_idx/LineWords]
                .words[word_idx%LineWords];
    }
    size_t num_words() const { return m_lines.size() * LineWords; }

    bool isall0() const;
    bool isall1() const;

    size_t popcnt() const;
    size_t popcnt(size_t startline, size_t lines) const;

    ///@returns number of continuous one/zero bits starts at bitpos
    size_t one_seq_len(size_t bitpos) const;
    size_t zero_seq_len(size_t bitpos) const;

    ///@returns number of continuous one/zero bits ends at endpos
    ///@note return_value = endpos - start;
    ///        where bits[start-1] is 0/1 and bits[start, ... endpos) are all 1/0
    size_t one_seq_revlen(size_t endpos) const;
    size_t zero_seq_revlen(size_t endpos) const;
    inline size_t rank0(size_t bitpos) const;
    inline size_t rank1(size_t bitpos) const;
    size_t select0(size_t Rank0) const;
    size_t select1(size_t Rank1) const;
    size_t max_rank1() const { return m_max_rank1; }
    size_t max_rank0() const { return m_max_rank0; }

    void build_cache(bool speed_select0, bool speed_select1);
    void risk_mmap_from(unsigned char* base, size_t length);
    void risk_release_ownership();
    void risk_set_data(void*  data) { m_lines.risk_set_data((Line*)(data)); }
    void risk_set_size(size_t bits) {
        m_lines.risk_set_size(BitsToLines(bits));
        m_size = bits;
    }
    void risk_set_capacity(size_t bits) { m_lines.risk_set_capacity(BitsToLines(bits)); }

    const Line* bldata() const { return m_lines.data(); }
    const Line* get_rank_cache() const { return m_lines.data(); }
    const uint32_t* get_sel0_cache() const { return m_fast_select0; }
    const uint32_t* get_sel1_cache() const { return m_fast_select1; }

    static inline size_t fast_rank0(const Line* bits, const Line* rankCache, size_t bitpos);
    static inline size_t fast_rank1(const Line* bits, const Line* rankCache, size_t bitpos);
    static inline size_t fast_select0(const Line* bits, const uint32_t* sel0, const Line* rankCache, size_t id);
    static inline size_t fast_select1(const Line* bits, const uint32_t* sel1, const Line* rankCache, size_t id);

    static inline bool fast_is1(const Line* l, size_t i) { return  terark_bit_test(l[i/LineBits].words, i%LineBits); }
    static inline bool fast_is0(const Line* l, size_t i) { return !terark_bit_test(l[i/LineBits].words, i%LineBits); }

    size_t excess1(size_t bp) const { return 2*rank1(bp) - bp; }
    static size_t fast_excess1(const Line* bits, const Line* rankCache, size_t bitpos)
        { return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos; }
};

inline size_t rank_select_il::
rank0(size_t bitpos) const {
    return bitpos - rank1(bitpos);
}

inline size_t rank_select_il::
rank1(size_t bitpos) const {
    assert(bitpos <= m_lines.size() * LineBits);
    const Line& line = m_lines[bitpos / LineBits];
    return line.rlev1 + line.rlev2[bitpos%LineBits / 64]
        + fast_popcount_trail(
            line.bit64[bitpos%LineBits / 64], bitpos % 64);
}

inline size_t rank_select_il::
fast_rank0(const Line* lines, const Line*, size_t bitpos) {
    return bitpos - fast_rank1(lines, lines, bitpos);
}

inline size_t rank_select_il::
fast_rank1(const Line* lines, const Line*, size_t bitpos) {
    const Line& line = lines[bitpos / LineBits];
    return line.rlev1 + line.rlev2[bitpos%LineBits / 64]
        + fast_popcount_trail(
            line.bit64[bitpos%LineBits / 64], bitpos % 64);
}

inline size_t rank_select_il::
fast_select0(const Line* lines, const uint32_t* sel0, const Line*, size_t Rank0) {
    size_t lo = sel0[Rank0 / LineBits];
    size_t hi = sel0[Rank0 / LineBits + 1];
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

inline size_t rank_select_il::
fast_select1(const Line* lines, const uint32_t* sel1, const Line*, size_t Rank1) {
    size_t lo = sel1[Rank1 / LineBits];
    size_t hi = sel1[Rank1 / LineBits + 1];
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

typedef rank_select_il rank_select_il_256;
typedef rank_select_il rank_select_il_256_32;

} // namespace terark

#endif // __terark_rank_select_il_256_hpp__

