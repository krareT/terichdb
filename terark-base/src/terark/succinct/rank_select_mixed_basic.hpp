#ifndef __terark_rank_select_mixed_basic_hpp__
#define __terark_rank_select_mixed_basic_hpp__

namespace terark {

template<class base_rank_select_mixed, size_t dimensions>
class TERARK_DLL_EXPORT rank_select_mixed_dimensions : protected base_rank_select_mixed {
	typedef base_rank_select_mixed super;
protected:
	using super::m_size;
    typedef typename super::RankCacheMixed base_rank_cache_mixed;
	struct TERARK_DLL_EXPORT RankCacheMixed : public base_rank_cache_mixed {
		operator size_t() const { return base_rank_cache_mixed::template get_base<dimensions>(); }
	};
public:
    typedef boost::mpl::true_ is_mixed;
    typedef typename super::index_t index_t;
    typedef typename super::bldata_t bldata_t;
	typedef rank_select_mixed_dimensions<super, 0> rank_select_view_0;
	typedef rank_select_mixed_dimensions<super, 1> rank_select_view_1;
	using super::get;
	using super::bldata;
	using super::data;
	using super::risk_release_ownership;
	using super::risk_mmap_from;

	void swap(rank_select_mixed_dimensions& y) { super::swap(y); }
    void resize(size_t newsize, bool val = false) {
        this->reserve(super::fix_resize_size(newsize));
        if (newsize > m_size[dimensions]) {
            if (val)
                this->template bits_range_set1_dx<dimensions>(m_size[dimensions], newsize);
            else
                this->template bits_range_set0_dx<dimensions>(m_size[dimensions], newsize);
        }
        m_size[dimensions] = newsize;
    }
    void resize_fill(size_t newsize, bool val = false) {
        this->reserve(super::fix_resize_size(newsize));
        if (val)
            this->template bits_range_set1_dx<dimensions>(0, newsize);
        else
            this->template bits_range_set0_dx<dimensions>(0, newsize);
        m_size[dimensions] = newsize;
    }

    size_t size() const { return m_size[dimensions]; }
    void set_word(size_t word_idx, bm_uint_t bits) { this->template set_word_dx<dimensions>(word_idx, bits); }
	bm_uint_t get_word(size_t word_idx) const { return this->template get_word_dx<dimensions>(word_idx); }
    size_t num_words() const { return this->template num_words_dx<dimensions>(); }

	void push_back(bool val) { this->template push_back_dx<dimensions>(val); }

    bool back() const { assert(m_size[dimensions] > 0); return is1(m_size[dimensions] - 1); }
    void pop_back() { assert(m_size[dimensions] > 0); --m_size[dimensions]; }

	bool operator[](size_t i) const {
		assert(i < m_size[dimensions]);
		return this->template is1_dx<dimensions>(i);
	}
	bool is0(size_t i) const { return this->template is0_dx<dimensions>(i); }
	bool is1(size_t i) const { return this->template is1_dx<dimensions>(i); }
	void set0(size_t i) { this->template set0_dx<dimensions>(i); }
	void set1(size_t i) { this->template set1_dx<dimensions>(i); }

	void set0(size_t first, size_t num) {
	    this->template bits_range_set0_dx<dimensions>(first, first + num);
    }
	void set1(size_t first, size_t num) {
	    this->template bits_range_set1_dx<dimensions>(first, first + num);
    }
    void set(size_t i, bool val) {
        val ? set1(i) : set0(i);
    }
    void set(size_t first, size_t num, bool val) {
        val ? set1(first, num) : set0(first, num);
    }
	using super::mem_size;

	size_t  one_seq_len(size_t bitpos) const { return this->template  one_seq_len_dx<dimensions>(bitpos); }
	size_t zero_seq_len(size_t bitpos) const { return this->template zero_seq_len_dx<dimensions>(bitpos); }
    size_t  one_seq_revlen(size_t endpos) const { return this->template  one_seq_revlen_dx<dimensions>(endpos); }
    size_t zero_seq_revlen(size_t endpos) const { return this->template zero_seq_revlen_dx<dimensions>(endpos); }

    void build_cache(bool speed_select0, bool speed_select1) {
        this->template build_cache_dx<dimensions>(speed_select0, speed_select1);
    }
    size_t rank0(size_t bitpos) const { return this->template rank0_dx<dimensions>(bitpos); }
    size_t rank1(size_t bitpos) const { return this->template rank1_dx<dimensions>(bitpos); }
    size_t select0(size_t id) const { return this->template select0_dx<dimensions>(id); }
    size_t select1(size_t id) const { return this->template select1_dx<dimensions>(id); }
	size_t max_rank1() const { return this->m_max_rank1[dimensions]; }
	size_t max_rank0() const { return this->m_max_rank0[dimensions]; }
    
	static size_t fast_is0(const bldata_t* bits, size_t bitpos) {
        return super::template fast_is0_dx<dimensions>(bits, bitpos);
    }
	static size_t fast_is1(const bldata_t* bits, size_t bitpos) {
        return super::template fast_is1_dx<dimensions>(bits, bitpos);
    }

	static size_t fast_rank0(const bldata_t* bits, const RankCacheMixed* rankCache, size_t bitpos) {
        return super::template fast_rank0_dx<dimensions>(bits, rankCache, bitpos);
    }
	static size_t fast_rank1(const bldata_t* bits, const RankCacheMixed* rankCache, size_t bitpos) {
        return super::template fast_rank1_dx<dimensions>(bits, rankCache, bitpos);
    }
	static size_t fast_select0(const bldata_t* bits, const uint32_t* sel0, const RankCacheMixed* rankCache, size_t id) {
        return super::template fast_select0_dx<dimensions>(bits, sel0, rankCache, id);
    }
	static size_t fast_select1(const bldata_t* bits, const uint32_t* sel1, const RankCacheMixed* rankCache, size_t id) {
        return super::template fast_select1_dx<dimensions>(bits, sel1, rankCache, id);
    }
	const RankCacheMixed* get_rank_cache() const {
        assert(this->m_flags & (1 << (dimensions == 0 ? 1 : 4)));
        return reinterpret_cast<const RankCacheMixed*>(this->get_rank_cache_base());
    }
	const uint32_t* get_sel0_cache() const {
        assert(this->m_flags & (1 << (dimensions == 0 ? 2 : 5)));
        return this->m_sel0_cache[dimensions];
    }
	const uint32_t* get_sel1_cache() const {
        assert(this->m_flags & (1 << (dimensions == 0 ? 3 : 6)));
        return this->m_sel1_cache[dimensions];
    }

    size_t excess1(size_t bp) const { return 2 * rank1(bp) - bp; }
    static size_t fast_excess1(const bldata_t* bits, const RankCacheMixed* rankCache, size_t bitpos) {
        return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos;
    }
};

} // namespace terark

#endif // __terark_rank_select_mixed_basic_hpp__

