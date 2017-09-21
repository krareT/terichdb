#ifndef __penglei_bitmap_h__
#define __penglei_bitmap_h__

#include "bitmanip.hpp"
#include "stdtypes.hpp"
#include "valvec.hpp"
#include <boost/mpl/bool.hpp>

namespace terark {

template<int TotalBitsMaybeUnaligned, class BlockT = bm_uint_t>
class static_bitmap {
public:
    enum { BlockBits = sizeof(BlockT) * 8 };
	enum { TotalBits = TotalBitsMaybeUnaligned };
    enum { TotalBitsAligned = (TotalBits + BlockBits - 1) & ~(BlockBits - 1)};
    enum { BlockN = TotalBitsAligned / sizeof(BlockT) / 8};
    enum { BlockCount = BlockN };
	enum { ExtraBits = TotalBitsAligned - TotalBitsMaybeUnaligned };
	enum {  LastBits = ExtraBits ? BlockBits - ExtraBits : 0 };
	static const BlockT ExtraMax = (BlockT(1) << ExtraBits) - 1;
	static const BlockT LastMask = (BlockT(1) <<  LastBits) - 1;
    typedef BlockT block_t;
	typedef boost::mpl::bool_<0!=ExtraBits> has_extra;
protected:
    BlockT  bm[BlockN];
public:
    explicit static_bitmap(bool val = false) { fill_all(val); }
	void fill(bool value) {
		if (ExtraBits) {
			const BlockT e = value ? ~BlockT(0) : 0;
			for (int i = 0; i < BlockN-1; ++i) bm[i] = e;
			BlockT& x = bm[BlockN-1];
			x = (x & ~LastMask) | (value ? LastMask : 0);
		} else {
			fill_all(value);
		}
	}
	void fill_all(bool value) {
        const BlockT e = value ? ~BlockT(0) : 0;
        for (int i = 0; i < BlockN; ++i) bm[i] = e;
	}

	// when TotalBits is not aligned with BlockBits,
	// there are some some extra free bits in the high bits
	// of last block
	BlockT get_extra() const { return get_extra_aux(has_extra()); }
	void set_extra(BlockT x) { set_extra_aux(x, has_extra()); }
private:
	BlockT get_extra_aux(boost::mpl::bool_<1>) const {
		assert(0 != ExtraBits);
		return (bm[BlockN-1] & ~LastMask) >> LastBits;
	}
	void set_extra_aux(BlockT x, boost::mpl::bool_<1>) {
		assert(x <= ExtraMax);
		assert(0 != ExtraBits);
		BlockT& last = bm[BlockN-1];
		last = (last & LastMask) | (x << LastBits);
	}
	BlockT get_extra_aux(boost::mpl::bool_<0>) const {
		assert(0);
		return 0;
	}
	void set_extra_aux(BlockT, boost::mpl::bool_<0>) {
		assert(0);
	}

public:
    BlockT  block(int i) const { return bm[i]; }
    BlockT& block(int i)       { return bm[i]; }

#define TERARK_DefineBitMapBinaryOperator(OP, Expression) \
	static_bitmap& operator OP(const static_bitmap& that) { \
		for (int i = 0; i < BlockN-(ExtraBits?1:0); ++i) { \
			BlockT& x = this->bm[i]; \
			BlockT  y = that. bm[i]; \
			x = Expression; \
		} \
		if (ExtraBits) { \
			BlockT& x = this->bm[BlockN-1]; \
			BlockT  y = that. bm[BlockN-1]; \
			x = ((Expression) & LastMask) | (x & ~LastMask); \
		} \
		return *this; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	TERARK_DefineBitMapBinaryOperator(-=, x &~y)
	TERARK_DefineBitMapBinaryOperator(+=, x | y) // same as |=
	TERARK_DefineBitMapBinaryOperator(|=, x | y)
	TERARK_DefineBitMapBinaryOperator(^=, x ^ y)
	TERARK_DefineBitMapBinaryOperator(&=, x & y)

    static_bitmap& operator<<=(int n) { shl(n); return *this; }
    void shl(int n, int realBlocks = BlockN) { // shift bit left
        assert(realBlocks <= BlockN);
        assert(n < BlockBits * realBlocks);
        assert(n > 0);
		if (ExtraBits) {
			BlockT extra = get_extra();
			set_extra(0);
			do_shl(n, realBlocks);
			set_extra(extra);
		} else {
			do_shl(n, realBlocks);
		}
	}
private:
    void do_shl(int n, int realBlocks) { // shift bit left
        int c = n / BlockBits;
        if (c > 0) {
            // copy backward, expected to be loop unrolled
            // when this function is calling by shl(n)
            for (int i = realBlocks-1; i >= c; --i)
                bm[i] = bm[i-c];
            memset(bm, 0, sizeof(BlockT)*c);
        }
        if ((n %= BlockBits) != 0) {
            BlockT r = bm[c] >> (BlockBits - n);
            bm[c] <<= n;
            for (int i = c+1; i < realBlocks; ++i) {
                BlockT r2 = bm[i] >> (BlockBits - n);
                bm[i] <<= n;
                bm[i]  |= r;
                r = r2;
            }
        }
    }
public:
    void set(int n, bool val) {
        assert(n >= 0);
        assert(n < TotalBits);
        if (val)
        	terark_bit_set1(bm, n);
        else
        	terark_bit_set0(bm, n);
    }
    void set1(int n) {
        assert(n >= 0);
        assert(n < TotalBits);
		terark_bit_set1(bm, n);
	}
    void set0(int n) {
        assert(n >= 0);
        assert(n < TotalBits);
		terark_bit_set0(bm, n);
    }
	bool operator[](int n) { // alias of 'is1'
        assert(n >= 0);
        assert(n < TotalBits);
		return terark_bit_test(bm, n);
	}
	bool operator[](int n) const { // same as is1(n)
        assert(n >= 0);
        assert(n < TotalBits);
		return terark_bit_test(bm, n);
	}
    bool is1(int n) const {
        assert(n >= 0);
        assert(n < TotalBits);
		return terark_bit_test(bm, n);
    }
    bool is0(int n) const {
        assert(n >= 0);
        assert(n < TotalBits);
		return !terark_bit_test(bm, n);
    }
	bool is_all0() const {
		if (ExtraBits) {
			for (int i = 0; i < BlockN-1; ++i)
				if (bm[i])
					return false;
			return (bm[BlockN-1] & LastMask) == 0;
		}
		for (int i = 0; i < BlockN; ++i)
			if (bm[i])
				return false;
		return true;
	}
	bool is_all1() const {
		if (ExtraBits) {
			for (int i = 0; i < BlockN-1; ++i)
				if (BlockT(-1) != bm[i])
					return false;
			return (bm[BlockN-1] & LastMask) == LastMask;
		}
		for (int i = 0; i < BlockN; ++i)
			if (BlockT(-1) != bm[i])
				return false;
		return true;
	}
	bool has_any0() const { return !is_all1(); }
	bool has_any1() const { return !is_all0(); }

    int popcnt() const {
        int c = 0; // small BlcokN will triger loop unroll optimization
        for (int i = 0; i < BlockN; ++i) c += fast_popcount(bm[i]);
		if (ExtraBits)
			c -= fast_popcount(bm[BlockN-1] & ~LastMask);
        return c;
    }

    /// popcnt in blocks [0, align_bits(bits)/BlockBits)
    int popcnt_aligned(int bits) const {
        assert(bits >= 1);
        assert(bits <= TotalBits);
        // align to block boundary
        int n = (bits + BlockBits-1) / BlockBits;
        int c = 0; // small BlcokN will trigger loop unroll optimization
        for (int i = 0; i < BlockN; ++i) {
            if (i < n)
                c += fast_popcount(bm[i]);
            else
                break;
        }
		if (ExtraBits && BlockN == n)
			c -= fast_popcount(bm[BlockN-1] & ~LastMask);
        return c;
    }

	/// popcnt in bits: [0, bitpos), not counting bm[bitpos]
    int popcnt_index(int bitpos) const {
        assert(bitpos >= 0);
        assert(bitpos < TotalBits);
	//	assert(is1(bitpos)); // often holds, but not required to always holds
        int n = bitpos/BlockBits;
        int c = 0; // small BlcokN will trigger loop unroll optimization
        for (int i = 0; i < BlockN; ++i) {
            if (i < n)
                c += fast_popcount(bm[i]);
            else
                break;
        }
		if (int shift = bitpos % BlockBits)
        	c += fast_popcount_trail(bm[n], shift);
        return c;
    }

    static int align_bits(int n) {
        return (n + BlockBits-1) & ~(BlockBits-1);
    }
};

class TERARK_DLL_EXPORT febitvec {
protected:
    bm_uint_t* m_words;
	size_t m_size;
	size_t m_capacity;
    static const size_t AllocUnitBits = 64;

	void push_back_slow_path(bool val);
	void ensure_set1_slow_path(size_t i);

public:
	static size_t align_bits(size_t nbits) { return (nbits+WordBits-1) & ~(WordBits-1); }

	febitvec() { m_words = NULL; m_size = 0; m_capacity = 0; }
    explicit febitvec(size_t bits, bool val = false);
    febitvec(size_t bits, bool val, bool padding);
	febitvec(size_t bits, valvec_no_init);
	febitvec(size_t bits, valvec_reserve);

	febitvec(const febitvec&); // copy-cons
	febitvec& operator=(const febitvec&); // assign
#if defined(HSM_HAS_MOVE)
	febitvec(febitvec&&) noexcept; // move-cons
	febitvec& operator=(febitvec&&) noexcept; // move-assign
#endif

	febitvec(const febitvec& y, size_t beg, size_t len);

	~febitvec();

	void append(const febitvec& y);
	void append(const febitvec& y, size_t beg, size_t len);
	void assign(const febitvec&);
	void risk_memcpy(const febitvec&);
	void copy(size_t destBeg, const febitvec& y);
	void copy(size_t destBeg, const febitvec& y, size_t srcBeg, size_t len);

	void clear();
	void erase_all();
	void fill(bool val);
	void grow(size_t cnt, bool val=false);
	void reserve(size_t newcap);
    void resize(size_t newsize, bool val=false);
	void resize_no_init(size_t newsize);
	void resize_fill(size_t newsize, bool val=false);
	void risk_release_ownership();
	void shrink_to_fit();

	febitvec& operator-=(const febitvec& y);
	febitvec& operator^=(const febitvec& y);
	febitvec& operator&=(const febitvec& y);
	febitvec& operator|=(const febitvec& y);
	void block_or(const febitvec& y, size_t yblstart, size_t blcnt);
	void block_and(const febitvec& y, size_t yblstart, size_t blcnt);

	void push_back(bool val) {
		if (m_size < m_capacity)
			this->set(m_size++, val);
		else
			push_back_slow_path(val);
	}

	void unchecked_push_back(bool val) {
		assert(m_size < m_capacity);
		this->set(m_size++, val);
	}

	void pop_back() { assert(m_size > 0); --m_size; }

    bool operator[](size_t i) const { // alias of 'is1'
        assert(i < m_size);
		return terark_bit_test(m_words, i);
	}
    bool is1(size_t i) const {
        assert(i < m_size);
		return terark_bit_test(m_words, i);
    }
    bool is0(size_t i) const {
        assert(i < m_size);
		return !terark_bit_test(m_words, i);
    }
	bool back() const {
		assert(m_size > 0);
		return terark_bit_test(m_words, m_size-1);
	}
	void set(size_t i, bool val) {
        assert(i < m_size);
		val ? set1(i) : set0(i);
	}
    void set0(size_t i) {
        assert(i < m_size);
		terark_bit_set0(m_words, i);
    }
    void set1(size_t i) {
        assert(i < m_size);
		terark_bit_set1(m_words, i);
    }
	void ensure_set(size_t i, bool val) {
		val ? ensure_set1(i) : ensure_set0(i);
	}
	void ensure_set0(size_t i) {
		if (terark_likely(i < m_size))
			terark_bit_set0(m_words, i);
		else
			resize(i+1);
	}
	void ensure_set1(size_t i) {
		if (terark_likely(i < m_size))
			terark_bit_set1(m_words, i);
		else
			ensure_set1_slow_path(i);
	}
	void set0(size_t first, size_t num);
	void set1(size_t first, size_t num);
	void set (size_t first, size_t num, bool val);

	void beg_end_set0(size_t beg, size_t end);
	void beg_end_set1(size_t beg, size_t end);
	void beg_end_set (size_t beg, size_t end, bool val);

	void set_word(size_t word_idx, bm_uint_t bits) {
		m_words[word_idx] = bits;
	}
	bm_uint_t get_word(size_t word_idx) const {
		return m_words[word_idx];
	}
	size_t num_words() const { return (m_size + WordBits - 1) / WordBits; }

	bool isall0() const;
   	bool isall1() const;

	size_t popcnt() const;
	size_t popcnt(size_t blstart, size_t blcnt) const;

	///@returns number of continuous one/zero bits starts at bitpos
	size_t one_seq_len(size_t bitpos) const;
	size_t zero_seq_len(size_t bitpos) const;

	///@returns number of continuous one/zero bits ends at endpos
	///@note return_value = endpos - start;
	///        where bits[start-1] is 0 and bits[start, ... endpos) are all 1/0
	size_t one_seq_revlen(size_t endpos) const;
	size_t zero_seq_revlen(size_t endpos) const;

	size_t mem_size() const {
		return ((m_size + AllocUnitBits-1) & ~(AllocUnitBits-1)) / 8;
	}
	static size_t s_mem_size(size_t bits) {
		return ((bits + AllocUnitBits-1) & ~(AllocUnitBits-1)) / 8;
	}
	size_t blsize() const { return (m_size + WordBits -1) / WordBits; }
	size_t size() const { return m_size; }
	size_t capacity() const { return m_capacity; }
	size_t unused() const { return m_capacity - m_size; }
	bool  empty() const { return m_size == 0; }
	void  swap(febitvec& y) {
		std::swap(m_words, y.m_words);
		std::swap(m_size , y.m_size);
		std::swap(m_capacity, y.m_capacity);
	}

protected:
	template<class Uint>
	void push_uint_tpl(size_t width, Uint val);
public:
	void push_uint(size_t width, byte_t val)
	   { push_uint(width, (uint)(val)); }
	void push_uint(size_t width, ushort val)
	   { push_uint(width, (uint)(val)); }
	void push_uint(size_t width, ulong val) {
#if (ULONG_MAX == UINT_MAX)
		push_uint(width, (uint)(val));
#elif (ULONG_MAX == ULLONG_MAX)
		push_uint(width, (ullong)(val));
#else
		#error "Bad long int type"
#endif
	}
	void push_uint(size_t width, uint  val);
	void push_uint(size_t width, ullong val);

protected:
	template<class Uint>
	void set_uint_tpl(size_t bitpos, size_t width, Uint val);
public:
	void set_uint(size_t bitpos, size_t width, byte_t val)
	   { set_uint(bitpos, width, (uint)(val)); }
	void set_uint(size_t bitpos, size_t width, ushort val)
	   { set_uint(bitpos, width, (uint)(val)); }
	void set_uint(size_t bitpos, size_t width, ulong val) {
#if (ULONG_MAX == UINT_MAX)
		set_uint(bitpos, width, (uint)(val));
#elif (ULONG_MAX == ULLONG_MAX)
		set_uint(bitpos, width, (ullong)(val));
#else
		#error "Bad long int type"
#endif
	}
	void set_uint(size_t bitpos, size_t width, uint val);
	void set_uint(size_t bitpos, size_t width, ullong val);

protected:
	template<class Uint>
	static
	void s_set_uint_tpl(Uint* base, size_t bitpos, size_t width, Uint val);
public:
	static
	void s_set_uint(byte_t* base, size_t bitpos, size_t width, byte_t val)
	   { s_set_uint((uint*)base, bitpos, width, (uint)(val)); }
	static
	void s_set_uint(ushort* base, size_t bitpos, size_t width, ushort val)
	   { s_set_uint((uint*)base, bitpos, width, (uint)(val)); }
	static
	void s_set_uint(ulong* base, size_t bitpos, size_t width, ulong val) {
#if (ULONG_MAX == UINT_MAX)
		s_set_uint((uint*)base, bitpos, width, (uint)(val));
#elif (ULONG_MAX == ULLONG_MAX)
		s_set_uint((ullong*)base, bitpos, width, (ullong)(val));
#else
		#error "Bad long int type"
#endif
	}
	static
	void s_set_uint(uint* base, size_t bitpos, size_t width, uint val);
	static
	void s_set_uint(ullong* base, size_t bitpos, size_t width, ullong val);

//---------------------------------------------------------------------------

	template<class Uint>
	static
	Uint s_get_uint(const Uint* base, size_t bitpos, size_t width) {
		const Uint UintBits = Uint(sizeof(Uint) * 8);
		const Uint* p = base + bitpos / UintBits;
		size_t offset = bitpos % UintBits;
		Uint mask = ~(Uint(-1) << width);
		Uint low  = p[0] >> offset;
		if (offset + width <= UintBits)
			return mask & low;
		else
			return mask & (low | (p[1] << (UintBits - offset)));
	}
	template<class Uint>
	Uint get_uint(size_t bitpos, size_t width) const {
		assert(bitpos < m_size);
		assert(bitpos + width <= m_size);
		return s_get_uint((const Uint*)(m_words), bitpos, width);
	}
	template<class Uint>
	void get2_uints(size_t bitpos, size_t width, Uint vals[2]) const {
		assert(bitpos < m_size);
		assert(bitpos + width <= m_size);
		const Uint* base = (const Uint*)(m_words);
		vals[0] = s_get_uint(base, bitpos, width);
		vals[1] = s_get_uint(base, bitpos+width, width);
	}

	      bm_uint_t* bldata()       { return m_words; }
	const bm_uint_t* bldata() const { return m_words; }

	      void* data()       { return m_words; }
	const void* data() const { return m_words; }

	void risk_mmap_from(unsigned char* base, size_t length) {
		assert((length * 8) % AllocUnitBits == 0);
		m_words = (bm_uint_t*)base;
		m_size = length * 8;
		m_capacity = m_size;
	}

	void risk_set_data(bm_uint_t*  bits) { m_words = bits; }
	void risk_set_size(    size_t nbits) { m_size = nbits; }
	void risk_set_capacity(size_t nbits) { assert(nbits % WordBits == 0); m_capacity = nbits; }

	static bool fast_is1(const bm_uint_t* bits, size_t i) { return  terark_bit_test(bits, i); }
	static bool fast_is0(const bm_uint_t* bits, size_t i) { return !terark_bit_test(bits, i); }
};

} // namespace terark


#endif

