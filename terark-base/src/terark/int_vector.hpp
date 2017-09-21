#ifndef __terark_int_vector_hpp__
#define __terark_int_vector_hpp__

#include "valvec.hpp"
#include "stdtypes.hpp"
#include <terark/util/throw.hpp>

namespace terark {

// memory layout is binary compatible to SortedUintVec
class UintVecMin0 {
protected:
	valvec<byte> m_data;
	size_t m_bits;
	size_t m_mask;
	size_t m_size;

	void nullize() {
		m_bits = 0;
		m_mask = 0;
		m_size = 0;
	}

public:
	template<class Uint>
	UintVecMin0(size_t num, Uint max_val) {
		m_bits = 0;
		m_mask = 0;
		m_size = 0;
		resize_with_wire_max_val(num, max_val);
	}
	UintVecMin0() { nullize(); }

	void clear() { m_data.clear(); nullize(); }

    void shrink_to_fit() {
        resize(m_size);
        m_data.shrink_to_fit();
    }

	void swap(UintVecMin0& y) {
		m_data.swap(y.m_data);
		std::swap(m_bits, y.m_bits);
		std::swap(m_mask, y.m_mask);
		std::swap(m_size, y.m_size);
	}

	void risk_release_ownership() {
		m_data.risk_release_ownership();
		nullize();
	}

	void risk_set_data(byte* Data, size_t num, size_t bits) {
		assert(m_bits <= 64);
#if TERARK_WORD_BITS == 64
		// allowing bits > 58 will incur performance punish in get/set.
		// 58 bit can span 9 bytes, but this only happens when start bit index
		// is odd, 58 is not odd, so 58 is safe.
		if (bits > 58) {
			assert(false);
			THROW_STD(logic_error, "bits=%zd is too large(max_allowed=58)", bits);
		}
#endif
		assert(bits <= sizeof(size_t) * 8);
		size_t Bytes = 0==num ? 0 : (bits*num + 7) / 8 + sizeof(size_t)-1 + 15;
		Bytes &= ~size_t(15); // align to 16
		m_bits = bits;
		m_mask = sizeof(size_t)*8 == bits ? size_t(-1) : (size_t(1)<<bits)-1;
		m_size = num;
		m_data.risk_set_data(Data, Bytes);
	}

	const byte* data() const { return m_data.data(); }
	size_t uintbits() const { return m_bits; }
	size_t uintmask() const { return m_mask; }
	size_t size() const { return m_size; }
	size_t mem_size() const { return m_data.size(); }

	void resize(size_t newsize) {
		assert(m_bits <= 64);
		size_t bytes = 0==newsize ? 0 : (m_bits*newsize + 7) / 8 + sizeof(size_t)-1 + 15;
		bytes &= ~size_t(15); // align to 16
		m_data.resize(bytes, 0);
		m_size = newsize;
	}

	size_t back() const { assert(m_size > 0); return get(m_size-1); }
	size_t operator[](size_t idx) const { return get(idx); }
	size_t get(size_t idx) const {
		assert(idx < m_size);
		assert(m_bits <= 64);
		return fast_get(m_data.data(), m_bits, m_mask, idx);
	}
	void get2(size_t idx, size_t aVals[2]) const {
		const byte*  data = m_data.data();
		const size_t bits = m_bits;
		const size_t mask = m_mask;
		assert(m_bits <= 64);
		aVals[0] = fast_get(data, bits, mask, idx);
		aVals[1] = fast_get(data, bits, mask, idx+1);
	}
	static
	size_t fast_get(const byte* data, size_t bits, size_t mask, size_t idx) {
		assert(bits <= 64);
		size_t bit_idx = bits * idx;
		size_t byte_idx = bit_idx / 8;
		size_t val = unaligned_load<size_t>(data + byte_idx);
		return (val >> bit_idx % 8) & mask;
	}

	void set_wire(size_t idx, size_t val) {
		assert(idx < m_size);
		assert(val <= m_mask);
		assert(m_bits <= 64);
		size_t bits = m_bits; // load member into a register
		size_t mask = m_mask;
		size_t bit_idx = bits * idx;
		size_t byte_idx = bit_idx / 8;
		size_t old_val = unaligned_load<size_t>(m_data.data() + byte_idx);
		size_t new_val = (old_val & ~(mask << bit_idx%8)) | (val << bit_idx%8);
	//	printf("%4zd %4zd %4zd %4zd\n", idx, val, bit_idx, byte_idx);
		unaligned_save(m_data.data() + byte_idx, new_val);
#if TERARK_WORD_BITS == 32
		if (bit_idx % 8 + m_bits > 32) {
			byte old_hival = m_data[byte_idx+4];
		   	byte new_hival = 0
			   	| (old_hival & ~((1 << (bit_idx % 8 + m_bits - 32)) - 1))
				| (val >> (32 - bit_idx % 8));
			m_data[byte_idx+4] = new_hival;
		}
#endif
	}

	template<class Uint>
	void resize_with_wire_max_val(size_t num, Uint max_val) {
		BOOST_STATIC_ASSERT(boost::is_unsigned<Uint>::value);
	//	assert(max_val > 0);
		assert(m_bits <= 64);
    size_t bits = compute_uintbits(max_val);
		resize_with_uintbits(num, bits);
	}

	void resize_with_uintbits(size_t num, size_t bits) {
		assert(m_bits <= 64);
#if TERARK_WORD_BITS == 64
		// allowing bits > 58 will incure performance punish in get/set.
		// 58 bit can span 9 bytes, but this only happens when start bit index
		// is odd, 58 is not odd, so 58 is safe.
		if (bits > 58) {
			THROW_STD(logic_error, "bits=%zu is too large(max=58)", bits);
		}
#endif
		clear();
		m_bits = bits;
		m_mask = sizeof(size_t)*8 == bits ? size_t(-1) : (size_t(1)<<bits)-1;
		m_size = num;
		if (num) {
			m_data.resize_fill(compute_mem_size(bits, num));
		}
	}

	void push_back(size_t val) {
		assert(m_bits <= 64);
		if (compute_mem_size(m_bits, m_size+1) >= m_data.size()) {
			m_data.resize(std::max(size_t(32), m_data.size()) * 2);
		}
		size_t idx = m_size++;
		set_wire(idx, val);
	}

	static size_t compute_mem_size(size_t bits, size_t num) {
		assert(bits <= 64);
		size_t usingsize = (bits * num + 7)/8;
		size_t touchsize =  usingsize + sizeof(uint64_t)-1;
		size_t alignsize = (touchsize + 15) &  ~size_t(15); // align to 16
		return alignsize;
	}

    static size_t compute_uintbits(size_t value) {
        size_t bits = 0;
        while (value) {
            bits++;
            value >>= 1;
        }
        return bits;
    }

    static size_t compute_mem_size_by_max_val(size_t max_val, size_t num) {
      size_t bits = compute_uintbits(max_val);
      return compute_mem_size(bits, num);
    }

	template<class Int>
	Int build_from(const valvec<Int>& y) { return build_from(y.data(), y.size()); }
	template<class Int>
	Int build_from(const Int* src, size_t num) {
		BOOST_STATIC_ASSERT(sizeof(Int) <= sizeof(size_t));
		assert(m_bits <= 64);
		if (0 == num) {
			clear();
			return 0;
		}
		assert(NULL != src);
		Int min_val = src[0];
		Int max_val = src[0];
		for (size_t i = 1; i < num; ++i) {
			if (max_val < src[i]) max_val = src[i];
			if (min_val > src[i]) min_val = src[i];
		}
		typedef typename boost::make_unsigned<Int>::type Uint;
		ullong wire_max = Uint(max_val - min_val);
		resize_with_wire_max_val(num, wire_max);
		for (size_t i = 0; i < num; ++i)
			set_wire(i, Uint(src[i] - min_val));
		return min_val;
	}
};

template<class Int>
class ZipIntVector : private UintVecMin0 {
	typedef Int int_value_t;
	Int  m_min_val;
public:
	template<class Int2>
	ZipIntVector(size_t num, Int2 min_val, Int2 max_val) {
		assert(min_val < max_val);
		UintVecMin0::m_bits = 0;
		UintVecMin0::m_mask = 0;
		UintVecMin0::m_size = 0;
		m_min_val = min_val;
#if TERARK_WORD_BITS < 64
		if (sizeof(Int2) > sizeof(size_t))
			resize_with_wire_max_val(num, (ullong)(max_val - min_val));
		else
#endif
			resize_with_wire_max_val(num, size_t(max_val - min_val));
	}
	ZipIntVector() : m_min_val(0) {}
	using UintVecMin0::data;
	using UintVecMin0::size;
	using UintVecMin0::mem_size;
	using UintVecMin0::compute_mem_size;
	using UintVecMin0::clear;
	using UintVecMin0::risk_release_ownership;
	using UintVecMin0::uintbits;
	using UintVecMin0::uintmask;
	void swap(ZipIntVector& y) {
		UintVecMin0::swap(y);
		std::swap(m_min_val, y.m_min_val);
	}
	void set(size_t idx, Int val) {
		assert(val >= Int(m_min_val));
		assert(val <= Int(m_min_val + m_mask));
		UintVecMin0::set_wire(idx, val - m_min_val);
	}
	Int operator[](size_t idx) const { return get(idx); }
	Int get(size_t idx) const {
		assert(idx < m_size);
		return fast_get(m_data.data(), m_bits, m_mask, m_min_val, idx);
	}
	void get2(size_t idx, Int aVals[2]) const {
		const byte*  data = m_data.data();
		const size_t bits = m_bits;
		const size_t mask = m_mask;
		Int minVal = m_min_val;
		aVals[0] = fast_get(data, bits, mask, minVal, idx);
		aVals[1] = fast_get(data, bits, mask, minVal, idx+1);
	}
	static
	Int fast_get(const byte* data, size_t bits, size_t mask,
				    size_t minVal, size_t idx) {
		return Int(minVal + UintVecMin0::fast_get(data, bits, mask, idx));
	}
	Int min_val() const { return Int(m_min_val); }

	template<class Int2>
	void risk_set_data(byte* Data, size_t num, Int2 min_val, size_t bits) {
		UintVecMin0::risk_set_data(Data, num, bits);
		m_min_val = min_val;
	}

	template<class Int2>
	void build_from(const valvec<Int2>& y) { build_from(y.data(), y.size()); }
	template<class Int2>
	void build_from(const Int2* src, size_t num) {
		m_min_val = UintVecMin0::build_from(src, num);
#if !defined(NDEBUG) && 0
		for(size_t i = 0; i < num; ++i) {
			Int2   x = src[i];
			size_t y = UintVecMin0::get(i);
			assert(Int2(m_min_val + y) == x);
		}
#endif
	}
};

typedef ZipIntVector<size_t>   UintVector;
typedef ZipIntVector<intptr_t> SintVector;

} // namespace terark

#endif // __terark_int_vector_hpp__

