#ifndef __penglei_bitmanip_h__
#define __penglei_bitmanip_h__

#include <assert.h>
#include <limits.h>
//#include <boost/static_assert.hpp>
#include "config.hpp"

#if defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4005
	#include <immintrin.h>
#endif

#if defined(_MSC_VER) && _MSC_VER >= 1500 || defined(__CYGWIN__)
	#include <intrin.h>
#endif

namespace nark {

// select best popcount implementations
//
#if defined(_MSC_VER) && _MSC_VER >= 1500 // VC 2008
//  different popcount instructions in different headers

	#define fast_popcount64 __popcnt64
	#define fast_clz32  __lzcnt
	#define fast_clz16  __lzcnt16

    #pragma intrinsic(_BitScanForward)
    #define fast_popcount32 __popcnt
    #if defined(_WIN64) || defined(_M_X64) || defined(_M_IA64)
        #pragma intrinsic(_BitScanForward64)
		#define fast_clz64  __lzcnt64
    #else
        inline int fast_clz64(unsigned __int64 x) {
			unsigned long lo32 = (unsigned long)(x & 0xFFFFFFFF);
            unsigned long hi32 = (unsigned long)(x >> 32);
            if (hi32)
				return __lzcnt(hi32);
            else
				return __lzcnt(lo32) + 32;
        }
        inline int fast_popcount64(unsigned __int64 x)
        {
            return
                __popcnt((unsigned)(x      )) +
                __popcnt((unsigned)(x >> 32));
        }
    #endif
        inline int fast_ctz32(unsigned int x) {
            assert(0 != x);
            unsigned long c;
            _BitScanForward(&c, x);
            return c;
        }
        inline int fast_ctz64(unsigned __int64 x) {
            assert(0 != x);
            unsigned long c = 0;
        #if defined(_WIN64) || defined(_M_X64) || defined(_M_IA64)
            _BitScanForward64(&c, x);
        #else
            unsigned long x1 = (unsigned long)(x & 0xFFFFFFFF);
            unsigned long x2 = (unsigned long)(x >> 32);
            if (x1) {
                _BitScanForward(&c, x1);
            } else {
                if (_BitScanForward(&c, x2))
	                c += 32;
				else
					c = 64; // all bits are zero
            }
        #endif
            return c;
        }
#elif defined(__GNUC__) && __GNUC__ >= 4 || \
      defined(__INTEL_COMPILER) && __INTEL_COMPILER >= 900 || \
      defined(__clang__)
    #define fast_popcount32 __builtin_popcount
	#define fast_clz32      __builtin_clz
	#define fast_ctz32      __builtin_ctz

    #ifdef ULONG_MAX
        #if ULONG_MAX == 0xFFFFFFFF
            inline int fast_popcount64(unsigned long long x) {
                return __builtin_popcount((unsigned)(x      )) +
                       __builtin_popcount((unsigned)(x >> 32));
            }
			#define fast_clz64      __builtin_clzll
            #define fast_ctz64      __builtin_ctzll
        #else
            #define fast_popcount64 __builtin_popcountl
			#define fast_clz64      __builtin_clzl
			#define fast_ctz64      __builtin_ctzl
        #endif
    #else
        #error "ULONG_MAX must be defined, use #include <limits.h>"
    #endif
#else // unknow or does not support popcount
  inline int fast_popcount32(unsigned int v) {
    v = v - ((v >> 1) & 0x55555555); // reuse input as temporary
    v = (v & 0x33333333) + ((v >> 2) & 0x33333333); // temp
    return (((v + (v >> 4)) & 0xF0F0F0F) * 0x1010101) >> 24;
  }
  #if NARK_WORD_BITS == 64
    inline long long fast_popcount64(unsigned long long v) {
      v = v - ((v >> 1) & 0x5555555555555555ull);
      v = (v & 0x3333333333333333ull) + ((v >> 2) & 0x3333333333333333ull);
      v = (v + (v >> 4)) & 0x0F0F0F0F0F0F0F0Full;
      return (v * 0x0101010101010101ull) >> 56;
    }
  #else
    inline int fast_popcount64(unsigned long long v) {
		return fast_popcount32((unsigned)(v))
			 + fast_popcount32((unsigned)(v >> 32));
    }
  #endif
#endif // select best popcount implementations

#if NARK_WORD_BITS >= 64
  #if defined(__INTEL_COMPILER) && !defined(_MSC_VER)
	inline unsigned nark_bsr_u64(unsigned __int64 i) {
		unsigned j;
		__asm
		{
			bsr rax, i
			mov j, rax
		}
		return j;
	}
  #elif defined(_MSC_VER)
	inline int nark_bsr_u64(unsigned __int64 x) {
		unsigned long Index = 0;
		_BitScanReverse64(&Index, x);
		return Index;
	}
  #elif defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4005
	#define nark_bsr_u64 __bsrq
  #endif
#endif

#if defined(_MSC_VER) && !defined(__INTEL_COMPILER)
	inline int nark_bsr_u32(unsigned long x) {
		unsigned long Index = 0;
		_BitScanReverse(&Index, x);
		return Index;
	}
#elif defined(__clang__)
	inline int nark_bsr_u32(unsigned x) { return 31 - __builtin_clz(x); }
#else
    // for gcc/clang/icc
	#define nark_bsr_u32 _bit_scan_reverse
#endif

#if defined(__CYGWIN__) && 0
// cygwin gcc bittest has problems
// other gcc has no bittest
//  #define NARK_BIT_TEST_USE_INTRINSIC
    #if NARK_WORD_BITS == 64
	  BOOST_STATIC_ASSERT(sizeof(long) == 8); // LP model
	  inline bool nark_bit_test(const long* a, long i) { return _bittest64(reinterpret_cast<const __int64*>(a), i) ? 1 : 0; }
	  inline bool nark_bit_test(const long long* a, long i) { return _bittest64(reinterpret_cast<const __int64*>(a), i) ? 1 : 0; }
	  inline bool nark_bit_test(const unsigned long* a, long i) { return _bittest64(reinterpret_cast<const __int64*>(a), i) ? 1 : 0; }
	  inline bool nark_bit_test(const unsigned long long* a, long i) { return _bittest64(reinterpret_cast<const __int64*>(a), i) ? 1 : 0; }

	  inline void nark_bit_set0(long* a, long i) { _bittestandreset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set0(long long* a, long i) { _bittestandreset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set0(unsigned long* a, long i) { _bittestandreset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set0(unsigned long long* a, long i) { _bittestandreset64(reinterpret_cast<__int64*>(a), i); }

	  inline void nark_bit_set1(long* a, long i) { _bittestandset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set1(long long* a, long i) { _bittestandset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set1(unsigned long* a, long i) { _bittestandset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set1(unsigned long long* a, long i) { _bittestandset64(reinterpret_cast<__int64*>(a), i); }
    #else
	  inline bool nark_bit_test(const int* a, long i) { return _bittest(a, i) ? 1 : 0; }
	  inline bool nark_bit_test(const unsigned int* a, long i) { return _bittest(reinterpret_cast<const int*>(a), i) ? 1 : 0; }
	  inline void nark_bit_set0(int* a, long i) { _bittestandreset(a, i); }
	  inline void nark_bit_set0(unsigned int* a, long i) { _bittestandreset(reinterpret_cast<int*>(a), i); }
	  inline void nark_bit_set1(int* a, long i) { _bittestandset(a, i); }
	  inline void nark_bit_set1(unsigned int* a, long i) { _bittestandset(reinterpret_cast<int*>(a), i); }
    #endif
#elif defined(_MSC_VER) || defined(__INTEL_COMPILER)
    #define NARK_BIT_TEST_USE_INTRINSIC
    #if NARK_WORD_BITS == 64
	  inline bool nark_bit_test(const __int64* a, __int64 i) { return _bittest64(a, i) ? 1 : 0; }
	  inline bool nark_bit_test(const unsigned __int64* a, __int64 i) { return _bittest64(reinterpret_cast<const __int64*>(a), i) ? 1 : 0; }
	  inline void nark_bit_set0(__int64* a, __int64 i) { _bittestandreset64(a, i); }
	  inline void nark_bit_set0(unsigned __int64* a, __int64 i) { _bittestandreset64(reinterpret_cast<__int64*>(a), i); }
	  inline void nark_bit_set1(__int64* a, __int64 i) { _bittestandset64(a, i); }
	  inline void nark_bit_set1(unsigned __int64* a, __int64 i) { _bittestandset64(reinterpret_cast<__int64*>(a), i); }
    #endif
	inline bool nark_bit_test(const long* a, long i) { return _bittest(a, i) ? 1 : 0; }
	inline bool nark_bit_test(const unsigned long* a, long i) { return _bittest(reinterpret_cast<const long*>(a), i) ? 1 : 0; }
	inline bool nark_bit_test(const int* a, long i) { return _bittest(reinterpret_cast<const long*>(a), i) ? 1 : 0; }
	inline bool nark_bit_test(const unsigned* a, long i) { return _bittest(reinterpret_cast<const long*>(a), i) ? 1 : 0; }

	inline void nark_bit_set0(int* a, long i) { _bittestandreset(reinterpret_cast<long*>(a), i); }
	inline void nark_bit_set0(unsigned* a, long i) { _bittestandreset(reinterpret_cast<long*>(a), i); }
	inline void nark_bit_set1(int* a, long i) { _bittestandset(reinterpret_cast<long*>(a), i); }
	inline void nark_bit_set1(unsigned* a, long i) { _bittestandset(reinterpret_cast<long*>(a), i); }
#endif

#if !defined(NARK_BIT_TEST_USE_INTRINSIC)
template<class Uint>
inline bool nark_bit_test(const Uint* a, size_t i) {
	return a[i/(sizeof(Uint)*8)] & (Uint(1)<<i%(sizeof(Uint)*8)) ? 1 : 0;
}
template<class Uint>
inline void nark_bit_set0(Uint* a, size_t i) {
	a[i/(sizeof(Uint)*8)] &= ~(Uint(1) << i % (sizeof(Uint) * 8));
}
template<class Uint>
inline void nark_bit_set1(Uint* a, size_t i) {
	a[i/(sizeof(Uint)*8)] |= (Uint(1) << i % (sizeof(Uint) * 8));
}
#endif

inline int fast_popcount(unsigned int x) { return fast_popcount32(x); }
inline NARK_IF_WORD_BITS_64(long long, int)
	   fast_popcount(unsigned long long x) { return fast_popcount64(x); }
#if ULONG_MAX > 0xFFFFFFFF
inline long fast_popcount(unsigned long x) { return fast_popcount64(x); }
#else
inline long fast_popcount(unsigned long x) { return fast_popcount32(x); }
#endif

#if defined(__BMI2__)
inline int fast_popcount_trail(unsigned int x, int n) { return fast_popcount32(_bzhi_u32(x, n)); }
#else
inline int fast_popcount_trail(unsigned int x, int n) { return fast_popcount32(x & ~(-1 << n)); }
#endif

#if defined(__BMI2__) && NARK_WORD_BITS >= 64
inline long long fast_popcount_trail(unsigned long long x, int n) { return fast_popcount64(_bzhi_u64(x, n)); }
#else
inline NARK_IF_WORD_BITS_64(long long, int)
fast_popcount_trail(unsigned long long x, int n) { return fast_popcount64(x & ~((unsigned long long)(-1) << n)); }
#endif

#if ULONG_MAX > 0xFFFFFFFF
inline long fast_popcount_trail(unsigned long x, int n) { return fast_popcount_trail((unsigned long long)x, n); }
#else
inline long fast_popcount_trail(unsigned long x, int n) { return fast_popcount_trail((unsigned int)x, n); }
#endif

inline int fast_clz(unsigned int x) { return fast_clz32(x); }
inline NARK_IF_WORD_BITS_64(long long, int)
	   fast_clz(unsigned long long x) { return fast_clz64(x); }
#if ULONG_MAX > 0xFFFFFFFF
inline int fast_clz(unsigned long x) { return fast_clz64(x); }
#else
inline int fast_clz(unsigned long x) { return fast_clz32(x); }
#endif

inline int fast_ctz(unsigned int x) { return fast_ctz32(x); }
inline int fast_ctz(unsigned long long x) { return fast_ctz64(x); }
#if ULONG_MAX > 0xFFFFFFFF
inline long fast_ctz(unsigned long x) { return fast_ctz64(x); }
#else
inline int fast_ctz(unsigned long x) { return fast_ctz32(x); }
#endif

template<class Uint>
static inline void bits_range_set0(Uint* data, size_t i, size_t k) {
	if (i == k) {
		return;
	}
	const static size_t UintBits = sizeof(Uint) * 8;
	size_t j = i / UintBits;
	if (j == (k - 1) / UintBits) {
		data[j] &= ~(Uint(-1) << i%UintBits)
			| (Uint(-2) << (k - 1) % UintBits);
	}
	else {
		if (i % UintBits)
			data[j++] &= ~(Uint(-1) << i%UintBits);
		while (j < k / UintBits)
			data[j++] = 0;
		if (k % UintBits)
			data[j] &= Uint(-1) << k%UintBits;
	}
}

template<class Uint>
static inline void bits_range_set1(Uint* data, size_t i, size_t k) {
	if (i == k) {
		return;
	}
	const static size_t UintBits = sizeof(Uint) * 8;
	size_t j = i / UintBits;
	if (j == (k - 1) / UintBits) {
		data[j] |= (Uint(-1) << i%UintBits)
			& ~(Uint(-2) << (k - 1) % UintBits);
	}
	else {
		if (i % UintBits)
			data[j++] |= (Uint(-1) << i%UintBits);
		while (j < k / UintBits)
			data[j++] = Uint(-1);
		if (k % UintBits)
			data[j] |= ~(Uint(-1) << k%UintBits);
	}
}

template<class Uint>
static inline void bits_range_set(Uint* data, size_t i, size_t k, bool val) {
	if (val)
		bits_range_set1(data, i, k);
	else
		bits_range_set0(data, i, k);
}

template<int Bits> class BitsToUint;
template<> class BitsToUint<32> { typedef unsigned int type; };
template<> class BitsToUint<64> { typedef unsigned long long type; };

} // namespace nark


#endif

