/* vim: set tabstop=4 : */
#ifndef __nark_io_byte_swap_impl_h__
#define __nark_io_byte_swap_impl_h__

/* The ISO C99 standard specifies that in C++ implementations these
 *    should only be defined if explicitly requested __STDC_CONSTANT_MACROS
 */
//has been defined in <nark/config.hpp>
//#define __STDC_CONSTANT_MACROS
//#include "../config.hpp"

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# include <intrin.h>
# include <stdlib.h>
#endif


#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#include <stdint.h>
#endif

//#include <boost/cstdint.hpp>
#include <limits.h>

#ifdef ULONG_MAX
#	if ULONG_MAX != 0xFFFFFFFFul
#		if ULONG_MAX != 0xFFFFFFFFFFFFFFFFul
#			error "ULONG_MAX error" is ULONG_MAX
#		endif
#	endif
#else
#	error "ULONG_MAX is not defined"
#endif

namespace nark {

#if defined(_MSC_VER) && (_MSC_VER >= 1020) //&& defined(_M_IX86)

inline unsigned short byte_swap(unsigned short x) { return _byteswap_ushort(x); }
inline short byte_swap(short x) { return _byteswap_ushort(x); }

inline unsigned int byte_swap(unsigned int x) { return _byteswap_ulong (x); }
inline int byte_swap(int x) { return _byteswap_ulong (x); }

inline unsigned long byte_swap(unsigned long x) { return _byteswap_ulong (x); }
inline long byte_swap(long x) { return _byteswap_ulong (x); }

inline unsigned __int64 byte_swap(unsigned __int64 x) { return _byteswap_uint64(x); }
inline __int64 byte_swap(__int64 x) { return _byteswap_uint64(x); }

inline wchar_t byte_swap(wchar_t x) { return _byteswap_ushort(x); }

#elif defined(__GNUC__) && defined(__GNUC_MINOR__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3))

inline unsigned short byte_swap(unsigned short x) { return x << 8 | x >> 8; }
inline short byte_swap(short x) { return x << 8 | (unsigned short)x >> 8; }

inline unsigned int byte_swap(unsigned int x) { return __builtin_bswap32(x); }
inline int byte_swap(int x) { return __builtin_bswap32 (x); }

inline long long byte_swap(long long x) { return __builtin_bswap64(x); }
inline unsigned long long byte_swap(unsigned long long x) { return __builtin_bswap64(x); }

#if ULONG_MAX == 0xFFFFFFFF
inline unsigned long byte_swap(unsigned long x) { return __builtin_bswap32(x); }
inline long byte_swap(long x) {	return __builtin_bswap32(x); }
#else
inline unsigned long byte_swap(unsigned long x) { return __builtin_bswap64(x); }
inline long byte_swap(long x) { return __builtin_bswap64(x); }
#endif // ULONG_MAX

inline wchar_t byte_swap(wchar_t x) { return __builtin_bswap32(x); }

#else

inline unsigned short byte_swap(unsigned short x) { return x << 8 | x >> 8; }
inline short byte_swap(short x) { return x << 8 | (unsigned short)x >> 8; }

inline unsigned int byte_swap(unsigned int i)
{
	unsigned int j;
	j  = (i << 24);
	j |= (i <<  8) & 0x00FF0000;
	j |= (i >>  8) & 0x0000FF00;
	j |= (i >> 24);
	return j;
}
inline int byte_swap(int i) { return byte_swap((unsigned int)i); }

inline unsigned long long byte_swap(unsigned long long i)
{
	unsigned long long j;
	j  = (i << 56);
	j |= (i << 40)& ((unsigned long long)0xFF)<<48;
	j |= (i << 24)& ((unsigned long long)0xFF)<<40;
	j |= (i <<  8)& ((unsigned long long)0xFF)<<32;
	j |= (i >>  8)& ((unsigned long long)0xFF)<<24;
	j |= (i >> 24)& ((unsigned long long)0xFF)<<16;
	j |= (i >> 40)& ((unsigned long long)0xFF)<< 8;
	j |= (i >> 56);
	return j;
}
inline long long byte_swap(long long i) { return byte_swap((unsigned long long)(i)); }

#if ULONG_MAX == 0xffffffff
inline unsigned long byte_swap(unsigned long x) { return byte_swap((unsigned int)x); }
inline long byte_swap(long x) {	return byte_swap((unsigned int)x); }
#else
inline unsigned long byte_swap(unsigned long x) { return byte_swap((unsigned long long)x); }
inline long byte_swap(long x) { return byte_swap((unsigned long long)x); }
#endif // ULONG_MAX

#endif

} // namespace nark

#endif // __nark_io_byte_swap_impl_h__

