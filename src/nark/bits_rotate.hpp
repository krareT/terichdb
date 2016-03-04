#ifndef __nark_bits_rotate_hpp__
#define __nark_bits_rotate_hpp__

#if defined(_MSC_VER)
// Seems Visual C++ didn't optimize rotate shift, so use intrinsics
#include <stdlib.h> // for rol/ror intrinsics
#endif

#include <boost/static_assert.hpp>
#include <boost/type_traits/is_unsigned.hpp>

namespace nark {

#if defined(_MSC_VER)
inline unsigned __int64
msc_rotate_left(unsigned __int64 val, int c) { return _rotl64(val, c); }
inline unsigned int
msc_rotate_left(unsigned   int   val, int c) { return _rotl  (val, c); }
inline unsigned short
msc_rotate_left(unsigned short   val, int c) { return _rotl16(val, c); }
inline unsigned char
msc_rotate_left(unsigned  char   val, int c) { return _rotl8 (val, c); }

inline unsigned __int64
msc_rotate_right(unsigned __int64 val, int c) { return _rotr64(val, c); }
inline unsigned int
msc_rotate_right(unsigned   int   val, int c) { return _rotr  (val, c); }
inline unsigned short
msc_rotate_right(unsigned short   val, int c) { return _rotr16(val, c); }
inline unsigned char
msc_rotate_right(unsigned  char   val, int c) { return _rotr8 (val, c); }

template<class Uint>
inline Uint BitsRotateLeft(Uint x, int c) {
	assert(c >= 0);
	assert(c < (int)sizeof(Uint)*8);
	BOOST_STATIC_ASSERT(boost::is_unsigned<Uint>::value);
	return msc_rotate_left(x, c);
}
template<class Uint>
inline Uint BitsRotateRight(Uint x, int c) {
	assert(c >= 0);
	assert(c < (int)sizeof(Uint)*8);
	BOOST_STATIC_ASSERT(boost::is_unsigned<Uint>::value);
	return msc_rotate_right(x, c);
}
#else
template<class Uint>
inline Uint BitsRotateLeft(Uint x, int c) {
	assert(c >= 0);
	assert(c < (int)sizeof(Uint)*8);
	BOOST_STATIC_ASSERT(boost::is_unsigned<Uint>::value);
	return x << c | x >> (sizeof(Uint)*8 - c);
}
template<class Uint>
inline Uint BitsRotateRight(Uint x, int c) {
	assert(c >= 0);
	assert(c < (int)sizeof(Uint)*8);
	BOOST_STATIC_ASSERT(boost::is_unsigned<Uint>::value);
	return x >> c | x << (sizeof(Uint)*8 - c);
}
#endif

} // namespace nark

#endif // __nark_bits_rotate_hpp__

