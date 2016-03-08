#ifndef __terark_io_var_int_inline_h__
#define __terark_io_var_int_inline_h__

#ifdef _MSC_VER
#  define TERARK_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__INTEL_COMPILER)
//#  define TERARK_FORCE_INLINE __attribute__((always_inline))
#  define TERARK_FORCE_INLINE inline
#else
#  define TERARK_FORCE_INLINE inline
#endif

#if !defined(BOOST_BIG_ENDIAN) && !defined(BOOST_LITTLE_ENDIAN)
#error must define byte endian
#endif

/*
#include <stdexcept>
#include <assert.h>

namespace terark {
*/

template<class T_uint>
TERARK_FORCE_INLINE
T_uint gg_load_var_uint(const unsigned char* buf, const unsigned char** endp, const char* func)
{
	const int maxshift = sizeof(T_uint) == 4 ? 28 : 63;
	T_uint x = 0;
	const unsigned char* p = buf;
#if 0
	// loop unroll is slower...
	//
#define	VAR_UINT_BYTE_FIRST() \
	b  = *p++; \
	x  = (b & 0x7F); \
	if ( (b & 0x80) == 0) { \
		*endp = p; \
	   	return x; \
	}
#define	VAR_UINT_BYTE_NEXT(shift) \
	b  = *p++; \
	x |= (b & 0x7F) << shift; \
	if ( (b & 0x80) == 0) { \
		*endp = p; \
	   	return x; \
	}
	/////////////////////////////////////////////
		VAR_UINT_BYTE_FIRST()
		VAR_UINT_BYTE_NEXT(07)
		VAR_UINT_BYTE_NEXT(14)
		VAR_UINT_BYTE_NEXT(21)
		VAR_UINT_BYTE_NEXT(28)
	if (sizeof(T_uint) == 8) { // const expr will be optimized
		VAR_UINT_BYTE_NEXT(35)
		VAR_UINT_BYTE_NEXT(42)
		VAR_UINT_BYTE_NEXT(49)
		VAR_UINT_BYTE_NEXT(56)
		VAR_UINT_BYTE_NEXT(63)
	}
#else
	for (int shift = 0; shift <= maxshift; shift += 7)
	{
		T_uint b = *p++;
		x |= (b & 0x7F) << shift;
		if ( (b & 0x80) == 0) {
			*endp = p;
			return x;
		}
	}
#endif
//	assert(0); // should not get here
	throw std::runtime_error(func);
}

template<class Stream, class T_uint>
TERARK_FORCE_INLINE
T_uint gg_load_var_uint_slow(Stream& s, const char* func)
{
	const int maxshift = sizeof(T_uint) == 4 ? 28 : 63;
	T_uint x = 0;
	for (int shift = 0; shift <= maxshift; shift += 7)
	{
		T_uint b = s.readByte(); // slower than *m_pos++
		x |= (b & 0x7F) << shift;
		if ( (b & 0x80) == 0)
			return x;
	}
//	assert(0); // should not get here
	throw std::runtime_error(func);
}

template<class T_uint>
TERARK_FORCE_INLINE
unsigned char* gg_save_var_uint(unsigned char* p, T_uint x)
{
#if 0
	while (x & ~(T_uint)0x7F)
	{
		*p++ = (unsigned char)((x & 0x7f) | 0x80);
		x >>= 7; //doing unsigned shift
	}
	*p++ = (unsigned char)(x);
	return p;
#else
	for (;;) {
		unsigned char b = (unsigned char)(x & 0x7f);
		if (x >>= 7)
			*p++ = b | 0x80;
		else {
			*p++ = b;
			return p;
		}
	}
#endif
}

//////////////////////////////////////////////////////////////////////////

// stream member function will ensure read extra is safe
#define DATA_IO_ALLOW_READ_EXTRA

TERARK_FORCE_INLINE
uint32_t gg_load_var_uint30(const unsigned char* buf, const unsigned char** endp)
{
#if !defined(DATA_IO_ALLOW_READ_EXTRA)
	uint32_t x = buf[0];
	*endp =(x & 3) + 1 + buf;
	switch (x & 3)
	{
	case 3:	x |= (uint32_t)(buf[3]) <<24; // fall through
	case 2:	x |= (uint32_t)(buf[2]) <<16; // fall through
	case 1:	x |= (uint32_t)(buf[1]) << 8; // fall through
	case 0:	break;
	}
#else
	// no branching needed
	// read more bytes, then unmask them
	uint32_t x = *(uint32_t*)buf;
  #if defined(BOOST_BIG_ENDIAN)
	x = byte_swap(x);
  #endif
	int n = x & 3;
	*endp = n + 1 + buf;
	x &= 0xFFFFFFFF >> (24 - n*8);
#endif
	return x >> 2;
}

TERARK_FORCE_INLINE
uint64_t gg_load_var_uint61(const unsigned char* buf, const unsigned char** endp)
{
#if !defined(DATA_IO_ALLOW_READ_EXTRA)
	uint64_t x = buf[0];
	*endp =(x & 7) + 1 + buf;
	switch (x & 7)
	{
	case 7:	x |= (uint64_t)(buf[7]) <<56; // fall through
	case 6:	x |= (uint64_t)(buf[6]) <<48; // fall through
	case 5:	x |= (uint64_t)(buf[5]) <<40; // fall through
	case 4:	x |= (uint64_t)(buf[4]) <<32; // fall through
	case 3:	x |= (uint64_t)(buf[3]) <<24; // fall through
	case 2:	x |= (uint64_t)(buf[2]) <<16; // fall through
	case 1:	x |= (uint64_t)(buf[1]) << 8; // fall through
	case 0:	break;
	}
#else
	// no branching needed
	// read more bytes, then unmask them
	uint64_t x = *(uint64_t*)buf;
  #if defined(BOOST_BIG_ENDIAN)
	x = byte_swap(x);
  #endif
	int n = x & 7;
	*endp = n + 1 + buf;
	x &= 0xFFFFFFFFFFFFFFFF >> (56 - n*8);
#endif
	return x >> 3;
}

template<class Stream>
TERARK_FORCE_INLINE
uint32_t gg_load_var_uint30_slow(Stream& s)
{
	uint32_t x = s.readByte();
	uint32_t y = 0;
	switch (x & 3)
	{
	case 3:	y =          (uint32_t)(s.readByte()); // fall through
	case 2:	y = y << 8 | (uint32_t)(s.readByte()); // fall through
	case 1:	y = y << 8 | (uint32_t)(s.readByte()); // fall through
	case 0:	break;
	}
	uint32_t z = x | y << 8;
	return z >> 2;
}

template<class Stream>
TERARK_FORCE_INLINE
uint64_t gg_load_var_uint61_slow(Stream& s)
{
	uint64_t x = s.readByte();
	uint64_t y = 0;
	switch (x & 7)
	{
	case 7:	y =          (uint64_t)(s.readByte()); // fall through
	case 6:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 5:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 4:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 3:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 2:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 1:	y = y << 8 | (uint64_t)(s.readByte()); // fall through
	case 0:	break;
	}
	uint64_t z = x | y << 8;
	return z >> 3;
}

// stream member function will ensure overwrite extra is safe
#define DATA_IO_ALLOW_OVERWRITE_EXTRA

TERARK_FORCE_INLINE
unsigned char* gg_save_var_uint30(unsigned char* p, uint32_t x)
{
	assert(x < 1 << 30);

	x <<= 2;

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_AMD64) || defined(_M_IA64))

// Is this always faster? It seems not, this may slower!

	unsigned long n; // index most significant 1 bit, from least significant bit, based on 0
	if (_BitScanReverse(&n, x)) {
		assert(n >= 0);
		assert(n <= 31);
		n /= 8;
	#if defined(DATA_IO_ALLOW_OVERWRITE_EXTRA)
		#if defined(BOOST_BIG_ENDIAN)
			*(uint32_t*)p = byte_swap(x) | byte_swap(n);
		#else
			*(uint32_t*)p = x | n;
		#endif
	#else
		p[0] = (unsigned char)(x) | (unsigned char)(n);
		switch (n)
		{
		case 3: p[3] = (unsigned char)(x>>24); // fall through
		case 2: p[2] = (unsigned char)(x>>16); // fall through
		case 1: p[1] = (unsigned char)(x>> 8); // fall through
		case 0: break;
		}
	#endif
		return p + n + 1;
	}
	else {
		*p = 0;
		return p + 1;
	}
#else
	if (terark_likely(!(x & 0xFFFF0000))) {
		if (terark_likely(!(x & 0xFF00))) {
			p[0] = (unsigned char)(x);
			return p + 1;
		}
		else {
			p[0] = (unsigned char)(x|1);
			p[1] = (unsigned char)(x>>8);
			return p + 2;
		}
	}
	else {
		p[1] = (unsigned char)(x>> 8);
		p[2] = (unsigned char)(x>>16);
		if (terark_likely(!(x & 0xFF000000))) {
			p[0] = (unsigned char)(x|2);
			return p + 3;
		}
		else {
			p[0] = (unsigned char)(x|3);
			p[3] = (unsigned char)(x>>24);
			return p + 4;
		}
	}
	// will not go here
#endif // _MSC_VER
}

//TERARK_FORCE_INLINE
inline // auto inline
unsigned char* gg_save_var_uint61(unsigned char* p, uint64_t x)
{
	assert(x < 1 << 30);

	x <<= 3;

#if defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_IA64))
	unsigned long n; // index most significant 1 bit, from least significant bit, based on 0
	if (_BitScanReverse64(&n, x)) {
		assert(n >= 0);
		assert(n <= 63);
		n /= 8;
	#if defined(DATA_IO_ALLOW_OVERWRITE_EXTRA)
		#if defined(BOOST_BIG_ENDIAN)
			*(uint64_t*)p = byte_swap(x) | byte_swap(n);
		#else
			*(uint64_t*)p = x | n;
		#endif
	#else
		p[0] = (unsigned char)(x) | (unsigned char)(n);
		switch (n)
		{
		case 7: p[7] = (unsigned char)(x>>56); // fall through
		case 6: p[6] = (unsigned char)(x>>48); // fall through
		case 5: p[5] = (unsigned char)(x>>40); // fall through
		case 4: p[4] = (unsigned char)(x>>32); // fall through
		case 3: p[3] = (unsigned char)(x>>24); // fall through
		case 2: p[2] = (unsigned char)(x>>16); // fall through
		case 1: p[1] = (unsigned char)(x>> 8); // fall through
		case 0: break;
		}
	#endif
		return p + n + 1;
	}
	else {
		*p = 0;
		return p + 1;
	}
#else
	// branch use binary search, need 3 conditions, 8 branches

	if (terark_likely(!(x & 0xFFFFFFFF00000000))) {
		if (terark_likely(!(x & 0xFFFF0000))) {
			if (terark_likely(!(x & 0xFF00))) {
				p[0] = (unsigned char)(x);
				return p + 1;
			}
			else {
				p[0] = (unsigned char)(x|1);
				p[1] = (unsigned char)(x>>8);
				return p + 2;
			}
		}
		else {
			p[1] = (unsigned char)(x>> 8);
			p[2] = (unsigned char)(x>>16);
			if (terark_likely(!(x & 0xFF000000))) {
				p[0] = (unsigned char)(x|2);
				return p + 3;
			}
			else {
				p[0] = (unsigned char)(x|3);
				p[3] = (unsigned char)(x>>24);
				return p + 4;
			}
		}
	}
	else { // high 32 bit not zero
		p[1] = (unsigned char)(x>> 8);
		p[2] = (unsigned char)(x>>16);
		p[3] = (unsigned char)(x>>24);
		p[4] = (unsigned char)(x>>32);
		if (terark_likely(!(x & 0xFFFF000000000000))) {
			if (terark_likely(!(x & 0x0000FF0000000000))) {
				p[0] = (unsigned char)(x|4);
				return p + 5;
			}
			else {
				p[0] = (unsigned char)(x|5);
				p[5] = (unsigned char)(x>>40);
				return p + 6;
			}
		}
		else {
			p[5] = (unsigned char)(x>>40);
			p[6] = (unsigned char)(x>>48);
			if (terark_likely(!(x & 0xFF00000000000000))) {
				p[0] = (unsigned char)(x|6);
				return p + 7;
			}
			else {
				p[0] = (unsigned char)(x|7);
				p[7] = (unsigned char)(x>>56);
				return p + 8;
			}
		}
	}
#endif // _MSC_VER
}


//} // namespace terark

#endif // __terark_io_var_int_inline_h__
