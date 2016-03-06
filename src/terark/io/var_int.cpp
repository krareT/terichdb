/* vim: set tabstop=4 : */
#include "var_int.hpp"
#include <assert.h>
#include <stdexcept>

#if defined(_MSC_VER)
# include <intrin.h>
#pragma intrinsic(_BitScanReverse)
//#pragma intrinsic(_BitScanReverse64)
#endif

#include <boost/version.hpp>
#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
# include <boost/detail/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif

#ifdef _MSC_VER
/* Always compile this module for speed, not size */
#pragma optimize("t", on)
#endif

namespace terark {

#include "var_int_inline.hpp"

uint32_t load_var_uint32(const unsigned char* buf, const unsigned char** endp)
{
	return gg_load_var_uint<uint32_t>(buf, endp, BOOST_CURRENT_FUNCTION);
}

uint64_t load_var_uint64(const unsigned char* buf, const unsigned char** endp)
{
	return gg_load_var_uint<uint64_t>(buf, endp, BOOST_CURRENT_FUNCTION);
}

int32_t load_var_int32(const unsigned char* buf, const unsigned char** endp)
{
	uint32_t x = load_var_uint32(buf, endp);
	return var_int32_u2s(x);
}

int64_t load_var_int64(const unsigned char* buf, const unsigned char** endp)
{
	return var_int64_u2s(load_var_uint64(buf, endp));
}

//////////////////////////////////////////////////////////////////////////

unsigned char* save_var_uint32(unsigned char* buf, uint32_t x)
{
   	return gg_save_var_uint(buf, x);
}

unsigned char* save_var_uint64(unsigned char* p, uint64_t x)
{
#if 0 // save max to 9 bytes
	for (int bytes = 0; bytes < 8; ++bytes)
	{
		if (x & ~(uint64_t)0x7F) {
			*p++ = (unsigned char)((x & 0x7f) | 0x80);
			x >>= 7; //doing unsigned shift
		} else
			break;
	}
	*p++ = (unsigned char)x;
	return p;
#else // save max to 10 bytes
	return gg_save_var_uint(p, x);
#endif
}

unsigned char* save_var_int32(unsigned char* buf, int32_t x) { return save_var_uint32(buf, var_int32_s2u(x)); }
unsigned char* save_var_int64(unsigned char* buf, int64_t x) { return save_var_uint64(buf, var_int64_s2u(x)); }


//##########################################################################################

uint32_t load_var_uint30(const unsigned char* buf, const unsigned char** endp) { return gg_load_var_uint30(buf, endp); }
uint64_t load_var_uint61(const unsigned char* buf, const unsigned char** endp) { return gg_load_var_uint61(buf, endp); }

int32_t load_var_int30(const unsigned char* buf, const unsigned char** endp) { return var_int30_u2s(load_var_uint30(buf, endp)); }
int64_t load_var_int61(const unsigned char* buf, const unsigned char** endp) { return var_int61_u2s(load_var_uint61(buf, endp)); }

//////////////////////////////////////////////////////////////////////////

unsigned char* save_var_uint30(unsigned char* buf, uint32_t x) { return gg_save_var_uint30(buf, x); }
unsigned char* save_var_uint61(unsigned char* buf, uint64_t x) { return gg_save_var_uint61(buf, x); }

unsigned char* save_var_int30(unsigned char* buf, int32_t x) { return save_var_uint30(buf, var_int30_s2u(x)); }
unsigned char* save_var_int61(unsigned char* buf, int64_t x) { return save_var_uint61(buf, var_int61_s2u(x)); }

//##########################################################################################

/**
 @brief reverse get var_uint32_t

 @note
   - if first var_int has read, *cur == buf-1
   - the sequence must all stored var_int, if not, the dilimeter byte's high bit must be 0
 */
uint32_t reverse_get_var_uint32(const unsigned char* buf, unsigned char const ** cur)
{
	assert(cur);
	assert(*cur);
	assert(*cur >= buf);

	const unsigned char* p = *cur;
	uint32_t x = 0;
	uint32_t w = *p;
	assert(!(x & 0x80));
	int shift = 0;
	--p;
	while (p >= buf && *p & 0x80)
	{
		x = x << 7 | (uint32_t)(*p & 0x7F);
		shift += 7;
		--p;
	}
	x |= w << shift;
	*cur = p;

	return x;
}

/**
 @brief reverse get var_int32_t

 @note if first var_int has read, *cur == buf-1
 */
int32_t reverse_get_var_int32(const unsigned char* buf, unsigned char const ** cur)
{
	return var_int32_u2s(reverse_get_var_uint32(buf, cur));
}

#if !defined(BOOST_NO_INT64_T)
/**
 @brief reverse get var_uint64_t

 @note if first var_int has read, *cur == buf-1
 */
uint64_t reverse_get_var_uint64(const unsigned char* buf, unsigned char const ** cur)
{
	assert(cur);
	assert(*cur);
	assert(*cur >= buf);

	const unsigned char* p = *cur;
	uint64_t x = 0;
	uint64_t w = *p;
	int shift = 0;
	--p;
	while (p >= buf && shift < 56 && *p & 0x80)
	{
		x = x << 7 | (uint64_t)(*p & 0x7F);
		shift += 7;
		--p;
	}
	assert(shift <= 56);

	x |= w << shift;

	*cur = p; // p now point to last byte of prev var_int

	return x;
}

/**
 @brief reverse get var_int64_t

 @note if first var_int has read, *cur == buf-1
 */
int64_t reverse_get_var_int64(const unsigned char* buf, unsigned char const ** cur)
{
	return var_int64_u2s(reverse_get_var_uint64(buf, cur));
}

#endif //BOOST_NO_INT64_T

} // namespace terark

