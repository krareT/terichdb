//#define _XOPEN_SOURCE 600
//#define _ISOC99_SOURCE

#include "lcast.hpp"
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <boost/current_function.hpp>

#ifdef __CYGWIN__
	#define strtold strtod
	#define strtof  strtod
#endif

#ifdef _MSC_VER
	#define strtoll     _strtoi64
	#define strtoull    _strtoui64
    #define strncasecmp _strnicmp
#endif

namespace terark {

lcast_from_str::operator bool() const {
	char* q;
	long val = strtol(p, &q, 10);
	if (q > p) {
		if (0 == val) return false;
	//	if (1 == val) return true; // strict
		return true; // relaxed
	}
	else {
		if (strncasecmp(p, "false", 5) == 0) return false;
		if (strncasecmp(p, "true" , 4) == 0) return true;
	}
	throw std::invalid_argument("bad lcast string to bool: " + std::string(p, n));
}

lcast_from_str::operator char() const {
	char* q;
	long l = strtol(p, &q, 10);
	if (q > p && l >= CHAR_MIN && l <= CHAR_MAX) {
		return l;
	}
	throw std::invalid_argument("bad lcast string to char: " + std::string(p, n));
}

lcast_from_str::operator signed char() const {
	char* q;
	long l = strtol(p, &q, 10);
	if (q > p && l >= SCHAR_MIN && l <= SCHAR_MAX) {
		return l;
	}
	throw std::invalid_argument("bad lcast string to schar: " + std::string(p, n));
}

lcast_from_str::operator unsigned char() const {
	char* q;
	long l = strtol(p, &q, 10);
	if (q > p && l >= 0 && l <= UCHAR_MAX) {
		return l;
	}
	throw std::invalid_argument("bad lcast string to uchar: " + std::string(p, n));
}

lcast_from_str::operator short() const {
	return atoi(p);
}

lcast_from_str::operator unsigned short() const {
	char* q;
	long l = strtol(p, &q, 10);
	if (q > p && l >= 0 && l <= USHRT_MAX) {
		return (unsigned short)l;
	}
	throw std::invalid_argument("bad lcast string to ushort");
}

lcast_from_str::operator int() const {
	return atoi(p);
}

lcast_from_str::operator unsigned() const {
	char* q;
	long l = strtoul(p, &q, 10);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator long() const {
	char* q;
	long l = strtol(p, &q, 10);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator unsigned long() const {
	char* q;
	unsigned long l = strtoul(p, &q, 10);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator long long() const {
	char* q;
	long long l = strtoll(p, &q, 10);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator unsigned long long() const {
	char* q;
	unsigned long long l = strtoull(p, &q, 10);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator float() const {
	char* q;
	float l = strtof(p, &q);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator double() const {
	char* q;
	double l = strtod(p, &q);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

lcast_from_str::operator long double() const {
	char* q;
	long double l = strtold(p, &q);
	if (q == p) {
		throw std::invalid_argument("bad lcast");
	}
	return l;
}

#define CONVERT(format) { \
	char buf[48]; \
	sprintf(buf, format, x); \
	return buf; \
}

std::string lcast(char x) CONVERT("%d")
std::string lcast(signed char x) CONVERT("%u")
std::string lcast(unsigned char x) CONVERT("%u")
std::string lcast(int x) CONVERT("%d")
std::string lcast(unsigned int x) CONVERT("%u")
std::string lcast(short x) CONVERT("%d")
std::string lcast(unsigned short x) CONVERT("%u")
std::string lcast(long x) CONVERT("%ld")
std::string lcast(unsigned long x) CONVERT("%lu")
std::string lcast(long long x) CONVERT("%lld")
std::string lcast(unsigned long long x) CONVERT("%llu")
std::string lcast(float x) CONVERT("%f")
std::string lcast(double x) CONVERT("%f")
std::string lcast(long double x) CONVERT("%Lf")

//////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////
// Hex Lexical cast

std::string hexlcast(char x) CONVERT("%X")
std::string hexlcast(signed char x) CONVERT("%X")
std::string hexlcast(unsigned char x) CONVERT("%X")
std::string hexlcast(int x) CONVERT("%X")
std::string hexlcast(unsigned int x) CONVERT("%X")
std::string hexlcast(short x) CONVERT("%X")
std::string hexlcast(unsigned short x) CONVERT("%X")
std::string hexlcast(long x) CONVERT("%lX")
std::string hexlcast(unsigned long x) CONVERT("%lX")
std::string hexlcast(long long x) CONVERT("%llX")
std::string hexlcast(unsigned long long x) CONVERT("%llX")

namespace {
static const signed char hex_tab[] = {
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

template<class Int>
Int hex_int_from_str(const char* str, size_t len) {
	Int x = 0;
//	printf("%s: str=%.*s, len=%zd\n", BOOST_CURRENT_FUNCTION, int(len), str, len);
	for (size_t i = 0; i < len; ++i) {
		int c = (unsigned char)(str[i]);
		signed char d = hex_tab[c];
		if (-1 == d) {
			break;
		}
		x <<= 4;
		x |= d;
	}
	return x;
}
} // namespace

#define HEX_FROM_STR(Int) \
hexlcast_from_str::operator Int() const { \
	return hex_int_from_str<Int>(p, n); \
}

HEX_FROM_STR(char)
HEX_FROM_STR(signed char)
HEX_FROM_STR(unsigned char)
HEX_FROM_STR(short)
HEX_FROM_STR(unsigned short)
HEX_FROM_STR(int)
HEX_FROM_STR(unsigned int)
HEX_FROM_STR(long)
HEX_FROM_STR(unsigned long)
HEX_FROM_STR(long long)
HEX_FROM_STR(unsigned long long)

/// allowing non-hex char in hex string, such as: ABCD-1234_56+78 90-def
/// @returns decoded hex chars(or required half bytes)
/// @note
///   # if the number of hex chars is odd, the last hex char is the
///     higher 4 bit of the last byte
///   # if databuf is too small, the returned value is the required
///     bufsize in half bytes
///   # the filled(or required) bytes is (retVal+1)/2
TERARK_DLL_EXPORT
size_t
hex_decode(const char* hex, size_t hexlen, void* databuf, size_t bufsize) {
	byte_t* outBytes = (byte_t*)databuf;
	size_t j = 0;
	for(size_t i = 0; i < hexlen; ) {
		byte_t c;
		signed char hi, lo;
		do {
			c = hex[i++];
			hi = hex_tab[c];
			if (terark_unlikely(i == hexlen)) {
				if (terark_likely(hi >= 0)) {
					if (terark_likely(j < bufsize)) {
					//	outBytes[j] = byte_t(hi << 4);
						outBytes[j] = hi; // use hi as lo, real hi is 0
					}
					j++;
					return 2*j-1;
				}
				return 2*j;
			}
		} while (hi < 0);
		do {
			c = hex[i++];
			lo = hex_tab[c];
		} while (i < hexlen && lo < 0);
		if (terark_likely(lo >= 0)) {
			if (terark_likely(j < bufsize)) {
				outBytes[j] = byte_t((hi << 4) | lo);
			}
			j++;
		}
	}
	return 2*j;
}

TERARK_DLL_EXPORT
void hex_encode(const void* data, size_t datalen, char* hexbuf) {
	const char* tab = "0123456789abcdef";
	const byte_t* bData = (const byte_t*)(data);
	for(size_t i = 0; i < datalen; ++i) {
		byte_t c = bData[i];
		hexbuf[2*i + 0] = tab[c >> 4];
		hexbuf[2*i + 1] = tab[c & 15];
	}
}

TERARK_DLL_EXPORT
std::string hex_encode(const void* data, size_t datalen) {
	std::string str(datalen*2, '\0');
	hex_encode(data, datalen, &str[0]);
	return str;
}

} // namespace terark

