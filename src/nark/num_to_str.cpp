#include "num_to_str.hpp"
#include <stdio.h>
#include <assert.h>
//#include <boost/type_traits.hpp>

namespace nark {

int num_to_str(char* buf, bool x) {
	buf[0] = "01"[x?1:0];
	buf[1] = 0;
	return 1;
}

int num_to_str(char* buf, float x) {
	return sprintf(buf, "%f", x);
}
int num_to_str(char* buf, double x) {
	return sprintf(buf, "%f", x);
}
int num_to_str(char* buf, long double x) {
	return sprintf(buf, "%Lf", x);
}

template<class Int>
inline
int signed_int_to_str(char* buf, Int x) {
	const static char digits[] = "9876543210123456789";
	const char* base = digits + 9;
	const Int radix = 10;
	int i = 0;
	bool sign = x < 0;
	do {
		Int mod = x % radix;
		x /= radix;
		buf[i++] = base[mod];
	} while (x);
	if (sign)
		buf[i++] = '-';
	assert(i >= 1);
	assert(i < 16);
	buf[i] = 0;
	int n = i;
	for (int j = 0; j < i; ++j) {
		--i;
		char tmp = buf[j];
		buf[j] = buf[i];
		buf[i] = tmp;
	}
	return n;
}
template<class Int>
inline
int unsigned_int_to_str(char* buf, Int x) {
	const static char digits[] = "0123456789";
	const Int radix = 10;
	int i = 0;
	do {
		Int mod = x % radix;
		x /= radix;
		buf[i++] = digits[mod];
	} while (x);
	assert(i >= 1);
	assert(i < 16);
	buf[i] = 0;
	int n = i;
	for (int j = 0; j < i; ++j) {
		--i;
		char tmp = buf[j];
		buf[j] = buf[i];
		buf[i] = tmp;
	}
	return n;
}

#define GEN_num_to_str(Sign, Int) \
	int num_to_str(char* buf, Sign Int x) { return Sign##_int_to_str(buf, x); }

//GEN_num_to_str(char)
//GEN_num_to_str(signed char)
//GEN_num_to_str(unsigned char)
GEN_num_to_str(  signed, short)
GEN_num_to_str(  signed, int)
GEN_num_to_str(  signed, long)
GEN_num_to_str(  signed, long long)
GEN_num_to_str(unsigned, short)
GEN_num_to_str(unsigned, int)
GEN_num_to_str(unsigned, long)
GEN_num_to_str(unsigned, long long)

} // namespace nark

