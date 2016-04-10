#ifndef __terark_util_throw_hpp__
#define __terark_util_throw_hpp__

#include "autofree.hpp"
#include <boost/current_function.hpp>
#include <stdio.h>
#include <errno.h>

#ifdef _MSC_VER
#define TERARK_THROW(Except, fmt, ...) \
	do { \
		char __buf[4096]; \
		int __len = _snprintf(__buf, sizeof(__buf), \
			"%s:%d: %s: errno=%d : " fmt, \
			__FILE__, __LINE__, BOOST_CURRENT_FUNCTION, errno, \
			##__VA_ARGS__); \
		fprintf(stderr, "%s\n", __buf); \
		std::string strMsg(__buf, __len); \
		throw Except(strMsg); \
	} while (0)

#else
#define TERARK_THROW(Except, fmt, ...) \
	do { \
		terark::AutoFree<char> __msg; \
		int __len = asprintf(&__msg.p, "%s:%d: %s: " fmt, \
			__FILE__, __LINE__, BOOST_CURRENT_FUNCTION, \
			##__VA_ARGS__); \
		fprintf(stderr, "%s\n", __msg.p); \
		std::string strMsg(__msg.p, __len); \
		throw Except(strMsg); \
	} while (0)
#endif

#define THROW_STD(Except, fmt, ...) \
	TERARK_THROW(std::Except, fmt, ##__VA_ARGS__)

#endif // __terark_util_throw_hpp__

