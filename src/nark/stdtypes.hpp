/* vim: set tabstop=4 : */
#ifndef __nark_stdtypes_h__
#define __nark_stdtypes_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4819)
# pragma warning(disable: 4290)
# pragma warning(disable: 4267) // conversion from 'size_t' to 'uint', possible loss of data
# pragma warning(disable: 4244) // conversion from 'difference_type' to 'int', possible loss of data
#endif

#include "config.hpp"

#include <boost/version.hpp>
#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif

#include <boost/cstdint.hpp>
#include <boost/current_function.hpp>

#include <limits.h>

#if defined(_MSC_VER)
	#if _MSC_VER >= 1800
		#include <stdint.h>
	#else
		using boost::int8_t;
		using boost::int16_t;
		using boost::int32_t;

		using boost::uint8_t;
		using boost::uint16_t;
		using boost::uint32_t;

	  #if !defined(BOOST_NO_INT64_T)
		using boost::int64_t;
		using boost::uint64_t;
	  #endif
	#endif
#else // assume c99/gcc compatible
	#include <stdint.h>
#endif

#include <string.h> // for memcpy

namespace nark {

typedef unsigned char   byte_t;
typedef unsigned char   byte;
typedef   signed char  sbyte_t;
typedef   signed char  sbyte;
typedef unsigned short ushort;
typedef unsigned int   uint;
typedef unsigned long  ulong;
typedef unsigned long long ullong;
typedef long long llong;

#if !defined(BOOST_NO_INT64_T)
typedef uint64_t stream_position_t;
typedef int64_t  stream_offset_t;
#else
typedef uint32_t stream_position_t;
typedef int32_t  stream_offset_t;
#endif

typedef uint32_t ip_addr_t;

//! 空类，多用于参数化继承时的基类占位符
class EmptyClass{};

#define TT_PAIR(T) std::pair<T,T>

template<class SizeT, class AlignT>
inline SizeT align_up(SizeT size, AlignT align_size)
{
	size = (size + align_size - 1);
	return size - size % align_size;
}

template<class SizeT, class AlignT>
inline SizeT align_down(SizeT size, AlignT align_size)
{
	return size - size % align_size;
}

#define NARK_HAS_BSET(set, subset) (((set) & (subset)) == (subset))

/*
 * iter = s.end();
 * ibeg = s.begin();
 * if (iter != ibeg) do { --iter;
 *     //
 *     // do something with iter
 *     //
 * } while (iter != ibeg);else;
 * //
 * // this 'else' is intend for use REVERSE_FOR_EACH
 * // within an if-else-while sub sentence
 *
 * // this is faster than using reverse_iterator
 *
 */
//#define REVERSE_FOR_EACH_BEGIN(iter, ibeg)  if (iter != ibeg) do { --iter
//#define REVERSE_FOR_EACH_END(iter, ibeg)    } while (iter != ibeg); else


/////////////////////////////////////////////////////////////
//
//! @note Need declare public/protected after call this macro!!
//
#define DECLARE_NONE_COPYABLE_CLASS(ThisClassName)	\
private:											\
	ThisClassName(const ThisClassName& rhs);		\
	ThisClassName& operator=(const ThisClassName& rhs);
/////////////////////////////////////////////////////////////

#define CURRENT_SRC_CODE_POSTION  \
	__FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " BOOST_CURRENT_FUNCTION

#if defined(_DEBUG) || defined(DEBUG) || !defined(NDEBUG)
#	define DEBUG_only(S) S
#	define DEBUG_perror		perror
#	define DEBUG_printf		printf
#	define DEBUG_fprintf	fprintf
#	define DEBUG_fflush		fflush
#	define NARK_IF_DEBUG(Then, Else)  Then
#	define NARK_RT_assert(exp, ExceptionT)  assert(exp)
#else
#	define DEBUG_only(S)
#	define DEBUG_perror(Msg)
#	define DEBUG_printf		1 ? (void)0 : (void)printf
#	define DEBUG_fprintf	1 ? (void)0 : (void)fprintf
#	define DEBUG_fflush(fp)
#	define NARK_IF_DEBUG(Then, Else)  Else
#	define NARK_RT_assert(exp, ExceptionT)  \
	if (!(exp)) { \
		string_appender<> oss;\
		oss << "expression=\"" << #exp << "\", exception=\"" << #ExceptionT << "\"\n" \
			<< __FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " \
			<< BOOST_CURRENT_FUNCTION; \
		throw ExceptionT(oss.str().c_str()); \
	}
#endif

} // namespace nark

template<class T>
inline T aligned_load(const void* p) {
   	return *reinterpret_cast<const T*>(p);
}
template<class T>
inline T unaligned_load(const void* p) {
   	T x;
   	memcpy(&x, p, sizeof(T));
   	return x;
}
template<class T>
inline T aligned_load(const void* p, size_t i) {
   	return reinterpret_cast<const T*>(p)[i];
}
template<class T>
inline T unaligned_load(const void* p, size_t i) {
   	T x;
   	memcpy(&x, (const char*)(p) + sizeof(T) * i, sizeof(T));
   	return x;
}

template<class T>
inline void   aligned_save(void* p,T x) { *reinterpret_cast<T*>(p) = x; }
template<class T>
inline void unaligned_save(void* p,T x) { memcpy(p, &x, sizeof(T)); }

template<class T>
inline void aligned_save(void* p, size_t i, T val) {
   	reinterpret_cast<T*>(p)[i] = val;
}
template<class T>
inline void unaligned_save(void* p, size_t i, T val) {
   	memcpy((char*)(p) + sizeof(T) * i, &val, sizeof(T));
}

#endif // __nark_stdtypes_h__
