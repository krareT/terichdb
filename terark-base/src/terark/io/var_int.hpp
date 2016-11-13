/* vim: set tabstop=4 : */
#ifndef __terark_io_var_int_h__
#define __terark_io_var_int_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <limits>

#include <boost/config.hpp>
#include <boost/serialization/strong_typedef.hpp>
#include <boost/static_assert.hpp>
#include <boost/type_traits.hpp>

#include <terark/stdtypes.hpp>
#include <terark/pass_by_value.hpp>

#include "byte_swap.hpp"

#undef min
#undef max

namespace terark {

template<class T> struct fixed_to_var;

#if 0

// var int is  var_+IntT, var_int32_t, var_uint32_t, ...
// var_int_org<IntT>::type is original int of the var int
// is_var_int is the typetrait of var int

template<class VarIntT> struct var_int_org;
template<class T> struct is_var_int : boost::mpl::false_ {};

#define TERARK_DEFINE_VAR_INT_IMPL(IntT, VarIntT)\
	BOOST_STRONG_TYPEDEF(IntT, VarIntT)	\
	template<> struct is_var_int<VarIntT> : boost::mpl::true_ {}; \
	template<> struct is_primitive<VarIntT> : boost::mpl::true_ {}; \
	template<> struct fixed_to_var<IntT> : VarIntT { typedef VarIntT type; }; \
	template<> struct fixed_to_var<VarIntT> : VarIntT { typedef VarIntT type; };	 \
	template<> struct var_int_org<VarIntT> { typedef IntT type; };
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#else
#define TERARK_DEFINE_VAR_INT_IMPL(IntT, VarIntT)\
	BOOST_STRONG_TYPEDEF(IntT, VarIntT)	\
	template<> struct fixed_to_var<IntT> : VarIntT { typedef VarIntT type; }; \
	template<> struct fixed_to_var<VarIntT> : VarIntT { typedef VarIntT type; };
#endif

#if defined(BOOST_HAS_LONG_LONG)
	TERARK_DEFINE_VAR_INT_IMPL(         long long, var_int64_t)
	TERARK_DEFINE_VAR_INT_IMPL(unsigned long long, var_uint64_t)
	BOOST_STRONG_TYPEDEF(         long long, var_int61_t)
	BOOST_STRONG_TYPEDEF(unsigned long long, var_uint61_t)
#elif defined(BOOST_HAS_MS_INT64)
	TERARK_DEFINE_VAR_INT_IMPL(         __int64, var_int64_t)
	TERARK_DEFINE_VAR_INT_IMPL(unsigned __int64, var_uint64_t)
	BOOST_STRONG_TYPEDEF(         __int64, var_int61_t)
	BOOST_STRONG_TYPEDEF(unsigned __int64, var_uint61_t)
#endif

#if ULONG_MAX == 0xFFFFFFFF
	TERARK_DEFINE_VAR_INT_IMPL(         long, var_int32_t)
	TERARK_DEFINE_VAR_INT_IMPL(unsigned long, var_uint32_t)
	BOOST_STRONG_TYPEDEF(		  long, var_int30_t)
	BOOST_STRONG_TYPEDEF(unsigned long, var_uint30_t)
  #if UINT_MAX == 0xFFFFFFFF
	template<> struct fixed_to_var<         int> : var_int32_t  { typedef var_int32_t type; };
	template<> struct fixed_to_var<unsigned int> : var_uint32_t { typedef var_uint32_t type; };
  #elif UINT_MAX == 0xFFFF
    #error "don't support 16bit int"
  #endif
#elif UINT_MAX == 0xFFFFFFFF
	TERARK_DEFINE_VAR_INT_IMPL(         int, var_int32_t)
	TERARK_DEFINE_VAR_INT_IMPL(unsigned int, var_uint32_t)
	BOOST_STRONG_TYPEDEF(         int, var_int30_t)
	BOOST_STRONG_TYPEDEF(unsigned int, var_uint30_t)
	template<> struct fixed_to_var<         long> : var_int64_t  { typedef var_int64_t type; };
	template<> struct fixed_to_var<unsigned long> : var_uint64_t { typedef var_uint64_t type; };
#else
#   error no int32
#endif

#if defined(__amd64__) || defined(__amd64) || \
	defined(__x86_64__) || defined(__x86_64) || \
	defined(_M_X64) || \
	defined(__ia64__) || defined(_IA64) || defined(__IA64__) || \
	defined(__ia64) ||\
	defined(_M_IA64)
  typedef var_uint64_t var_size_t_base;
#else
  typedef var_uint32_t var_size_t_base;
#endif
struct var_size_t : public var_size_t_base {
	var_size_t() {}
	template<class T>
	explicit var_size_t(const T y) : var_size_t_base(y) {}
};

inline void byte_swap_in(var_int32_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_int64_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_uint32_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_uint64_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }

inline void byte_swap_in(var_int30_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_int61_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_uint30_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }
inline void byte_swap_in(var_uint61_t& x, boost::mpl::true_) { x.t = byte_swap(x.t); }

template<class IntT>
class as_var_int_ref
{
	IntT& val;

public:
	explicit as_var_int_ref(IntT& x) : val(x) {}

	template<class Input>
	friend void DataIO_loadObject(Input& in, as_var_int_ref x)
	{
		typename fixed_to_var<IntT>::type v;
		in >> v;
		x.val = v.t;
	}
	template<class Output>
	friend void DataIO_saveObject(Output& out, as_var_int_ref x)
	{
		// 必须有这个函数，因为有可能把一个 non-const int& 传递给 as_var_int 来输出
		out << typename fixed_to_var<IntT>::type(x.val);
	}
};

//! for load as var int
template<class IntT>
inline
pass_by_value<as_var_int_ref<IntT> >
as_var_int(IntT& x)
{
	return pass_by_value<as_var_int_ref<IntT> >(as_var_int_ref<IntT>(x));
}

//! for save as var int
template<class IntT>
inline
typename fixed_to_var<IntT>::type
as_var_int(const IntT& x)
{
	return typename fixed_to_var<IntT>::type(x);
}

// lowest bit is sign bit
//
template<class IntType, class UIntType>
inline UIntType var_int_s2u(IntType x)
{
	BOOST_STATIC_ASSERT(boost::is_signed<IntType>::value);
	BOOST_STATIC_ASSERT(boost::is_unsigned<UIntType>::value);
	BOOST_STATIC_ASSERT(sizeof(IntType)==sizeof(UIntType));

	if (x < 0) {
		if (std::numeric_limits<IntType>::min() == x)
			return UIntType(1);
		else
			return UIntType(-x << 1 | 1);
	} else
		return UIntType(x << 1);
}

template<class UIntType, class IntType>
inline IntType var_int_u2s(UIntType u)
{
	BOOST_STATIC_ASSERT(boost::is_signed<IntType>::value);
	BOOST_STATIC_ASSERT(boost::is_unsigned<UIntType>::value);
	BOOST_STATIC_ASSERT(sizeof(IntType)==sizeof(UIntType));

	if (u & 1) {
		if (0 == u >> 1)
			return std::numeric_limits<IntType>::min();
		else
			return -(IntType)(u >> 1);
	} else
		return (IntType)(u >> 1);
}

inline uint32_t var_int32_s2u(int32_t x) { return var_int_s2u<int32_t, uint32_t>(x); }
inline uint32_t var_int30_s2u(int32_t x) { return var_int_s2u<int32_t, uint32_t>(x); } // same as 32

inline int32_t var_int32_u2s(uint32_t u) { return var_int_u2s<uint32_t, int32_t>(u); }
inline int32_t var_int30_u2s(uint32_t u) { return var_int_u2s<uint32_t, int32_t>(u); } // same as 32

#if !defined(BOOST_NO_INT64_T)
inline uint64_t var_int64_s2u(int64_t x) { return var_int_s2u<int64_t, uint64_t>(x); }
inline uint64_t var_int61_s2u(int64_t x) { return var_int_s2u<int64_t, uint64_t>(x); } // same as 64
inline int64_t var_int64_u2s(uint64_t u) { return var_int_u2s<uint64_t, int64_t>(u); }
inline int64_t var_int61_u2s(uint64_t u) { return var_int_u2s<uint64_t, int64_t>(u); } // same as 64
#endif

////////////////////////////////////////////////////////////////////////////////////////
TERARK_DLL_EXPORT uint32_t load_var_uint32(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT uint32_t load_var_uint30(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT uint64_t load_var_uint64(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT uint64_t load_var_uint61(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT  int32_t load_var_int32(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT  int32_t load_var_int30(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT  int64_t load_var_int64(const unsigned char* buf, const unsigned char** endp);
TERARK_DLL_EXPORT  int64_t load_var_int61(const unsigned char* buf, const unsigned char** endp);

//--------------------------------------------------------------------------------------
TERARK_DLL_EXPORT unsigned char* save_var_uint32(unsigned char* buf, uint32_t x);
TERARK_DLL_EXPORT unsigned char* save_var_uint30(unsigned char* buf, uint32_t x);
TERARK_DLL_EXPORT unsigned char* save_var_uint64(unsigned char* buf, uint64_t x);
TERARK_DLL_EXPORT unsigned char* save_var_uint61(unsigned char* buf, uint64_t x);
TERARK_DLL_EXPORT unsigned char* save_var_int32(unsigned char* buf, int32_t x);
TERARK_DLL_EXPORT unsigned char* save_var_int30(unsigned char* buf, int32_t x);
TERARK_DLL_EXPORT unsigned char* save_var_int64(unsigned char* buf, int64_t x);
TERARK_DLL_EXPORT unsigned char* save_var_int61(unsigned char* buf, int64_t x);

////////////////////////////////////////////////////////////////////////////////////////
TERARK_DLL_EXPORT uint32_t reverse_get_var_uint32(const unsigned char* buf, unsigned char const ** cur);
TERARK_DLL_EXPORT int32_t reverse_get_var_int32(const unsigned char* buf, unsigned char const ** cur);


#if !defined(BOOST_NO_INT64_T)
TERARK_DLL_EXPORT uint64_t reverse_get_var_uint64(const unsigned char* buf, unsigned char const ** cur);
TERARK_DLL_EXPORT int64_t reverse_get_var_int64(const unsigned char* buf, unsigned char const ** cur);
#endif

/// pos must be const byte_t* or byte_t*
/// result should be size_t
/// use macros is faster than an inline function
#define FAST_READ_VAR_UINT32(pos, result) \
  do { \
		size_t _tmpUint_ = *pos++; \
		result = _tmpUint_ & 0x7F; \
		if ( terark_unlikely((_tmpUint_ & 0x80)) ) { \
			_tmpUint_ = *pos++; \
			result |= (_tmpUint_ & 0x7F) << 7; \
			if ( terark_unlikely((_tmpUint_ & 0x80)) ) { \
				_tmpUint_ = *pos++; \
				result |= (_tmpUint_ & 0x7F) << 14; \
				if ( terark_unlikely((_tmpUint_ & 0x80)) ) { \
					_tmpUint_ = *pos++; \
					result |= (_tmpUint_ & 0x7F) << 21; \
					if ( terark_unlikely((_tmpUint_ & 0x80)) ) { \
						_tmpUint_ = *pos++; \
						result |= (_tmpUint_ & 0x7F) << 28; \
						assert((_tmpUint_ & 0x80) == 0); \
					} \
				} \
			} \
		} \
	} while (0)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

} // namespace terark

#endif // __terark_io_var_int_h__

