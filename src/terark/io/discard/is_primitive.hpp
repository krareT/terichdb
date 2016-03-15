/* vim: set tabstop=4 : */
#ifndef __terark_io_is_primitive_h__
#define __terark_io_var_int_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <boost/cstdint.hpp>

#include <string>

// should be the last #include
#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace terark {

BOOST_TT_AUX_BOOL_TRAIT_DEF1(is_primitive,T,false)

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, char, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned char, true)

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, int, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, long, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, short, true)

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned int, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned long, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned short, true)

#if defined(BOOST_HAS_LONG_LONG)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, long long, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned long long, true)
#elif defined(BOOST_HAS_MS_INT64)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, __int64, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, unsigned __int64, true)
#endif

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, std::string, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_primitive, std::wstring, true)


} // namespace terark


#include "boost/type_traits/detail/bool_trait_undef.hpp"


#endif // __terark_io_var_int_h__

