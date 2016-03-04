// To the extent possible under law, the author(s) have dedicated all copyright
// and related and neighboring rights to this software to the public domain
// worldwide. This software is distributed without any warranty.
//
// You should have received a copy of the CC0 Public Domain Dedication along
// with this software.
// If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
#ifndef __nark_cxx_features_hpp__
#define __nark_cxx_features_hpp__

#ifndef CXXUTILS_FEATURES_HPP
#define CXXUTILS_FEATURES_HPP

#ifndef __has_feature
#define __has_feature(x) 0
#endif  // __has_feature

/* ----- C++1y Features ----- */

#ifdef __cpp_binary_literals
#define CXX_BINARY_LITERALS __cpp_binary_literals
#elif __has_feature(cxx_binary_literals)
#define CXX_BINARY_LITERALS 201304
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#define CXX_BINARY_LITERALS 201304
#endif  // __cpp_binary_literals

#ifdef __cpp_init_captures
#define CXX_INIT_CAPTURES __cpp_init_captures
#elif __has_feature(cxx_generalized_capture)
#define CXX_INIT_CAPTURES 201304
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 5)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_INIT_CAPTURES 201304
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_init_captures

#ifdef __cpp_generic_lambdas
#define CXX_GENERIC_LAMBDAS __cpp_generic_lambdas
#elif __has_feature(cxx_generic_lambda)
#define CXX_GENERIC_LAMBDAS 201304
#endif  // __cpp_generic_lambdas

#ifdef __cpp_decltype_auto
#define CXX_DECLTYPE_AUTO __cpp_decltype_auto
#elif __has_feature(cxx_decltype_auto)
#define CXX_DECLTYPE_AUTO 201304
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 9)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_DECLTYPE_AUTO 201304
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_decltype_auto

#ifdef __cpp_return_type_deduction
#define CXX_RETURN_TYPE_DEDUCTION __cpp_return_type_deduction
#elif __has_feature(cxx_return_type_deduction)
#define CXX_RETURN_TYPE_DEDUCTION 201304
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_RETURN_TYPE_DEDUCTION 201304
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_return_type_deduction

#ifdef __cpp_runtime_arrays
#define CXX_RUNTIME_ARRAYS __cpp_runtime_arrays
#elif __has_feature(cxx_runtime_array)
#define CXX_RUNTIME_ARRAYS 201304
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 9)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_RUNTIME_ARRAYS 201304
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_runtime_arrays

#ifdef __cpp_aggregate_nsdmi
#define CXX_AGGREGATE_NSDMI __cpp_aggregate_nsdmi
#elif __has_feature(cxx_aggregate_nsdmi)
#define CXX_AGGREGATE_NSDMI 201304
#endif  // __cpp_aggregate_nsdmi

#ifdef __cpp_variable_templates
#define CXX_VARIABLE_TEMPLATES __cpp_variable_templates
#elif __has_feature(cxx_variable_templates)
#define CXX_VARIABLE_TEMPLATES 201304
#endif  // __cpp_variable_templates

/* ----- C++11 Features ----- */

#ifdef __cpp_unicode_characters
#define CXX_UNICODE_CHARACTERS __cpp_unicode_characters
#elif defined(__clang__)
#define CXX_UNICODE_CHARACTERS 200704
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 4)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_UNICODE_CHARACTERS 200704
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_unicode_characters

#ifdef __cpp_raw_strings
#define CXX_RAW_STRINGS __cpp_raw_strings
#elif __has_feature(cxx_raw_string_literals)
#define CXX_RAW_STRINGS 200710
#elif(__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 5)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_RAW_STRINGS 200710
#endif  // defined(__GXX_EXPERIMETNAL_CXX0X__)
#elif(_MSC_FULL_VER >= 170051025)
#define CXX_RAW_STRINGS 200710
#endif  // __cpp_raw_strings

#ifdef __cpp_unicode_literals
#define CXX_UNICODE_LITERALS __cpp_unicode_literals
#elif __has_feature(cxx_unicode_literals)
#define CXX_UNICODE_LITERALS 200710
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 5)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_UNICODE_LITERALS 200710
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_unicode_literals

#ifdef __cpp_user_defined_literals
#define CXX_USER_DEFINED_LITERALS __cpp_user_defined_literals
#elif __has_feature(cxx_user_literals)
#define CXX_USER_DEFINED_LITERALS 200809
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_USER_DEFINED_LITERALS 200809
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_user_defined_literals

#ifdef __cpp_constexpr
#define CXX_CONSTEXPR __cpp_constexpr
#elif __has_feature(cxx_constexpr)
#if __has_feature(cxx_relaxed_constexpr)
#define CXX_CONSTEXPR 201304
#else  // __has_feature(cxx_relaxed_constexpr)
#define CXX_CONSTEXPR 200704
#endif  // __has_feature(cxx_relaxed_constexpr)
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1300) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_CONSTEXPR 200704
#endif  //(__INTEL_COMPILER >= 1300) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif !defined(__EDG__) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 6)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_CONSTEXPR 200704
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif defined(__IBMCPP_CONSTEXPR)
#define CXX_CONSTEXPR 200704
#endif  // __cpp_constexpr

#ifdef __cpp_decltype
#define CXX_DECLTYPE __cpp_decltype
#elif __has_feature(cxx_decltype)
#define CXX_DECLTYPE 200707
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1100) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_DECLTYPE 200707
#endif  // (__INTEL_COMPILER >= 1100) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif defined(__EDG__)
#if (__EDG_VERSION__ >= 410)
#define CXX_DECLTYPE 200707
#endif  // (__EDG_VERSION__ >= 410)
#elif(__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_DECLTYPE 200707
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif(_MSC_VER >= 1600)
#define CXX_DECLTYPE 200707
#elif defined(__IBMCPP_DECLTYPE)
#define CXX_DECLTYPE 200707
#endif  // __cpp_decltype

#ifdef __cpp_attributes
#define CXX_ATTRIBUTES __cpp_attributes
#elif __has_feature(cxx_attributes)
#define CXX_ATTRIBUTES 200809
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_ATTRIBUTES 200809
#endif  // (__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif !defined(__EDG__) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_ATTRIBUTES 200809
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // __cpp_attributes

#ifdef __cpp_rvalue_reference
#define CXX_RVALUE_REFERENCE __cpp_rvalue_reference
#elif __has_feature(cxx_rvalue_references)
#define CXX_RVALUE_REFERENCE 200610
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1110) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_RVALUE_REFERENCE 200610
#endif  //(__INTEL_COMPILER >= 1110) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif defined(__EDG__)
#if (__EDG_VERSION__ >= 410)
#define CXX_RVALUE_REFERENCE 200610
#endif  // (__EDG_VERSION__ >= 410)
#elif(__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_RVALUE_REFERENCE 200610
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif(_MSC_VER >= 1600)
#define CXX_RVALUE_REFERENCE 200610
#elif defined(__IBMCPP_RVALUE_REFERENCES)
#define CXX_RVALUE_REFERENCE 200610
#endif  // __cpp_rvalue_reference

#ifdef __cpp_variadic_templates
#define CXX_VARIADIC_TEMPLATES __cpp_variadic_templates
#elif __has_feature(cxx_variadic_templates)
#define CXX_VARIADIC_TEMPLATES 200704
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_VARIADIC_TEMPLATES 200704
#endif  //(__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif defined(__EDG__)
#if (__EDG_VERSION__ >= 410)
#define CXX_VARIADIC_TEMPLATES 200704
#endif  // (__EDG_VERSION__ >= 410)
#elif(__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 4)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_VARIADIC_TEMPLATES 200704
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif(_MSC_FULL_VER >= 170051025)
#define CXX_VARIADIC_TEMPLATES 200704
#elif defined(__IBMCPP_VARIADIC_TEMPLATES)
#define CXX_VARIADIC_TEMPLATES 200704
#endif  // __cpp_variadic_templates

/* ----- Extensions ----- */

#if __has_feature(cxx_alias_templates)
#define CXX_ALIAS_TEMPLATES 200704
#elif defined(__ICL)
#if (__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_ALIAS_TEMPLATES 200704
#endif  //(__INTEL_COMPILER >= 1210) && defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif !defined(__EDG__) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_ALIAS_TEMPLATES 200704
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#elif(_MSC_VER >= 1800)
#define CXX_ALIAS_TEMPLATES 200704
#endif  // cxx_alias_templates

#if __has_feature(cxx_noexcept)
#define CXX_NOEXCEPT 201003
#elif !defined(__EDG__) && !defined(__ICL) && \
    (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 6)
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#define CXX_NOEXCEPT 201003
#endif  // defined(__GXX_EXPERIMENTAL_CXX0X__)
#endif  // cxx_noexcept
#endif  // CXXUTILS_FEATURES_HPP

#endif  // __nark_cxx_features_hpp__

