/* vim: set tabstop=4 : */
#ifndef __terark_io_custom_int_h__
#define __terark_io_custom_int_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <boost/config.hpp>
#include <boost/static_assert.hpp>
#include <terark/stdtypes.hpp>

// should be the last #include
#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace terark {

template<class BaseInt, int Bits>
class custom_int
{
	BaseInt m_val;
	BOOST_STATIC_ASSERT(Bits % 8 == 0);
public:
	typedef BaseInt presentation_type;
	BOOST_STATIC_CONSTANT(int, bits  = Bits);
	BOOST_STATIC_CONSTANT(int, bytes = Bits / 8);

	custom_int(BaseInt val = BaseInt()) : m_val(val % (BaseInt(1) << Bits)) {}

	operator BaseInt() const { return m_val; }

	BaseInt& value() { return m_val; }
	BaseInt  value() const { return m_val; }

#define TERARK_custom_int_gen_operator(op) \
	BaseInt operator op(custom_int y) const { return m_val op y.m_val; }

	TERARK_custom_int_gen_operator(+)
	TERARK_custom_int_gen_operator(-)
	TERARK_custom_int_gen_operator(*)
	TERARK_custom_int_gen_operator(/)
	TERARK_custom_int_gen_operator(%)
	TERARK_custom_int_gen_operator(^)
	TERARK_custom_int_gen_operator(&)
	TERARK_custom_int_gen_operator(|)
	TERARK_custom_int_gen_operator(<<)
	TERARK_custom_int_gen_operator(>>)

#undef TERARK_custom_int_gen_operator

	bool operator&&(custom_int y) const { return m_val && y.m_val; }
	bool operator||(custom_int y) const { return m_val || y.m_val; }
	bool operator!() const { return !m_val; }

	BaseInt operator~() const { return ~m_val; }
	BaseInt operator-() const { return -m_val; }

	custom_int operator+() const { return +m_val; }

	custom_int operator++() { return m_val = ++m_val % (BaseInt(1) << Bits); }
	custom_int operator--() { return m_val = --m_val % (BaseInt(1) << Bits); }

	custom_int operator++(int)
	{
		BaseInt t = m_val;
		m_val = ++m_val % (BaseInt(1) << Bits);
		return t;
	}
	custom_int operator--(int)
	{
		BaseInt t = m_val;
		m_val = --m_val % (BaseInt(1) << Bits);
		return t;
	}

#define TERARK_custom_int_gen_assign_op(op)\
	custom_int& operator op(custom_int y)	\
	{	m_val op y;						\
		m_val %= (BaseInt(1) << Bits);	\
		return *this;					\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	TERARK_custom_int_gen_assign_op(+=)
	TERARK_custom_int_gen_assign_op(-=)
	TERARK_custom_int_gen_assign_op(*=)
#undef TERARK_custom_int_gen_assign_op

// not need: "m_val %= (BaseInt(1) << Bits);"
#define TERARK_custom_int_gen_assign_op(op)\
	custom_int& operator op(custom_int y)	\
	{	m_val op y;						\
		return *this;					\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	TERARK_custom_int_gen_assign_op(/=)
	TERARK_custom_int_gen_assign_op(%=)
	TERARK_custom_int_gen_assign_op(&=)
	TERARK_custom_int_gen_assign_op(|=)
	TERARK_custom_int_gen_assign_op(^=)
	TERARK_custom_int_gen_assign_op(<<=)
	TERARK_custom_int_gen_assign_op(>>=)
#undef TERARK_custom_int_gen_assign_op

	template<class Input> friend void DataIO_loadObject(Input& input, custom_int& x)
	{
		unsigned char b;
		x.m_val = 0;
		for (int bytes = 0; bytes != Bits/8; ++bytes)
		{
			input >> b;
			x.m_val = x.m_val << 8 | b;
		}
	}
	template<class Output> friend void DataIO_saveObject(Output& output, custom_int x)
	{
		BaseInt y = x.m_val;
		for (int bytes = 0; bytes != Bits/8; ++bytes)
		{
			output << b;
			y >>= 8;
		}
	}
};

typedef custom_int<int32_t, 24> int24_t;
typedef custom_int<int64_t, 40> int40_t;
typedef custom_int<int64_t, 48> int48_t;
typedef custom_int<int64_t, 56> int56_t;

typedef custom_int<uint32_t, 24> uint24_t;
typedef custom_int<uint64_t, 40> uint40_t;
typedef custom_int<uint64_t, 48> uint48_t;
typedef custom_int<uint64_t, 56> uint56_t;

BOOST_TT_AUX_BOOL_TRAIT_DEF1(is_custom_int, T, false)

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, int24_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, int40_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, int48_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, int56_t, true)

BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, uint24_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, uint40_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, uint48_t, true)
BOOST_TT_AUX_BOOL_TRAIT_CV_SPEC1(is_custom_int, uint56_t, true)

//! declaration
template<class IntType> struct presentation_type;

template<> struct presentation_type<int8_t>  { typedef int8_t type; };
template<> struct presentation_type<int16_t> { typedef int16_t type; };
template<> struct presentation_type<int32_t> { typedef int32_t type; };
template<> struct presentation_type<int64_t> { typedef int64_t type; };

template<> struct presentation_type<uint8_t>  { typedef uint8_t type; };
template<> struct presentation_type<uint16_t> { typedef uint16_t type; };
template<> struct presentation_type<uint32_t> { typedef uint32_t type; };
template<> struct presentation_type<uint64_t> { typedef uint64_t type; };

template<> struct presentation_type<int24_t> { typedef int32_t type; };
template<> struct presentation_type<int40_t> { typedef int64_t type; };
template<> struct presentation_type<int48_t> { typedef int64_t type; };
template<> struct presentation_type<int56_t> { typedef int64_t type; };

template<> struct presentation_type<uint24_t> { typedef uint32_t type; };
template<> struct presentation_type<uint40_t> { typedef uint64_t type; };
template<> struct presentation_type<uint48_t> { typedef uint64_t type; };
template<> struct presentation_type<uint56_t> { typedef uint64_t type; };

} // namespace terark

#include "boost/type_traits/detail/bool_trait_undef.hpp"

#endif // __terark_io_custom_int_h__

