/* vim: set tabstop=4 : */
#ifndef __nark_io_DataIO_Basic_h__
#define __nark_io_DataIO_Basic_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <utility>

#ifndef BOOST_INTRUSIVE_PTR_HPP_INCLUDED
#  include <boost/intrusive_ptr.hpp>
#endif
#ifndef BOOST_SCOPED_PTR_HPP_INCLUDED
#  include <boost/scoped_ptr.hpp>
#endif
#ifndef BOOST_SHARED_PTR_HPP_INCLUDED
#  include <boost/shared_ptr.hpp>
#endif

#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
# include <boost/detail/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif
#include <boost/cstdint.hpp>
#include <boost/mpl/bool.hpp>
#include <nark/cxx_features.hpp>
#include "var_int.hpp"
//#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace nark {

//////////////////////////////////////////////////////////////////////////

#ifdef BOOST_LITTLE_ENDIAN
	#define DATA_IO_BSWAP_FOR_BIG(T)    typename DataIO_need_bswap<T>::type
	#define DATA_IO_BSWAP_FOR_LITTLE(T) ByteSwap_false
	#define BYTE_SWAP_IF_LITTLE_ENDIAN(x) x = nark::byte_swap(x)
	#define BYTE_SWAP_IF_BIG_ENDIAN(x)
#elif defined(BOOST_BIG_ENDIAN)
	#define DATA_IO_BSWAP_FOR_BIG(T)    ByteSwap_false
	#define DATA_IO_BSWAP_FOR_LITTLE(T) typename DataIO_need_bswap<T>::type
	#define BYTE_SWAP_IF_LITTLE_ENDIAN(x)
	#define BYTE_SWAP_IF_BIG_ENDIAN(x)  x = nark::byte_swap(x)
#else
	#error "must define BOOST_LITTLE_ENDIAN or BOOST_BIG_ENDIAN"
#endif

// When DataIO_is_dump<DataIO, Type> is true, it means:
//   1. Type can be memcpy'ed
//   2. Type may need byte-swap for fix endian: When Read/Write BigEndian on
//      LittleEndian machine, DataIO_is_dump is true, the data will be
//      byte-swaped before dump-write or after dump-read

typedef boost::mpl::true_  IsDump_true;
typedef boost::mpl::false_ IsDump_false;

#if defined(CXX_RETURN_TYPE_DEDUCTION) && CXX_RETURN_TYPE_DEDUCTION >= 201304
  template<class DataIO, class T>
  IsDump_false Deduce_DataIO_is_dump(DataIO*, T*) { return IsDump_false(); }
  template<class DataIO, class T>
  struct DataIO_is_dump :
    public decltype(Deduce_DataIO_is_dump((DataIO*)NULL, (T*)NULL))
  {};
#else
  //! default false
  template<class DataIO, class T>
  struct DataIO_is_dump : public IsDump_false {};
#endif

#if defined(NARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE)

	#define DataIO_IsDump_TypeTrue1(T)
	#define DataIO_IsDump_TypeTrue2(ByteOrder, T)

#else

	#define DataIO_IsDump_TypeTrue1(T) \
	template<class DataIO> struct DataIO_is_dump<DataIO, T> : public IsDump_true {};

	#define DataIO_IsDump_TypeTrue2(ByteOrder, T)		\
	template<class Stream>								\
	struct DataIO_is_dump<ByteOrder##Input<Stream>, T>	\
		: public IsDump_true {};				\
	template<class Stream>								\
	struct DataIO_is_dump<ByteOrder##Output<Stream>, T>	\
		: public IsDump_true {};				\
	//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#endif // NARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE


#if !defined(NARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE) && \
  defined(CXX_RETURN_TYPE_DEDUCTION) && CXX_RETURN_TYPE_DEDUCTION >= 201304

  // [dio Members] must be inside a member function of [Class],
  // so [typeof] or [decltype] is not suffice,
  // it needs [auto] return type (c++14 feature)
    #define DATA_IO_GEN_DUMP_TYPE_TRAITS(Class, Members) \
      template<class DataIO> \
      auto _M_Deduce_DataIO_is_realdump(DataIO*) { \
        return nark::DataIO_is_realdump<DataIO,Class,0,true>((Class*)NULL)Members;\
      }
//	#define HERE_FOR_GCC_FRIEND_AUTO_WORK_AROUND auto
	#define HERE_FOR_GCC_FRIEND_AUTO_WORK_AROUND(Derived) \
	  decltype(((Derived*)NULL)->_M_Deduce_DataIO_is_realdump \
				((DataIO*)NULL).is_dumpable())
    #define DATA_IO_GEN_DUMP_TYPE_TRAITS_REG(Friend, Derived, Class) \
      template<class DataIO> \
      Friend HERE_FOR_GCC_FRIEND_AUTO_WORK_AROUND(Derived)	  \
	  Deduce_DataIO_is_dump(DataIO*, Class*) { \
        return ((Derived*)NULL)->_M_Deduce_DataIO_is_realdump \
				((DataIO*)NULL).is_dumpable(); \
	  }
#else
    #define DATA_IO_GEN_DUMP_TYPE_TRAITS(Class, Members)
    #define DATA_IO_GEN_DUMP_TYPE_TRAITS_REG(Friend, Derived, Class)
#endif // CXX_RETURN_TYPE_DEDUCTION && CXX_RETURN_TYPE_DEDUCTION


DataIO_IsDump_TypeTrue1(float)
DataIO_IsDump_TypeTrue1(double)
DataIO_IsDump_TypeTrue1(long double)

DataIO_IsDump_TypeTrue1(char)
DataIO_IsDump_TypeTrue1(signed char)
DataIO_IsDump_TypeTrue1(unsigned char)

DataIO_IsDump_TypeTrue1(short)
DataIO_IsDump_TypeTrue1(unsigned short)
DataIO_IsDump_TypeTrue1(int)
DataIO_IsDump_TypeTrue1(unsigned int)
DataIO_IsDump_TypeTrue1(long)
DataIO_IsDump_TypeTrue1(unsigned long)
#if defined(BOOST_HAS_LONG_LONG)
DataIO_IsDump_TypeTrue1(long long)
DataIO_IsDump_TypeTrue1(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
DataIO_IsDump_TypeTrue1(__int64)
DataIO_IsDump_TypeTrue1(unsigned __int64)
#endif

// support std::pair
template<class DataIO, class X, class Y>
struct DataIO_is_dump<DataIO, std::pair<X,Y> > :
	public ::boost::mpl::bool_<DataIO_is_dump<DataIO, X>::value &&
							   DataIO_is_dump<DataIO, Y>::value &&
							   sizeof(std::pair<X,Y>) == sizeof(X)+sizeof(Y)>
{
};

template<class DataIO, class T, int Dim>
struct DataIO_is_dump<DataIO, T[Dim]> : public DataIO_is_dump<DataIO, T>::type
{
};

template<class DataIO, class T, int Dim>
struct DataIO_is_dump<DataIO, const T[Dim]> : public DataIO_is_dump<DataIO, T>::type
{
};

//////////////////////////////////////////////////////////////////////////
template<class DataIO, class Outer, int Size, bool MembersDumpable>
struct DataIO_is_realdump
{
	BOOST_STATIC_CONSTANT(int, size = Size);
	typedef boost::mpl::bool_<MembersDumpable && sizeof(Outer)==Size> is_dump_t;

	is_dump_t is_dumpable() const { return is_dump_t(); }

#if (defined(_DEBUG) || !defined(NDEBUG)) && !defined(DATA_IO_DONT_CHECK_REAL_DUMP)
	const Outer* address;
	const char*  prev_member;
	DataIO_is_realdump(const Outer* p, const void* prev_member)
		: address(p), prev_member((const char*)prev_member) {}
	DataIO_is_realdump(const Outer* p)
		: address(p), prev_member((const char*)p) {}
#else
	DataIO_is_realdump(const Outer*, const void* = 0) {}
#endif

#if (defined(_DEBUG) || !defined(NDEBUG)) && !defined(DATA_IO_DONT_CHECK_REAL_DUMP)
	template<class T>
	void check_member_order(const T& x, IsDump_true)
	{
	// if member declaration order of &a&b&c&d is different with where they defined in class
	// here will raise an assertion fail
	//
	// 如果成员序列化声明的顺序 &a&b&c&d 和它们在类中定义的顺序不同
	// 这里会引发一个断言
	// 如果序列化了一个非类的成员（比如 &a&b&c&d 中可能一个标识符引用了不是类成员的某个变量），也会引发断言
		assert((const char*)&x >= (const char*)address);
		assert((const char*)&x <= (const char*)address + sizeof(Outer) - sizeof(T));
		if (prev_member != (const char*)address)
			assert(prev_member < (const char*)&x);
	}
	template<class T>
	void check_member_order(const T&, IsDump_false) {}
#endif

	template<class T>
	DataIO_is_realdump<DataIO, Outer, Size+sizeof(T), boost::mpl::bool_<MembersDumpable && DataIO_is_dump<DataIO, T>::value>::value>
	operator&(const T& x)
	{
		typedef DataIO_is_realdump<DataIO, Outer, Size+sizeof(T), boost::mpl::bool_<MembersDumpable && DataIO_is_dump<DataIO, T>::value>::value> ret_t;
#if (defined(_DEBUG) || !defined(NDEBUG)) && !defined(DATA_IO_DONT_CHECK_REAL_DUMP)
		check_member_order(x, ::boost::mpl::bool_<MembersDumpable && DataIO_is_dump<DataIO, T>::value>());
		return ret_t(address, &x);
#else
		(void)(x); // use x
		return ret_t(NULL);
#endif
	}
};

} // namespace nark

//#include <boost/type_traits/detail/bool_trait_undef.hpp>

#endif // __nark_io_DataIO_Basic_h__

