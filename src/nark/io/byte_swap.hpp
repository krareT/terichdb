/* vim: set tabstop=4 : */
#ifndef __nark_io_byte_swap_h__
#define __nark_io_byte_swap_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4127)
#endif

#include <nark/util/byte_swap_impl.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/static_assert.hpp>

namespace nark {

// inline void byte_swap_in(float&) {}
// inline void byte_swap_in(double&) {}
// inline void byte_swap_in(long double&) {}
// inline void byte_swap_in(char&) {}
// inline void byte_swap_in(signed char&) {}
// inline void byte_swap_in(unsigned char&) {}

inline void byte_swap_in(unsigned short& x, boost::mpl::true_)
{
	x = byte_swap(x);
}
inline void byte_swap_in(short& x, boost::mpl::true_)
{
	x = byte_swap(x);
}

inline void byte_swap_in(unsigned int& i, boost::mpl::true_)
{
	i = byte_swap(i);
}
inline void byte_swap_in(int& i, boost::mpl::true_)
{
	i = byte_swap((unsigned int)i);
}
inline void byte_swap_in(float& i, boost::mpl::true_)
{
	BOOST_STATIC_ASSERT(sizeof(float) == sizeof(unsigned int));
	*(unsigned int*)&i = byte_swap(*(unsigned int*)&i);
}
#if defined(BOOST_HAS_LONG_LONG)
inline void byte_swap_in(unsigned long long& i, boost::mpl::true_)
{
	i = byte_swap(i);
}
inline void byte_swap_in(long long& i, boost::mpl::true_)
{
	i = byte_swap((unsigned long long)(i));
}
inline void byte_swap_in(double& i, boost::mpl::true_)
{
	BOOST_STATIC_ASSERT(sizeof(double) == sizeof(unsigned long long));
	*(unsigned long long*)&i = byte_swap(*(unsigned long long*)&i);
}
#elif defined(BOOST_HAS_MS_INT64)
inline void byte_swap_in(unsigned __int64& i, boost::mpl::true_)
{
	i = byte_swap(i);
}
inline void byte_swap_in(__int64& i, boost::mpl::true_)
{
	i = byte_swap((unsigned __int64)(i));
}
inline void byte_swap_in(double& i, boost::mpl::true_)
{
	BOOST_STATIC_ASSERT(sizeof(double) == sizeof(unsigned __int64));
	*(unsigned __int64*)&i = byte_swap(*(unsigned __int64*)&i);
}
#endif
/**
 * Don't support
inline void byte_swap_in(long double& i, boost::mpl::true_)
{
}
*/

#if ULONG_MAX == 0xffffffff
inline void byte_swap_in(unsigned long& x, boost::mpl::true_)
{
	x = byte_swap((unsigned int)x);
}
inline void byte_swap_in(long& x, boost::mpl::true_)
{
	x = byte_swap((unsigned int)x);
}
#else
inline void byte_swap_in(unsigned long& x, boost::mpl::true_)
{
	x = byte_swap((unsigned long long)x);
}
inline void byte_swap_in(long& x, boost::mpl::true_)
{
	x = byte_swap((unsigned long long)x);
}
#endif // ULONG_MAX

template<class T, int Dim>
void byte_swap_in(T (&a)[Dim], boost::mpl::true_)
{
	for (int i = 0; i < Dim; ++i)
		byte_swap_in(a[i], boost::mpl::true_());
}

template<class T>
void byte_swap_in(T&, boost::mpl::false_)
{
	// do nothing
}

typedef boost::mpl::true_   ByteSwap_true;
typedef boost::mpl::false_  ByteSwap_false;

//////////////////////////////////////////////////////////////////////////
// DataIO_need_bswap
#if defined(BOOST_TYPEOF_NATIVE)
  #ifdef BOOST_TYPEOF_KEYWORD
    #define NARK_TYPEOF_KEYWORD BOOST_TYPEOF_KEYWORD
  #else
    #define NARK_TYPEOF_KEYWORD decltype
  #endif
  template<class T>
  struct WorkAround_typeof : T {};
  template<class T>
  ByteSwap_true Deduce_DataIO_need_bswap(T*);
  template<class T>
  struct DataIO_need_bswap :
    public WorkAround_typeof<
	  NARK_TYPEOF_KEYWORD(Deduce_DataIO_need_bswap((T*)NULL))
	> {};
#else
  template<class T>
  struct DataIO_need_bswap : public boost::mpl::true_ { };
#endif
#define DATA_IO_NEED_BYTE_SWAP(T, cbool) \
	template<> struct DataIO_need_bswap<T> : public boost::mpl::bool_<cbool> {};

DATA_IO_NEED_BYTE_SWAP(		    char, false)
DATA_IO_NEED_BYTE_SWAP(  signed char, false)
DATA_IO_NEED_BYTE_SWAP(unsigned char, false)
//DATA_IO_NEED_BYTE_SWAP(		   float, false)
//DATA_IO_NEED_BYTE_SWAP(		  double, false)
//DATA_IO_NEED_BYTE_SWAP(  long double, false)

template<class T1, class T2>
struct DataIO_need_bswap<std::pair<T1, T2> > : public
	boost::mpl::bool_<DataIO_need_bswap<T1>::value||DataIO_need_bswap<T1>::value>
{
};

template<class T1, class T2>
void byte_swap_in(std::pair<T1, T2>& x, typename DataIO_need_bswap<std::pair<T1,T2> >::type)
{
	byte_swap_in(x.first,  typename DataIO_need_bswap<T1>::type());
	byte_swap_in(x.second, typename DataIO_need_bswap<T2>::type());
}

template<class Bswap>
class ByteSwapChain;

// only boost::mpl::true_ is applied
template<> class ByteSwapChain<boost::mpl::true_>
{
public:
	template<class T> ByteSwapChain operator&(T& x) const
	{
	//	BOOST_STATIC_ASSERT(DataIO_need_bswap<T>::value);
		byte_swap_in(x, typename DataIO_need_bswap<T>::type());
		return *this;
	}
};

template<class T>
void byte_swap(T* p, size_t n)
{
	for (size_t i = n; i; --i, ++p)
		byte_swap_in(*p, boost::mpl::true_());
}

template<class Iter>
void byte_swap(Iter first, Iter last)
{
	for (; first != last; ++first)
		byte_swap_in(*first, boost::mpl::true_());
}

inline void byte_swap(char* /*buffer*/, size_t /*length*/) { }
inline void byte_swap(signed char* /*buffer*/, size_t /*length*/) { }
inline void byte_swap(unsigned char* /*buffer*/, size_t /*length*/) { }

} // namespace nark

//#include <boost/type_traits/detail/bool_trait_undef.hpp>

#endif // __nark_io_byte_swap_h__


