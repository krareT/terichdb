/* vim: set tabstop=4 : */
#ifndef __terark_io_byte_swap_h__
#define __terark_io_byte_swap_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4127)
#endif

#include <terark/util/byte_swap_impl.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/static_assert.hpp>

namespace terark {

inline char MplBoolTrueToSizeOne(boost::mpl::true_);
inline long MplBoolTrueToSizeOne(boost::mpl::false_);

template<class T> T& DataIO_ReturnObjRef(T*);

struct DummyDataIO {};

// DataIO is just used for workarond <<incomplete type>> compilation errors
template<class DataIO, class T>
boost::mpl::true_ Deduce_DataIO_need_bswap(DataIO*, T&);

#define DataIO_need_bswap_by_sizeof(DataIO, T) \
  ( sizeof(MplBoolTrueToSizeOne(Deduce_DataIO_need_bswap( \
    (DataIO*)(NULL), DataIO_ReturnObjRef((T*)(NULL))))) == 1 )

template<class D>boost::mpl::false_ Deduce_DataIO_need_bswap(D*,char&);
template<class D>boost::mpl::false_ Deduce_DataIO_need_bswap(D*,signed char&);
template<class D>boost::mpl::false_ Deduce_DataIO_need_bswap(D*,unsigned char&);

template<class DataIO, class T1, class T2>
boost::mpl::bool_<
  DataIO_need_bswap_by_sizeof(DataIO, T1) ||
  DataIO_need_bswap_by_sizeof(DataIO, T2)
>
Deduce_DataIO_need_bswap(DataIO*, std::pair<T1, T2>&);

template<class DataIO, class T, size_t Dim>
boost::mpl::bool_<DataIO_need_bswap_by_sizeof(DataIO, T)>
Deduce_DataIO_need_bswap(DataIO*, T (&)[Dim]);

template<class T>
struct DataIO_need_bswap :
	public boost::mpl::bool_<DataIO_need_bswap_by_sizeof(DummyDataIO, T)>
{};

// for Deduce_DataIO_need_bswap in DATA_IO_LOAD_SAVE
template<class DataIO, bool CurrBswap>
struct DataIO_need_bswap_class {
	DataIO_need_bswap_class(){}

	typedef boost::mpl::bool_<CurrBswap> need_bswap_t;
	need_bswap_t need_bswap() const { return need_bswap_t(); }

	template<class T>
	DataIO_need_bswap_class<
		DataIO,
		CurrBswap || DataIO_need_bswap_by_sizeof(DataIO, T)
	>
	operator&(const T&) const;
};

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
template<class T1, class T2>
void byte_swap_in(std::pair<T1, T2>& x,
				  boost::mpl::bool_<
					DataIO_need_bswap_by_sizeof(DummyDataIO, T1) ||
					DataIO_need_bswap_by_sizeof(DummyDataIO, T2) > )
{
	byte_swap_in(x.first,  boost::mpl::bool_<DataIO_need_bswap_by_sizeof(DummyDataIO, T1)>());
	byte_swap_in(x.second, boost::mpl::bool_<DataIO_need_bswap_by_sizeof(DummyDataIO, T2)>());
}

template<class Bswap>
class ByteSwapChain;

// only boost::mpl::true_ is applied
template<> class ByteSwapChain<boost::mpl::true_>
{
public:
	template<class T> ByteSwapChain operator&(T& x) const
	{
		byte_swap_in(x, boost::mpl::bool_<DataIO_need_bswap_by_sizeof(DummyDataIO, T)>());
		return *this;
	}
};

template<class T>
void byte_swap(T* p, size_t n)
{
	if (Deduce_DataIO_need_bswap((DummyDataIO*)NULL, *p).value) {
		for (size_t i = n; i; --i, ++p)
			byte_swap_in(*p, boost::mpl::true_());
	}
}

template<class Iter>
void byte_swap(Iter first, Iter last)
{
	if (Deduce_DataIO_need_bswap((DummyDataIO*)NULL, *first).value) {
		for (; first != last; ++first)
			byte_swap_in(*first, boost::mpl::true_());
	}
}

inline void byte_swap(char* /*buffer*/, size_t /*length*/) { }
inline void byte_swap(signed char* /*buffer*/, size_t /*length*/) { }
inline void byte_swap(unsigned char* /*buffer*/, size_t /*length*/) { }

} // namespace terark

//#include <boost/type_traits/detail/bool_trait_undef.hpp>

#endif // __terark_io_byte_swap_h__


