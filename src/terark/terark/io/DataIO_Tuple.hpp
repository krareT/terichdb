/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_Tuple_h__
#define __terark_io_DataIO_Tuple_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <boost/tuple/tuple.hpp>
#include <boost/type_traits.hpp>
#include <boost/static_assert.hpp>

namespace terark {
//////////////////////////////////////////////////////////////////////////
//! boost::tuple io

template <class DataIO,
		  class T0, class T1, class T2, class T3, class T4,
          class T5, class T6, class T7, class T8, class T9>
inline void DataIO_loadObject(DataIO& dio, boost::tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>& t)
{
	typedef typename boost::tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>::inherited cons_t;
	DataIO_loadObject(dio, static_cast<cons_t&>(t));
}

template<class DataIO, class T1>
inline void DataIO_loadObject(DataIO& dio, boost::tuples::cons<T1, boost::tuples::null_type>& t)
{
  dio >> t.head;
}

template<class DataIO, class T1>
inline void DataIO_loadObject(DataIO&, boost::tuples::null_type&)
{ }

template<class DataIO, class T1, class T2>
inline void DataIO_loadObject(DataIO& dio, boost::tuples::cons<T1, T2>& t)
{
  dio >> t.head;

#if defined(BOOST_NO_TEMPLATE_PARTIAL_SPECIALIZATION)
  if (boost::tuples::length<T2>::value == 0)
    return dio;
#endif  // BOOST_NO_TEMPLATE_PARTIAL_SPECIALIZATION

  DataIO_loadObject(dio, t.tail);
}

//---------------------------------------------------------------------------------------------------

template <class DataIO,
		  class T0, class T1, class T2, class T3, class T4,
          class T5, class T6, class T7, class T8, class T9>
inline void DataIO_saveObject(DataIO& dio, const boost::tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>& t)
{
	typedef typename boost::tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>::inherited cons_t;
	DataIO_saveObject(dio, static_cast<const cons_t&>(t));
}

template<class DataIO, class T1>
inline void DataIO_saveObject(DataIO& dio, const boost::tuples::cons<T1, boost::tuples::null_type>& t)
{
  dio << t.head;
}

template<class DataIO, class T1>
inline void DataIO_saveObject(DataIO&, const boost::tuples::null_type&)
{  }

template<class DataIO, class T1, class T2>
inline void DataIO_saveObject(DataIO& dio, const boost::tuples::cons<T1, T2>& t)
{
  dio << t.head;

#if defined(BOOST_NO_TEMPLATE_PARTIAL_SPECIALIZATION)
  if (boost::tuples::length<T2>::value == 0)
    return;
#endif  // BOOST_NO_TEMPLATE_PARTIAL_SPECIALIZATION

  DataIO_saveObject(dio, t.tail);
}


} // namespace terark

#endif // __terark_io_DataIO_Tuple_h__
