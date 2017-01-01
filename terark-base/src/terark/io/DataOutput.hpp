/* vim: set tabstop=4 : */
#ifndef __terark_io_DataOutput_h__
#define __terark_io_DataOutput_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <string.h> // for strlen,wcslen
//#include <typeinfo>
//#include <stdexcept>

#include <string>
#include <deque>
#include <list>
#include <map>
#include <set>
#include <vector>

#include <boost/version.hpp>

#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
# include <boost/detail/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif
#include <boost/cstdint.hpp>
#include <terark/util/function.hpp> // for reference_wrapper

#if !defined(BOOST_BIG_ENDIAN) && !defined(BOOST_LITTLE_ENDIAN)
	#error must define byte endian
#endif

#include <terark/pass_by_value.hpp>
#include "byte_swap.hpp"
#include "DataIO_Basic.hpp"
#include "DataIO_Version.hpp"
#include "DataIO_Tuple.hpp"
#include "var_int.hpp"
#include "IOException.hpp"
#include <terark/valvec.hpp>

#if !defined(BOOST_BIG_ENDIAN) && !defined(BOOST_LITTLE_ENDIAN)
	#error must define byte endian
#endif

namespace terark {

template<class Output, class T>
void DataIO_saveObject(Output& output, const T& x)
{
#ifdef DATA_IO_ALLOW_DEFAULT_SERIALIZE
	output.ensureWrite(&x, sizeof(x));
#else
	x.MustDefineCustomSave(output);
#endif
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
template<class DataIO, class T, class Vector>
void
DataIO_save_vector_raw(DataIO& dio, T*, const Vector& x, ByteSwap_false)
{
	if (!x.empty())
		dio.ensureWrite(&*x.begin(), sizeof(T) * x.size());
}

template<class DataIO, class T, class Vector>
void
DataIO_save_vector_raw(DataIO& dio, T*, const Vector& x, ByteSwap_true)
{
	typename Vector::const_iterator i = x.begin(), End = x.end();
	for ( ; i != End; ++i)
	{
		T e(*i);
		byte_swap_in(e, ByteSwap_true());
		dio.ensureWrite(&e, sizeof(T));
	}
}

template<class DataIO, class T, class Vector, class Bswap>
void
DataIO_save_vector_aux(DataIO& dio, T*, const Vector& x, Bswap, IsDump_true)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	dio << var_size_t(x.size());
	DataIO_save_vector_raw(dio, (T*)NULL, x, Bswap());
}

template<class DataIO, class T, class Vector, class Bswap>
void
DataIO_save_vector_aux(DataIO& dio, T*, const Vector& x, Bswap, IsDump_false)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	dio << var_size_t(x.size());
	typename Vector::const_iterator i = x.begin(), End = x.end();
	for ( ; i != End; ++i)
	{
		dio << *i;
	}
}

template<class DataIO, class T1, class T2, class Vector, class Bswap>
void DataIO_save_vector(DataIO& dio, std::pair<T1,T2>*, const Vector& x, Bswap)
{
	typedef std::pair<T1,T2> P;
	DataIO_save_vector_aux(dio, (P*)NULL, x, Bswap(), DataIO_is_dump<DataIO, P>());
}

template<class DataIO, class T, class Vector, class Bswap>
void DataIO_save_vector(DataIO& dio, T*, const Vector& x, Bswap)
{
	DataIO_save_vector_aux(dio, (T*)NULL, x, Bswap(), DataIO_is_dump<DataIO, T>());
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

template<class DataIO, class T>
void DataIO_save_array_raw(DataIO& dio, const T* a, size_t n, ByteSwap_false)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	dio.ensureWrite(a, sizeof(T) * n);
}

template<class DataIO, class T>
void DataIO_save_array_raw(DataIO& dio, const T* a, size_t n, ByteSwap_true)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	for (size_t i = n; i; --i, ++a)
	{
		T e(*a);
		byte_swap_in(e, ByteSwap_true());
		dio.ensureWrite(&e, sizeof(T));
	}
}

template<class DataIO, class T, class Bswap>
void DataIO_save_array_aux(DataIO& dio, const T* a, size_t n, Bswap, IsDump_true)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	DataIO_save_array_raw(dio, a, n, Bswap());
}

template<class DataIO, class T, class Bswap>
void DataIO_save_array_aux(DataIO& dio, const T* a, size_t n, Bswap, IsDump_false)
{
//	printf("%s\n", BOOST_CURRENT_FUNCTION);
	for (size_t i = n; i; --i, ++a)
		dio << *a;
}

template<class DataIO, class T1, class T2, class Bswap>
void DataIO_save_array(DataIO& dio, const std::pair<T1,T2>* a, size_t n, Bswap)
{
	typedef std::pair<T1,T2> P;
	DataIO_save_array_aux(dio, a, n, Bswap(), DataIO_is_dump<DataIO, P>());
}

template<class DataIO, class T, class Bswap>
void DataIO_save_array(DataIO& dio, const T* a, size_t n, Bswap)
{
	DataIO_save_array_aux(dio, a, n, Bswap(), DataIO_is_dump<DataIO, T>());
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

template<class DataIO, class T>
void DataIO_save_elem_raw(DataIO& dio, const T& x, ByteSwap_false)
{
	dio.ensureWrite(&x, sizeof(T));
}

template<class DataIO, class T>
void DataIO_save_elem_raw(DataIO& dio, const T& x, ByteSwap_true)
{
	T e(x);
	byte_swap_in(e, ByteSwap_true());
	dio.ensureWrite(&e, sizeof(T));
}

template<class DataIO, class T, class Bswap>
void DataIO_save_elem_aux(DataIO& dio, const T& x, Bswap, IsDump_true)
{
	DataIO_save_elem_raw(dio, x, Bswap());
}

template<class DataIO, class T, class Bswap>
void DataIO_save_elem_aux(DataIO& dio, const T& x, Bswap, IsDump_false)
{
	DataIO_saveObject(dio, x);
}

template<class DataIO, class T, class Bswap>
void DataIO_save_elem(DataIO& dio, const T& x, Bswap)
{
	DataIO_save_elem_aux(dio, x, Bswap(), DataIO_is_dump<DataIO, T>());
}

//////////////////////////////////////////////////////////////////////////

#define DATA_IO_GEN_DUMP_OUTPUT(Type)		\
	MyType& operator<<(Type x) {			\
		this->ensureWrite(&x, sizeof(Type));\
		return *this;						\
	}										\
	template<int Dim>						\
	MyType& operator<<(const Type (&x)[Dim]) { \
		this->ensureWrite(x, sizeof(Type)*Dim);\
		return *this;						\
	}										\
	template<class Alloc>					\
	MyType& operator<<(const std::vector<Type, Alloc>& x) {\
		*this << var_size_t(x.size());		\
		if (terark_likely(!x.empty()))		\
		  this->ensureWrite(&x[0], sizeof(Type)*x.size());\
	return *this; \
}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_GEN_BSWAP_INT_OUTPUT(Int)\
	MyType& operator<<(Int x) {		\
		x = byte_swap(x);					\
		this->ensureWrite(&x, sizeof(Int));	\
		return *this; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#ifdef BOOST_LITTLE_ENDIAN
  #define DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT    DATA_IO_GEN_BSWAP_INT_OUTPUT
  #define DATA_IO_GEN_LITTLE_ENDIAN_INT_OUTPUT DATA_IO_GEN_DUMP_OUTPUT
#elif defined(BOOST_BIG_ENDIAN)
  #define DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT    DATA_IO_GEN_DUMP_OUTPUT
  #define DATA_IO_GEN_LITTLE_ENDIAN_INT_OUTPUT DATA_IO_GEN_BSWAP_INT_OUTPUT
#else
  #error "must define BOOST_LITTLE_ENDIAN or BOOST_BIG_ENDIAN"
#endif
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define TERARK_DataOutput_NestStreamPtr(DataOutput) \
private: \
  StreamT* m_stream; \
public: \
  typedef StreamT stream_t; \
  StreamT* getStream() { return m_stream; } \
  explicit DataOutput(StreamT* stream) { m_stream = stream; } \
  void flush() { m_stream->flush(); } \
  void writeByte(byte b) { return m_stream->writeByte(b); } \
  size_t write(const void* data, size_t len) { return m_stream->write(data, len); } \
  void ensureWrite(const void* data, size_t len) { m_stream->ensureWrite(data, len); }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

template<class StreamT>
class LittleEndianDataOutput : public StreamT {
public:
#if (defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L) && \
	(defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4008) || \
    defined(__cpp_inheriting_constructors) || \
	defined(_MSC_VER) && _MSC_VER >= 1800
	using StreamT::StreamT;
#endif
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef LittleEndianDataOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_LittleEndian.hpp"
#include "DataOutput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class LittleEndianDataOutput<StreamT*> {
	TERARK_DataOutput_NestStreamPtr(LittleEndianDataOutput)
typedef LittleEndianDataOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_LittleEndian.hpp"
#include "DataOutput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class BigEndianDataOutput : public StreamT {
public:
#if (defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L) && \
	(defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4008) || \
    defined(__cpp_inheriting_constructors) || \
	defined(_MSC_VER) && _MSC_VER >= 1800
	using StreamT::StreamT;
#endif
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef BigEndianDataOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_BigEndian.hpp"
#include "DataOutput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class BigEndianDataOutput<StreamT*> {
	TERARK_DataOutput_NestStreamPtr(BigEndianDataOutput)
typedef BigEndianDataOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_BigEndian.hpp"
#include "DataOutput_VarIntAsVarLen.hpp"
};

//////////////////////////////////////////////////////////////////////////
template<class StreamT>
class LittleEndianNoVarIntOutput : public StreamT {
public:
#if (defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L) && \
	(defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4008) || \
    defined(__cpp_inheriting_constructors) || \
	defined(_MSC_VER) && _MSC_VER >= 1800
	using StreamT::StreamT;
#endif
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef LittleEndianNoVarIntOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_LittleEndian.hpp"
#include "DataOutput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class LittleEndianNoVarIntOutput<StreamT*> {
	TERARK_DataOutput_NestStreamPtr(LittleEndianNoVarIntOutput)
typedef LittleEndianNoVarIntOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_LittleEndian.hpp"
#include "DataOutput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class BigEndianNoVarIntOutput : public StreamT {
public:
#if (defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L) && \
	(defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4008) || \
    defined(__cpp_inheriting_constructors) || \
	defined(_MSC_VER) && _MSC_VER >= 1800
	using StreamT::StreamT;
#endif
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef BigEndianNoVarIntOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_BigEndian.hpp"
#include "DataOutput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class BigEndianNoVarIntOutput<StreamT*> {
	TERARK_DataOutput_NestStreamPtr(BigEndianNoVarIntOutput)
typedef BigEndianNoVarIntOutput MyType;
#include "DataOutput_Basic.hpp"
#include "DataOutput_String.hpp"
#include "DataOutput_BigEndian.hpp"
#include "DataOutput_VarIntAsFixLen.hpp"
};

#define PortableDataOutput		BigEndianDataOutput
#define PortableNoVarIntOutput	BigEndianNoVarIntOutput

//////////////////////////////////////////////////////////////////////////

template<class Output, class FirstT, class SecondT>
void DataIO_saveObject(Output& output, const std::pair<FirstT, SecondT>& x)
{
	output << x.first << x.second;
}

#define DATA_IO_REG_SAVE(Class) \
  template<class Output> friend \
  void DataIO_saveObject(Output& output, const Class& x) { x.dio_save(output); }

#define DATA_IO_REG_SAVE_V(Class, CurrentVersion)	\
	template<class Output>										\
	friend void DataIO_saveObject(Output& out, const Class& x)	\
	{															\
		out << terark::serialize_version_t(CurrentVersion);		\
		x.dio_save(out, CurrentVersion);						\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_DISABLE_SAVE(Class) \
  template<class DataIO> friend void \
  DataIO_saveObject(DataIO& dio, const Class& x) { dio.DisableSaveClass(x); }

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#ifdef TERARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE

#define DATA_IO_OPTIMIZE_VECTOR_SAVE(Class, Members)
#define DATA_IO_OPTIMIZE_ELEMEN_SAVE(Class, Members)
#define DATA_IO_OPTIMIZE_ARRAY__SAVE(Class, Members)
#define DATA_IO_OPTIMIZE_VECTOR_SAVE_REG(Self, Class, Members)
#define DATA_IO_OPTIMIZE_ELEMEN_SAVE_REG(Self, Class, Members)
#define DATA_IO_OPTIMIZE_ARRAY__SAVE_REG(Self, Class, Members)

#else

//! 宏中的成员函数使用较长的参数名，目的是避免和 Members 中的某个成员同名
//!
#define DATA_IO_OPTIMIZE_VECTOR_SAVE(Class, Members)\
	template<class DataIO, class Vector, class Bswap>\
	void save_vector								\
	(DataIO& aDataIO, const Vector& _vector_, Bswap)\
	{												\
		using namespace terark;						\
		DataIO_save_vector_aux(						\
		  aDataIO, (Class*)NULL, _vector_, Bswap(),	\
		  (DataIO_is_realdump<DataIO,Class,0,true>	\
		    (this) Members).is_dumpable());			\
	}
#define DATA_IO_OPTIMIZE_VECTOR_SAVE_REG(Friend, Derived, Class)\
	template<class DataIO, class Vector, class Bswap>\
	Friend void DataIO_save_vector					\
	(DataIO& dio, Class*, const Vector& x, Bswap)	\
	{												\
	   ((Derived*)0)->save_vector(dio, x, Bswap()); \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_OPTIMIZE_ARRAY__SAVE(Class, Members)\
	template<class DataIO, class Bswap>				\
	void save_array(DataIO& aDataIO					\
		, const Class* _vector_, size_t _N_count	\
		, Bswap)									\
	{												\
		using namespace terark;						\
		DataIO_save_array_aux(						\
			aDataIO, _vector_, _N_count, Bswap(),	\
		(DataIO_is_realdump<DataIO,Class,0,true>	\
		 (this) Members).is_dumpable());			\
	}
#define DATA_IO_OPTIMIZE_ARRAY__SAVE_REG(Friend, Derived, Class) \
	template<class DataIO, class Bswap>				\
	Friend void DataIO_save_array					\
	(DataIO& dio, const Class* a, size_t n, Bswap)	\
	{												\
		((Derived*)0)->save_array(dio, a, n, Bswap());\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_OPTIMIZE_ELEMEN_SAVE(Class, Members)\
	template<class DataIO, class Bswap>				\
	void opt_save(DataIO& aDataIO, Bswap) const		\
	{												\
		using namespace terark;						\
		DataIO_save_elem_aux(aDataIO,				\
		  static_cast<const Class&>(*this), Bswap(),\
			(DataIO_is_realdump<DataIO,Class,0,true>\
			  (this) Members).is_dumpable());		\
	}
#define DATA_IO_OPTIMIZE_ELEMEN_SAVE_REG(Friend, Self, Class)\
	template<class DataIO, class Bswap>	 \
	Friend void DataIO_save_elem		 \
	(DataIO& dio, const Class& x, Bswap) \
	{									 \
		Self(x).opt_save(dio, Bswap());	 \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#endif // TERARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE

#ifdef BOOST_LITTLE_ENDIAN
	#define NativeDataOutput LittleEndianDataOutput
	#define NativeNoVarIntOutput LittleEndianNoVarIntOutput
#elif defined(BOOST_BIG_ENDIAN)
	#define NativeDataOutput BigEndianDataOutput
	#define NativeNoVarIntOutput BigEndianNoVarIntOutput
#else
	#error "must define BOOST_LITTLE_ENDIAN or BOOST_BIG_ENDIAN"
#endif

} // namespace terark


#endif // __terark_io_DataOutput_h__

