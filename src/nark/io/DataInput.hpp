/* vim: set tabstop=4 : */
#ifndef __nark_io_DataInput_h__
#define __nark_io_DataInput_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <string.h>
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
#include <boost/type_traits.hpp>
#include <boost/ref.hpp>

#if !defined(BOOST_BIG_ENDIAN) && !defined(BOOST_LITTLE_ENDIAN)
	#error must define byte endian
#endif

#include <nark/pass_by_value.hpp>
#include "byte_swap.hpp"
#include "DataIO_Basic.hpp"
#include "DataIO_Version.hpp"
#include "DataIO_Tuple.hpp"
#include "DataIO_Exception.hpp"
#include "var_int.hpp"
#include <nark/valvec.hpp>

#if !defined(BOOST_BIG_ENDIAN) && !defined(BOOST_LITTLE_ENDIAN)
# error must define byte endian
#endif

namespace nark {

template<class Input, class Class>
void DataIO_loadObject(Input& input, Class& x)
{
#ifdef DATA_IO_ALLOW_DEFAULT_SERIALIZE
	input.ensureRead(&x, sizeof(Class));
#else
	x.MustDefineCustomLoad(input);
#endif
}

////////////////////////////////////////////////////////////////////////////////
template<class DataIO, class T, class Vector>
void DataIO_load_vector_raw(DataIO& dio, T*, Vector& x, ByteSwap_false)
{
	dio.ensureRead(&*x.begin(), sizeof(T) * x.size());
}

template<class DataIO, class T, class Vector>
void DataIO_load_vector_raw(DataIO& dio, T*, Vector& x, ByteSwap_true)
{
	dio.ensureRead(&*x.begin(), sizeof(T) * x.size());
	byte_swap(&*x.begin(), x.size());
}

template<class DataIO, class T, class Vector, class Bswap>
void DataIO_load_vector_aux(DataIO& dio, T*, Vector& x, Bswap, IsDump_true)
{
	var_size_t size0;
   	dio >> size0;
	x.resize(size0.t);
	if (size0.t)
		DataIO_load_vector_raw(dio, (T*)NULL, x, Bswap());
}

template<class DataIO, class T, class Vector, class Bswap>
void DataIO_load_vector_aux(DataIO& dio, T*, Vector& x, Bswap, IsDump_false)
{
	var_size_t size0;
   	dio >> size0;
	x.resize(0);
	x.reserve(size0.t);
	const size_t n = size0.t;
	for (size_t i = 0; i < n; ++i) {
		T t;
	   	dio >> t;
	#if defined(HSM_HAS_MOVE)
		x.push_back(std::move(t));
	#else
		x.push_back(t);
	#endif
	}
}

template<class DataIO, class T1, class T2, class Vector, class Bswap>
void DataIO_load_vector(DataIO& dio, std::pair<T1,T2>*, Vector& x, Bswap)
{
	typedef std::pair<T1,T2> P;
	DataIO_load_vector_aux(dio, (P*)NULL, x, Bswap(), DataIO_is_dump<DataIO, P>());
}

template<class DataIO, class T, class Vector, class Bswap>
void DataIO_load_vector(DataIO& dio, T*, Vector& x, Bswap)
{
	DataIO_load_vector_aux(dio, (T*)NULL, x, Bswap(), DataIO_is_dump<DataIO, T>());
}

///////////////////////////////////////////////////////////////////////////////////

template<class DataIO, class T>
void DataIO_load_array_raw(DataIO& dio, T* a, size_t n, ByteSwap_false)
{
	dio.ensureRead(a, sizeof(T) * n);
}

template<class DataIO, class T>
void DataIO_load_array_raw(DataIO& dio, T* a, size_t n, ByteSwap_true)
{
	dio.ensureRead(a, sizeof(T) * n);
	byte_swap(a, n);
}

template<class DataIO, class T, class Bswap>
void DataIO_load_array_aux(DataIO& dio, T* a, size_t n, Bswap, IsDump_true)
{
	DataIO_load_array_raw(dio, a, n, Bswap());
}

template<class DataIO, class T, class Bswap>
void DataIO_load_array_aux(DataIO& dio, T* a, size_t n, Bswap, IsDump_false)
{
	for (size_t i = n; i; --i, ++a)
		dio >> *a;
}

template<class DataIO, class T1, class T2, class Bswap>
void DataIO_load_array(DataIO& dio, std::pair<T1,T2>* a, size_t n, Bswap)
{
	typedef std::pair<T1,T2> P;
	DataIO_load_array_aux(dio, a, n, Bswap(), DataIO_is_dump<DataIO, P>());
}

template<class DataIO, class T, class Bswap>
void DataIO_load_array(DataIO& dio, T* a, size_t n, Bswap)
{
	DataIO_load_array_aux(dio, a, n, Bswap(), DataIO_is_dump<DataIO, T>());
}

//////////////////////////////////////////////////////////////////////////
template<class DataIO, class T>
void DataIO_load_elem_raw(DataIO& dio, T& x, ByteSwap_false)
{
	dio.ensureRead(&x, sizeof(T));
}

template<class DataIO, class T>
void DataIO_load_elem_raw(DataIO& dio, T& x, ByteSwap_true)
{
	dio.ensureRead(&x, sizeof(T));
	byte_swap_in(x, ByteSwap_true());
}

template<class DataIO, class T, class Bswap>
void DataIO_load_elem_aux(DataIO& dio, T& x, Bswap, IsDump_true)
{
	DataIO_load_elem_raw(dio, x, Bswap());
}

template<class DataIO, class T, class Bswap>
void DataIO_load_elem_aux(DataIO& dio, T& x, Bswap, IsDump_false)
{
	DataIO_loadObject(dio, x);
}

template<class DataIO, class T, class Bswap>
void DataIO_load_elem(DataIO& dio, T& x, Bswap)
{
	DataIO_load_elem_aux(dio, x, Bswap(), DataIO_is_dump<DataIO, T>());
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

#define DATA_IO_GEN_DUMP_INPUT(Type)		\
	MyType& operator>>(Type& x)				\
	{	this->ensureRead(&x, sizeof(Type));	\
		return *this;						\
	}										\
	template<int Dim>						\
	MyType& operator>>(Type (&x)[Dim])		\
	{	this->ensureRead(x, sizeof(Type)*Dim);\
		return *this;						\
	}										\
	MyType& operator>>(valvec<Type>& x)		\
	{	var_size_t n;						\
		*this >> n;							\
		x.resize_no_init(n.t);				\
	    if (nark_likely(n.t))				\
		  this->ensureRead(&x[0], sizeof(Type)*n.t);\
		return *this; \
	} \
	template<class Alloc>					\
	MyType& operator>>(std::vector<Type, Alloc>& x)\
	{	var_size_t n;						\
		*this >> n;\
		x.resize(n.t);						\
	    if (nark_likely(n.t))				\
		  this->ensureRead(&x[0], sizeof(Type)*n.t);\
		return *this; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_GEN_BSWAP_INT_INPUT(Int)	\
	MyType& operator>>(Int& x) {			\
	  this->ensureRead(&x, sizeof(Int));	\
	  x = byte_swap(x);						\
	  return *this;							\
	}										\
	template<int Dim>						\
	MyType& operator>>(Int (&x)[Dim]) {		\
	  this->ensureRead(x, sizeof(Int)*Dim); \
	  byte_swap(x, Dim);					\
	  return *this;							\
	}										\
	MyType& operator>>(valvec<Int>& x) {	\
	  var_size_t n;							\
	  *this >> n;							\
	  x.resize(n.t);						\
	  if (nark_likely(n.t)) {				\
	    this->ensureRead(&x[0], sizeof(Int)*n.t);\
	    byte_swap(x.begin(), x.end());		\
	  }										\
	  return *this;							\
	}										\
	template<class Alloc>					\
	MyType& operator>>(std::vector<Int, Alloc>& x) {\
	  var_size_t n;							\
	  *this >> n;							\
	  x.resize(n.t);						\
	  if (nark_likely(n.t)) {				\
	    this->ensureRead(&x[0], sizeof(Int)*n.t);\
	    byte_swap(x.begin(), x.end());		\
	  }										\
	  return *this; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#ifdef BOOST_LITTLE_ENDIAN
  #define DATA_IO_GEN_BIG_ENDIAN_INT_INPUT    DATA_IO_GEN_BSWAP_INT_INPUT
  #define DATA_IO_GEN_LITTLE_ENDIAN_INT_INPUT DATA_IO_GEN_DUMP_INPUT
#elif defined(BOOST_BIG_ENDIAN)
  #define DATA_IO_GEN_BIG_ENDIAN_INT_INPUT    DATA_IO_GEN_DUMP_INPUT
  #define DATA_IO_GEN_LITTLE_ENDIAN_INT_INPUT DATA_IO_GEN_BSWAP_INT_INPUT
#else
  #error "must define BOOST_LITTLE_ENDIAN or BOOST_BIG_ENDIAN"
#endif
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define NARK_DataInput_NestStreamPtr(DataInput) \
private: \
  StreamT* m_stream; \
public: \
  typedef StreamT stream_t; \
  StreamT* getStream() { return m_stream; } \
  explicit DataInput(StreamT* stream) { m_stream = stream; } \
  size_t read(void* data, size_t len) { return m_stream->read(data, len); } \
  void ensureRead(void* data, size_t len) { m_stream->ensureRead(data, len); }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

template<class StreamT>
class LittleEndianDataInput : public StreamT {
public:
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef LittleEndianDataInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_LittleEndian.hpp"
#include "DataInput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class LittleEndianDataInput<StreamT*> {
	NARK_DataInput_NestStreamPtr(LittleEndianDataInput)
typedef LittleEndianDataInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_LittleEndian.hpp"
#include "DataInput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class BigEndianDataInput : public StreamT {
public:
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef BigEndianDataInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_BigEndian.hpp"
#include "DataInput_VarIntAsVarLen.hpp"
};

template<class StreamT>
class BigEndianDataInput<StreamT*> {
	NARK_DataInput_NestStreamPtr(BigEndianDataInput)
typedef BigEndianDataInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_BigEndian.hpp"
#include "DataInput_VarIntAsVarLen.hpp"
};

//////////////////////////////////////////////////////////////////////////
template<class StreamT>
class LittleEndianNoVarIntInput : public StreamT {
public:
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef LittleEndianNoVarIntInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_LittleEndian.hpp"
#include "DataInput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class LittleEndianNoVarIntInput<StreamT*> {
	NARK_DataInput_NestStreamPtr(LittleEndianNoVarIntInput)
typedef LittleEndianNoVarIntInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_LittleEndian.hpp"
#include "DataInput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class BigEndianNoVarIntInput : public StreamT {
public:
	typedef StreamT stream_t;
	StreamT* getStream() { return this; }
typedef BigEndianNoVarIntInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_BigEndian.hpp"
#include "DataInput_VarIntAsFixLen.hpp"
};

template<class StreamT>
class BigEndianNoVarIntInput<StreamT*> {
	NARK_DataInput_NestStreamPtr(BigEndianNoVarIntInput)
typedef BigEndianNoVarIntInput MyType;
#include "DataInput_Basic.hpp"
#include "DataInput_String.hpp"
#include "DataInput_BigEndian.hpp"
#include "DataInput_VarIntAsFixLen.hpp"
};

#define PortableDataInput		BigEndianDataInput
#define PortableNoVarIntInput	BigEndianNoVarIntInput

//////////////////////////////////////////////////////////////////////////

//! call Class::dio_load(Input, version)
template<class Input>
unsigned int DataIO_load_check_version(Input& in, unsigned int curr_version, const char* className)
{
	serialize_version_t loaded_version(0);
	in >> loaded_version;
	if (nark_unlikely(loaded_version.t > curr_version))
	{
// 		if (0 == className)
// 			className = typeid(Class).name();
		throw BadVersionException(loaded_version.t, curr_version, className);
	}
	return loaded_version.t;
}

#define DATA_IO_REG_LOAD(Class) \
  template<class Input> friend void DataIO_loadObject(Input& in, Class& x) { x.dio_load(in); }

#define DATA_IO_REG_LOAD_V(Class, CurrentVersion)			\
	template<class Input>									\
	friend void DataIO_loadObject(Input& in, Class& x)		\
	{														\
		using namespace nark;								\
		x.dio_load(in, DataIO_load_check_version(				\
			in, CurrentVersion, BOOST_STRINGIZE(Class)));	\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_DISABLE_LOAD(Class) \
  template<class DataIO>		\
  friend void DataIO_loadObject(DataIO& dio, Class& x) { dio.DisableLoadClass(x); }

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#if defined(NARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE)
#define DATA_IO_OPTIMIZE_VECTOR_LOAD(Class, Members)
#define DATA_IO_OPTIMIZE_ELEMEN_LOAD(Class, Members)
#define DATA_IO_OPTIMIZE_ARRAY__LOAD(Class, Members)
#define DATA_IO_OPTIMIZE_VECTOR_LOAD_REG(Friend, Self, Class)
#define DATA_IO_OPTIMIZE_ELEMEN_LOAD_REG(Friend, Self, Class)
#define DATA_IO_OPTIMIZE_ARRAY__LOAD_REG(Friend, Self, Class)
#else
#define DATA_IO_OPTIMIZE_VECTOR_LOAD(Class, Members)\
	template<class DataIO, class Vector, class Bswap>\
	void load_vector(DataIO& aDataIO, Vector& _vector_, Bswap) \
	{												\
		using namespace nark;						\
		DataIO_load_vector_aux(aDataIO,				\
			(Class*)NULL, _vector_, Bswap(),		\
		(DataIO_is_realdump<DataIO, Class, 0, true> \
		  (this) Members).is_dumpable());			\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// for DATA_IO_LOAD_SAVE_E, DataIO_load_vector should be defined
// in namespace, not in 'Derived' class
#define DATA_IO_OPTIMIZE_VECTOR_LOAD_REG(Friend, Derived, Class)\
	template<class DataIO, class Vector, class Bswap>\
	Friend void DataIO_load_vector					\
	(DataIO& dio, Class*, Vector& x, Bswap)			\
	{												\
		((Derived*)0)->load_vector(dio, x, Bswap());\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_OPTIMIZE_ARRAY__LOAD(Class, Members)\
	template<class DataIO, class Bswap>				\
	void load_array(DataIO& aDataIO,				\
		Class* _vector_, size_t _N_count,	Bswap)	\
	{												\
		using namespace nark;						\
	  DataIO_load_array_aux(aDataIO,				\
		_vector_, _N_count, Bswap(),				\
		(DataIO_is_realdump<DataIO,Class,0,true>	\
		  (this) Members).is_dumpable());			\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define DATA_IO_OPTIMIZE_ARRAY__LOAD_REG(Friend, Derived, Class) \
	template<class DataIO, class Bswap>				\
	Friend void DataIO_load_array					\
	(DataIO& dio, Class* a, size_t n, Bswap)		\
	{												\
	  ((Derived*)0)->load_array(dio, a, n, Bswap());\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define DATA_IO_OPTIMIZE_ELEMEN_LOAD(Class, Members)\
	template<class Bswap>							\
	void _M_byte_swap_in(Bswap)						\
	{												\
	   	nark::ByteSwapChain<Bswap>()Members;		\
	}												\
	template<class DataIO, class Bswap>				\
	void opt_load(DataIO& aDataIO, Bswap)			\
	{												\
		using namespace nark;						\
	  DataIO_load_elem_aux(aDataIO,					\
		static_cast<Class&>(*this), Bswap(),		\
		(DataIO_is_realdump<DataIO,Class,0,true>	\
		  (this) Members).is_dumpable() );			\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define DATA_IO_OPTIMIZE_ELEMEN_LOAD_REG(Friend, Self, Class)\
	template<class Bswap>							\
	Friend void byte_swap_in(Class& x, Bswap)		\
	{												\
		Self(x)._M_byte_swap_in(Bswap());			\
	}												\
	template<class DataIO, class Bswap>				\
	Friend void										\
	DataIO_load_elem(DataIO& dio, Class& x, Bswap)	\
	{												\
		Self(x).opt_load(dio, Bswap());				\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#endif // NARK_DATA_IO_DISABLE_OPTIMIZE_DUMPABLE

#ifdef BOOST_LITTLE_ENDIAN
	#define NativeDataInput     LittleEndianDataInput
	#define NativeNoVarIntInput LittleEndianNoVarIntInput
#elif defined(BOOST_BIG_ENDIAN)
	#define NativeDataInput     BigEndianDataInput
	#define NativeNoVarIntInput BigEndianNoVarIntInput
#else
	#error "must define BOOST_LITTLE_ENDIAN or BOOST_BIG_ENDIAN"
#endif

}

#endif // __nark_io_DataInput_h__

