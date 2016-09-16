/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_h__
#define __terark_io_DataIO_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "DataInput.hpp"
#include "DataOutput.hpp"

#ifndef BOOST_INTRUSIVE_PTR_HPP_INCLUDED
#  include <boost/intrusive_ptr.hpp>
#endif
#ifndef BOOST_SCOPED_PTR_HPP_INCLUDED
#  include <boost/scoped_ptr.hpp>
#endif
#ifndef BOOST_SHARED_PTR_HPP_INCLUDED
#  include <boost/shared_ptr.hpp>
#endif

namespace terark {

//////////////////////////////////////////////////////////////////////////

DataIO_IsDump_TypeTrue2(LittleEndianNoVarInt, var_uint32_t)
DataIO_IsDump_TypeTrue2(LittleEndianNoVarInt, var_uint64_t)
DataIO_IsDump_TypeTrue2(LittleEndianNoVarInt, var_int32_t)
DataIO_IsDump_TypeTrue2(LittleEndianNoVarInt, var_int64_t)

DataIO_IsDump_TypeTrue2(PortableNoVarInt, var_uint32_t)
DataIO_IsDump_TypeTrue2(PortableNoVarInt, var_uint64_t)
DataIO_IsDump_TypeTrue2(PortableNoVarInt, var_int32_t)
DataIO_IsDump_TypeTrue2(PortableNoVarInt, var_int64_t)

// use template, only when use the member, it will instantiate the template
// this trick will avoid 'no default construct' compile error
#if 0
#define DATA_IO_DYNA_CREATE(Class) \
	template<class Dummy> static Class* newInstance() { return new Class; }
#else
#define DATA_IO_DYNA_CREATE(Class)
#endif

/**
 @{
 @brief 同时声明 load/save
 */
#define DATA_IO_REG_LOAD_SAVE(Class) \
	DATA_IO_DYNA_CREATE(Class)		\
	DATA_IO_REG_LOAD(Class)	\
	DATA_IO_REG_SAVE(Class)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define DATA_IO_REG_LOAD_SAVE_V(Class, CurrentVersion) \
	DATA_IO_DYNA_CREATE(Class)		\
	DATA_IO_REG_LOAD_V(Class, CurrentVersion)	\
	DATA_IO_REG_SAVE_V(Class, CurrentVersion)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//@}
#define DATA_IO_IDENTITY(x) x
//!@{
//! can be used by code generator!!
//
#define DATA_IO_LOAD_SAVE(Class, Members)	\
	template<class DataIO> void dio_load(DataIO& aDataIO)	    { aDataIO Members; }\
	template<class DataIO> void dio_save(DataIO& aDataIO) const { aDataIO Members; }\
	DATA_IO_GEN_DUMP_TYPE_TRAITS(Class, Members) \
	DATA_IO_OPTIMIZE_ELEMEN_LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_ELEMEN_SAVE(Class, Members) \
	DATA_IO_OPTIMIZE_VECTOR_LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_VECTOR_SAVE(Class, Members) \
	DATA_IO_OPTIMIZE_ARRAY__LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_ARRAY__SAVE(Class, Members) \
	DATA_IO_GEN_DUMP_TYPE_TRAITS_REG(friend, Class, Class) \
	DATA_IO_OPTIMIZE_ELEMEN_LOAD_REG(friend, DATA_IO_IDENTITY, Class) \
	DATA_IO_OPTIMIZE_ELEMEN_SAVE_REG(friend, DATA_IO_IDENTITY, Class) \
	DATA_IO_OPTIMIZE_VECTOR_LOAD_REG(friend, Class, Class) \
	DATA_IO_OPTIMIZE_VECTOR_SAVE_REG(friend, Class, Class) \
	DATA_IO_OPTIMIZE_ARRAY__LOAD_REG(friend, Class, Class) \
	DATA_IO_OPTIMIZE_ARRAY__SAVE_REG(friend, Class, Class) \
	DATA_IO_REG_LOAD(Class) \
	DATA_IO_REG_SAVE(Class)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//@}

//!@{
// should be declared in the same namespace as Class
//
#define DATA_IO_LOAD_SAVE_E(Class, Members)			\
  struct Class##_fdio : public Class				\
  {													\
	template<class DataIO> void dio_load(DataIO& aDataIO)	    { aDataIO Members; }\
	template<class DataIO> void dio_save(DataIO& aDataIO) const { aDataIO Members; }\
	DATA_IO_GEN_DUMP_TYPE_TRAITS(Class, Members) \
	DATA_IO_OPTIMIZE_ELEMEN_LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_ELEMEN_SAVE(Class, Members) \
	DATA_IO_OPTIMIZE_VECTOR_LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_VECTOR_SAVE(Class, Members) \
	DATA_IO_OPTIMIZE_ARRAY__LOAD(Class, Members) \
	DATA_IO_OPTIMIZE_ARRAY__SAVE(Class, Members) \
  }; \
	DATA_IO_GEN_DUMP_TYPE_TRAITS_REG(inline, Class##_fdio, Class) \
	DATA_IO_OPTIMIZE_ELEMEN_LOAD_REG(inline, static_cast<      Class##_fdio&>, Class)\
	DATA_IO_OPTIMIZE_ELEMEN_SAVE_REG(inline, static_cast<const Class##_fdio&>, Class)\
	DATA_IO_OPTIMIZE_VECTOR_LOAD_REG(inline, Class##_fdio, Class) \
	DATA_IO_OPTIMIZE_VECTOR_SAVE_REG(inline, Class##_fdio, Class) \
	DATA_IO_OPTIMIZE_ARRAY__LOAD_REG(inline, Class##_fdio, Class) \
	DATA_IO_OPTIMIZE_ARRAY__SAVE_REG(inline, Class##_fdio, Class) \
	template<class DataIO>							\
	void DataIO_loadObject(DataIO& dio, Class& x)	\
	{												\
		static_cast<Class##_fdio&>(x).dio_load(dio);\
	}												\
	template<class DataIO>							\
	void DataIO_saveObject(DataIO&dio,const Class&x)\
	{												\
		static_cast<const Class##_fdio&>(x).dio_save(dio);\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//@}

/**
 @{
 @name versioned_serialization

 can easily use:
  DATA_IO_LOAD_SAVE_V(Class, member1 & vmg.since(20, member2) & member3 & vmg.get_version(version))
  - 'vmg.since(20, member2)' means member2 will be serialized only when Class's version large or equal than 20
  - 'vmg.get_version(version)' will copy version to Class's member 'version'
 */

#define DATA_IO_GEN_LOAD_SAVE_V(Class, Members)					\
	template<class DataIO> void									\
	dio_load(DataIO& aDataIO, unsigned _U_version)				\
	{															\
		terark::DataIO_version_manager<Class> vmg(_U_version);	\
		aDataIO Members;										\
	}															\
	template<class DataIO> void									\
	dio_save(DataIO& aDataIO, unsigned _U_version) const		\
	{															\
		terark::DataIO_version_manager<Class> vmg(_U_version);	\
		aDataIO Members;										\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//!@{
#define DATA_IO_LOAD_SAVE_V(Class, CurrentVersion, Members)	\
	DATA_IO_GEN_LOAD_SAVE_V(Class, Members)	\
	DATA_IO_REG_LOAD_SAVE_V(Class, CurrentVersion)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//@}

//!@{
#define DATA_IO_LOAD_SAVE_EV(Class, CurrentVersion, Members)\
	struct Class##_fdio : public Class						\
	{														\
		DATA_IO_GEN_LOAD_SAVE_V(Class##_fdio, Members)		\
	};														\
	template<class DataIO>									\
	void DataIO_loadObject(DataIO& dio, Class& x)			\
	{														\
		static_cast<Class##_fdio&>(x).dio_load(dio,			\
		DataIO_load_check_version(							\
			dio, CurrentVersion, BOOST_STRINGIZE(Class)));	\
	}														\
	template<class DataIO>									\
	void DataIO_saveObject(DataIO& dio, const Class& x)		\
	{														\
		dio << terark::serialize_version_t(CurrentVersion);	\
		static_cast<const Class##_fdio&>(x)					\
		.dio_save(dio, CurrentVersion);						\
	}														\
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//@}

#define DATA_IO_DISABLE_LOAD_SAVE(Class) \
		DATA_IO_DISABLE_LOAD(Class) \
		DATA_IO_DISABLE_SAVE(Class)

//////////////////////////////////////////////////////////////////////////////////////////
/// not required be called in global namespace
/// should be called in Class's namespace, or any outer namespace of it
#define DATA_IO_DUMP_RAW_MEM(Class) \
		DATA_IO_DUMP_RAW_MEM_IMPL(friend, Class)
#define DATA_IO_DUMP_RAW_MEM_E(Class) \
		DATA_IO_DUMP_RAW_MEM_IMPL(inline, Class)
#define DATA_IO_DUMP_RAW_MEM_IMPL(Friend, Class)		\
	template<class Input>								\
	Friend												\
	void DataIO_loadObject(Input& in, Class& x)			\
	{													\
		in.ensureRead(&x, sizeof(Class));				\
	}													\
	template<class Output>								\
	Friend												\
	void DataIO_saveObject(Output& out, const Class& x)	\
	{													\
		out.ensureWrite(&x, sizeof(Class));				\
	}													\
	template<class DataIO>                              \
	Friend ByteSwap_false								\
	Deduce_DataIO_need_bswap(DataIO*, Class&);			\
	template<class DataIO>                              \
	Friend												\
	IsDump_true Deduce_DataIO_is_dump(DataIO*, Class&);


//! define an input/output object of stream
//!
//! @param dio_t obj type of input/output
//! @param dio_v obj name of input/output
//! @param stream stream which you want to trait as input/output object
//!
//! @note
//!  -# template expression of dio_t must has not any comma(',')
//!  -# now all dio_t take a stream(not a stream*) has no extra member
//!     so it is safe to trait a stream as such a dio_t
//!  -# but must assert sizeof(dio_t) == sizeof(stream)
//!     if I add dynamic load/save polymorphic object future, this would need
//!     dynamic type list in an input/output object, so can not trait stream
//!     as such input/output object, the static_assert is needable
//!
#define DATA_IO_DEFINE_REF(dio_t, dio_v, stream) \
	dio_t& dio_v = static_cast<dio_t&>(stream); \
	BOOST_STATIC_ASSERT(sizeof(dio_t) == sizeof(stream))
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

} // namespace terark

#endif // __terark_io_DataIO_h__
