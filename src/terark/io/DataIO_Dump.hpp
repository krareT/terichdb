/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_Dump_h__
#define __terark_io_DataIO_Dump_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

namespace terark {

//! called by dump_load_from_file/dump_save_to_file...
#define DATA_IO_DUMP_LOAD_SAVE(Class)							\
	template<class Input>										\
	friend void DataIO_dump_load_object(Input& in, Class& x)	\
	{															\
		x.dump_load(in);										\
	}															\
	template<class Output>										\
	friend void DataIO_dump_save_object(Output& out, const Class& x)\
	{															\
		x.dump_save(out);										\
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//! @{
//! default dump_load/dump_save
//!
//! @note
//!   if dump_load/dump_save is not public member,
//!   please use DATA_IO_DUMP_LOAD_SAVE
template<class Input, class Class>
void DataIO_dump_load_object(Input& in, Class& x)
{
	x.dump_load(in);
}
template<class Output, class Class>
void DataIO_dump_save_object(Output& out, const Class& x)
{
	x.dump_save(out);
}
//@}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
template<class DataIO, class T, class Alloc>
void DataIO_dump_load_object(DataIO& dio, std::vector<T, Alloc>& vec)
{
	uint32_t size;
	dio >> size;
	vec.resize(size);
	dio.ensureRead(&*vec.begin(), sizeof(T)*size);
}
template<class DataIO, class T, class Alloc>
void DataIO_dump_save_object(DataIO& dio, const std::vector<T, Alloc>& vec)
{
	uint32_t size = vec.size();
	dio << size;
	dio.ensureWrite(&*vec.begin(), sizeof(T)*size);
}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//!
#define DATA_IO_SMART_PTR_DUMP_LOAD_SAVE(SmartPtrTemplate)		\
template<class T, class DataIO>										\
void DataIO_dump_load_object(DataIO& dio, SmartPtrTemplate<T>& x)	\
{																	\
	x.reset(new T);													\
	dio >> *x;														\
}																	\
template<class T, class DataIO>										\
void DataIO_dump_save_object(DataIO& dio, const SmartPtrTemplate<T>& x)\
{																	\
	dio << *x;														\
}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DATA_IO_SMART_PTR_DUMP_LOAD_SAVE(std::auto_ptr)
DATA_IO_SMART_PTR_DUMP_LOAD_SAVE(boost::intrusive_ptr)
DATA_IO_SMART_PTR_DUMP_LOAD_SAVE(boost::scoped_ptr)
DATA_IO_SMART_PTR_DUMP_LOAD_SAVE(boost::shared_ptr)


} // namespace terark

#endif // __terark_io_DataIO_Dump_h__
