/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_Version_h__
#define __terark_io_DataIO_Version_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/stdtypes.hpp>
//#include "var_int.hpp"
#include <boost/serialization/strong_typedef.hpp>
#include <terark/pass_by_value.hpp>

namespace terark {

BOOST_STRONG_TYPEDEF(uint32_t, serialize_version_t)

//! 可以在 DATA_IO_VERSIONED_LOAD_SAVE 宏中使用
//!
//! 用来声明只序列化更高 version 的对象成员
//! @see DataIO_version_manager::since
//!
template<class Class>
class DataIO_since_version_proxy
{
	unsigned int m_min_owner_version;
	unsigned int m_version;
	Class& m_object;

public:
	DataIO_since_version_proxy(unsigned int min_owner_version, unsigned int loaded_owner_version, Class& object)
		: m_min_owner_version(min_owner_version)
		, m_version(loaded_owner_version)
		, m_object(object)
	{
	}
	template<class Input> friend void
		DataIO_loadObject(Input& input, DataIO_since_version_proxy x)
	{
		if (x.m_version >= x.m_min_owner_version)
		{
			input >> x.m_object;
		}
	}
	template<class Output> friend void
		DataIO_saveObject(Output& output, const DataIO_since_version_proxy& x)
	{
		if (x.m_version >= x.m_min_owner_version)
		{
			output << x.m_object;
		}
	}
};

//! 可以在 DATA_IO_VERSIONED_LOAD_SAVE 宏中使用
//!
//! 当载入对象时，用来把 version 拷贝到成员
//! @see DataIO_version_manager::get_version
//!
class DataIO_copy_version_proxy
{
	uint32_t& m_version;
	uint32_t  m_loadedVersion;
public:
	DataIO_copy_version_proxy(uint32_t& version, uint32_t loadedVersion)
		: m_version(version), m_loadedVersion(loadedVersion) {}

	template<class Input> friend void
		DataIO_loadObject(Input&, DataIO_copy_version_proxy x)
	{
		x.m_version = x.m_loadedVersion;
	}

	template<class Output> friend void
		DataIO_saveObject(Output&, const DataIO_copy_version_proxy&)
	{
		// ignore
	}
};

//! 可以在 DATA_IO_REG_LOAD_SAVE_V 宏中使用
//!
//! 用来在派生类中调用基类的 dio_load(object, version)/dio_save(object, version)
//!
//! @see DataIO_version_manager::base
template<class BaseClassPtr>
class DataIO_base_class_version_load_save
{
	BaseClassPtr m_base;
	unsigned int m_version;

public:
	DataIO_base_class_version_load_save(BaseClassPtr pbase, unsigned int version)
		: m_base(pbase), m_version(version) {}

	template<class Input> friend void
		DataIO_loadObject(Input& input, DataIO_base_class_version_load_save x)
	{
		x.m_base->dio_load(input, x.m_version);
	}
	template<class Output> friend void
		DataIO_saveObject(Output& output, const DataIO_base_class_version_load_save& x)
	{
		x.m_base->dio_save(output, x.m_version);
	}
};

//! 版本管理
//!
//! @see DATA_IO_REG_VERSION_SERIALIZE/DATA_IO_REG_LOAD_SAVE_V
//!
template<class ThisClass> class DataIO_version_manager
{
	unsigned int m_version;

public:
	DataIO_version_manager(unsigned int loaded_owner_version)
		: m_version(loaded_owner_version)
	{
	}

	template<class Class>
	pass_by_value<DataIO_since_version_proxy<Class> >
	since(unsigned int min_owner_version, Class& object)
	{
		return pass_by_value<DataIO_since_version_proxy<Class> >
			(DataIO_since_version_proxy<Class>(min_owner_version, m_version, object));
	}
	template<class Class>
	DataIO_since_version_proxy<const Class>
	since(unsigned int min_owner_version, const Class& object)
	{
		return DataIO_since_version_proxy<const Class>(min_owner_version, m_version, object);
	}

	pass_by_value<DataIO_copy_version_proxy>
	get_version(uint32_t& version)
	{
		return pass_by_value<DataIO_copy_version_proxy>
				(DataIO_copy_version_proxy(version, m_version));
	}

	//! version is const when dio_save()
	DataIO_copy_version_proxy
	get_version(const uint32_t&/*version*/)
	{
		static uint32_t v2;
		return DataIO_copy_version_proxy(v2, m_version);
	}

	//! @{
	//! if base class version is same with the derived class,
	//! use this function to read base object
	//! for loading
	template<class BaseClass>
	pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
	base(BaseClass* self)
	{
		return pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
			(DataIO_base_class_version_load_save<BaseClass*>(self, m_version));
	}

	//! for saving
	template<class BaseClass>
	DataIO_base_class_version_load_save<const BaseClass*>
	base(const BaseClass* self)
	{
		return DataIO_base_class_version_load_save<const BaseClass*>(self, m_version);
	}
	//@}

	//! @{
	//! if base class is a prev version, use this function to read base object
	//! for loading
	template<class BaseClass>
	pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
	base(BaseClass* self, unsigned base_version)
	{
		return pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
			(DataIO_base_class_version_load_save<BaseClass*>(self, base_version));
	}

	//! for saving
	template<class BaseClass>
	DataIO_base_class_version_load_save<const BaseClass*>
	base(const BaseClass* self, unsigned base_version)
	{
		return DataIO_base_class_version_load_save<const BaseClass*>(self, base_version);
	}
	//@}
};

} // namespace terark

#endif // __terark_io_DataIO_Version_h__

