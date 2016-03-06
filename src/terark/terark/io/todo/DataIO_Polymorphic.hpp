/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_Polymorphic_h__
#define __terark_io_DataIO_Polymorphic_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <typeinfo>
#include <stdexcept>

#include <boost/tuple/tuple.hpp>
#include <boost/type_traits.hpp>
#include <boost/static_assert.hpp>

#include <terark/stdtypes.hpp>
#include <terark/io/StreamBuffer.hpp>

// 目前还未完成

namespace terark {

class DataIO_Mash_Base
{
public:
	virtual ~DataIO_Mash_Base() {}
	virtual std::string name() const = 0;
	virtual void* create() const = 0;
	virtual void load(PortableDataInput<InputBuffer>& input, void* obj) const = 0;
	virtual void save(PortableDataOutput<OutputBuffer>& input, void* obj) const = 0;

	static std::map<std::string, const DataIO_Mash_Base*> S_dynamic_mash_map;
};


template<class Class>
class DataIO_Mash_Impl : public DataIO_Mash_Base
{
	const static DataIO_Mash_Impl<Class> S_my_mash;
	DataIO_Mash_Impl()
	{
		S_dynamic_mash_map.insert(std::make_pair(typeid(*this).name(), this));
	}
	friend class Class;
public:
	virtual std::string name() const
	{
		return typeid(Class).name();
	}
	virtual void* create() const
	{
		return new Class;
	}
	virtual void load(PortableDataInput<InputBuffer>& input, void* obj) const
	{
		input >> *(Class*)(obj);
	}
	virtual void save(PortableDataOutput<OutputBuffer>& output, void* obj) const
	{
		output << *(const Class*)(obj);
	}
};
template<class Class>
const static DataIO_Mash_Impl<Class> DataIO_Mash_Impl<Class>::S_my_mash;

#define DATA_IO_LOAD_SAVE_D(Class, Members)	\
	DATA_IO_LOAD_SAVE(Class, Members)	\
	static const DataIO_Mash_Impl<Class> S_dynamic_load_save_singleton;

#define DATA_IO_LOAD_SAVE_DV(Class, Version, Members)	\
	DATA_IO_LOAD_SAVE_V(Class, Version, Members)	\
	static const DataIO_Mash_Impl<Class> S_dynamic_load_save_singleton;

//////////////////////////////////////////////////////////////////////////
/*
namespace serialization	{ namespace polymorphic {


//	typedef typename serialization::polymorphic::Factory<DataInput>::local_factory_t local_factory_t;
//	local_factory_t m_factory;

	template<class Object>
	void load_polym(Object& x, uint32_t classID)
	{
		factory_t::iterator found = m_factory.find(classID);
		if (m_factory.end() == found)
		{
			string_appender<> oss;
			oss << "class=" << typeid(x).name() << ", classID=" << classID;
			throw NotFoundFactoryException(oss.str());
		}
		(*found).second->load(*this, (void*)(x), classID);
	}


	struct IPolymorphicObject
	{
		virtual void* create() const = 0;
		virtual const std::type_info& getType() const = 0;
		virtual const char* className() const = 0;
	};

	//! proxy for load polymorphic object
	template<class Object>
	struct PolymorphicObject : public IPolymorphicObject
	{
		Object& object;
		explicit PolymorphicObject(Object& object) : object(object) {}
	};

	//! only used for make short symbol for convenient
	struct DataIO_PolymorphicSerialization
	{
		template<class Object>
		PolymorphicObject<Object> operator()(Object& object)
		{
			return PolymorphicObject<Object>(object);
		}
	};


	//! 声明一个类可多态序列化时，只能把序列化方法绑定到一个 DataIO
	//! 因而必须使用 IDataInput/IDataOutput

	using namespace boost::multi_index;
	template<class Interface>
	struct Factory
	{
		struct CmpTypeInfo
		{
			bool operator()(const std::type_info& left, const std::type_info& right)
			{
				return left.before(right);
			}
		};
		typedef multi_index_container<
			const Interface*,
			indexed_by<
				ordered_unique<member<Interface, std::string, &Interface::className> >,
				ordered_unique<const_mem_fun<Interface, const std::type_info&, &Interface::getType>, CmpTypeInfo>
			>
		> factory_t;

		typedef auto_id_container<multi_index_container<
			const Interface*,
			indexed_by<
				ordered_unique<member<Interface, uint32_t, &Interface::classID> >,
				ordered_unique<const_mem_fun<Interface, const std::type_info&, &Interface::getType>, CmpTypeInfo>
			>
		> > local_factory_t;

		static factory_t& getFactory()
		{
			static factory_t factory;
			return factory;
		}
	};

	template<class Input>
	struct InputInterface
	{
		virtual void* create() const = 0;
		virtual void  load(Input& input, IPolymorphicObject& x) const = 0;
		virtual const std::type_info& getType() const = 0;

		std::string className;
		InputInterface(const char* className) : className(className) {}
	};

	template<class Output>
	struct OutputInterface
	{
		virtual void  save(Output& output, IPolymorphicObject& x) const = 0;
		virtual const std::type_info& getType() const = 0;

		std::string className;
		OutputInterface(const char* className) : className(className) {}
	};

	template<class Input, class Object>
	struct InputImplement : public InputInterface<Input>
	{
		BOOST_STATIC_ASSERT(boost::is_pointer<Object>::value);

		void load(Input& input, IPolymorphicObject& x) const
		{
			PolymorphicObject<Object>* y = dynamic_cast<PolymorphicObject<Object>*>(&x);
			assert(y);
			input >> y->object;
		}
		void* create() const
		{
			return new PolymorphicObject(new Object);
		}
		virtual const std::type_info& getType() const { return typeid(Object); }

	private:
		InputImplement(const char* className)
			: InputInterface<Input>(className)
		{
			Factory<InputInterface<Input> >::getFactory().insert(this);
		}
		static InputImplement s_single_instance;
	};

	template<class Output, class Object>
	struct OutputImplement : public OutputInterface<Output>
	{
		BOOST_STATIC_ASSERT(boost::is_pointer<Object>::value);

		void save(Output& output, IPolymorphicObject& x) const
		{
			PolymorphicObject<Object>* y = dynamic_cast<PolymorphicObject<Object>*>(&x);
			assert(y);
			output << y->object;
		}
		virtual const std::type_info& getType() const { return typeid(Object); }

	private:
		OutputImplement(const char* className)
			: OutputInterface<Output>(className)
		{
			Factory<OutputInterface<Output> >::getFactory().insert(this);
		}
		static OutputImplement s_single_instance;
	};

	template<class DataIO, class Object>
	void DataIO_loadObject(DataIO& input, PolymorphicObject<Object*> x)
	{
		typedef  Factory<InputInterface<DataIO> > factory_t;
		typename factory_t::factory_t& factory = factory_t::getFactory();

		assert(0 == x.object); //< ptr must be null before load

		var_uint32_t classID;
		input >> classID;
		InputInterface<DataIO>* creator = 0;
		if (0 == classID) {
			std::string className;
			input >> className;
			classID.t = input.m_factory.alloc_id(className);

			typename factory_t::factory_t::iterator found = factory.find(className);
			if (factory.end() == found)
			{
				string_appender<> oss;
				oss << "not found loader, class.typeid=" << typeid(*x.object).name() << "\n"
					<< "className=" << className;
				throw NotFoundFactoryException(oss.str());
			}
			input.m_factory[classID.t] = creator = &*found;
		} else {
			typename factory_t::local_factory_t::iterator found = input.m_factory.find(classID.t);
			if (input.m_factory.end() == found)
			{
				string_appender<> oss;
				oss << "not found loader, class.typeid=" << typeid(*x.object).name() << "\n"
					<< "classID=" << classID.t;
				throw NotFoundFactoryException(oss.str());
			}
			creator = &*found;
		}
		IPolymorphicObject* obj = creator->create();
		x.object = reinterpret_cast<Object*>(obj);
		creator->load(input, x);
	}

	template<class DataIO, class Object>
	void DataIO_saveObject(DataIO& output, PolymorphicObject<const Object*> x)
	{
		typedef  Factory<OutputInterface<DataIO> > factory_t;
		typename factory_t::factory_t factory = factory_t::getFactory();

		var_uint32_t classID = output.getClassID(typeid(x.getRef()));
		if (0 == classID) {
			typename factory_t::iterator found = factory.find(typeid(x.getRef()));
			if (factory.end() == found) {
				string_appender<> oss;
				oss << "not found loader, class.typeid=" << typeid(x.getRef()).name();
				throw NotFoundFactoryException(oss.str());
			}
			output << classID;
			output << (*found).className;
			classID = output.newClassID(className);
		}
		OutputInterface<Output>* saver = output.getSaver(classID);
		saver.save(output, x);
	}

//////////////////////////////////////////////////////////////////////////

#define DATA_IO_POLYM_LOAD_EX(Input,Class)	\
	terark::serialization::polymorphic::InputImplement<Input, Class> \
	InputImplement<Input, Class>::s_single_instance(BOOST_STRINGIZE(Class));

#define DATA_IO_POLYM_LOAD(Class) DATA_IO_POLYM_LOAD_EX(IDataInput,Class)

#define DATA_IO_POLYM_SAVE_EX(Input,Class)	\
	terark::serialization::polymorphic::InputImplement<Input, Class> \
	InputImplement<Input, Class>::s_single_instance(BOOST_STRINGIZE(Class));

#define DATA_IO_POLYM_SAVE(Class) DATA_IO_POLYM_SAVE_EX(IDataOutput,Class)

#define DATA_IO_POLYM_SERIALIZE_EX(Input,Output,Class)	\
	DATA_IO_POLYM_LOAD_EX(Input,Class)	\
	DATA_IO_POLYM_SAVE_EX(Output,Class)

#define DATA_IO_POLYM_SERIALIZE(Class)	DATA_IO_POLYM_SERIALIZE_EX(IDataInput, IDataOutput, Class)

} } // serialization::polymorphic
*/
} // namespace terark

#endif // __terark_io_DataIO_Polymorphic_h__
