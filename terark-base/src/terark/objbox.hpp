#ifndef __terark_objbox_hpp__
#define __terark_objbox_hpp__

#include <terark/config.hpp>
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>

#include <stddef.h>
#include <string>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/utility/enable_if.hpp>

namespace terark { namespace objbox {

	template<class> struct obj_val; // map boxed type to boxer type

	class TERARK_DLL_EXPORT obj : boost::noncopyable {
	public:
		ptrdiff_t refcnt;
		friend void intrusive_ptr_add_ref(obj* p) { ++p->refcnt; }
		friend void intrusive_ptr_release(obj* p);
		obj() { refcnt = 0; }
		virtual ~obj();
	};

	class TERARK_DLL_EXPORT obj_ptr : public boost::intrusive_ptr<obj> {
		typedef boost::intrusive_ptr<obj> super;
	public:
		obj_ptr(obj* p = NULL) : super(p) {}
		obj_ptr(fstring);
		obj_ptr(const std::string&); // just for enable COW

		template<class T>
		obj_ptr(T* p,
				typename boost::enable_if<boost::is_base_of<obj,T> >::type* = NULL)
	   	 : super(p) {}

		template<class T>
		obj_ptr(const boost::intrusive_ptr<T>& p,
				typename boost::enable_if<boost::is_base_of<obj,T> >::type* = NULL)
		 : super(p) {}

		template<class T>
		obj_ptr(const T& x, typename obj_val<T>::type* = NULL)
	   	{ this->reset(new typename obj_val<T>::type(x)); }

		template<class T>
		typename boost::enable_if
			< boost::is_base_of<obj, typename obj_val<T>::type>
			, obj_ptr
			>::type&
		operator=(const T& x) { this->reset(new typename obj_val<T>::type(x)); }
		obj_ptr& operator=(obj* x) { super::reset(x); return *this; }
	};

	template<class T> const T& obj_cast(const obj_ptr& p) {
		if (NULL == p.get())
			throw std::logic_error("obj_cast: dereferencing NULL pointer");
		return dynamic_cast<const typename obj_val<T>::type&>(*p).get_boxed();
	}
	template<class T> T& obj_cast(obj_ptr& p) {
		if (NULL == p.get())
			throw std::logic_error("obj_cast: dereferencing NULL pointer");
		return dynamic_cast<typename obj_val<T>::type&>(*p).get_boxed();
	}
	template<class T> T& obj_cast(obj* p) {
		if (NULL == p)
			throw std::logic_error("obj_cast: dereferencing NULL pointer");
		return dynamic_cast<typename obj_val<T>::type&>(*p).get_boxed();
	}
	template<class T> const T& obj_cast(const obj* p) {
		if (NULL == p)
			throw std::logic_error("obj_cast: dereferencing NULL pointer");
		return dynamic_cast<const typename obj_val<T>::type&>(*p).get_boxed();
	}

// Boxer::get_boxed() is non-virtual
#define TERARK_BOXING_OBJECT(Boxer, Boxed) \
	class TERARK_DLL_EXPORT Boxer : public obj { public: \
		typedef Boxed boxed_t; \
		Boxed t; \
		Boxer(); \
		explicit Boxer(const Boxed& y); \
		~Boxer(); \
		const Boxed& get_boxed() const { return t; } \
		      Boxed& get_boxed()       { return t; } \
	}; \
	template<> struct obj_val<Boxed> { typedef Boxer type; };
//-----------------------------------------------------------------
#define TERARK_BOXING_OBJECT_IMPL(Boxer, Boxed) \
	Boxer::Boxer() {} \
	Boxer::Boxer(const Boxed& y) : t(y) {} \
	Boxer::~Boxer() {}
//-----------------------------------------------------------------

#define TERARK_BOXING_OBJECT_DERIVE(Boxer, Boxed) \
	class TERARK_DLL_EXPORT Boxer : public obj, public Boxed { \
	public: \
		typedef Boxed boxed_t; \
		Boxer(); \
		explicit Boxer(const Boxed& y); \
		~Boxer(); \
		const Boxer& get_boxed() const { return *this; } \
		      Boxer& get_boxed()       { return *this; } \
	}; \
	template<> struct obj_val<Boxed > { typedef Boxer type; }; \
	template<> struct obj_val<Boxer > { typedef Boxer type; };
//-----------------------------------------------------------------
#define TERARK_BOXING_OBJECT_DERIVE_IMPL(Boxer, Boxed) \
	Boxer::Boxer() {} \
	Boxer::Boxer(const Boxed& y) : Boxed(y) {} \
	Boxer::~Boxer() {}
//-----------------------------------------------------------------

	TERARK_BOXING_OBJECT(obj_bool   ,   bool)
	TERARK_BOXING_OBJECT(obj_short  ,   signed short)
	TERARK_BOXING_OBJECT(obj_ushort , unsigned short)
	TERARK_BOXING_OBJECT(obj_int    ,   signed int)
	TERARK_BOXING_OBJECT(obj_uint   , unsigned int)
	TERARK_BOXING_OBJECT(obj_long   ,   signed long)
	TERARK_BOXING_OBJECT(obj_ulong  , unsigned long)
	TERARK_BOXING_OBJECT(obj_llong  ,   signed long long)
	TERARK_BOXING_OBJECT(obj_ullong , unsigned long long)
	TERARK_BOXING_OBJECT(obj_float  , float)
	TERARK_BOXING_OBJECT(obj_double , double)
	TERARK_BOXING_OBJECT(obj_ldouble, long double)
	TERARK_BOXING_OBJECT_DERIVE(obj_array , valvec<obj_ptr>)

	class TERARK_DLL_EXPORT obj_string : public obj, public std::string {
	public:
		typedef std::string boxed_t;
		~obj_string();
		obj_string();
		explicit obj_string(fstring y);
		explicit obj_string(const char* y);
		explicit obj_string(const char* str, size_t len);
		explicit obj_string(const std::string& y);
		const obj_string& get_boxed() const { return *this; }
		      obj_string& get_boxed()       { return *this; }
	};
	template<> struct obj_val<const char*> { typedef obj_string type; };
	template<> struct obj_val<std::string> { typedef obj_string type; };
	template<> struct obj_val< obj_string> { typedef obj_string type; };
	template<> struct obj_val<    fstring> { typedef obj_string type; };

} } // namespace terark::objbox

#endif // __terark_objbox_hpp__

