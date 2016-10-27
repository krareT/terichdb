/* vim: set tabstop=4 : */
#ifndef __terark_io_id_generator_h__
#define __terark_io_id_generator_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#if defined(__GNUC__) || defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
# include <stdint.h>
#endif

#if defined(linux) || defined(__linux) || defined(__linux__)
	#include <linux/types.h>
#elif defined(_MSC_VER)
	#include <crtdefs.h> // for uintptr_t
#else
	#include <sys/types.h>
#endif

#include <assert.h>
#include <string>
#include <vector>
#include <boost/current_function.hpp>
#include <boost/intrusive_ptr.hpp>

#include <terark/config.hpp>

namespace terark {

class TERARK_DLL_EXPORT id_generator
{
	// use free-list structure, same as memory management
	// id may not be 0
	std::vector<uintptr_t> id_list; // id_list[0] is linked list head
	uintptr_t m_nUsed;

	void chain(uintptr_t newHead);

public:
	void clear() { m_nUsed = 0; id_list.clear(); }

	explicit id_generator(uintptr_t maxID);
	virtual ~id_generator();

	uintptr_t alloc_id();
	void free_id(uintptr_t id);

	uintptr_t add_val(uintptr_t val);
	uintptr_t get_val(uintptr_t id) const {
		assert(id >= 1);
		assert(id <= uintptr_t(id_list.size()-1));
		return id_list[id];
	}

	bool is_valid(uintptr_t id) const {
		return id >= 1 && id <= uintptr_t(id_list.size()-1);
	}

	uintptr_t maxid() const { return id_list.size()-1; }
	uintptr_t size() const { return m_nUsed; }

	void get_used_id(std::vector<uintptr_t>* used_id) const;
};

class TERARK_DLL_EXPORT access_byid : public id_generator
{
protected:
	virtual void on_destroy(void* vp);
	void* get_ptr_imp(uintptr_t id, const char* func) const;
	using id_generator::add_val; // hide it

public:
	access_byid(uintptr_t maxID = 3) : id_generator(maxID) { }

	//! delete all object in list, and clear self
	void destroy();

	//! get the object by id
	void* get_ptr(uintptr_t id) const {
		return get_ptr_imp(id, BOOST_CURRENT_FUNCTION);
	}

	//! add `x` to managed object pool
	//! @return a new allocated id for `x`
	uintptr_t add_ptr(void* x) { return this->add_val((uintptr_t)(x)); }

	~access_byid();
};

// do not allow T to be non-pointer type
template<class T> class AccessByNameID;

template<> class TERARK_DLL_EXPORT AccessByNameID<void*>
{
protected:
	access_byid m_byid;
	class NameMapForAccessByNameID* m_byname;
	virtual void on_destroy(void* vp);
public:
	uintptr_t add_ptr(void* x, const std::string& name, void** existed);
	uintptr_t add_ptr(void* x) { return m_byid.add_ptr(x); }
	void* get_byid(uintptr_t id) const { return m_byid.get_ptr(id); }
	void* get_byname(const std::string& name) const;
	bool is_valid(uintptr_t id) const { return m_byid.is_valid(id); }
	void destroy();

	void remove(uintptr_t id, const std::string& name);
	void remove(uintptr_t id);
	uintptr_t size() const { return m_byid.size(); }
	bool check_id(uintptr_t id, const char* szClassName, std::string& err) const;

	AccessByNameID();
	virtual ~AccessByNameID();
};

template<class T> class AccessByNameID<T*> : public AccessByNameID<void*>
{
	typedef AccessByNameID<void*> super;
	virtual void on_destroy(void* vp) { delete (T*)vp; }
public:
	uintptr_t add_ptr(T* x, const std::string& name, T** existed) {
		return super::add_ptr(x, name, (void**)existed);
	}
	uintptr_t add_ptr(T* x) { // add without name
		assert(0 != x);
		return m_byid.add_ptr(x);
	}
	T* get_byid(uintptr_t id) const { return (T*)m_byid.get_ptr(id); }
	T* get_byname(const std::string& name) const {
		return (T*)super::get_byname(name);
	}
	~AccessByNameID() {
		// must call destroy() before destructor was called
		assert(m_byid.size() == 0);
	}
};

template<class T>
class AccessByNameID<boost::intrusive_ptr<T> > : public AccessByNameID<void*>
{
	typedef boost::intrusive_ptr<T> ptr_t;
	typedef AccessByNameID<void*>   super;
	virtual void on_destroy(void* vp) { intrusive_ptr_release((T*)vp); }
public:
	uintptr_t add_ptr(ptr_t x, const std::string& name, ptr_t* existed) {
		assert(0 != x.get());
		T* vpExisted;
		uintptr_t id = super::add_ptr(x.get(), name, (void**)&vpExisted);
		if (0 == vpExisted) {
			intrusive_ptr_add_ref(x.get());
			*existed = vpExisted;
		}
		return id;
	}
	uintptr_t add_ptr(ptr_t x) { // add without name
		assert(0 != x.get());
		intrusive_ptr_add_ref(x.get());
		return m_byid.add_ptr(x.get());
	}
	uintptr_t add_ptr(T* x, const std::string& name, T** existed) {
		assert(0 != x);
		assert(0 != existed);
		uintptr_t id = super::add_ptr(x, name, (void**)existed);
		if (0 == *existed)
			intrusive_ptr_add_ref(x);
		return id;
	}
	uintptr_t add_ptr(T* x) { // add without name
		assert(0 != x);
		intrusive_ptr_add_ref(x);
		return m_byid.add_ptr(x);
	}
	ptr_t get_byid(uintptr_t id) const { return (T*)m_byid.get_ptr(id); }
	ptr_t get_byname(const std::string& name) const {
		return (T*)super::get_byname(name);
	}
	T* get_rawptr_byid(uintptr_t id) const { return (T*)m_byid.get_ptr(id); }
	T* get_rawptr_byname(const std::string& name) const {
		return (T*)super::get_byname(name);
	}

	~AccessByNameID() {
		// must call destroy() before destructor was called
		assert(m_byid.size() == 0);
	}
};

} // namespace terark

#endif // __terark_io_id_generator_h__
