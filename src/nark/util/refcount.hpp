/* vim: set tabstop=4 : */
#ifndef __nark_refcount_hpp__
#define __nark_refcount_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "../stdtypes.hpp"
#include <assert.h>
#include <boost/detail/atomic_count.hpp>

namespace nark {

//! for add ref count ability to your class..
//!
//! has no virtual functions, so no v-table,
//! but if Clazz is inherited from 'RefCounter', it will be virtual...
template<class Clazz>
class RefCountable : public Clazz
{
	DECLARE_NONE_COPYABLE_CLASS(RefCountable)
protected:
	boost::detail::atomic_count nRef;
public:
	explicit RefCountable(long nInitRef = 0) : nRef(nInitRef) { }
	long get_refcount() const  { return nRef; }
	friend void intrusive_ptr_add_ref(RefCountable* p) { ++p->nRef; }
	friend void intrusive_ptr_release(RefCountable* p) { if (0 == --p->nRef) delete p; }
};

/**
 @brief 一般 RefCounter，使用虚函数实现，用于 boost::nark_ptr
 */
class NARK_DLL_EXPORT RefCounter
{
	DECLARE_NONE_COPYABLE_CLASS(RefCounter)
	boost::detail::atomic_count nRef;

public:
	explicit RefCounter(long nInitRef = 0) : nRef(nInitRef) { }
	virtual ~RefCounter() {}

	long get_refcount() const  { assert(this); return nRef; }
	void add_ref() { assert(this); ++nRef; }
	void release() { assert(this); if (0 == --nRef) delete this; }

	friend void intrusive_ptr_add_ref(RefCounter* p) { ++p->nRef; }
	friend void intrusive_ptr_release(RefCounter* p) { if (0 == --p->nRef) delete p; }
};

template<class T>
class nark_ptr_ref_cnt_base : public RefCounter
{
public:
	T* p;
	explicit nark_ptr_ref_cnt_base(T* p) : RefCounter(1), p(p) {}
};

template<class T>
class nark_ptr_ref_cnt_auto : public nark_ptr_ref_cnt_base<T>
{
public:
	explicit nark_ptr_ref_cnt_auto(T* p) : nark_ptr_ref_cnt_base<T>(p) {}
	virtual ~nark_ptr_ref_cnt_auto() { delete this->p; }
};

template<class T, class Del>
class nark_ptr_ref_cnt_user : public nark_ptr_ref_cnt_base<T>
{
	Del del;
public:
	nark_ptr_ref_cnt_user(T* p, const Del& del)
		: nark_ptr_ref_cnt_base<T>(p), del(del) {}
	virtual ~nark_ptr_ref_cnt_user() { del(this->p); }
};

template<class T> class nark_ptr
{
private:
    typedef nark_ptr this_type;

	nark_ptr_ref_cnt_base<T>* p;

public:
    typedef T element_type;
    typedef T value_type;
    typedef T * pointer;

	typedef nark_ptr_ref_cnt_base<T>* internal_ptr;

	internal_ptr get_internal_p() const { return p; }

	T* get() const  { return p->p; }

	nark_ptr() : p(0) {}

	template<class Y>
	explicit nark_ptr(Y* y) : p(y ? new nark_ptr_ref_cnt_auto<T>(y) : 0) {}
	explicit nark_ptr(T* x) : p(x ? new nark_ptr_ref_cnt_auto<T>(x) : 0) {}

	template<class Y, class Del>
	nark_ptr(Y* y, const Del& del)
		: p(y ? new nark_ptr_ref_cnt_user<T,Del>(y, del) : 0) {}

	template<class Del>
	nark_ptr(T* x, const Del& del)
		: p(x ? new nark_ptr_ref_cnt_user<T,Del>(x, del) : 0) {}

	nark_ptr(const nark_ptr<T>& x) : p(x.p)
	{
		if (x.p) x.p->add_ref();
	}

	template<class Y>
	nark_ptr(const nark_ptr<Y>& y)
		: p(reinterpret_cast<nark_ptr_ref_cnt_auto<T>*>(y.get_internal_p()))
	{
		// reinterpret_cast is not safe, so check it
		// if not convertible, this line will raise a compile error
		T* check_convertible = (Y*)0;
		if (p) p->add_ref();
	}

	~nark_ptr() {	if (p) p->release(); }

	T* operator->() const { assert(p); return  p->p; }
	T& operator* () const { assert(p); return *p->p; }

	template<class Y>
	void reset(Y* y) { nark_ptr<T>(y).swap(*this); }
	void reset(T* y) { nark_ptr<T>(y).swap(*this); }

	void swap(nark_ptr<T>& y)
	{
		T* t = y.p;
		y.p = this->p;
		this->p = t;
	}
	const nark_ptr<T>& operator=(const nark_ptr<T>& y)
	{
		nark_ptr<T>(y).swap(*this);
		return *this;
	}
	template<class Y>
	const nark_ptr<T>& operator=(const nark_ptr<Y>& y)
	{
		nark_ptr<T>(y).swap(*this);
		return *this;
	}
/*
#if defined(__SUNPRO_CC) && BOOST_WORKAROUND(__SUNPRO_CC, <= 0x530)

    operator bool () const
    {
        return p != 0;
    }

#elif defined(__MWERKS__) && BOOST_WORKAROUND(__MWERKS__, BOOST_TESTED_AT(0x3003))
    typedef T * (this_type::*unspecified_bool_type)() const;

    operator unspecified_bool_type() const // never throws
    {
        return p == 0? 0: &this_type::get;
    }

#else
*/
    typedef T * this_type::*unspecified_bool_type;

    operator unspecified_bool_type () const
    {
        return p == 0? 0: &this_type::p;
    }

//#endif

    // operator! is a Borland-specific workaround
    bool operator! () const
    {
        return p == 0;
    }

	//!{@ caution!!!
	void add_ref() { assert(p); p->add_ref(); }
	void release() { assert(p); p->release(); }
	//@}

	long get_refcount() const { return p ? p->get_refcount() : 0; }

	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const nark_ptr<T>& x)
	{
		assert(x.p);
		dio << *x.p->p;
	}
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, nark_ptr<T>& x)
	{
		x.reset(new T);
		dio >> *x.p->p;
	}
};

template<class T, class U> inline bool operator==(nark_ptr<T> const & a, nark_ptr<U> const & b)
{
    return a.get_internal_p() == b.get_internal_p();
}

template<class T, class U> inline bool operator!=(nark_ptr<T> const & a, nark_ptr<U> const & b)
{
    return a.get_internal_p() != b.get_internal_p();
}

template<class T, class U> inline bool operator==(nark_ptr<T> const & a, U * b)
{
    return a.get_internal_p() == b;
}

template<class T, class U> inline bool operator!=(nark_ptr<T> const & a, U * b)
{
    return a.get_internal_p() != b;
}

template<class T, class U> inline bool operator==(T * a, nark_ptr<U> const & b)
{
    return a == b.get_internal_p();
}

template<class T, class U> inline bool operator!=(T * a, nark_ptr<U> const & b)
{
    return a != b.get_internal_p();
}

#if __GNUC__ == 2 && __GNUC_MINOR__ <= 96

// Resolve the ambiguity between our op!= and the one in rel_ops

template<class T> inline bool operator!=(nark_ptr<T> const & a, nark_ptr<T> const & b)
{
    return a.get_internal_p() != b.get_internal_p();
}

#endif

template<class T> inline bool operator<(nark_ptr<T> const & a, nark_ptr<T> const & b)
{
    return a.get_internal_p() < b.get_internal_p();
}

template<class T> void swap(nark_ptr<T> & lhs, nark_ptr<T> & rhs)
{
    lhs.swap(rhs);
}

// mem_fn support

template<class T> T * get_pointer(nark_ptr<T> const & p)
{
    return p.get();
}

template<class T, class U> nark_ptr<T> static_pointer_cast(nark_ptr<U> const & p)
{
    return static_cast<T *>(p.get());
}

template<class T, class U> nark_ptr<T> const_pointer_cast(nark_ptr<U> const & p)
{
    return const_cast<T *>(p.get());
}

template<class T, class U> nark_ptr<T> dynamic_pointer_cast(nark_ptr<U> const & p)
{
    return dynamic_cast<T *>(p.get());
}

} // name space nark

#endif // __nark_refcount_hpp__

