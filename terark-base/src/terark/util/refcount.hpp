/* vim: set tabstop=4 : */
#ifndef __terark_refcount_hpp__
#define __terark_refcount_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "../stdtypes.hpp"
#include <assert.h>
#include <boost/detail/atomic_count.hpp>

namespace terark {

//! for add ref count ability to your class..
//!
//! has no virtual functions, so no v-table,
//! but if Clazz is inherited from 'RefCounter', it will be virtual...
template<class Clazz>
class RefCountable : public Clazz
{
	DECLARE_NONE_COPYABLE_CLASS(RefCountable)
protected:
	boost::detail::atomic_count m_intrusive_refcnt;
public:
	explicit
	RefCountable(long nInitRef = 0) : m_intrusive_refcnt(nInitRef) {
		assert(nInitRef >= 0);
	}
	long get_refcount() const  {
		assert(m_intrusive_refcnt >= 0);
		return m_intrusive_refcnt;
	}
	friend void intrusive_ptr_add_ref(RefCountable* p) {
		assert(NULL != p);
		assert(p->m_intrusive_refcnt >= 0);
		++p->m_intrusive_refcnt;
	}
	friend void intrusive_ptr_release(RefCountable* p) {
		assert(NULL != p);
		assert(p->m_intrusive_refcnt > 0);
		if (0 == --p->m_intrusive_refcnt)
			delete p;
	}
};

/**
 @brief 一般 RefCounter，使用虚函数实现，用于 boost::terark_ptr
 */
class TERARK_DLL_EXPORT RefCounter
{
	DECLARE_NONE_COPYABLE_CLASS(RefCounter)
	boost::detail::atomic_count nRef;

public:
	explicit RefCounter(long nInitRef = 0) : nRef(nInitRef) {
		assert(nInitRef >= 0);
	}
	virtual ~RefCounter() {}
	long get_refcount() const { assert(nRef >= 0); return nRef; }
	void add_ref() { assert(nRef >= 0); ++nRef; }
	void release() { assert(nRef > 0);
		if (0 == --nRef)
			delete this;
	}
	friend void intrusive_ptr_add_ref(RefCounter* p) {
		assert(NULL != p);
		assert(p->nRef >= 0);
		++p->nRef;
	}
	friend void intrusive_ptr_release(RefCounter* p) {
		assert(NULL != p);
		assert(p->nRef > 0);
		if (0 == --p->nRef)
			delete p;
	}
};

template<class T>
class terark_ptr_ref_cnt_base : public RefCounter
{
public:
	T* p;
	explicit terark_ptr_ref_cnt_base(T* p) : RefCounter(1), p(p) {}
};

template<class T>
class terark_ptr_ref_cnt_auto : public terark_ptr_ref_cnt_base<T>
{
public:
	explicit terark_ptr_ref_cnt_auto(T* p) : terark_ptr_ref_cnt_base<T>(p) {}
	virtual ~terark_ptr_ref_cnt_auto() { delete this->p; }
};

template<class T, class Del>
class terark_ptr_ref_cnt_user : public terark_ptr_ref_cnt_base<T>
{
	Del del;
public:
	terark_ptr_ref_cnt_user(T* p, const Del& del)
		: terark_ptr_ref_cnt_base<T>(p), del(del) {}
	virtual ~terark_ptr_ref_cnt_user() { del(this->p); }
};

template<class T> class terark_ptr
{
private:
    typedef terark_ptr this_type;

	terark_ptr_ref_cnt_base<T>* p;

public:
    typedef T element_type;
    typedef T value_type;
    typedef T * pointer;

	typedef terark_ptr_ref_cnt_base<T>* internal_ptr;

	internal_ptr get_internal_p() const { return p; }

	T* get() const  { return p->p; }

	terark_ptr() : p(0) {}

	template<class Y>
	explicit terark_ptr(Y* y) : p(y ? new terark_ptr_ref_cnt_auto<T>(y) : 0) {}
	explicit terark_ptr(T* x) : p(x ? new terark_ptr_ref_cnt_auto<T>(x) : 0) {}

	template<class Y, class Del>
	terark_ptr(Y* y, const Del& del)
		: p(y ? new terark_ptr_ref_cnt_user<T,Del>(y, del) : 0) {}

	template<class Del>
	terark_ptr(T* x, const Del& del)
		: p(x ? new terark_ptr_ref_cnt_user<T,Del>(x, del) : 0) {}

	terark_ptr(const terark_ptr<T>& x) : p(x.p)
	{
		if (x.p) x.p->add_ref();
	}

	template<class Y>
	terark_ptr(const terark_ptr<Y>& y)
		: p(reinterpret_cast<terark_ptr_ref_cnt_auto<T>*>(y.get_internal_p()))
	{
		// reinterpret_cast is not safe, so check it
		// if not convertible, this line will raise a compile error
		T* check_convertible = (Y*)0;
		if (p) p->add_ref();
	}

	~terark_ptr() {	if (p) p->release(); }

	T* operator->() const { assert(p); return  p->p; }
	T& operator* () const { assert(p); return *p->p; }

	template<class Y>
	void reset(Y* y) { terark_ptr<T>(y).swap(*this); }
	void reset(T* y) { terark_ptr<T>(y).swap(*this); }

	void swap(terark_ptr<T>& y)
	{
		T* t = y.p;
		y.p = this->p;
		this->p = t;
	}
	const terark_ptr<T>& operator=(const terark_ptr<T>& y)
	{
		terark_ptr<T>(y).swap(*this);
		return *this;
	}
	template<class Y>
	const terark_ptr<T>& operator=(const terark_ptr<Y>& y)
	{
		terark_ptr<T>(y).swap(*this);
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
	friend void DataIO_saveObject(DataIO& dio, const terark_ptr<T>& x)
	{
		assert(x.p);
		dio << *x.p->p;
	}
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, terark_ptr<T>& x)
	{
		x.reset(new T);
		dio >> *x.p->p;
	}
};

template<class T, class U> inline bool operator==(terark_ptr<T> const & a, terark_ptr<U> const & b)
{
    return a.get_internal_p() == b.get_internal_p();
}

template<class T, class U> inline bool operator!=(terark_ptr<T> const & a, terark_ptr<U> const & b)
{
    return a.get_internal_p() != b.get_internal_p();
}

template<class T, class U> inline bool operator==(terark_ptr<T> const & a, U * b)
{
    return a.get_internal_p() == b;
}

template<class T, class U> inline bool operator!=(terark_ptr<T> const & a, U * b)
{
    return a.get_internal_p() != b;
}

template<class T, class U> inline bool operator==(T * a, terark_ptr<U> const & b)
{
    return a == b.get_internal_p();
}

template<class T, class U> inline bool operator!=(T * a, terark_ptr<U> const & b)
{
    return a != b.get_internal_p();
}

#if __GNUC__ == 2 && __GNUC_MINOR__ <= 96

// Resolve the ambiguity between our op!= and the one in rel_ops

template<class T> inline bool operator!=(terark_ptr<T> const & a, terark_ptr<T> const & b)
{
    return a.get_internal_p() != b.get_internal_p();
}

#endif

template<class T> inline bool operator<(terark_ptr<T> const & a, terark_ptr<T> const & b)
{
    return a.get_internal_p() < b.get_internal_p();
}

template<class T> void swap(terark_ptr<T> & lhs, terark_ptr<T> & rhs)
{
    lhs.swap(rhs);
}

// mem_fn support

template<class T> T * get_pointer(terark_ptr<T> const & p)
{
    return p.get();
}

template<class T, class U> terark_ptr<T> static_pointer_cast(terark_ptr<U> const & p)
{
    return static_cast<T *>(p.get());
}

template<class T, class U> terark_ptr<T> const_pointer_cast(terark_ptr<U> const & p)
{
    return const_cast<T *>(p.get());
}

template<class T, class U> terark_ptr<T> dynamic_pointer_cast(terark_ptr<U> const & p)
{
    return dynamic_cast<T *>(p.get());
}

} // name space terark

#endif // __terark_refcount_hpp__

