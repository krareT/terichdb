/* vim: set tabstop=4 : */
#ifndef __terark_io_dump_allocator_h__
#define __terark_io_dump_allocator_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

namespace terark {

/**
 @brief only a proxy, for template typename dispatch
 */
template<class BaseAlloc>
class dump_allocator : public BaseAlloc
{
public:
	typedef BaseAlloc _MySuper;

	typedef typename BaseAlloc::value_type		value_type;
	typedef typename BaseAlloc::pointer			pointer;
	typedef typename BaseAlloc::reference		reference;
	typedef typename BaseAlloc::const_pointer   const_pointer;
	typedef typename BaseAlloc::const_reference const_reference;

	typedef typename BaseAlloc::size_type size_type;
	typedef typename BaseAlloc::difference_type difference_type;

	template<class _Other>
	struct rebind
	{	// convert an dump_allocator<BaseAlloc> to an dump_allocator <_Other>
	private:
		typedef typename BaseAlloc::template rebind<_Other>::other BaseRebind;
	public:
		typedef dump_allocator<BaseRebind> other;
	};

	dump_allocator()
	{	// construct default dump_allocator (do nothing)
	}

	dump_allocator(const dump_allocator<_Ty>& rhs)
		: BaseAlloc(rhs)
	{	// construct by copying (do nothing)
	}

	template<class _Other>
	dump_allocator(const dump_allocator<_Other>& other)
		: BaseAlloc(static_cast<const typename dump_allocator<_Other>::_MySuper&>(other))
	{	// construct from a related dump_allocator (do nothing)
	}

	template<class _Other>
	dump_allocator<_Ty>& operator=(const dump_allocator<_Other>& other)
	{	// assign from a related dump_allocator (do nothing)
		typedef typename dump_allocator<_Other>::_MySuper _OtherSuper;
		BaseAlloc::operator=(static_cast<const _OtherSuper&>(other));
		return (*this);
	}
};

// dump_allocator TEMPLATE OPERATORS
template<class _Ty, class _Other> inline
bool operator==(const dump_allocator<_Ty>& l, const dump_allocator<_Other>& r)
{	// test for dump_allocator equality (always true)
	typedef typename dump_allocator<_Ty>::_MySuper _MySuper;
	typedef typename dump_allocator<_Other>::_MySuper _HeSuper;
	return static_cast<const _MySuper&>(l) == static_cast<const _HeSuper&>(r);
}

template<class _Ty, class _Other> inline
bool operator!=(const dump_allocator<_Ty>& l, const dump_allocator<_Other>& r)
{	// test for dump_allocator inequality (always false)
	typedef typename dump_allocator<_Ty>::_MySuper _MySuper;
	typedef typename dump_allocator<_Other>::_MySuper _HeSuper;
	return static_cast<const _MySuper&>(l) != static_cast<const _HeSuper&>(r);
}

} // namespace terark

#endif // __terark_io_dump_allocator_h__

