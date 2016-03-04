#ifndef __nark_node_layout_hpp__
#define __nark_node_layout_hpp__

//#include <stddef.h> // part of C99, not available in all compiler
//#include <stdint.h>

#include <sys/types.h>
#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include <iterator>
#include <stdexcept>
#include <utility>
#include <memory>

//#include <tr1/type_traits>
#include <boost/version.hpp>
#include <boost/static_assert.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/mpl/if.hpp>
#include <boost/type_traits.hpp>
#if BOOST_VERSION > 103801
	#include <boost/swap.hpp>
#endif

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L || \
	defined(_MSC_VER) && _MSC_VER >= 1700
	#include <initializer_list>
	#ifndef HSM_HAS_MOVE
		#define HSM_HAS_MOVE
	#endif
#endif

#if defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__clang__)
	#define HSM_FORCE_INLINE __attribute__((always_inline))
#elif defined(_MSC_VER)
	#define HSM_FORCE_INLINE __forceinline
#else
	#define HSM_FORCE_INLINE inline
#endif

namespace nark {

//************************************************************************
// std components that could use FastCopy:
//     std::vector
//     std::string
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// some implementation of std components use pointers to embedded objects,
// so you shouldn't use FastCopy for it, because there is no a type traits
// for memcpy-able, and for performance issue, this lib will use FastCopy
// by default, if some class couldn't be memcpy-able, you must use SafeCopy
//     std::list
//     std::*stream, all iostream component shouldn't use FastCopy
//     std::map/set/multimap/multiset couldn't use FastCopy
// std components that couldn't use FastCopy:
// anything you are not sure be memcpy-able shouldn't use FastCopy
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
struct SafeCopy {
	enum { is_fast_copy = 0 };
	template<class T> // when dst and src overlap
	static void move_cons_backward(T* dst, T& src) {
		char tmp[sizeof(T)];
		move_cons(reinterpret_cast<T*>(tmp), src);
		move_cons(dst, *reinterpret_cast<T*>(tmp));
	}
	template<class T> // when dst and src overlap
	static void move_cons_forward(T* dst, T& src) {
		move_cons_backward(dst, src);
	}
#ifdef HSM_HAS_MOVE
	template<class T>
	static void move_cons(T* dst_raw, T& src) {
		new(dst_raw)T(std::move(src));
	}
	template<class T>
	static void move_assign(T* dst, T& src) {
		*dst = std::move(src);
	}
#else
	typedef boost::mpl::true_  has_nothrow_cons_yes;
	typedef boost::mpl::false_ has_nothrow_cons_no;
	template<class T>
	static void move_cons_aux(T* dst_raw, T& src, has_nothrow_cons_yes) {
		// assume nothrow cons is fast enough, such as stl containers
		new(dst_raw)T();
		std::swap(*dst_raw, src);
		src.~T();
	}
	template<class T>
	static void move_cons_aux(T* dst_raw, T& src, has_nothrow_cons_no) {
		new(dst_raw)T(src);
		src.~T();
	}
	template<class T>
	static void move_cons(T* dst_raw, T& src) {
		move_cons_aux(dst_raw, src, boost::has_nothrow_constructor<T>());
	}
	template<class T>
	static void move_assign(T* dst, T& src) {
	#if BOOST_VERSION > 103801
		boost::swap(*dst, src);
	#else
		std::swap(*dst, src);
	#endif
		src.~T();
	}
#endif
};

template<int> struct bytes2uint;
template<   > struct bytes2uint<sizeof(char     )> { typedef unsigned char      type; };
template<   > struct bytes2uint<sizeof(short    )> { typedef unsigned short     type; };
template<   > struct bytes2uint<sizeof(int      )> { typedef unsigned int       type; };
template<   > struct bytes2uint<sizeof(long long)> { typedef unsigned long long type; };

struct FastCopy {
	enum { is_fast_copy = 1 };
	template<class Unit>
	static void cp_backward(Unit* dst, const Unit* src, ptrdiff_t n) {
		for (ptrdiff_t i = n-1; i >= 0; --i)
			dst[i] = src[i];
	}
	template<class T>
	static void move_cons_backward(T* dst, T& src) {
		assert(sizeof(T) % 4 == 0);
		typedef typename bytes2uint<
			sizeof(void*)== 8 &&
		   	sizeof(T)% 8 == 0 ?  8 :
		   	sizeof(T)% 4 == 0 ?  4 :
			sizeof(T)% 2 == 0 ?  2 :
			1 >::type Unit;
		Unit* p = reinterpret_cast<Unit*>(&src);
		Unit* q = reinterpret_cast<Unit*>( dst);
		cp_backward(q, p, sizeof(T)/sizeof(Unit));
	}
	template<class T>
	static void move_cons_forward(T* dst, T& src) {
		memcpy(dst, &src, sizeof(T)); // let compiler do optimization
	}
	template<class T>
	static void move_cons(T* dst, T& src) {
		memcpy(dst, &src, sizeof(T));
	}
	template<class T>
	static void move_assign(T* dst, T& src) {
		dst->~T();
		memcpy(dst, &src, sizeof(T));
	}
};

struct ValueInline {
	enum { is_value_out = 0 };

	template<class Data, class Link>
	struct Node {
		Data data;
		Link link;
	};

	template<class Data, class NODE>
	class iterator {
		NODE* p;

	public:
		typedef std::random_access_iterator_tag iterator_category;
		typedef Data       value_type;
		typedef Data&      reference;
		typedef Data*      pointer;
		typedef ptrdiff_t  difference_type;

		explicit iterator(NODE* q = NULL) : p(q) {}

		template<class NonConstData, class NonConstNODE>
		iterator(iterator<NonConstData, NonConstNODE> y):p(y.get_node_ptr()){}

		NODE* get_node_ptr() const { return p; }

		Data& operator* () const { return  p->data; }
		Data* operator->() const { return &p->data; }

		iterator& operator++() { ++p; return *this; }
		iterator& operator--() { --p; return *this; }
		iterator  operator++(int) { NODE* t = p; ++p; return iterator(t); }
		iterator  operator--(int) { NODE* t = p; --p; return iterator(t); }

		iterator& operator+=(ptrdiff_t n) { p += n; return *this; }
		iterator& operator-=(ptrdiff_t n) { p -= n; return *this; }

		iterator  operator+(ptrdiff_t n) const { return iterator(p + n); }
		iterator  operator-(ptrdiff_t n) const { return iterator(p - n); }

		Data& operator[](ptrdiff_t n) const { return p[n].data; }

		friend iterator operator+(ptrdiff_t n, iterator i) { return iterator(i.p + n); }

		friend ptrdiff_t operator-(iterator x, iterator y) { return x.p - y.p; }
		friend bool operator< (iterator x, iterator y) { return x.p <  y.p; }
		friend bool operator> (iterator x, iterator y) { return x.p >  y.p; }
		friend bool operator<=(iterator x, iterator y) { return x.p <= y.p; }
		friend bool operator>=(iterator x, iterator y) { return x.p >= y.p; }
		friend bool operator==(iterator x, iterator y) { return x.p == y.p; }
		friend bool operator!=(iterator x, iterator y) { return x.p != y.p; }
	};
	template<class Data, class Link>
	struct is_packed :
	  boost::mpl::bool_<sizeof(Data)+sizeof(Link)==sizeof(Node<Data,Link>)>{};
};
struct ValueOut {
	enum { is_value_out = 1 };
};

template< class Data
		, class Link
		, class CopyStrategy = typename boost::mpl::if_<boost::has_trivial_copy<Data>, FastCopy, SafeCopy>::type
		, class DataPlace    = typename boost::mpl::if_<ValueInline::is_packed<Data,Link>, ValueInline, ValueOut>::type
		>
struct node_layout;

template<class Data, class Link>
struct node_layout_base_inline
{
	enum { is_value_out = 0 };
	typedef ValueInline::Node<Data, Link> Node;
	typedef Link link_t;
	typedef ValueInline::iterator<      Data,       Node>       iterator;
	typedef ValueInline::iterator<const Data, const Node> const_iterator;

//	BOOST_CONSTANT(size_t, data_stride = sizeof(Node));

	Node* aNode;

	node_layout_base_inline() { aNode = NULL; }
	bool is_null() const { return NULL == aNode; }

	      iterator begin()       { return       iterator(aNode); }
	const_iterator begin() const { return const_iterator(aNode); }

	      Data& data(size_t index)       { return aNode[index].data; }
	const Data& data(size_t index) const { return aNode[index].data; }
	      Link& link(size_t index)       { return aNode[index].link; }
	const Link& link(size_t index) const { return aNode[index].link; }

	void free() { if (aNode) ::free(aNode), aNode = NULL; }
};
template<class Data, class Link>
struct node_layout_base_out
{
	enum { is_value_out = 1 };
	typedef Link link_t;
	typedef       Data*       iterator;
	typedef const Data* const_iterator;

//	BOOST_CONSTANT(size_t, data_stride = sizeof(Data));

	Data* aData;
	Link* aLink;

	node_layout_base_out() { aData = NULL; aLink = NULL; }
	bool is_null() const { return NULL == aData; }

	      iterator begin()       { return       iterator(aData); }
	const_iterator begin() const { return const_iterator(aData); }
	      Data& data(size_t index)       { return aData[index]; }
	const Data& data(size_t index) const { return aData[index]; }
	      Link& link(size_t index)       { return aLink[index]; }
	const Link& link(size_t index) const { return aLink[index]; }

	void free() {
		if (aData) ::free(aData), aData = NULL;
		if (aLink) ::free(aLink), aLink = NULL;
	}
};

template<class Data, class Link>
struct node_layout<Data, Link, FastCopy, ValueInline>
	 : node_layout_base_inline<Data, Link>
{
	void reserve(size_t old_size, size_t new_capacity) {
		typedef ValueInline::Node<Data, Link> Node;
		assert(old_size < new_capacity);
		(void)old_size; // unused
		Node* pn = (Node*)realloc(this->aNode, sizeof(Node)*new_capacity);
		if (NULL == pn) throw std::bad_alloc();
		this->aNode = pn;
	}
	template<class IsNotFree>
	void reserve(size_t old_size, size_t new_capacity, IsNotFree) {
		reserve(old_size, new_capacity);
	}
	typedef FastCopy copy_strategy;
	enum { is_fast_copy = 1 };
};

template<class Data, class Link>
struct node_layout<Data, Link, FastCopy, ValueOut>
	 : node_layout_base_out<Data, Link>
{
	void reserve(size_t old_size, size_t new_capacity) {
		assert(old_size < new_capacity);
		(void)old_size; // unused
		Data* d = (Data*)realloc(this->aData, sizeof(Data)*new_capacity);
		if (NULL == d) throw std::bad_alloc();
		this->aData = d;
		Link* l = (Link*)realloc(this->aLink, sizeof(Link)*new_capacity);
		if (NULL == l) throw std::bad_alloc();
		this->aLink = l;
	}
	template<class IsNotFree>
	void reserve(size_t old_size, size_t new_capacity, IsNotFree) {
		reserve(old_size, new_capacity);
	}
	typedef FastCopy copy_strategy;
	enum { is_fast_copy = 1 };
};

//---------------------------------------------------------------------------
// SafeCopy

template<class Data, class Link>
struct node_layout<Data, Link, SafeCopy, ValueInline>
	 : node_layout_base_inline<Data, Link>
{
	void reserve(size_t old_size, size_t new_capacity) {
		assert(old_size < new_capacity);
		Node* pn = (Node*)malloc(sizeof(Node)*new_capacity);
		if (NULL == pn) throw std::bad_alloc();
		Node* p0 = this->aNode;
		for (size_t i = 0; i < old_size; ++i) { // nothrow
			SafeCopy::move_cons(&pn[i], p0[i]);
		}
		if (p0)
		   	::free(p0);
		this->aNode = pn;
	}
	template<class IsNotFree>
	void reserve(size_t old_size, size_t new_capacity, IsNotFree is_not_free) {
		assert(old_size < new_capacity);
		Node* pn = (Node*)malloc(sizeof(Node)*new_capacity);
		if (NULL == pn) throw std::bad_alloc();
		Node* p0 = this->aNode;
		size_t const salt_size = std::min(sizeof(Data), sizeof(Link));
		for (size_t i = 0; i < old_size; ++i) { // nothrow
			if (is_not_free(pn[i].link = p0[i].link))
				SafeCopy::move_cons(&pn[i].data, p0[i].data);
			else // assume some SALT is save on free slot
				memcpy(&pn[i].data, &p0[i].data, salt_size);
		}
		if (p0)
		   	::free(p0);
		this->aNode = pn;
	}
	typedef SafeCopy copy_strategy;
	typedef ValueInline::Node<Data, Link> Node;
	enum { is_fast_copy = 0 };
};

template<class Data, class Link>
struct node_layout<Data, Link, SafeCopy, ValueOut>
	 : node_layout_base_out<Data, Link>
{
	void reserve(size_t old_size, size_t new_capacity) {
		assert(old_size < new_capacity);
		Link* l = (Link*)realloc(this->aLink, sizeof(Link)*new_capacity);
		if (NULL == l) throw std::bad_alloc();
		this->aLink = l;
		Data* d = (Data*)malloc(sizeof(Data)*new_capacity);
		if (NULL == d) throw std::bad_alloc();
		Data* d0 = this->aData;
		for (size_t i = 0; i < old_size; ++i) { // nothrow
			SafeCopy::move_cons(d+i, d0[i]);
		}
		if (d0)
			::free(d0);
		this->aData = d;
	}
	template<class IsNotFree>
	void reserve(size_t old_size, size_t new_capacity, IsNotFree is_not_free) {
		assert(old_size < new_capacity);
		Link* l = (Link*)realloc(this->aLink, sizeof(Link)*new_capacity);
		if (NULL == l) throw std::bad_alloc();
		this->aLink = l;
		Data* d = (Data*)malloc(sizeof(Data)*new_capacity);
		if (NULL == d) throw std::bad_alloc();
		Data* d0 = this->aData;
		size_t const salt_size = std::min(sizeof(Data), sizeof(Link));
		for (size_t i = 0; i < old_size; ++i) { // nothrow
			if (is_not_free(l[i]))
				SafeCopy::move_cons(d+i, d0[i]);
			else // assume some SALT is save on free slot
				memcpy(d+i, d0+i, salt_size);
		}
		if (d0)
			::free(d0);
		this->aData = d;
	}
	typedef SafeCopy copy_strategy;
	enum { is_fast_copy = 0 };
};


template<class Data, class Link, class CopyStrategy>
void node_layout_copy_cons(
		      node_layout<Data, Link, CopyStrategy, ValueInline> x,
		const node_layout<Data, Link, CopyStrategy, ValueInline> y,
		size_t size)
{
	typedef ValueInline::Node<Data, Link> Node;
	      Node* px = x.aNode;
	const Node* py = y.aNode;
	size_t i = 0;
	try {
		for (; i < size; ++i)
			new(px+i)Node(py[i]);
	}
   	catch (...) { // unwind
		for (; i; --i)
			px[i-1].~Node();
		throw;
	}
}

template<class Data, class Link, class CopyStrategy, class IsNotFree>
void node_layout_copy_cons(
		      node_layout<Data, Link, CopyStrategy, ValueInline> x,
		const node_layout<Data, Link, CopyStrategy, ValueInline> y,
		size_t size,
		IsNotFree is_not_free)
{
	typedef ValueInline::Node<Data, Link> Node;
	      Node* px = x.aNode;
	const Node* py = y.aNode;
	size_t i = 0;
	size_t const salt_size = std::min(sizeof(Data), sizeof(Link));
	try {
		for (; i < size; ++i) {
			if (is_not_free(px[i].link = py[i].link))
				new(&px[i].data)Data(py[i].data);
			else
				memcpy(&px[i].data, &py[i].data, salt_size);
		}
	}
   	catch (...) { // unwind
		for (; i; --i)
			if (is_not_free(py[i-1].link))
				px[i-1].~Node();
		throw;
	}
}

template<class Data, class Link, class CopyStrategy>
void node_layout_copy_cons(
		      node_layout<Data, Link, CopyStrategy, ValueOut>& x,
		const node_layout<Data, Link, CopyStrategy, ValueOut>& y,
		size_t size)
{
	      Data* dx = x.aData;
	const Data* dy = y.aData;
	size_t i = 0;
	try {
		for (; i < size; ++i)
			new(dx+i)Data(dy[i]);
	}
   	catch (...) { // unwind
		for (; i; --i)
			dx[i-1].~Data();
		throw;
	}
	memcpy(x.aLink, y.aLink, sizeof(Link)*size);
}

template<class Data, class Link, class CopyStrategy, class IsNotFree>
void node_layout_copy_cons(
		      node_layout<Data, Link, CopyStrategy, ValueOut> x,
		const node_layout<Data, Link, CopyStrategy, ValueOut> y,
		size_t size,
		IsNotFree is_not_free)
{
	      Data* dx = x.aData;
	const Data* dy = y.aData;
	const Link* ly = y.aLink;
	size_t i = 0;
	size_t const salt_size = std::min(sizeof(Data), sizeof(Link));
	try {
		for (; i < size; ++i)
			if (is_not_free(ly[i]))
				new(dx+i)Data(dy[i]);
			else
				memcpy(dx+i, dy+i, salt_size);
	}
   	catch (...) { // unwind
		for (; i; --i)
			if (is_not_free(ly[i-1]))
				dx[i-1].~Data();
		throw;
	}
	memcpy(x.aLink, ly, sizeof(Link)*size);
}

} // namespace nark

#endif // __nark_node_layout_hpp__


