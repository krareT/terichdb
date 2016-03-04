/* vim: set tabstop=4 : */
#ifndef __nark_compare_hpp__
#define __nark_compare_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
#  pragma once
#endif

#include <functional>
#include <boost/multi_index/member.hpp>

#include <boost/type_traits/remove_reference.hpp>
#include <boost/mpl/has_xxx.hpp>

// should be the last #include
#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace nark {

//-----------------------------------------------------------------------------------------
//! 测试 Compare 是否有 int compare(...) 成员，
//! 如果有，就表示 Compare 的比较操作代价不低，用 int compare(....) 更合算

BOOST_MPL_HAS_XXX_TRAIT_DEF(has_tri_compare)

//! @{
//! 如果 Compare 有 has_tri_compare 这个成员
//!  HasTriCompare<Compare>::type 就是 Compare::has_tri_compare

template<class Compare, bool YesOrNo>
struct HasTriCompare_Impl
	BOOST_TT_AUX_BOOL_C_BASE(false)
{
	BOOST_TT_AUX_BOOL_TRAIT_VALUE_DECL(false)
};
template<class Compare>
struct HasTriCompare_Impl<Compare, true>
	BOOST_TT_AUX_BOOL_C_BASE(Compare::has_tri_compare::value)
{
	BOOST_TT_AUX_BOOL_TRAIT_VALUE_DECL(Compare::has_tri_compare::value)
};

template<class Compare>
struct HasTriCompare
	BOOST_TT_AUX_BOOL_C_BASE((HasTriCompare_Impl<Compare, has_has_tri_compare<Compare>::value>::value))
{
	BOOST_TT_AUX_BOOL_TRAIT_VALUE_DECL((HasTriCompare_Impl<Compare, has_has_tri_compare<Compare>::value>::value))
};

//-----------------------------------------------------------------------------------------
//@}

template<class Compare, class T1, class T2>
inline int EffectiveCompare_Aux(const Compare& comp, const T1& t1, const T2& t2, boost::mpl::false_ hasTriCompare)
{
	if (comp(t1, t2))
		return -1;
	if (comp(t2, t1))
		return 1;
	else
		return 0;
}

template<class Compare, class T1, class T2>
inline int EffectiveCompare_Aux(const Compare& comp, const T1& t1, const T2& t2, boost::mpl::true_ hasTriCompare)
{
	return comp.compare(t1, t2);
}

template<class Compare, class T1, class T2>
int EffectiveCompare(const Compare& comp, const T1& t1, const T2& t2)
{
	return EffectiveCompare_Aux(comp, t1, t2, typename HasTriCompare<Compare>::type());
}

//-----------------------------------------------------------------------------------------
template<class Compare>
class InverseCompare : private Compare
{
public:
	typedef typename HasTriCompare<Compare>::type has_tri_compare;

	explicit InverseCompare(const Compare& comp) : Compare(comp) {}
	InverseCompare() {}

	template<class ValueT>
	bool operator()(const ValueT& left, const ValueT& right) const
	{
		return Compare::operator()(right, left);
	}
	template<class ValueT>
	int compare(const ValueT& left, const ValueT& right) const
	{
		return Compare::compare(right, left);
	}
};
BOOST_TT_AUX_BOOL_TRAIT_PARTIAL_SPEC1_1(
	class Compare,
	HasTriCompare,
	InverseCompare<Compare>,
	HasTriCompare<Compare>::value)
//-----------------------------------------------------------------------------------------

template<class T, class Cmp>
class CompareAdaptor
{
	Cmp m_cmp;

public:
	CompareAdaptor(Cmp cmp) : m_cmp(cmp) {}

	bool operator()(T x, T y) const
	{
		return m_cmp(x, y) < 0;
	}
	int compare(T x, T y) const
	{
		return m_cmp(x, y);
	}
	typedef boost::mpl::true_ has_tri_compare;
};


//-----------------------------------------------------------------------------------------
class XIdentity
{
public:
	template<class T> const T& operator()(const T& x) const { return x; }
};

template<class Class, class MemberType, MemberType Class::*MemberPtr>
struct MemberExtractor
{
	const MemberType& operator()(const MemberType& x) const
	{
		return x;
	}
	template<class ClassOrPtr>
	const MemberType& operator()(const ClassOrPtr& x) const
	{
		return deref(x).*MemberPtr;
	}
	template<class ChainedPtr>	// can be raw pointer or smart pointer or multi-level-pointer
	const ChainedPtr& deref(const ChainedPtr& x) const { return deref(*x); }
	const Class& deref(const Class& x) const { return  x; }
};
#define NARK_MemberExtractor(Class,MemberType,MemberName) \
	nark::MemberExtractor<Class, MemberType, &Class::MemberName>()
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//!
//! @code
//!   OffsetExtractor<Class, MemberType> ex(offsetof(Class, MemberName));
//! @endcode
template<class Class, class Member>
class OffsetExtractor
{
	size_t m_offset;
public:
	OffsetExtractor(size_t offset) : m_offset(offset) {}

	const Member& operator()(const Class& x) const
	{
		return *(Member*)((const char*)(&x) + m_offset);
	}
	Member& operator()(Class& x) const
	{
		return *(Member*)((char*)(&x) + m_offset);
	}
};
#define NARK_OffsetExtractor(Class,MemberType,MemberName) \
	nark::OffsetExtractor<Class, MemberType>(offsetof(Class, MemberName))
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

template<class DataVec, class T = typename DataVec::value_type>
struct GetDataByIndex
{
	DataVec&  data;

	explicit GetDataByIndex(DataVec& data)
		: data(data) {}

	T operator()(int x) const { return data[x]; }
};

struct ExtractFirst
{
	template<class First, class Second>
	const First& operator()(const std::pair<First,Second>& x) const
	{
		return x.first;
	}
};

struct ExtractSecond
{
	template<class First, class Second>
	const Second& operator()(const std::pair<First,Second>& x) const
	{
		return x.second;
	}
};

template< class Class
		, class MemberType, MemberType Class::*MemberPtr
		, class MemberTypeComparator = std::less<MemberType>
		>
class CompareMember :
	private MemberTypeComparator,
	private MemberExtractor<Class, MemberType, MemberPtr>
{
private:
	typedef MemberExtractor<Class, MemberType, MemberPtr> mem_extractor;

public:
	typedef typename HasTriCompare<MemberTypeComparator>::type has_tri_compare;

	CompareMember() {}
	CompareMember(const MemberTypeComparator& comp) : MemberTypeComparator(comp) {}

	template<class T1, class T2>
	bool operator()(const T1& left, const T2& right) const
	{
		return MemberTypeComparator::operator()(
			mem_extractor::operator()(left),
			mem_extractor::operator()(right));
	}
	template<class T1, class T2>
	int compare(const T1& left, const T2& right) const
	{
		return MemberTypeComparator::compare(
			mem_extractor::operator()(left),
			mem_extractor::operator()(right));
	}
};
template< class Class
		, class MemberType, MemberType Class::*MemberPtr
		, class MemberTypeComparator
		>
struct HasTriCompare<CompareMember<Class, MemberType, MemberPtr, MemberTypeComparator> >
	BOOST_TT_AUX_BOOL_C_BASE(HasTriCompare<MemberTypeComparator>::value)
{
    BOOST_TT_AUX_BOOL_TRAIT_VALUE_DECL(HasTriCompare<MemberTypeComparator>::value)
};
//-----------------------------------------------------------------------------------------

/**
  @brief 生成一个比较器(Comparator)，兼键提取(KeyExtractor)类

  使用这个宏生成的比较器可以作用在不同的对象上，只要这些对象有相同名称的成员，
  并且可以作用在类型为成员类型的对象上。

  - 假设：

    - 有 n 个类 class[1], class[2], ... class[n]，都有类型为 MemberType ，名称为 MemberName 的数据成员
	- 那么以下类型的对象可以使用该类相互比较，并且可以从这些对象中提取出 MemberType 类型的键：
		class[1] ... class[n], MemberType, 以及所有这些类型的任意级别的指针

  @param ComparatorName 比较器类的名字
  @param MemberType     要比较的对象的成员类型
  @param MemberName     要比较的对象的成员名字，也可以是一个成员函数调用，
                        前面必须加 '.' 或者 '->', 加 '->' 只是为用于 smart_ptr/iterator/proxy 等重载 '->' 的对象
						当用于裸指针时，仍使用 '.'，这意味着裸指针和 smart_ptr/iterator/proxy
						不能使用同一个生成的 Comparator，虽然裸指针的语法和它们都相同
  @param ComparePred    比较准则，这个比较准则将被应用到 XXXX MemberName

  @note
    - 这个类不是从 ComparePred 继承，为的是可以允许 ComparePred 是个函数，
	  但这样（不继承）阻止了编译器进行空类优化
    - 不在内部使用 const MemberType&, 而是直接使用 MemberType,
	  是为了允许 MemberName 是一个函数时，返回一个即时计算出来的 Key；
	  - 当为了效率需要使用引用时，将 const MemberType& 作为 MemberType 传进来
    - 当 MemberType 是个指针时，将 Type* 作为 MemberType ，而非 const Type*，即使 MemberType 真的是 const Type*
	- 注意 C++ 参数推导机制：
	@code
	  template<T> void f(const T& x) { } // f1
	  template<T> void f(const T* x) { } // f2
	  template<T> void g(const T& x) { } // g1
	  template<T> void g(const T* x) { } // g2
	  template<T> void g(      T& x) { } // g3
	  template<T> void g(      T* x) { } // g4
	  void foo()
	  {
		 int a;
		 const int b;
		 f(&a); // call f1, T was deduced as int*, and then convert to 'const int*&', so match f1, not f2
		 f(&b); // call f2, T was deduced as int
		 g(&a); // call g4, T was deduced as int
		 g(&b); // call g2, T was deduced as int
	  }
    @endcode
	  在上述代码已经表现得比较明白了，这就是要生成四个不同 deref 版本的原因
    - 为了配合上述机制，传入的 MemberType 不要有任何 const 修饰符
 */
#define SAME_NAME_MEMBER_COMPARATOR_EX(ComparatorName, MemberType, MemberRef, MemberName, ComparePred)	\
class ComparatorName										\
{															\
	ComparePred m_comp;										\
public:														\
	typedef bool		result_type;						\
	typedef MemberType  key_type;							\
	typedef boost::integral_constant<bool,					\
		nark::HasTriCompare<ComparePred>::value			\
	> has_tri_compare;										\
															\
	ComparatorName()  {}									\
	ComparatorName(const ComparePred& rhs)					\
		: m_comp(rhs) {}									\
															\
	template<class T>const T&deref(T*x)const{return deref(*x);}\
	template<class T>const T&deref(T&x)const{return x;}		\
	template<class T>const T&deref(const T*x)const{return deref(*x);}\
	template<class T>const T&deref(const T&x)const{return x;}\
															\
	MemberRef operator()(MemberRef x)const{return x;}		\
	template<class T>MemberRef operator()(const T&x)const{return deref(x)MemberName;}\
															\
	template<class Tx, class Ty>							\
	bool operator()(const Tx&x, const Ty&y) const			\
	{														\
		return m_comp((*this)(x),(*this)(y));				\
	}														\
	template<class Tx, class Ty>							\
	int compare(const Tx&x, const Ty&y) const				\
	{														\
		return SAME_MEMBER_COMPARATOR_compare(x,y);			\
	}														\
};
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#if defined(__GNUC__) && __GNUC__ >= 3
#  define SAME_MEMBER_COMPARATOR_compare(x,y) EffectiveCompare(m_comp,(*this)(x),(*this)(y))
#else
#  define SAME_MEMBER_COMPARATOR_compare(x,y) m_comp.compare((*this)(x),(*this)(y))
#endif
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//! @note if MemberType must not be a reference, neither const nor non-const
#define SAME_NAME_MEMBER_COMPARATOR(ComparatorName, MemberType, MemberName)	\
	SAME_NAME_MEMBER_COMPARATOR_EX(ComparatorName, MemberType, const MemberType, MemberName, std::less<MemberType>)

//! compare unspecified::first, normally std::pair<...>
template< class FirstType
		, class Compare = std::less<typename boost::remove_reference<FirstType>::type>
//		, class Compare = std::less<FirstType>
		>
SAME_NAME_MEMBER_COMPARATOR_EX(CompareFirst, FirstType, const FirstType&, .first, Compare)

//! compare unspecified::second, normally std::pair<...>
template< class SecondType
		, class Compare = std::less<typename boost::remove_reference<SecondType>::type>
//		, class Compare = std::less<SecondType>
		>
SAME_NAME_MEMBER_COMPARATOR_EX(CompareSecond, SecondType, const SecondType&, .second, Compare)

//! compare unspecified::template get<Index>(), normally boost::tuples::tuple<...>
//! ElemType can be const-ref
template<class ElemType
		, int Index
		, class Compare = std::less<typename boost::remove_reference<ElemType>::type>
//		, class Compare = std::less<ElemType>
		>
SAME_NAME_MEMBER_COMPARATOR_EX(CompareTuple, ElemType, const ElemType&, .template get<Index>(), Compare)

//////////////////////////////////////////////////////////////////////////

template<class Compare>
class CompareAnyFirst
{
	Compare m_comp;

public:
	CompareAnyFirst(const Compare& comp = Compare()) : m_comp(comp) {}

	template<class T1, class T2>
	const T1& operator()(const std::pair<T1,T2>& x) const
	{
		return x.first;
	}
	template<class T>
	const T& operator()(const T& x) const
	{
		return x;
	}

	template<class T1, class T2>
	bool operator()(const T1& x, const T2& y) const
	{
		return m_comp(operator()(x), operator()(y));
	}

	template<class T1, class T2>
	int compare(const T1& x, const T2& y) const
	{
		return m_comp.compare(operator()(x), operator()(y));
	}

	typedef boost::integral_constant<bool, HasTriCompare<Compare>::value> has_tri_compare;
};

template<class Compare>
class CompareAnySecond
{
	Compare m_comp;

public:
	CompareAnySecond(const Compare& comp = Compare()) : m_comp(comp) {}

	template<class T1, class T2>
	const T1& operator()(const std::pair<T1,T2>& x) const
	{
		return x.second;
	}
	template<class T>
	const T& operator()(const T& x) const
	{
		return x;
	}

	template<class T1, class T2>
	bool operator()(const T1& x, const T2& y) const
	{
		return m_comp(operator()(x), operator()(y));
	}

	template<class T1, class T2>
	int compare(const T1& x, const T2& y) const
	{
		return m_comp.compare(operator()(x), operator()(y));
	}

	typedef boost::integral_constant<bool, HasTriCompare<Compare>::value> has_tri_compare;
};

//////////////////////////////////////////////////////////////////////////

template<class KeyExtractor, class KeyCompare>
class ExtractCompare
{
	KeyExtractor m_extractor;
	KeyCompare   m_comp;

public:
	ExtractCompare(const KeyExtractor& ext = KeyExtractor(),
					const KeyCompare& comp = KeyCompare())
		: m_extractor(ext), m_comp(comp) {}

	template<class T1, class T2>
	bool operator()(const T1& t1, const T2& t2) const
	{
		return m_comp(m_extractor(t1), m_extractor(t2));
	}
	template<class T1, class T2>
	int compare(const T1& t1, const T2& t2) const
	{
		return m_comp.compare(m_extractor(t1), m_extractor(t2));
	}
	typedef typename HasTriCompare<KeyCompare>::type has_tri_compare;
};
template<class KeyExtractor, class KeyCompare>
struct HasTriCompare<ExtractCompare<KeyExtractor, KeyCompare> >
	BOOST_TT_AUX_BOOL_C_BASE(HasTriCompare<KeyCompare>::value)
{
    BOOST_TT_AUX_BOOL_TRAIT_VALUE_DECL(HasTriCompare<KeyCompare>::value)
};

template<class Compare1, class Compare2>
class JoinCompare2 : private Compare1, private Compare2
{
public:
	typedef boost::integral_constant<bool,
		HasTriCompare<Compare1>::value ||
		HasTriCompare<Compare2>::value
	>
	has_tri_compare;

	JoinCompare2() {}
	JoinCompare2(const Compare1& c1, const Compare2& c2)
		: Compare1(c1), Compare2(c2) {}

	template<class T1, class T2>
	bool operator()(const T1& x, const T2& y) const
	{
		return comp_aux(x, y, has_tri_compare());
	}

	template<class T1, class T2>
	int compare(const T1& x, const T2& y) const
	{
		int cmp = EffectiveCompare(static_cast<const Compare1&>(*this), x, y);
		if (cmp != 0)
			return cmp;
		else
			return EffectiveCompare(static_cast<const Compare2&>(*this), x, y);
	}

private:
	template<class T1, class T2>
	bool comp_aux(const T1& x, const T2& y, boost::mpl::true_ hasTriComp) const
	{
		return compare(x, y) < 0;
	}
	template<class T1, class T2>
	bool comp_aux(const T1& x, const T2& y, boost::mpl::false_ hasTriComp) const
	{
		if (Compare1::operator()(x, y))
			return true;
		else if (Compare1::operator()(y, x))
			return false;
		else
			return Compare2::operator()(x, y);
	}
};

template<class Compare1, class Compare2, class Compare3>
class JoinCompare3 : public JoinCompare2<Compare1, JoinCompare2<Compare2, Compare3> >
{
public:
	JoinCompare3() {}
	JoinCompare3(const Compare1& c1, const Compare2& c2, const Compare3& c3)
		: JoinCompare2<Compare1, JoinCompare2<Compare2, Compare3> >
		( c1, JoinCompare2<Compare2, Compare3>(c2, c3) )
	{}
};

template<class Compare1, class Compare2, class Compare3, class Compare4>
class JoinCompare4 : public JoinCompare2<Compare1, JoinCompare3<Compare2, Compare3, Compare4> >
{
public:
	JoinCompare4() {}
	JoinCompare4(const Compare1& c1, const Compare2& c2, const Compare3& c3, const Compare4& c4)
		: JoinCompare2<Compare1, JoinCompare3<Compare2, Compare3, Compare4> >
		( c1, JoinCompare3<Compare2, Compare3, Compare4>(c2, c3, c4) )
	{}
};

} // namespace nark

#include <boost/type_traits/detail/bool_trait_undef.hpp>

#endif // __nark_compare_hpp__

