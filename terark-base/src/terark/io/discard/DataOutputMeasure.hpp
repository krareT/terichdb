/* vim: set tabstop=4 : */

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4127)
#endif

#include "DataOutput.hpp"

#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace terark {

inline unsigned int sizeof_int(char x) { return 1; }
inline unsigned int sizeof_int(signed char x) { return 1; }
inline unsigned int sizeof_int(unsigned char x) { return 1; }

inline unsigned int sizeof_int(uint16_t x) { return 2; }
inline unsigned int sizeof_int(uint32_t x) { return 4; }
inline unsigned int sizeof_int(uint64_t x) { return 8; }

inline unsigned int sizeof_int(int16_t x) { return 2; }
inline unsigned int sizeof_int(int32_t x) { return 4; }
inline unsigned int sizeof_int(int64_t x) { return 8; }

inline unsigned int sizeof_int(var_uint32_t x)
{
	if (x.t < uint32_t(1)<< 7) return 1;
	if (x.t < uint32_t(1)<<14) return 2;
	if (x.t < uint32_t(1)<<21) return 3;
	if (x.t < uint32_t(1)<<28) return 4;
	return 5;
}

inline unsigned int sizeof_int(var_uint64_t x)
{
	if (x.t < uint64_t(1)<< 7) return 1;
	if (x.t < uint64_t(1)<<14) return 2;
	if (x.t < uint64_t(1)<<21) return 3;
	if (x.t < uint64_t(1)<<28) return 4;
	if (x.t < uint64_t(1)<<35) return 5;
	if (x.t < uint64_t(1)<<42) return 6;
	if (x.t < uint64_t(1)<<49) return 7;
	if (x.t < uint64_t(1)<<56) return 8;
	return 9;
}

inline unsigned int sizeof_int(var_int32_t x)
{
	if (x.t >= 0)
		return sizeof_int(var_uint32_t(x.t << 1));
	else
		return sizeof_int(var_uint32_t(-x.t << 1));
}

inline unsigned int sizeof_int(var_int64_t x)
{
	if (x.t >= 0)
		return sizeof_int(var_uint64_t(x.t << 1));
	else
		return sizeof_int(var_uint64_t(-x.t << 1));
}


class TestFixedSizeOutput
{
public:
	bool isFixed;

	TestFixedSizeOutput() : isFixed(true) {}

	template<class T> TestFixedSizeOutput& operator<<(const T& x)
	{
		DataIO_saveObject(*this, x);
		return *this;
	}
	template<class T> TestFixedSizeOutput& operator &(const T& x) {	return *this << x; }

#define DATA_IO_GEN_IsFixedSize(type) \
	TestFixedSizeOutput& operator &(const type&) { return *this; } \
	TestFixedSizeOutput& operator<<(const type&) { return *this; }

	DATA_IO_GEN_IsFixedSize(float)
	DATA_IO_GEN_IsFixedSize(double)
	DATA_IO_GEN_IsFixedSize(long double)

	DATA_IO_GEN_IsFixedSize(char)
	DATA_IO_GEN_IsFixedSize(unsigned char)
	DATA_IO_GEN_IsFixedSize(signed char)
	DATA_IO_GEN_IsFixedSize(wchar_t)

	DATA_IO_GEN_IsFixedSize(int)
	DATA_IO_GEN_IsFixedSize(unsigned int)
	DATA_IO_GEN_IsFixedSize(short)
	DATA_IO_GEN_IsFixedSize(unsigned short)
	DATA_IO_GEN_IsFixedSize(long)
	DATA_IO_GEN_IsFixedSize(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	DATA_IO_GEN_IsFixedSize(long long)
	DATA_IO_GEN_IsFixedSize(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	DATA_IO_GEN_IsFixedSize(__int64)
	DATA_IO_GEN_IsFixedSize(unsigned __int64)
#endif

	template<class T> TestFixedSizeOutput& operator&(pass_by_value<T> x) { return *this << x.val; }
	template<class T> TestFixedSizeOutput& operator&(reference_wrapper<T> x) { return *this << x.get(); }

	template<class T> TestFixedSizeOutput& operator<<(pass_by_value<T> x) { return *this << x.val; }
	template<class T> TestFixedSizeOutput& operator<<(reference_wrapper<T> x) { return *this << x.get(); }

	template<class T> TestFixedSizeOutput& operator&(const T* x) { return *this << *x; }
	template<class T> TestFixedSizeOutput& operator&(T* x) { return *this << *x; }

	template<class T> TestFixedSizeOutput& operator<<(const T* x) { return *this << *x; }
	template<class T> TestFixedSizeOutput& operator<<(T* x) { return *this << *x; }

	template<class T, int Dim>
	Final_Output& operator<<(const T (&x)[Dim]) { return *this << x[0]; }

	Final_Output& operator<<(const char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(const signed char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      signed char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(const unsigned char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      unsigned char* s) { isFixed = false; return *this; }

	Final_Output& operator<<(const wchar_t* s){ isFixed = false; return *this; }
	Final_Output& operator<<(      wchar_t* s){ isFixed = false; return *this; }

	Final_Output& operator<<(const std::string& x){ isFixed = false; return *this; }
	Final_Output& operator<<(const std::wstring& x){ isFixed = false; return *this; }

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::vector<Elem, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::list<Elem, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::deque<Elem, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::set<Elem, Compare, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::multiset<Elem, Compare, Alloc>& x){ isFixed = false; return *this; }

	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::map<Key, Val, Compare, Alloc>& x){ isFixed = false; return *this; }

	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::multimap<Key, Val, Compare, Alloc>& x){ isFixed = false; return *this; }
};

template<class T> bool IsFixedSize(const T& x)
{
	TestFixedSizeOutput test;
	test & x;
	return test.isFixed;
}

template<class Final_Output> class DataOutputMeasureBase
{
protected:
	size_t m_size;

public:
	BOOST_STATIC_CONSTANT(bool, value = false);
	typedef boost::mpl::true_ type;

	DataOutputMeasureBase() { this->m_size = 0; }

	size_t size() const { return this->m_size(); }

	void ensureWrite(void* , size_t length) { this->m_size += length; }

	Final_Output& operator<<(serialize_version_t x) { return static_cast<Final_Output&>(*this) << var_uint32_t(x.t); }

#define TERARK_GEN_MEASURE_SIZE(type, size) \
	Final_Output& operator<<(type x) { this->m_size += size; return static_cast<Final_Output&>(*this); }

#define TERARK_GEN_MEASURE_SIZE_FUN(type) \
	Final_Output& operator<<(type x) { this->m_size += sizeof(type); return static_cast<Final_Output&>(*this); }

#define TERARK_GEN_MEASURE_VAR_INT(type) \
	Final_Output& operator<<(type x) { this->m_size += sizeof_int(x); return static_cast<Final_Output&>(*this); }

	TERARK_GEN_MEASURE_VAR_INT(var_int16_t)
	TERARK_GEN_MEASURE_VAR_INT(var_uint16_t)
	TERARK_GEN_MEASURE_VAR_INT(var_int32_t)
	TERARK_GEN_MEASURE_VAR_INT(var_uint32_t)

#if !defined(BOOST_NO_INTRINSIC_INT64_T)
	TERARK_GEN_MEASURE_VAR_INT(var_int64_t)
	TERARK_GEN_MEASURE_VAR_INT(var_uint64_t)
#endif

	TERARK_GEN_MEASURE_SIZE_FUN(char)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned char)
	TERARK_GEN_MEASURE_SIZE_FUN(signed char)

	TERARK_GEN_MEASURE_SIZE_FUN(short)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned short)

	TERARK_GEN_MEASURE_SIZE_FUN(int)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned int)
	TERARK_GEN_MEASURE_SIZE_FUN(long)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	TERARK_GEN_MEASURE_SIZE_FUN(long long)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	TERARK_GEN_MEASURE_SIZE_FUN(__int64)
	TERARK_GEN_MEASURE_SIZE_FUN(unsigned __int64)
#endif

	TERARK_GEN_MEASURE_SIZE_FUN(float)
	TERARK_GEN_MEASURE_SIZE_FUN(double)
	TERARK_GEN_MEASURE_SIZE_FUN(long double)

	Final_Output& operator<<(const char* s)
	{
		var_uint32_t n(strlen(s));
		this->m_size += sizeof_int(n) + n;
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const wchar_t* s)
	{
		var_uint32_t n(wcslen(s));
		this->m_size += sizeof_int(n) + n * sizeof(wchar_t);
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const std::string& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(n) + n.t;
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const std::wstring& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(n) + n.t * sizeof(wchar_t);
		return static_cast<Final_Output&>(*this);
	}

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::vector<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::list<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::deque<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}

	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::set<Elem, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::multiset<Elem, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::map<Key, Val, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::multimap<Key, Val, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}

protected:
	template<class Seq>
	Final_Output& measure_seq(const Seq& x)
	{
		if (x.empty())
			return 1; // 0 byte store var_uint32(x.size()=0)
		if (IsFixedSize(*x.begin()))
			measure_seq_fixed_elem(x);
		else
			measure_seq_var_elem(x);
		return static_cast<Final_Output&>(*this);
	}
	template<class Seq>
	void measure_seq_fixed_elem(const Seq& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(x) + sizeof(typename Seq::value_type) * n.t;
	}
	template<class Seq>
	void measure_seq_var_elem(const Seq& x)
	{
		var_uint32_t n(x.size());
		static_cast<Final_Output&>(*this) << n;
		for (typename Seq::const_iterator i = x.begin(); i != x.end(); ++i)
			static_cast<Final_Output&>(*this) << *i;
	}
};

class DataOutputMeasure
	: public DataOutput<DataOutputMeasureBase, DataOutputMeasure>
{
public:
};

} // namespace terark


