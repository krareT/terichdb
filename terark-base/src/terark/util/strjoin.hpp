#ifndef __terark_util_strjoin_hpp__
#define __terark_util_strjoin_hpp__

#include <string>
#include <stddef.h>

template<class String>
class strjoin_helper {
private:
	String v;
	typedef typename String::value_type char_t;
	typedef strjoin_helper me;
public:
	template<class StrX> explicit strjoin_helper(const StrX& x) : v(x) {}
	template<class Char> explicit strjoin_helper(const Char* s, ptrdiff_t n) : v(s, n) {}
	operator String() const { return v; }
	me& operator+(const String& y) { v += y; return *this; }
	me& operator+(const char_t* y) { v += y; return *this; }
	me& operator+(const me  & y) { v += y.v; return *this; }
	friend me operator+(const char_t* x, const me& y) { me t(x); t.v += y.v; return t; }
	friend me operator+(const String& x, const me& y) { me t(x); t.v += y.v; return t; }
};

template<class AnyString>
strjoin_helper<AnyString> strjoin(const AnyString& x) { return strjoin_helper<AnyString>(x); }

strjoin_helper<std::string> strjoin(const char* s) { return strjoin_helper<std::string>(s); }
strjoin_helper<std::string> strjoin(const char* s, ptrdiff_t n) { return strjoin_helper<std::string>(s, n); }

strjoin_helper<std::wstring> strjoin(const wchar_t* s) { return strjoin_helper<std::wstring>(s); }
strjoin_helper<std::wstring> strjoin(const wchar_t* s, ptrdiff_t n) { return strjoin_helper<std::wstring>(s, n); }

#endif // __terark_util_strjoin_hpp__


