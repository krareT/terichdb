#ifndef __nark_lcast_hpp_penglei__
#define __nark_lcast_hpp_penglei__

#include <string>
#include <nark/fstring.hpp>

namespace nark {

/// lcast_from_str must be an expiring object
class NARK_DLL_EXPORT lcast_from_str {
	const char* p;
	size_t      n; // n maybe not used

	explicit lcast_from_str(const char* p1, size_t n1) : p(p1), n(n1) {}
	lcast_from_str(const lcast_from_str& y) : p(y.p), n(y.n) {}
	lcast_from_str& operator=(const lcast_from_str& y);

public:
	operator char() const;
	operator signed char() const;
	operator unsigned char() const;
	operator short() const;
	operator unsigned short() const;
	operator int() const;
	operator unsigned() const;
	operator long() const;
	operator unsigned long() const;
	operator long long() const;
	operator unsigned long long() const;

	operator float() const;
	operator double() const;
	operator long double() const;

	operator const std::string() const { return p; }

// non-parameter-dependent friends
friend const lcast_from_str lcast(const fstring s);
friend const lcast_from_str lcast(const std::string& s);
friend const lcast_from_str lcast(const char* s, size_t n);
friend const lcast_from_str lcast(const char* s, const char* t);
friend const lcast_from_str lcast(const char* s);

};

inline const lcast_from_str lcast(const fstring s) { return lcast_from_str(s.p, s.n); }
inline const lcast_from_str lcast(const std::string& s) { return lcast_from_str(s.data(), s.size()); }
inline const lcast_from_str lcast(const char* s, size_t n) { return lcast_from_str(s, n); }
inline const lcast_from_str lcast(const char* s, const char* t) { return lcast_from_str(s, t-s); }
inline const lcast_from_str lcast(const char* s) { return lcast_from_str(s, strlen(s)); }

NARK_DLL_EXPORT std::string lcast(char x);
NARK_DLL_EXPORT std::string lcast(signed char x);
NARK_DLL_EXPORT std::string lcast(unsigned char x);
NARK_DLL_EXPORT std::string lcast(int x);
NARK_DLL_EXPORT std::string lcast(unsigned int x);
NARK_DLL_EXPORT std::string lcast(short x);
NARK_DLL_EXPORT std::string lcast(unsigned short x);
NARK_DLL_EXPORT std::string lcast(long x);
NARK_DLL_EXPORT std::string lcast(unsigned long x);
NARK_DLL_EXPORT std::string lcast(long long x);
NARK_DLL_EXPORT std::string lcast(unsigned long long x);
NARK_DLL_EXPORT std::string lcast(float x);
NARK_DLL_EXPORT std::string lcast(double x);
NARK_DLL_EXPORT std::string lcast(long double x);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
// HEX Lexical cast

/// lcast_from_str must be an expiring object
class NARK_DLL_EXPORT hexlcast_from_str {
	const char* p;
	size_t      n; // n maybe not used

	explicit hexlcast_from_str(const char* p1, size_t n1) : p(p1), n(n1) {}
	hexlcast_from_str(const hexlcast_from_str& y) : p(y.p), n(y.n) {}
	hexlcast_from_str& operator=(const hexlcast_from_str& y);

public:
	operator char() const;
	operator signed char() const;
	operator unsigned char() const;
	operator short() const;
	operator unsigned short() const;
	operator int() const;
	operator unsigned() const;
	operator long() const;
	operator unsigned long() const;
	operator long long() const;
	operator unsigned long long() const;

	operator const std::string() const { return p; }

// non-parameter-dependent friends
friend const hexlcast_from_str hexlcast(const fstring s);
friend const hexlcast_from_str hexlcast(const std::string& s);
friend const hexlcast_from_str hexlcast(const char* s, size_t n);
friend const hexlcast_from_str hexlcast(const char* s, const char* t);
friend const hexlcast_from_str hexlcast(const char* s);

};

inline const hexlcast_from_str hexlcast(const fstring s) { return hexlcast_from_str(s.p, s.n); }
inline const hexlcast_from_str hexlcast(const std::string& s) { return hexlcast_from_str(s.data(), s.size()); }
inline const hexlcast_from_str hexlcast(const char* s, size_t n) { return hexlcast_from_str(s, n); }
inline const hexlcast_from_str hexlcast(const char* s, const char* t) { return hexlcast_from_str(s, t-s); }
inline const hexlcast_from_str hexlcast(const char* s) { return hexlcast_from_str(s, strlen(s)); }

NARK_DLL_EXPORT std::string hexlcast(char x);
NARK_DLL_EXPORT std::string hexlcast(signed char x);
NARK_DLL_EXPORT std::string hexlcast(unsigned char x);
NARK_DLL_EXPORT std::string hexlcast(int x);
NARK_DLL_EXPORT std::string hexlcast(unsigned int x);
NARK_DLL_EXPORT std::string hexlcast(short x);
NARK_DLL_EXPORT std::string hexlcast(unsigned short x);
NARK_DLL_EXPORT std::string hexlcast(long x);
NARK_DLL_EXPORT std::string hexlcast(unsigned long x);
NARK_DLL_EXPORT std::string hexlcast(long long x);
NARK_DLL_EXPORT std::string hexlcast(unsigned long long x);

} // namespace nark

#endif // __nark_lcast_hpp_penglei__

