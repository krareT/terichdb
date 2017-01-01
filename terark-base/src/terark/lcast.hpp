#ifndef __terark_lcast_hpp_penglei__
#define __terark_lcast_hpp_penglei__

#include <string>
#include <terark/fstring.hpp>

namespace terark {

/// lcast_from_str must be an expiring object
class TERARK_DLL_EXPORT lcast_from_str {
	const char* p;
	size_t      n; // n maybe not used

	explicit lcast_from_str(const char* p1, size_t n1) : p(p1), n(n1) {}
	lcast_from_str(const lcast_from_str& y) : p(y.p), n(y.n) {}
	lcast_from_str& operator=(const lcast_from_str& y);

public:
	template<class T> T cast() const { return static_cast<T>(*this); }

	operator bool() const;
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

	operator const std::string() const { return std::string(p, n); }

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

TERARK_DLL_EXPORT std::string lcast(char x);
TERARK_DLL_EXPORT std::string lcast(signed char x);
TERARK_DLL_EXPORT std::string lcast(unsigned char x);
TERARK_DLL_EXPORT std::string lcast(int x);
TERARK_DLL_EXPORT std::string lcast(unsigned int x);
TERARK_DLL_EXPORT std::string lcast(short x);
TERARK_DLL_EXPORT std::string lcast(unsigned short x);
TERARK_DLL_EXPORT std::string lcast(long x);
TERARK_DLL_EXPORT std::string lcast(unsigned long x);
TERARK_DLL_EXPORT std::string lcast(long long x);
TERARK_DLL_EXPORT std::string lcast(unsigned long long x);
TERARK_DLL_EXPORT std::string lcast(float x);
TERARK_DLL_EXPORT std::string lcast(double x);
TERARK_DLL_EXPORT std::string lcast(long double x);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
// HEX Lexical cast

/// lcast_from_str must be an expiring object
class TERARK_DLL_EXPORT hexlcast_from_str {
	const char* p;
	size_t      n; // n maybe not used

	explicit hexlcast_from_str(const char* p1, size_t n1) : p(p1), n(n1) {}
	hexlcast_from_str(const hexlcast_from_str& y) : p(y.p), n(y.n) {}
	hexlcast_from_str& operator=(const hexlcast_from_str& y);

public:
	template<class T> T cast() const { return static_cast<T>(*this); }

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

	operator const std::string() const { return std::string(p, n); }

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

TERARK_DLL_EXPORT std::string hexlcast(char x);
TERARK_DLL_EXPORT std::string hexlcast(signed char x);
TERARK_DLL_EXPORT std::string hexlcast(unsigned char x);
TERARK_DLL_EXPORT std::string hexlcast(int x);
TERARK_DLL_EXPORT std::string hexlcast(unsigned int x);
TERARK_DLL_EXPORT std::string hexlcast(short x);
TERARK_DLL_EXPORT std::string hexlcast(unsigned short x);
TERARK_DLL_EXPORT std::string hexlcast(long x);
TERARK_DLL_EXPORT std::string hexlcast(unsigned long x);
TERARK_DLL_EXPORT std::string hexlcast(long long x);
TERARK_DLL_EXPORT std::string hexlcast(unsigned long long x);

TERARK_DLL_EXPORT size_t hex_decode(const char* hex, size_t hexlen, void* databuf, size_t bufsize);

///@note size of hexbuf must at least (2*datalen)
TERARK_DLL_EXPORT void hex_encode(const void* data, size_t datalen, char* hexbuf);
inline void hex_encode(fstring data, char* hexbuf) { hex_encode(data.data(), data.size(), hexbuf); }

TERARK_DLL_EXPORT std::string hex_encode(const void* data, size_t datalen);
inline std::string hex_encode(fstring data) { return hex_encode(data.data(), data.size()); }

} // namespace terark

#endif // __terark_lcast_hpp_penglei__

