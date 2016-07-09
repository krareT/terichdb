#ifndef __terark_num_to_str_hpp__
#define __terark_num_to_str_hpp__

#include <string>
#include <string.h> // for strlen

#include "config.hpp"
#include "fstring.hpp"

namespace terark {

TERARK_DLL_EXPORT int num_to_str(char* buf, bool x);
TERARK_DLL_EXPORT int num_to_str(char* buf, short x);
TERARK_DLL_EXPORT int num_to_str(char* buf, int x);
TERARK_DLL_EXPORT int num_to_str(char* buf, long x);
TERARK_DLL_EXPORT int num_to_str(char* buf, long long x);
TERARK_DLL_EXPORT int num_to_str(char* buf, unsigned short x);
TERARK_DLL_EXPORT int num_to_str(char* buf, unsigned int x);
TERARK_DLL_EXPORT int num_to_str(char* buf, unsigned long x);
TERARK_DLL_EXPORT int num_to_str(char* buf, unsigned long long x);

TERARK_DLL_EXPORT int num_to_str(char* buf, float x);
TERARK_DLL_EXPORT int num_to_str(char* buf, double x);
TERARK_DLL_EXPORT int num_to_str(char* buf, long double x);

template<class String = std::string>
struct string_appender : public String {
	const String& str() const { return *this; }

	string_appender& operator<<(const fstring x) { this->append(x.data(), x.size()); return *this; }
	string_appender& operator<<(const char*   x) { this->append(x, strlen(x)); return *this; }
	string_appender& operator<<(const char    x) { this->push_back(x); return *this; }
	string_appender& operator<<(const bool    x) { this->push_back(x ? '1' : '0'); return *this; }

	string_appender& operator<<(short x) { char buf[16]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(int x) { char buf[32]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(long x) { char buf[48]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(long long x) { char buf[64]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(unsigned short x) { char buf[16]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(unsigned int x) { char buf[32]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(unsigned long x) { char buf[48]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(unsigned long long x) { char buf[64]; this->append(buf, num_to_str(buf, x)); return *this; };

	string_appender& operator<<(float x) { char buf[96]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(double x) { char buf[96]; this->append(buf, num_to_str(buf, x)); return *this; };
	string_appender& operator<<(long double x) { char buf[96]; this->append(buf, num_to_str(buf, x)); return *this; };
};

} // namespace terark

#endif // __terark_num_to_str_hpp__

