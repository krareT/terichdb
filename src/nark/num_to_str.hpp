#ifndef __nark_num_to_str_hpp__
#define __nark_num_to_str_hpp__

#include <string>
#include <string.h> // for strlen

#include "config.hpp"

namespace nark {

NARK_DLL_EXPORT int num_to_str(char* buf, bool x);
NARK_DLL_EXPORT int num_to_str(char* buf, short x);
NARK_DLL_EXPORT int num_to_str(char* buf, int x);
NARK_DLL_EXPORT int num_to_str(char* buf, long x);
NARK_DLL_EXPORT int num_to_str(char* buf, long long x);
NARK_DLL_EXPORT int num_to_str(char* buf, unsigned short x);
NARK_DLL_EXPORT int num_to_str(char* buf, unsigned int x);
NARK_DLL_EXPORT int num_to_str(char* buf, unsigned long x);
NARK_DLL_EXPORT int num_to_str(char* buf, unsigned long long x);

NARK_DLL_EXPORT int num_to_str(char* buf, float x);
NARK_DLL_EXPORT int num_to_str(char* buf, double x);
NARK_DLL_EXPORT int num_to_str(char* buf, long double x);

template<class String = std::string>
struct string_appender : public String {
	const String& str() const { return *this; }

	string_appender& operator<<(const String& x) { this->append(x.data(), x.size()); return *this; }
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

} // namespace nark

#endif // __nark_num_to_str_hpp__

