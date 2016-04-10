#ifndef __terark_util_autoclose_hpp__
#define __terark_util_autoclose_hpp__

#include <stdio.h>
#include <boost/noncopyable.hpp>

#ifdef _MSC_VER
#include <io.h>
#else
#include <unistd.h>
#endif

namespace terark {
class Auto_fclose : boost::noncopyable {
	FILE* f;
public:
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L || \
	defined(_MSC_VER) && _MSC_VER >= 1700
	Auto_fclose(Auto_fclose&& y) {
		this->f = y.f;
		y.f = NULL;
	}
#endif
	operator FILE*() const { return f; }
	bool operator!() const { return NULL == f; }
	FILE* operator->() const { return f; } // feof(fp) maybe a macro
	explicit Auto_fclose(FILE* fp = NULL) { f = fp; }
	~Auto_fclose() { if (NULL != f) ::fclose(f); }
	void operator=(FILE* f0) { f = f0; } // disable chained assign
	FILE* self_or(FILE* f2) const { return f ? f : f2; }
};
typedef Auto_fclose Auto_close_fp;

class Auto_close_fd : boost::noncopyable {
	int f;
public:
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L || \
	defined(_MSC_VER) && _MSC_VER >= 1700
	Auto_close_fd(Auto_close_fd&& y) {
		this->f = y.f;
		y.f = -1;
	}
#endif
	operator int() const { return f; }
	bool operator!() const { return f < 0; }
	explicit Auto_close_fd(int fd = -1) { f = fd; }
	~Auto_close_fd() {
	#ifdef _MSC_VER
		if (f >= 0) ::_close(f);
	#else
		if (f >= 0) ::close(f);
	#endif
	}
	void operator=(int f0) { f = f0; } // disable chained assign
	int self_or(int f2) const { return f >= 0 ? f : f2; }
};

} // namespace terark

#endif // __terark_util_autoclose_hpp__

