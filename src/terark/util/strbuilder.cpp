// strbuilder.cpp
#include "strbuilder.hpp"

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include <stdexcept>
#include <terark/util/throw.hpp>

namespace terark {
#if defined(__GLIBC__) || defined(__CYGWIN__) || \
	defined(__DARWIN_C_LEVEL) && defined(__DARWIN_C_FULL) && __DARWIN_C_LEVEL >= __DARWIN_C_FULL
	StrPrintf::StrPrintf(const char* format, ...) {
		assert(NULL != format);
		va_list  ap;
		va_start(ap, format);
		n = vasprintf(&s, format, ap);
		va_end(ap);
		if (n < 0) {
			throw std::bad_alloc();
		}
		return;
	}
	StrPrintf::StrPrintf(std::string& dest, const char* format, ...) {
		assert(NULL != format);
		va_list  ap;
		va_start(ap, format);
		n = vasprintf(&s, format, ap);
		va_end(ap);
		if (n < 0) {
			throw std::bad_alloc();
		}
		dest.append(s, n);
	}
	StrPrintf::~StrPrintf() {
		assert(NULL != s);
		free(s);
	}
	StrPrintf::operator std::string() const {
		assert(NULL != s);
		return std::string(s, n);
	}
#endif

#if defined(__GLIBC__) || defined(__CYGWIN__)
	// StrBuilder

	StrBuilder::StrBuilder() {
		s = NULL;
		n = 0;
		memFile = open_memstream(&s, &n);
		if (NULL == memFile) {
			THROW_STD(runtime_error, "open_memstream");
		}
	}
	StrBuilder::~StrBuilder() {
		assert(NULL != memFile);
		assert(NULL != s);
		fclose(memFile);
		free(s);
	}
	StrBuilder& StrBuilder::printf(const char* format, ...) {
		assert(NULL != format);
		assert(NULL != memFile);
		va_list ap;
		va_start(ap, format);
		int rv = vfprintf(memFile, format, ap);
		va_end(ap);
		if (rv < 0) {
			THROW_STD(runtime_error, "vfprintf on memstream");
		}
		return *this;
	}
	void StrBuilder::clear() {
		assert(NULL != memFile);
		::rewind(memFile);
		this->flush();
	}
	StrBuilder& StrBuilder::flush() {
		assert(NULL != memFile);
		int rv = fflush(memFile);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
	//	assert(NULL != s);
		return *this;
	}
	const char* StrBuilder::c_str() {
		assert(NULL != memFile);
		int rv = fflush(memFile);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
		assert(NULL != s);
		return s;
	}
	StrBuilder::operator std::string() const {
		assert(NULL != memFile);
		int rv = fflush(memFile);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
		assert(NULL != s);
		return std::string(s, n);
	}
	void StrBuilder::setEof(int end_offset) {
		assert(end_offset < 0);
		assert(NULL != memFile);
		int rv = fflush(memFile);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
		s[n+end_offset] = '\0';
		rv = fseek(memFile, end_offset, SEEK_END);
		if (rv != 0) {
			THROW_STD(runtime_error, "fseek on memstream");
		}
		assert(NULL != s);
		assert((long)n + end_offset >= 0);
	}
	void StrBuilder::setEof(int end_offset, const char* endmark) {
		assert(end_offset < 0);
		assert(NULL != endmark);
		assert(NULL != memFile);
		int rv = fseek(memFile, end_offset, SEEK_END);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
		rv = fputs(endmark, memFile);
		if (EOF == rv) {
			THROW_STD(runtime_error, "fputs(endmark, memFile)");
		}
		rv = fflush(memFile);
		if (rv != 0) {
			THROW_STD(runtime_error, "fflush on memstream");
		}
		assert(NULL != s);
		assert((long)n + end_offset >= 0);
	}
#else
  #pragma message("strbuilder skiped because not in glibc")
#endif

} // namespace terark

