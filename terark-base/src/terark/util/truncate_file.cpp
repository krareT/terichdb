#include "truncate_file.hpp"
#include <terark/util/autoclose.hpp>
#include <terark/util/throw.hpp>

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <string.h>

namespace terark {

TERARK_DLL_EXPORT
void truncate_file(const char* fpath, unsigned long long size) {
#ifdef _MSC_VER
	Auto_close_fd fd(::_open(fpath, O_CREAT|O_BINARY|O_RDWR, 0644));
#else
	Auto_close_fd fd(::open(fpath, O_CREAT|O_RDWR, 0644));
#endif
	if (fd < 0) {
		THROW_STD(logic_error
			, "FATAL: ::open(%s, O_CREAT|O_BINARY|O_RDWR) = %s"
			, fpath, strerror(errno));
	}
#ifdef _MSC_VER
	int err = ::_chsize_s(fd, size);
	if (err) {
		THROW_STD(logic_error, "FATAL: ::_chsize_s(%s, %lld) = %s"
			, fpath, size, strerror(errno));
	}
#else
	int err = ::ftruncate(fd, size);
	if (err) {
		THROW_STD(logic_error, "FATAL: ::truncate(%s, %lld) = %s"
			, fpath, size, strerror(errno));
	}
#endif
}

} // namespace terark
