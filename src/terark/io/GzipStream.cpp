/* vim: set tabstop=4 : */
#include "GzipStream.hpp"

#include <assert.h>
#include <string.h>

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#   if 1 //ZLIB_VERNUM >= 0x1240
#      pragma comment(lib, "zlibwapi.lib")
#   else
#      pragma comment(lib, "zlib.lib")
#   endif
#else
#	include <unistd.h>
#	include <sys/stat.h>
#	include <sys/types.h>
#	include <fcntl.h>
#	include <errno.h>
#endif

#if defined(__CYGWIN__) || defined(__CYGWIN32__)
#   define off64_t _off64_t
#endif

//#define ZLIB_DLL
#define ZLIB_WINAPI
#include <zlib.h>
#include <stdio.h> // for sprintf and EOF

#include "byte_io_impl.hpp"

#ifndef ZLIB_VERNUM
#  error "ZLIB_VERNUM is not defined"
#endif

#include <terark/num_to_str.hpp>

namespace terark {

void GzipStreamBase::ThrowOpenFileException(const char* fpath, const char* mode)
{
	string_appender<> oss;
	oss << "mode=" << mode;
	throw OpenFileException(fpath, oss.str().c_str());
}

// only can call on unopened GzipInputStream
void GzipStreamBase::open(const char* fpath, const char* mode)
{
	assert(0 == m_fp);
	m_fp = gzopen(fpath, mode);
	if (0 == m_fp)
		ThrowOpenFileException(fpath, mode);
}

bool GzipStreamBase::xopen(const char* fpath, const char* mode)
{
	assert(0 == m_fp);
	m_fp = gzopen(fpath, mode);
	return 0 != m_fp;
}

void GzipStreamBase::dopen(int fd, const char* mode)
{
	assert(0 == m_fp);
	m_fp = gzdopen(fd, mode);
	if (0 == m_fp)
	{
		char szbuf[64];
		sprintf(szbuf, "fd=%d", fd);
		ThrowOpenFileException(szbuf, mode);
	}
}

void GzipStreamBase::close()
{
	assert(m_fp);
	gzclose((gzFile)m_fp);
	m_fp = 0;
}

GzipStreamBase::~GzipStreamBase()
{
	if (m_fp)
		gzclose((gzFile)m_fp);
}

///////////////////////////////////////////////////////////////////////////////////////

GzipInputStream::GzipInputStream(const char* fpath, const char* mode)
{
	m_fp = 0;
   	open(fpath, mode);
}

GzipInputStream::GzipInputStream(int fd, const char* mode)
{
	m_fp = 0;
   	dopen(fd, mode);
}

bool GzipInputStream::eof() const
{
	return !!gzeof((gzFile)m_fp);
}

size_t GzipInputStream::read(void* buf, size_t size) throw()
{
	assert(m_fp);
	return gzread((gzFile)m_fp, buf, size);
}

TERARK_GEN_ensureRead (GzipInputStream::)

///////////////////////////////////////////////////////

GzipOutputStream::GzipOutputStream(const char* fpath, const char* mode)
{
	m_fp = 0;
   	open(fpath, mode);
}

GzipOutputStream::GzipOutputStream(int fd, const char* mode)
{
	m_fp = 0;
   	dopen(fd, mode);
}

void GzipOutputStream::flush()
{
	assert(m_fp);
	if (gzflush((gzFile)m_fp, Z_SYNC_FLUSH) == EOF)
		throw DelayWriteException(BOOST_CURRENT_FUNCTION);
}

size_t GzipOutputStream::write(const void* buf, size_t size) throw()
{
	assert(m_fp);
	return gzwrite((gzFile)m_fp, buf, size);
}
TERARK_GEN_ensureWrite(GzipOutputStream::)

} // namespace terark

