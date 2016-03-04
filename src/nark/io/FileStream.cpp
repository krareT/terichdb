/* vim: set tabstop=4 : */
#include "FileStream.hpp"

#include <assert.h>
#include <string.h>

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#else
#	include <unistd.h>
#endif

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <stdexcept>
#include "byte_io_impl.hpp"
#include "var_int.hpp"

#include <nark/num_to_str.hpp>
#include <nark/util/autofree.hpp>

namespace nark {

FileStream::FileStream(const char* fpath, const char* mode)
{
	m_fp = NULL;
   	open(fpath, mode);
}

FileStream::FileStream(int fd, const char* mode)
{
	dopen(fd, mode);
}

FileStream::~FileStream() {
	if (m_fp)
	   	::fclose(m_fp);
}

void FileStream::ThrowOpenFileException(const char* fpath, const char* mode)
{
	std::string errMsg = strerror(errno);
	string_appender<> oss;
	oss << "mode=" << mode << ", errMsg: " << errMsg;
	throw OpenFileException(fpath, oss.str().c_str());
}

// only can call on unopened FileStream
void FileStream::open(const char* fpath, const char* mode)
{
	assert(NULL == m_fp);
	m_fp = fopen(fpath, mode);
	if (NULL == m_fp)
		ThrowOpenFileException(fpath, mode);
}

bool FileStream::xopen(const char* fpath, const char* mode) throw()
{
	assert(NULL == m_fp);
	m_fp = fopen(fpath, mode);
	return NULL != m_fp;
}

void FileStream::dopen(int fd, const char* mode) throw()
{
	assert(NULL == m_fp);
#ifdef _MSC_VER
	m_fp = ::_fdopen(fd, mode);
#else
	m_fp = ::fdopen(fd, mode);
#endif
	if (NULL == m_fp)
	{
		char szbuf[64];
		sprintf(szbuf, "fd=%d", fd);
		ThrowOpenFileException(szbuf, mode);
	}
}

void FileStream::attach(::FILE* fp) throw()
{
	assert(NULL == m_fp);
	this->m_fp = fp;
}

FILE* FileStream::detach() throw()
{
	assert(m_fp);
	FILE* temp = m_fp;
	m_fp = NULL;
	return temp;
}

void FileStream::close() throw()
{
	assert(m_fp);
	fclose(m_fp);
	m_fp = NULL;
}

stream_position_t FileStream::tell() const
{
	assert(m_fp);
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	fpos_t pos;
	if (fgetpos(m_fp, &pos) != 0)
		throw IOException(BOOST_CURRENT_FUNCTION);
	return stream_position_t(pos);
#else
	return (size_t)::ftell(m_fp);
#endif
}

void FileStream::rewind()
{
	assert(m_fp);
	::rewind(m_fp);
}

void FileStream::seek(stream_offset_t offset, int origin)
{
	assert(m_fp);
	if (::fseek(m_fp, offset, origin) != 0)
		throw IOException(BOOST_CURRENT_FUNCTION);
}

void FileStream::seek(stream_position_t pos)
{
	assert(m_fp);
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	fpos_t x_fpos = pos;
	if (::fsetpos(m_fp, &x_fpos) != 0)
		throw IOException(BOOST_CURRENT_FUNCTION);
#else
	seek(long(pos), 0);
#endif
}

void FileStream::flush()
{
	assert(m_fp);
	if (::fflush(m_fp) == EOF)
		throw DelayWriteException(BOOST_CURRENT_FUNCTION);
}

stream_position_t FileStream::size() const
{
	assert(m_fp);
#if (defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)) && !defined(_POSIX_)
	int fno = ::_fileno(m_fp);
	return ::_filelength(fno);
#else
	struct stat x;
	if (::fstat(fileno(m_fp), &x) == 0)
		return x.st_size;
	else
		throw IOException("fstat failed");
#endif
}

size_t FileStream::read(void* buf, size_t size) throw()
{
	assert(m_fp);
	return ::fread(buf, 1, size, m_fp);
}

size_t FileStream::write(const void* buf, size_t size) throw()
{
	assert(m_fp);
	return ::fwrite(buf, 1, size, m_fp);
}

#if defined(__GLIBC__) || defined(_MSC_VER) && _MSC_VER <= 1800

byte FileStream::readByte_slow() throw(EndOfFileException)
{
	assert(m_fp);
#ifdef __USE_MISC
	int ch = fgetc_unlocked(m_fp);
#else
	int ch = fgetc(m_fp);
#endif
	if (-1 == ch)
		throw EndOfFileException(BOOST_CURRENT_FUNCTION);
	return ch;
}

void FileStream::writeByte_slow(byte b)
{
	assert(m_fp);
#ifdef __USE_MISC
	if (EOF == fputc_unlocked(b, m_fp))
#else
	if (EOF == fputc(b, m_fp))
#endif
		throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
}

void FileStream::ensureRead_slow(void* vbuf, size_t length) {
	assert(m_fp);
	size_t n = ::fread(vbuf, 1, length, m_fp);
	if (n != length) {
		throw EndOfFileException(BOOST_CURRENT_FUNCTION);
	}
}

void FileStream::ensureWrite_slow(const void* vbuf, size_t length) {
	assert(m_fp);
	size_t n = ::fwrite(vbuf, 1, length, m_fp);
	if (n != length) {
		throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
	}
}

#else

NARK_GEN_ensureRead (FileStream::)
NARK_GEN_ensureWrite(FileStream::)

#endif

// var_int read/write
#if defined(__GLIBC__)

#define m_pos (unsigned char*&)(m_fp->_IO_read_ptr)
#define buf_remain_bytes reader_remain_bytes
#define STREAM_READER FileStream
#include "var_int_io.hpp"
#undef m_pos
#undef buf_remain_bytes

#define m_pos (unsigned char*&)(m_fp->_IO_write_ptr)
#define buf_remain_bytes writer_remain_bytes
#define STREAM_WRITER FileStream
#include "var_int_io.hpp"
#undef m_pos
#undef buf_remain_bytes

#elif defined(_MSC_VER) && _MSC_VER <= 1800

/* Always compile this module for speed, not size */
#pragma optimize("t", on)
#include "var_int_inline.hpp"

uint32_t FileStream::read_var_uint32()
{
	FILE* fp = m_fp;
	if (nark_likely(fp->_cnt >= 5))
	{
		unsigned char* p = (unsigned char*)(fp->_ptr);
		uint32_t v = gg_load_var_uint<uint32_t>(p, (const unsigned char**)&fp->_ptr, BOOST_CURRENT_FUNCTION);
		fp->_cnt -= (unsigned char*)(fp->_ptr) - p;
		return v;
	}
	else // slower branch
   	{
		return gg_load_var_uint_slow<FileStream, uint32_t>(*this, BOOST_CURRENT_FUNCTION);
	}
}

uint64_t FileStream::read_var_uint64()
{
	FILE* fp = m_fp;
	if (nark_likely(fp->_cnt >= 10))
	{
		unsigned char* p = (unsigned char*)(fp->_ptr);
		uint64_t v = gg_load_var_uint<uint64_t>(p, (const unsigned char**)&fp->_ptr, BOOST_CURRENT_FUNCTION);
		fp->_cnt -= (unsigned char*)(fp->_ptr) - p;
		return v;
	}
	else // slower branch
   	{
		return gg_load_var_uint_slow<FileStream, uint64_t>(*this, BOOST_CURRENT_FUNCTION);
	}
}

int32_t FileStream::read_var_int32()
{
	return var_int32_u2s(read_var_uint32());
}

int64_t FileStream::read_var_int64()
{
	return var_int64_u2s(read_var_uint64());
}

///////////////////////////////////////////////////////////////////////////////////////////

uint32_t FileStream::read_var_uint30()
{
	FILE* fp = m_fp;
	if (nark_likely(fp->_cnt >= 4))
	{
		unsigned char* p = (unsigned char*)(fp->_ptr);
		uint32_t v = gg_load_var_uint30(p, (const unsigned char**)&fp->_ptr);
		fp->_cnt -= (unsigned char*)(fp->_ptr) - p;
		return v;
	}
	else // slower branch
	{
		return gg_load_var_uint30_slow<FileStream>(*this);
	}
}

uint64_t FileStream::read_var_uint61()
{
	FILE* fp = m_fp;
	if (nark_likely(fp->_cnt >= 8))
	{
		unsigned char* p = (unsigned char*)(fp->_ptr);
		uint64_t v = gg_load_var_uint61(p, (const unsigned char**)&fp->_ptr);
		fp->_cnt -= (unsigned char*)(fp->_ptr) - p;
		return v;
	}
	else // slower branch
	{
		return gg_load_var_uint61_slow<FileStream>(*this);
	}
}

int32_t FileStream::read_var_int30()
{
	return var_int30_u2s(read_var_uint30());
}

int64_t FileStream::read_var_int61()
{
	return var_int61_u2s(read_var_uint61());
}

void FileStream::read_string(std::string& str)
{
	size_t len = read_var_uint32();
	str.resize(len);
	if (nark_likely(len))
		ensureRead(&str[0], len);
}

void FileStream::write_var_uint32(uint32_t x)
{
	FILE* fp = m_fp;
	if (fp->_cnt >= 5)
	{
		unsigned char* begp = (unsigned char*)fp->_ptr;
		unsigned char* endp = gg_save_var_uint<uint32_t>(begp, x);
		assert(endp - begp <= 5);
		fp->_cnt -= endp - begp;
		fp->_ptr  = (char*)endp;
	}
	else
   	{
		unsigned char tmpbuf[5];
		ptrdiff_t len = gg_save_var_uint<uint32_t>(tmpbuf, x) - tmpbuf;
		assert(len <= 5);
		ensureWrite(tmpbuf, len);
	}
}

void FileStream::write_var_uint64(uint64_t x)
{
	FILE* fp = m_fp;
	if (fp->_cnt >= 10)
   	{
		unsigned char* begp = (unsigned char*)fp->_ptr;
		unsigned char* endp = gg_save_var_uint<uint64_t>(begp, x);
		fp->_cnt -= endp - begp;
		fp->_ptr  = (char*)endp;
	}
	else {
		unsigned char tmpbuf[10];
		ptrdiff_t len = gg_save_var_uint<uint64_t>(tmpbuf, x) - tmpbuf;
		assert(len <= 10);
		ensureWrite(tmpbuf, len);
	}
}

void FileStream::write_var_int32(int32_t x)
{
	write_var_uint32(var_int32_s2u(x));
}

void FileStream::write_var_int64(int64_t x)
{
	write_var_uint64(var_int64_s2u(x));
}

///////////////////////////////////////////////////////////////////////////////////////////

void FileStream::write_var_uint30(uint32_t x)
{
	FILE* fp = m_fp;
	if (fp->_cnt >= 4)
	{
		unsigned char* begp = (unsigned char*)fp->_ptr;
		unsigned char* endp = gg_save_var_uint30(begp, x);
		assert(endp - begp <= 4);
		fp->_cnt -= endp - begp;
		fp->_ptr  = (char*)endp;
	}
	else
	{
		unsigned char tmpbuf[4];
		ptrdiff_t len = gg_save_var_uint30(tmpbuf, x) - tmpbuf;
		assert(len <= 4);
		ensureWrite(tmpbuf, len);
	}
}

void FileStream::write_var_uint61(uint64_t x)
{
	FILE* fp = m_fp;
	if (fp->_cnt >= 8)
	{
		unsigned char* begp = (unsigned char*)fp->_ptr;
		unsigned char* endp = gg_save_var_uint61(begp, x);
		assert(endp - begp <= 8);
		fp->_cnt -= endp - begp;
		fp->_ptr  = (char*)endp;
	}
	else {
		unsigned char tmpbuf[8];
		ptrdiff_t len = gg_save_var_uint61(tmpbuf, x) - tmpbuf;
		assert(len <= 8);
		ensureWrite(tmpbuf, len);
	}
}

void FileStream::write_var_int30(int32_t x)
{
	write_var_uint30(var_int30_s2u(x));
}

void FileStream::write_var_int61(int64_t x)
{
	write_var_uint61(var_int61_s2u(x));
}


void FileStream::write_string(const std::string& str)
{
	write_var_uint32(str.size());
	ensureWrite(str.data(), str.size());
}

#if 0
/**
 * manually write a string
 *
 * only for writing, no corresponding read method.
 * compitible with `write_string(const std::string& str)`.
 */
void FileStream::write_string(const char* str, size_t len)
{
	write_var_uint32(len);
	ensureWrite(str, len);
}
#endif // 0

#endif // _MSC_VER

void FileStream::disbuf() throw()
{
	assert(m_fp);
	setvbuf(m_fp, NULL, _IONBF, 0);
}

/**
 * not reentrent
 */
size_t FileStream::pread(stream_position_t pos, void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t n = this->read(vbuf, length);
	this->seek(old);
	return n;
}

/**
 * not reentrent
 */
size_t FileStream::pwrite(stream_position_t pos, const void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t n = this->write(vbuf, length);
	this->seek(old);
	return n;
}

NonOwnerFileStream::~NonOwnerFileStream() {
   	m_fp = NULL;
}

//////////////////////////////////////////////////////////////////////
bool FileStream::copyFile(const char* srcPath, const char* dstPath)
{
	FileStream fsrc(srcPath, "rb");
	FileStream fdst(dstPath, "wb+");

	if (fsrc && fdst)
	{
		setvbuf(fsrc.fp(), NULL, _IONBF, 0);
		setvbuf(fdst.fp(), NULL, _IONBF, 0);
		size_t nbuf = 64 * 1024;
		AutoFree<char> pbuf(nbuf);
		while (!fsrc.eof())
		{
			size_t nRead  = fsrc.read(pbuf, nbuf);
			size_t nWrite = fdst.write(pbuf, nRead);
			if (nWrite != nRead) {
				throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
			}
		}
		return true;
	}
	return false;
}


} // namespace nark

