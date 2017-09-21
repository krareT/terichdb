/* vim: set tabstop=4 : */
#ifndef __terark_io_FileStream_h__
#define __terark_io_FileStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <stdio.h>
#include <assert.h>

#if defined(__GLIBC__)
	#include <libio.h>
#endif

#include <terark/stdtypes.hpp>
#include <terark/util/refcount.hpp>
#include <terark/fstring.hpp>
#include "IOException.hpp"
#include "IStream.hpp"

namespace terark {

/**
 @brief FileStream encapsulate FILE* as RefCounter, IInputStream, IOutputStream, ISeekable
 @note
  -# FileStream::eof maybe not thread-safe
  -# FileStream::getByte maybe not thread-safe
  -# FileStream::readByte maybe not thread-safe
  -# FileStream::writeByte maybe not thread-safe
 */
class TERARK_DLL_EXPORT FileStream
	: public RefCounter
	, public IInputStream
	, public IOutputStream
	, public ISeekable
{
	DECLARE_NONE_COPYABLE_CLASS(FileStream)
protected:
	FILE* m_fp;

public:
	typedef boost::mpl::true_ is_seekable;

	static bool copyFile(fstring srcPath, fstring dstPath);
	static void ThrowOpenFileException(fstring fpath, fstring mode);

public:
	FileStream(fstring fpath, fstring mode);
	FileStream(int fd, fstring mode);
//	explicit FileStream(FILE* fp = 0) throw() : m_fp(fp) {}
	FileStream() throw() : m_fp(0) {} // 不是我打开的文件，请显式 attach/detach
	~FileStream();

	bool isOpen() const throw() { return 0 != m_fp; }
	operator FILE*() const throw() { return m_fp; }

	void open(fstring fpath, fstring mode);

	//! no throw
	bool xopen(fstring fpath, fstring mode);

	void dopen(int fd, fstring mode);

	void close() throw();

	void attach(::FILE* fp) throw();
	FILE* detach() throw();

	FILE* fp() const throw() { return m_fp; }
#ifdef __USE_MISC
	bool eof() const throw() { return !!feof_unlocked(m_fp); }
	int  getByte() throw() { return fgetc_unlocked(m_fp); }
#else
	bool eof() const throw() { return !!feof(m_fp); }
	int  getByte() throw() { return fgetc(m_fp); }
#endif
	byte readByte();
	void writeByte(byte b);

	void ensureRead(void* vbuf, size_t length);
	void ensureWrite(const void* vbuf, size_t length);

	size_t read(void* buf, size_t size) throw();
	size_t write(const void* buf, size_t size) throw();
	void flush();
	void puts(fstring text);

	void rewind();
	void seek(stream_offset_t offset, int origin);
	void seek(stream_position_t pos);
	stream_position_t tell() const;
	stream_position_t size() const;

	size_t pread(stream_position_t pos, void* vbuf, size_t length);
	size_t pwrite(stream_position_t pos, const void* vbuf, size_t length);

	void disbuf() throw();

#if defined(__GLIBC__) || defined(_MSC_VER) && _MSC_VER <= 1800
public:
	#include "var_int_declare_read.hpp"
	#include "var_int_declare_write.hpp"
private:
#if defined(__GLIBC__)
	size_t reader_remain_bytes() const { FILE* fp = m_fp; return fp->_IO_read_end - fp->_IO_read_ptr; }
	size_t writer_remain_bytes() const { FILE* fp = m_fp; return fp->_IO_write_end - fp->_IO_write_ptr; }
#endif
	byte readByte_slow();
	void writeByte_slow(byte b);
	void ensureRead_slow(void* vbuf, size_t length);
	void ensureWrite_slow(const void* vbuf, size_t length);
#endif

public:
	uint64_t cat(FILE* fp);
	uint64_t cat(fstring fpath);
	uint64_t fsize() const;
	static uint64_t fpsize(FILE* fp);
	static uint64_t fdsize(int fd);

	void chsize(uint64_t newfsize) const;
	static void fpchsize(FILE* fp, uint64_t newfsize);
	static void fdchsize(int fd, uint64_t newfsize);
};

class TERARK_DLL_EXPORT NonOwnerFileStream : public FileStream {
public:
	explicit NonOwnerFileStream(FILE* fp) throw() { m_fp = fp; }
	NonOwnerFileStream(const char* fpath, const char* mode) : FileStream(fpath, mode) {}
	NonOwnerFileStream(int fd, const char* mode) : FileStream(fd, mode) {}
	~NonOwnerFileStream();
};

#if defined(__GLIBC__)

inline byte FileStream::readByte() {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_IO_read_ptr < fp->_IO_read_end) {
		return *(byte*)(fp->_IO_read_ptr++);
	} else {
		return readByte_slow();
	}
}

inline void FileStream::writeByte(byte b) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_IO_write_ptr < fp->_IO_write_end) {
		*fp->_IO_write_ptr++ = b;
	} else {
		writeByte_slow(b);
	}
}

inline void FileStream::ensureRead(void* vbuf, size_t length) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_IO_read_ptr + length <= fp->_IO_read_end) {
		memcpy(vbuf, fp->_IO_read_ptr, length);
		fp->_IO_read_ptr += length;
	} else {
		ensureRead_slow(vbuf, length);
	}
}

inline void FileStream::ensureWrite(const void* vbuf, size_t length) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_IO_write_ptr + length <= fp->_IO_write_end) {
		memcpy(fp->_IO_write_ptr, vbuf, length);
		fp->_IO_write_ptr += length;
	} else {
		ensureWrite_slow(vbuf, length);
	}
}

#elif defined(_MSC_VER) && _MSC_VER <= 1800

inline byte FileStream::readByte() {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_cnt > 0) {
		fp->_cnt--;
		return *(byte*)(fp->_ptr++);
	} else {
		return readByte_slow();
	}
}

inline void FileStream::writeByte(byte b) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_cnt > 0) {
		fp->_cnt--;
		*fp->_ptr++ = b;
	} else {
		writeByte_slow(b);
	}
}

inline void FileStream::ensureRead(void* vbuf, size_t length) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_cnt >= intptr_t(length)) {
		memcpy(vbuf, fp->_ptr, length);
		fp->_cnt -= length;
		fp->_ptr += length;
	} else {
		ensureRead_slow(vbuf, length);
	}
}

inline void FileStream::ensureWrite(const void* vbuf, size_t length) {
	assert(m_fp);
	FILE* fp = m_fp;
	if (fp->_cnt >= intptr_t(length)) {
		memcpy(fp->_ptr, vbuf, length);
		fp->_cnt -= length;
		fp->_ptr += length;
	} else {
		ensureWrite_slow(vbuf, length);
	}
}

#else

inline byte FileStream::readByte()
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

inline void FileStream::writeByte(byte b)
{
	assert(m_fp);
#ifdef __USE_MISC
	if (EOF == fputc_unlocked(b, m_fp))
#else
	if (EOF == fputc(b, m_fp))
#endif
		throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
}

#endif // __GLIBC__

inline void FileStream::puts(fstring text) {
	assert(m_fp);
	ensureWrite(text.data(), text.size());
}

} // namespace terark

#endif

