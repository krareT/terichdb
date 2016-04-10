/* vim: set tabstop=4 : */
#ifndef __terark_io_ZeroCopy_h__
#define __terark_io_ZeroCopy_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <boost/mpl/bool.hpp>
#include <assert.h>
#include <stdio.h> // for getFILE
#include <string.h>
#include <stdarg.h>
#include <string>
#include <terark/stdtypes.hpp>

namespace terark {

class TERARK_DLL_EXPORT IZeroCopyInputStream
{
public:
	typedef boost::mpl::false_ is_seekable;

	virtual ~IZeroCopyInputStream();

	/**
	 * get readbuffer for zero copy
	 */
	virtual const void* zcRead(size_t length, size_t* readed) = 0;

	/**
	 @brief End Of File

	  only InputStream has eof() mark, OutputStream does not have eof()
	 */
	virtual bool eof() const = 0;
};

class TERARK_DLL_EXPORT IZeroCopyOutputStream
{
public:
	typedef boost::mpl::false_ is_seekable;

	virtual ~IZeroCopyOutputStream();

	/**
	 * get write buffer for zero copy
	 */
	virtual void* zcWrite(size_t length, size_t* writable) = 0;

	/**
	 * after last zcWrite, caller may be not have written the whole buffer
	 * zcFlush tells the Stream how many bytes has actual written,
	 * next zcWrite will get the unwritten buffer lies in last buffer end.
	 */
	virtual void zcFlush(size_t nWritten) = 0;
};

class TERARK_DLL_EXPORT ZeroCopyBase
{
protected:
	unsigned char* m_beg;
	unsigned char* m_pos;
	unsigned char* m_end;
	size_t m_bufsize;

	void init(size_t bufsize)
	{
		m_beg = NULL;
		m_pos = NULL;
		m_end = NULL;
		m_bufsize = bufsize;
	}

public:
	typedef boost::mpl::false_ is_seekable;

	ptrdiff_t buf_remain_bytes() const { return m_end - m_pos; }
};

class TERARK_DLL_EXPORT ZcReader : public ZeroCopyBase
{
	IZeroCopyInputStream* m_is;

public:
	explicit ZcReader(IZeroCopyInputStream* stream = NULL, size_t bufsize = BUFSIZ)
	{
		attach(stream, BUFSIZ);
	}

	void attach(IZeroCopyInputStream* stream, size_t bufsize = BUFSIZ)
	{
		init(bufsize);
		m_is = stream;
	}

	bool eof() { return m_pos == m_end && test_eof(); }

	/**
	 * @return actually readed bytes, never return 0
	 *   -  if actually readed bytes is 0, throw EndOfFileException
	 */
	size_t read(void* vbuf, size_t length)
	{
		if (terark_likely(m_pos+length <= m_end)) {
			memcpy(vbuf, m_pos, length);
			m_pos += length;
			return length;
		} else
			return fill_and_read(vbuf, length);
	}

	/**
	 * always read requested bytes, or throw EndOfFileException
	 */
	void ensureRead(void* vbuf, size_t length)
	{
		// 为了效率，这么实现可以让编译器更好地 inline 这个函数
		// inline 后的函数体并尽可能小
		if (terark_likely(m_pos+length <= m_end)) {
			memcpy(vbuf, m_pos, length);
			m_pos += length;
		} else
			fill_and_ensureRead(vbuf, length);
	}

	byte readByte()
	{
		if (terark_likely(m_pos < m_end))
			return *m_pos++;
		else
			return fill_and_read_byte();
	}

	void getline(std::string& line, size_t maxlen = UINT_MAX);

	#include "var_int_declare_read.hpp"

protected:
	size_t fill_and_read(void* vbuf, size_t length);
	void   fill_and_ensureRead(void* vbuf, size_t length);
	byte   fill_and_read_byte();
	int    test_eof();

	size_t do_fill_and_read(void* vbuf, size_t length);
};

class TERARK_DLL_EXPORT ZcWriter : public ZeroCopyBase
{
	IZeroCopyOutputStream* m_os;

public:
	explicit ZcWriter(IZeroCopyOutputStream* stream = NULL, size_t bufsize = BUFSIZ)
	{
		attach(stream, BUFSIZ);
	}
	~ZcWriter();

	void attach(IZeroCopyOutputStream* stream, size_t bufsize = BUFSIZ)
	{
		init(bufsize);
		m_os = stream;
	}

	void flush();

	size_t write(const void* vbuf, size_t length)
	{
		if (terark_likely(m_pos+length <= m_end)) {
			memcpy(m_pos, vbuf, length);
			m_pos += length;
			return length;
		} else
			return flush_and_write(vbuf, length);
	}

	void ensureWrite(const void* vbuf, size_t length)
	{
		// 为了效率，这么实现可以让编译器更好地 inline 这个函数
		// inline 后的函数体并尽可能小
		if (terark_likely(m_pos+length <= m_end)) {
			memcpy(m_pos, vbuf, length);
			m_pos += length;
		} else
			flush_and_ensureWrite(vbuf, length);
	}

	void writeByte(byte b)
	{
		if (terark_likely(m_pos < m_end))
			*m_pos++ = b;
		else
			flush_and_write_byte(b);
	}

	size_t printf(const char* format, ...)
#ifdef __GNUC__
	__attribute__ ((__format__ (__printf__, 2, 3)))
#endif
	;

	size_t vprintf(const char* format, va_list ap)
#ifdef __GNUC__
	__attribute__ ((__format__ (__printf__, 2, 0)))
#endif
	;

	#include "var_int_declare_write.hpp"

protected:
	size_t flush_and_write(const void* vbuf, size_t length);
	void   flush_and_ensureWrite(const void* vbuf, size_t length);
	void   flush_and_write_byte(byte b);

	size_t do_flush_and_write(const void* vbuf, size_t length);
};


} // namespace terark

#endif

