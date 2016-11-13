/* vim: set tabstop=4 : */
#ifndef __terark_io_StreamBuffer_h__
#define __terark_io_StreamBuffer_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/util/refcount.hpp>
#include <boost/mpl/bool.hpp>
#include <string.h>
#include <stdarg.h>
#include <string>

/**
 @file 以 Stream 的 Concept, 实现的 Buffer

 可以和 Stream 一样的用，但提供了高速缓冲——是 C FILE 的大约 20 倍

 - 使用缓冲倒置：
   -# 一般的实现(BufferStream)是给 Stream 增加缓冲功能
   -# 而这个实现(StreamBuffer)是给缓冲增加 Stream 功能
   -# 这里的主体是 Buffer, 而非 Stream

 - 高性能主要通过以下几点技法实现
   -# 让调用最频繁的函数 inline, 并保持函数内容最短, 如 read/ensureRead/readByte, 等
   -# 在这些 inline 函数最频繁的执行路径上, 执行最少的代码 @see InputBuffer::readByte
   -# 只有极少情况下会执行的分支, 封装到一个非虚函数(非虚函数的调用代码比虚函数小)
   -# 如此, inline 函数的执行效率会非常高, 在 Visual Studio 下, ensureRead 中的 memcpy 在大多数情况下完全被优化掉了:
	 @code
	  LittleEndianDataInput<InputBuffer> input(&buf);
	  int x;
	  input >> x;
	  // 在这个代码段中, input >> x;
	  // 的最频繁分支甚至被优化成了等价代码: x = *(int*)m_pos;
	  // 底层函数调用的 memcpy 完全被优化掉了
	 @endcode

 - 共有五种 StreamBuffer: InputBuffer/OutputBuffer/SeekableInputBuffer/SeekableOutputBuffer/SeekableBuffer
   -# 每种 buffer 都可以附加(attach)一个支持相应功能的流
   -# SeekableInputBuffer 并不要求 stream 必须是 ISeekableInputStream,
      只需要 stream 同时实现了 ISeekable 和 IInputStream 即可
   -# SeekableBuffer 并不要求 stream 是 ISeekableStream,
      只需要 stream 同时实现了 ISeekable/IInputStream/IOutputStream
 */

namespace terark {

class IInputStream;
class IOutputStream;
class ISeekable;

class TERARK_DLL_EXPORT IOBufferBase : public RefCounter
{
private:
	// can not copy
	IOBufferBase(const IOBufferBase&);
	const IOBufferBase& operator=(const IOBufferBase&);

public:
	IOBufferBase();
	virtual ~IOBufferBase();

	//! 设置 buffer 尺寸并分配 buffer 内存
	//! 在整个生存期内只能调用一次
	//!
	void initbuf(size_t capacity);

	//! 如果在 init 之前调用，仅设置 buffer 尺寸
	//! 否则重新分配 buffer 并设置相应的指针
	//!
	void set_bufsize(size_t size);

	byte*  bufbeg() const { return m_beg; }
	byte*  bufcur() const { return m_pos; }
	byte*  bufend() const { return m_end; }

	size_t bufpos()  const { return m_pos-m_beg; }
	size_t bufsize() const { return m_end-m_beg; }
	size_t bufcapacity() const { return m_capacity; }

	void resetbuf() { m_pos = m_end = m_beg; }

	void risk_set_bufpos(size_t pos) {
		assert(pos <= size_t(m_end - m_beg));
	   	m_pos = m_beg + pos;
   	}

	//! set buffer eof
	//!
	//! most for m_is/m_os == 0
	void set_bufeof(size_t eofpos);

	bool is_bufeof() const { return m_pos == m_end; }

	ptrdiff_t buf_remain_bytes() const { return m_end - m_pos; }

protected:
	//! 当调用完 stream.read/write 时，使用该函数来同步内部 pos 变量
	//!
	//! 对 non-seekable stream, 这个函数是空, SeekableBufferBase 改写了该函数
	//! @see SeekableBufferBase::update_pos
	virtual void update_pos(size_t inc); // empty for non-seekable

protected:
	// dummy, only for OutputBufferBase::attach to use
	void attach(void*) { }

protected:
	// for  InputBuffer, [m_beg, m_pos) is readed,  [m_pos, m_end) is prefetched
	// for OutputBuffer, [m_beg, m_pos) is written, [m_pos, m_end) is undefined

	byte*  m_beg;	// buffer ptr
	byte*  m_pos;	// current read/write position
	byte*  m_end;   // end mark, m_end <= m_beg + m_capacity && m_end >= m_beg
	size_t m_capacity; // buffer capacity
};

class TERARK_DLL_EXPORT InputBuffer : public IOBufferBase
{
public:
	typedef boost::mpl::false_ is_seekable;

	explicit InputBuffer(IInputStream* stream = NULL)
		: m_is(stream)
	{
	}

	void attach(IInputStream* stream)
	{
		m_is = stream;
	}

	IInputStream* getInputStream() const { return m_is; }

	bool eof() { return m_pos == m_end && test_eof(); }

	size_t read(void* vbuf, size_t length)
	{
		if (terark_likely(m_pos+length <= m_end)) {
			memcpy(vbuf, m_pos, length);
			m_pos += length;
			return length;
		} else
			return fill_and_read(vbuf, length);
	}
	void ensureRead(void* vbuf, size_t length)
	{
		// 为了效率，这么实现可以让编译器更好地 inline 这个函数
		// inline 后的函数体并尽可能小
		//
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
	int getByte()
	{
		if (terark_likely(m_pos < m_end))
			return *m_pos++;
		else
			return this->fill_and_get_byte();
	}

	void getline(std::string& line, size_t maxlen = UINT_MAX);

	template<class OutputStream>
	void to_output(OutputStream& output, size_t length)
	{
		size_t total = 0;
		while (total < length)
		{
			using namespace std; // for min
			if (terark_unlikely(m_pos == m_end))
				this->fill_and_read(m_beg, m_end-m_beg);
			size_t nWrite = min(size_t(m_end-m_pos), size_t(length-total));
			output.ensureWrite(m_pos, nWrite);
			total += nWrite;
			m_pos += nWrite;
		}
	}

	#include "var_int_declare_read.hpp"

protected:
	size_t fill_and_read(void* vbuf, size_t length);
	void   fill_and_ensureRead(void* vbuf, size_t length);
	byte   fill_and_read_byte();
	int    fill_and_get_byte();
	size_t read_min_max(void* vbuf, size_t min_length, size_t max_length);
	int    test_eof();

	virtual size_t do_fill_and_read(void* vbuf, size_t length);

protected:
	IInputStream* m_is;
};

template<class BaseClass>
class TERARK_DLL_EXPORT OutputBufferBase : public BaseClass
{
public:
	typedef boost::mpl::false_ is_seekable;

	explicit OutputBufferBase(IOutputStream* os = NULL) : m_os(os)
	{
	}
	virtual ~OutputBufferBase();

	template<class Stream>
	void attach(Stream* stream)
	{
		BaseClass::attach(stream);
		m_os = stream;
	}
	
	IOutputStream* getOutputStream() const { return m_os; }

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
		//
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

	template<class InputStream>
	void from_input(InputStream& input, size_t length)
	{
		size_t total = 0;
		while (total < length)
		{
			using namespace std; // for min
			if (terark_unlikely(m_pos == m_end))
				flush_buffer();
			size_t nRead = min(size_t(m_end-m_pos), size_t(length-total));
			input.ensureRead(m_pos, nRead);
			total += nRead;
			m_pos += nRead;
		}
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

	virtual void flush_buffer(); // only write to m_os, not flush m_os

protected:
	size_t flush_and_write(const void* vbuf, size_t length);
	void   flush_and_ensureWrite(const void* vbuf, size_t length);
	void   flush_and_write_byte(byte b);

	virtual size_t do_flush_and_write(const void* vbuf, size_t length);

protected:
	IOutputStream* m_os;
	using BaseClass::m_pos;
	using BaseClass::m_beg;
	using BaseClass::m_end;
	using BaseClass::m_capacity;
};

class TERARK_DLL_EXPORT OutputBuffer : public OutputBufferBase<IOBufferBase>
{
public:
	explicit OutputBuffer(IOutputStream* os = NULL)
	   	: OutputBufferBase<IOBufferBase> (os)
	{ }

	#include "var_int_declare_write.hpp"
};

template<class BaseClass>
class TERARK_DLL_EXPORT SeekableBufferBase : public BaseClass
{
protected:
	using BaseClass::m_beg;
	using BaseClass::m_pos;
	using BaseClass::m_end;
	using BaseClass::m_capacity;

public:
	typedef boost::mpl::true_ is_seekable;

	//! constructor
	//!
	//! 如果以 append 方式打开流，这个 m_stream_pos 是不对的
	//! 不过一般这种情况下很少会调用 seek/tell
	//! 如果真这么做，会导致未定义行为
	//!
	explicit SeekableBufferBase()
	{
		m_seekable = 0;
		m_stream_pos = 0;
	}

	virtual ~SeekableBufferBase() {}

	template<class Stream>
	void attach(Stream* stream)
	{
		BaseClass::attach(stream);
		m_seekable = stream;
	}

	void seek(stream_position_t pos);
	void seek(stream_offset_t offset, int origin);

	void skip(ptrdiff_t diff);

	stream_position_t tell() const;
	stream_position_t size() const;

protected:
	virtual void update_pos(size_t inc); //!< override
	virtual void invalidate_buffer() = 0;

	//! 如果已预取，m_stream_pos 对应缓冲区末尾 m_end
	//! 否则 m_stream_pos 对应缓冲区开始
	//!
	virtual int is_prefetched() const = 0;

protected:
	ISeekable* m_seekable;
	stream_position_t m_stream_pos;
};

class TERARK_DLL_EXPORT SeekableInputBuffer : public SeekableBufferBase<InputBuffer>
{
	typedef SeekableBufferBase<InputBuffer> super;
public:
	SeekableInputBuffer() { }
protected:
	virtual void invalidate_buffer();
	virtual int is_prefetched() const;
};

class TERARK_DLL_EXPORT SeekableOutputBuffer : public SeekableBufferBase<OutputBuffer>
{
	typedef SeekableBufferBase<OutputBuffer> super;

public:
//	typedef boost::mpl::true_ is_seekable;

	//! constructor
	//!
	//! 如果以 append 方式打开流，这个 m_stream_pos 是不对的
	//! 不过一般这种情况下很少会调用 seek/tell
	//! 如果真这么做，会导致未定义行为
	//!
	SeekableOutputBuffer() {}

protected:
	virtual void invalidate_buffer();
	virtual int is_prefetched() const;
};

class TERARK_DLL_EXPORT SeekableBuffer :
	public SeekableBufferBase<OutputBufferBase<InputBuffer> >
{
	typedef SeekableBufferBase<OutputBufferBase<InputBuffer> > super;

public:
	SeekableBuffer();
	~SeekableBuffer();

	size_t read(void* vbuf, size_t length)
	{
		if (terark_likely(m_pos+length <= m_end && m_prefetched)) {
			memcpy(vbuf, m_pos, length);
			m_pos += length;
			return length;
		} else
			return fill_and_read(vbuf, length);
	}
	void ensureRead(void* vbuf, size_t length)
	{
		// 为了效率，这么实现可以让编译器更好地 inline 这个函数
		// inline 后的函数体并尽可能小
		//
		if (terark_likely(m_pos+length <= m_end && m_prefetched)) {
			memcpy(vbuf, m_pos, length);
			m_pos += length;
		} else
			fill_and_ensureRead(vbuf, length);
	}

	byte readByte()
	{
		if (terark_likely(m_pos < m_end && m_prefetched))
			return *m_pos++;
		else
			return fill_and_read_byte();
	}
	int getByte()
	{
		if (terark_likely(m_pos < m_end && m_prefetched))
			return *m_pos++;
		else
			return fill_and_get_byte();
	}

	size_t write(const void* vbuf, size_t length)
	{
		m_dirty = true;
		return super::write(vbuf, length);
	}

	void ensureWrite(const void* vbuf, size_t length)
	{
		m_dirty = true;
		super::ensureWrite(vbuf, length);
	}

	void writeByte(byte b)
	{
		m_dirty = true;
		super::writeByte(b);
	}

protected:
	virtual size_t do_fill_and_read(void* vbuf, size_t length) ; //!< override
	virtual size_t do_flush_and_write(const void* vbuf, size_t length) ; //!< override

	virtual void flush_buffer(); //!< override
	virtual void invalidate_buffer(); //!< override
	virtual int is_prefetched() const;

private:
	int m_dirty;
	int m_prefetched;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////
//
class TERARK_DLL_EXPORT FileStreamBuffer : public SeekableBuffer
{
public:
	explicit FileStreamBuffer(const char* fname, const char* mode, size_t capacity = 8*1024);
	~FileStreamBuffer();
};

} // terark

#endif // __terark_io_StreamBuffer_h__


