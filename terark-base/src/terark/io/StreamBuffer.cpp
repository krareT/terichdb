/* vim: set tabstop=4 : */
#include <stdexcept>
#include "StreamBuffer.hpp"
#include "IStreamWrapper.hpp"
#include "FileStream.hpp"
#include "IStream.hpp"
#include "IOException.hpp"
#include <terark/num_to_str.hpp>

#if defined(_MSC_VER)
# include <intrin.h>
#pragma intrinsic(_BitScanReverse)
//#pragma intrinsic(_BitScanReverse64)
#endif

#include <stdlib.h>

#include <boost/version.hpp>
#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
# include <boost/detail/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif

#include <algorithm>
#include "var_int.hpp"

namespace terark {

IOBufferBase::IOBufferBase()
{
	m_beg = m_pos = m_end = 0;
	m_capacity = 8*1024;
}

IOBufferBase::~IOBufferBase()
{
	if (m_beg) free(m_beg);
}

void IOBufferBase::initbuf(size_t capacity)
{
#if defined(_DEBUG) || !defined(NDEBUG)
// raise assert when debug
	assert(0 == m_beg);
	m_beg = (byte*)malloc(capacity);
#else
// when release, free m_beg for avoid memory leak
	m_beg = (byte*)realloc(m_beg, capacity);
#endif
	m_pos = m_end = m_beg; // set as buffer overflow
	m_capacity = capacity;
}

// 在 initbuf 之前，可以调用多次，而不会引发内存分配动作
// 在 initbuf 之后，会重新分配内存
void IOBufferBase::set_bufsize(size_t size)
{
	if (0 == m_beg)
	{
		m_capacity = size;
	}
	else
	{
		assert(m_capacity);
		byte* pnewbuf = (byte*)realloc(m_beg, size);
		if (0 == pnewbuf)
		{
			throw std::bad_alloc();
		}
		m_pos = pnewbuf + (m_pos-m_beg);
		m_end = pnewbuf + (m_end-m_beg);
		m_beg = pnewbuf;
		m_capacity = size;
	}
}

void IOBufferBase::set_bufeof(size_t eofpos)
{
	assert(0 != m_beg);
	assert(eofpos <= m_capacity);
	assert(m_pos <= m_beg + eofpos);

	m_end = m_beg + eofpos;

	// for compiled with release, avoid error...
	if (m_pos > m_end)
		m_pos = m_end;
}
void IOBufferBase::update_pos(size_t inc) {
	; // empty for non-seekable
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//
//! @return retval is 0, or in range[min_length, max_length]
size_t InputBuffer::read_min_max(void* vbuf, size_t min_length, size_t max_length)
{
	assert(min_length <= max_length);

	if (terark_unlikely(0 == m_is))
	{
		string_appender<> oss;
		oss << "in " << BOOST_CURRENT_FUNCTION << "m_is==NULL, regard as eof";
		throw EndOfFileException(oss);
	}
	size_t total = 0;
	while (total < min_length)
	{
		size_t n = m_is->read((unsigned char*)vbuf+total, max_length-total);
		if (0 == n) break;
		total += n;
	}
	return total;
}

size_t InputBuffer::fill_and_read(void* vbuf, size_t length)
{
	return do_fill_and_read(vbuf, length);
}

// this function should not inline
size_t InputBuffer::do_fill_and_read(void* vbuf, size_t length)
{
//	assert(length != 0);
//	assert(m_pos + length > m_end);

	// (0 == length && m_pos == m_end) means prefetch only
	assert((0 == length && m_pos == m_end) || m_pos + length > m_end);

	if (terark_unlikely(NULL == m_is))
	{
		string_appender<> oss;
		oss << "in " << BOOST_CURRENT_FUNCTION << ", m_is==NULL, regard as eof";
		throw EndOfFileException(oss);
	}

	if (terark_unlikely(NULL == m_beg))
	{
		if (terark_unlikely(0 == m_capacity)) // unbuffered
		{
			length = read_min_max(vbuf, length, length);
			if (terark_unlikely(0 == length))
		   		throw EndOfFileException("InputBuffer::fill_and_read: unbuffered");
			return length;
		}
		else
			initbuf(m_capacity);
	}

	size_t nRead;
	size_t n1 = m_end - m_pos; // remain bytes in buffer
	size_t n2 = length - n1;   // remain bytes to read
	byte*  pb = (byte*)vbuf + n1; // bytes after n1

	memcpy(vbuf, m_pos, n1);
	if (n2 >= m_capacity) // remain bytes more than buffer capacity
	{
		m_pos = m_end = m_beg; // set buffer eof
		nRead = read_min_max(pb, n2, n2);
		if (terark_unlikely(0 == nRead))
        {
            char szMsg[128];
            sprintf(szMsg, "InputBuffer::do_fill_and_read, 1 read 0 byte, n1=%u, n2=%u", (unsigned)n1, (unsigned)n2);
            throw EndOfFileException(szMsg);
        }
		this->update_pos(nRead);
		return n1 + nRead;
	}
	nRead = read_min_max(m_beg, n2, m_capacity);
	if (terark_unlikely(0 == nRead))
	{
		char szMsg[128];
		sprintf(szMsg, "InputBuffer::do_fill_and_read, 2 read 0 byte, n1=%u, n2=%u", (unsigned)n1, (unsigned)n2);
		throw EndOfFileException(szMsg);
	}
	this->update_pos(nRead);
	m_end = m_beg + nRead;
	if (terark_unlikely(nRead < n2))
	{ // readed bytes too less
		memcpy(pb, m_beg, nRead);
		m_pos = m_end;
		return n1 + nRead;
	}
   	else {
		memcpy(pb, m_beg, n2);
		m_pos = m_beg + n2;
		return length;
	}
}

// this function should not inline
void InputBuffer::fill_and_ensureRead(void* vbuf, size_t length)
{
	size_t n = do_fill_and_read(vbuf, length);
	if (terark_unlikely(n != length))
	{
		string_appender<> oss;
		oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
			<< ", ReadBytes[want=" << length << ", read=" << n << "]";
		throw EndOfFileException(oss);
	}
}

// this function should not inline
byte InputBuffer::fill_and_read_byte()
{
	byte b;
	if (terark_likely(do_fill_and_read(&b, 1)))
		return b;
	else
		throw EndOfFileException(BOOST_CURRENT_FUNCTION);
}

int InputBuffer::fill_and_get_byte()
{
	byte b;
	if (terark_likely(do_fill_and_read(&b, 1)))
		return b;
	else
		return -1;
}

/**
 @brief 检测是否真的到了 eof

  在 fileptr 到达文件末尾但之前的一次 fread(buf,1,len,fp)==len 时，
  feof(fp) 返回 false，并非应该预期的 true
  所以，在这里，做一次真实的判断，看是否的确到了 eof

 @note
  # 只有当 m_pos == m_end 时，该函数才会被调用
 */
int InputBuffer::test_eof()
{
	// 只有当 m_pos == m_end 时，该函数才会被调用
	assert(m_pos == m_end);

	if (terark_unlikely(0 == m_is))
		return 1;

	try {
		byte tmp;
		if (terark_likely(do_fill_and_read(&tmp, 1))) {
			// readed 1 byte
			assert(m_beg + 1 == m_pos);
			m_pos = m_beg; // push_back this byte
			return 0;
		} else
			return 1;
	}
	catch (const EndOfFileException&)
	{
		return 1;
	}
}

void InputBuffer::getline(std::string& line, size_t maxlen)
{
	line.resize(0);
	size_t len = 0;
	for (;;)
	{
		for (byte* p = m_pos; ; ++p, ++len)
		{
			if (terark_unlikely(len == maxlen))
			{
				line.append((char*)m_pos, (char*)p);
				assert(line.size() == len);
				m_pos = p;
				return;
			}
			if (terark_unlikely(p == m_end))
			{
				if (0 == m_beg)
				{
					if (0 == m_capacity) {
						throw IOException("InputStream::getline() only support buffered mode");
					}
					initbuf(m_capacity);
					break;
				}
				line.append((char*)m_pos, (char*)m_end);
				assert(line.size() == len);
				m_pos = m_end;

				size_t nRead = read_min_max(m_beg, 1, m_capacity);
				if (0 == nRead)
				{
					if (line.empty())
						throw EndOfFileException("InputBuffer::getline, read 0 byte");
					else {
						m_pos = m_end = m_beg;
						return;
					}
				}
				this->update_pos(nRead);
				m_end = m_beg + nRead;
				m_pos = m_beg;
				break;
			}
			// 换行有三种，在这里都支持
			//  1. "\r\n"
			//  2. "\r"
			//  3. "\n"
			if (terark_unlikely('\r' == *p))
			{
				line.append((char*)m_pos, (char*)p);
				assert(line.size() == len);

				// m_pos move to next char point by p, maybe p+1 == m_end
				m_pos = p + 1;

				// 如果下一个字符是换行，就直接吃掉它
				// 如果是文件末尾，直接返回
				int nextCh = getByte();
				if (-1 == nextCh) // eof
					return;
				if ('\n' != nextCh)
					// not line feed, push back the byte
					--m_pos;

				return;
			}
			if (terark_unlikely('\n' == *p))
			{
				line.append((char*)m_pos, (char*)p);
				assert(line.size() == len);
				m_pos = p + 1;
				return;
			}
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

template<class BaseClass>
OutputBufferBase<BaseClass>::~OutputBufferBase()
{
	if (m_os) {
		try {
			flush_buffer();
		}
		catch (const std::exception& exp)
		{
			fprintf(stderr
				, "fatal: caught exception in %s, what=%s, ignored!\n"
				, BOOST_CURRENT_FUNCTION, exp.what());
		}
	}
}

// this function should not inline
template<class BaseClass>
void OutputBufferBase<BaseClass>::flush_buffer()
{
	if (terark_unlikely(NULL == m_beg)) // unbuffered
	{
		return;
	}
	if (terark_unlikely(NULL == m_os))
	{
		string_appender<> oss;
		oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
			<< ", m_os==NULL, regard as DelayWriteException";
		throw DelayWriteException(oss.str());
	}
	if (terark_likely(m_pos != m_beg))
	{
		assert(m_beg);
		// write [m_beg, m_pos) to stream
		//
		size_t n1 = m_pos-m_beg;
		size_t n2 = m_os->write(m_beg, n1);
		this->update_pos(n2);
		if (n1 != n2)
		{
			string_appender<> oss;
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
				<< ", WriteBytes[want=" << n1 << ", written=" << n2 << "]";
			throw DelayWriteException(oss);
		}
	}

	// set all buffer available
	m_pos = m_beg;
	m_end = m_beg + m_capacity;
}

template<class BaseClass>
void OutputBufferBase<BaseClass>::flush()
{
	flush_buffer();
	m_os->flush();
}

// this function should not inline
template<class BaseClass>
size_t OutputBufferBase<BaseClass>::flush_and_write(const void* vbuf, size_t length)
{
	return do_flush_and_write(vbuf, length);
}

template<class BaseClass>
size_t OutputBufferBase<BaseClass>::do_flush_and_write(const void* vbuf, size_t length)
{
	assert(length != 0);
	assert(m_pos + length > m_end);

	if (terark_unlikely(0 == m_os))
	{
		std::string msg;
		msg.append("\"").append(BOOST_CURRENT_FUNCTION).append("\"");
		msg.append(", m_is==NULL, regard as DelayWriteException");
		throw DelayWriteException(msg);
	}
	if (terark_unlikely(length >= m_capacity)) // remain bytes more than capacity, or unbuffered
	{
		flush_buffer();
		size_t n = m_os->write(vbuf, length);
		if (0 == n)
		{
			throw OutOfSpaceException(m_capacity ?
				   	"flush_and_write" :
				  	"flush_and_write: unbuffered");
		}
		this->update_pos(n);
		return n;
	}
	else if (terark_unlikely(0 == m_beg))
	{
		BaseClass::initbuf(this->m_capacity);
	}

	size_t n = m_end-m_pos; // remain free bytes in buffer
	memcpy(m_pos, vbuf, n);
	m_pos = m_end; // must set as m_end before flush
	flush_buffer();

	memcpy(m_beg, (byte*)vbuf+n, length-n);
	m_pos = m_beg + (length-n);

	return length;
}

// this function should not inline
template<class BaseClass>
void OutputBufferBase<BaseClass>::flush_and_ensureWrite(const void* vbuf, size_t length)
{
	size_t n = do_flush_and_write(vbuf, length);
	if (n != length)
	{
		string_appender<> oss;
		oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
			<< ", WriteBytes[want=" << length << ", written=" << n << "]";
		throw OutOfSpaceException(oss);
	}
}

// this function should not inline
template<class BaseClass>
void OutputBufferBase<BaseClass>::flush_and_write_byte(byte b)
{
	assert(m_pos == m_end);
	flush_and_ensureWrite(&b, 1);
}

template<class BaseClass>
size_t OutputBufferBase<BaseClass>::printf(const char* format, ...)
{
	va_list ap;
	size_t n;
	va_start(ap, format);
	n = this->vprintf(format, ap);
	va_end(ap);
	return n;
}

// this function should not inline
template<class BaseClass>
size_t OutputBufferBase<BaseClass>::vprintf(const char* format, va_list ap)
{
	if (terark_unlikely(0 == m_capacity)) {
		throw IOException("Invalid, OutputBuffer::vprintf only support buffered mode");
	}
	if (terark_unlikely(NULL == m_beg)) {
		this->initbuf(m_capacity);
	}
	while (1) {
		ptrdiff_t n, size = m_end - m_pos;

		/* Try to print in the allocated space. */
		n = ::vsnprintf((char*)m_pos, size, format, ap);

		/* If that worked, return the written bytes. */
		if (terark_likely(n > -1 && n < size)) {
			m_pos += n;
			return n;
		}
		this->flush();

		/* Else try again with more space. */
		if (n > -1)    /* glibc 2.1 */
			size = n+1; /* precisely what is needed */
		else           /* glibc 2.0 */
			size *= 2;  /* twice the old size */

		if (size > (ptrdiff_t)m_capacity)
			this->set_bufsize(align_up(size, m_capacity));
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

// this function should not inline
template<class BaseClass>
void SeekableBufferBase<BaseClass>::seek(stream_position_t pos)
{
	stream_position_t pos_beg, pos_end;
	if (is_prefetched()) {
		//! m_stream_pos is mapped to m_end
		pos_beg = m_stream_pos - (m_end - m_beg);
		pos_end = m_stream_pos;
	} else {
		//! m_stream_pos is mapped to m_beg
		pos_beg = m_stream_pos;
		pos_end = m_stream_pos + (m_end - m_beg);
	}
	if (m_beg!=m_end && pos_beg <= pos && pos <= pos_end)
	{
		// pos is in [pos_beg, pos_end]
		// map m_pos to pos_cur, not need change m_stream_pos and call m_seekable->seek(..)
		m_pos = m_beg + (pos - m_stream_pos);
	}
	else if (m_seekable)
	{
		invalidate_buffer();
		m_seekable->seek(pos);
		m_stream_pos = m_seekable->tell();
		m_end = m_pos = m_beg; // discard buffer
	}
	else
	{
		string_appender<> oss;
		oss << "\"" << BOOST_CURRENT_FUNCTION << "\"" << "(pos=" << pos << ")";
		throw OutOfSpaceException(oss);
	}
}

// this function should not inline
template<class BaseClass>
void SeekableBufferBase<BaseClass>::seek(stream_offset_t offset, int origin)
{
	switch (origin)
	{
	default:
		{
			string_appender<> oss;
			oss << "in " << BOOST_CURRENT_FUNCTION
				<< "[offset=" << offset << ", origin=" << origin << "(invalid)]";
			throw std::invalid_argument(oss.str().c_str());
		}
	case 0:
		seek(offset);
		break;
	case 1:
		if (m_beg!=m_end)
		{
			stream_position_t pos_beg, pos_end;
			if (is_prefetched()) {
				//! m_stream_pos is mapped to m_end
				pos_beg = m_stream_pos - (m_end - m_beg);
				pos_end = m_stream_pos;
			} else {
				//! m_stream_pos is mapped to m_beg
				pos_beg = m_stream_pos;
				pos_end = m_stream_pos + (m_end - m_beg);
			}
			// pos_cur should map to m_pos
			// calculate new pos_cur
			stream_position_t pos_cur = pos_beg + (m_pos-m_beg) + offset;

			if (pos_beg <= pos_cur && pos_cur <= pos_end)
			{
				// cur is in [m_stream_pos, end]
				// map m_pos to pos_cur, not need change m_stream_pos and call m_seekable->seek(..)
				m_pos += offset;
			}
			else if (m_seekable)
			{
				invalidate_buffer();
				m_seekable->seek(offset, 1);
				m_stream_pos = m_seekable->tell();
				m_end = m_pos = m_beg; // discard buffer
			}
			else
			{
				string_appender<> oss;
				oss << "\"" << BOOST_CURRENT_FUNCTION << "\", no seekable stream, "
					<< "(offset=" << offset << ", origin=" << origin << ")";
				throw OutOfSpaceException(oss.str().c_str());
			}
		}
		else if (m_seekable)
		{
			m_seekable->seek(offset, 1);
			m_stream_pos = m_seekable->tell();
		}
		else
		{
			string_appender<> oss;
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\", no seekable stream, "
				<< "(offset=" << offset << ", origin=" << origin << ")";
			throw OutOfSpaceException(oss.str().c_str());
		}
		break;
	case 2:
	// --------------------------------------------------------------------
	// 这样会丢弃可能仍然有用的 buffer
	// 但系统调用的次数少，也不需要 m_seekable->size
	//	m_end = m_pos = m_beg; // discard buffer
	//	m_seekable->seek(offset, 2);
	//  m_stream_pos = m_seekable->tell();
	//  m_last_inc = 0;
	// --------------------------------------------------------------------
	// 还是这样？这样可能会避免丢弃仍然有用的 buffer，但系统调用次数多
		if (0 == m_seekable)
		{
			string_appender<> oss;
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\", no seekable stream, "
				<< "(offset=" << offset << ", origin=" << origin << ")";
			throw OutOfSpaceException(oss.str().c_str());
		}
		if (m_end == m_beg) {
			m_seekable->seek(offset, 2);
			m_stream_pos = m_seekable->tell();
		} else
			seek(m_seekable->size() + offset);
		break;
	}
}

template<class BaseClass>
void SeekableBufferBase<BaseClass>::skip(ptrdiff_t diff)
{
	seek(diff, 1);
}

template<class BaseClass>
stream_position_t SeekableBufferBase<BaseClass>::size() const
{
	using namespace std;
	if (m_beg==m_end || is_prefetched())
		return m_seekable->size();
	else {
		assert(!is_prefetched());
		stream_position_t cur_size = m_stream_pos + (m_pos-m_beg);
		stream_position_t str_size = m_seekable->size();
		return max(cur_size, str_size);
	}
}

// this function should not inline
template<class BaseClass>
stream_position_t SeekableBufferBase<BaseClass>::tell() const
{
	if (m_beg==m_end)
		return m_seekable->tell();
	else {
		if (is_prefetched())
			return m_stream_pos - (m_end - m_pos);
		else
			return m_stream_pos + (m_pos - m_beg);
	}
}

template<class BaseClass>
void SeekableBufferBase<BaseClass>::update_pos(size_t inc)
{
	m_stream_pos += inc;
}

//------------------------------------------------------------------------
void SeekableInputBuffer::invalidate_buffer()
{
	// do nothing...
}
int SeekableInputBuffer::is_prefetched() const
{
	return true;
}

void SeekableOutputBuffer::invalidate_buffer()
{
	flush_buffer();
}
int SeekableOutputBuffer::is_prefetched() const
{
	return false;
}

//////////////////////////////////////////////////////////////////////////
SeekableBuffer::SeekableBuffer()
{
	m_dirty = false;
	m_prefetched = false;
}

SeekableBuffer::~SeekableBuffer()
{
	if (terark_likely(NULL != m_os))
	   	flush_buffer();

// reset buffer, to avoid super's destructor to flush again...
	m_pos = m_end = m_beg;
}

size_t SeekableBuffer::do_fill_and_read(void* vbuf, size_t length)
{
	invalidate_buffer();
	length = super::do_fill_and_read(vbuf, length);
	m_prefetched = true;
	return length;
}

size_t SeekableBuffer::do_flush_and_write(const void* vbuf, size_t length)
{
	invalidate_buffer();
	length = super::do_flush_and_write(vbuf, length);
	m_prefetched = false;
	m_dirty = true;
	return length;
}

void SeekableBuffer::flush_buffer()
{
	if (m_dirty) {
		super::flush_buffer();
		m_prefetched = false;
		m_dirty = false;
	}
}

void SeekableBuffer::invalidate_buffer()
{
	flush_buffer();
	m_end = m_pos = m_beg;
}

int SeekableBuffer::is_prefetched() const
{
	return m_prefetched;
}
//////////////////////////////////////////////////////////////////////////

FileStreamBuffer::FileStreamBuffer(const char* fname, const char* mode, size_t capacity)
{
	this->set_bufsize(capacity);
	attach(new SeekableStreamWrapper<FileStream>(fname, mode));
}

FileStreamBuffer::~FileStreamBuffer()
{
	flush_buffer();
	delete m_os;
	m_os = 0;
	m_is = 0;
}

//////////////////////////////////////////////////////////////////////////
#if 0 //defined(__GNUC__) && __GNUC__ >= 4
#else
template class OutputBufferBase<InputBuffer>;
template class OutputBufferBase<IOBufferBase>;

template class SeekableBufferBase<InputBuffer>;
template class SeekableBufferBase<OutputBuffer>;
template class SeekableBufferBase<OutputBufferBase<InputBuffer> >;
#endif
//////////////////////////////////////////////////////////////////////////


#define STREAM_READER InputBuffer
#define STREAM_WRITER OutputBuffer
#include "var_int_io.hpp"

/*
 //! 功能和效率类似于 AutoGrownMemIO
 //!
 //! 现在 AutoGrownMemIO 更新了，为了效率，也使用指针存储 pos
 //! @note
 //!  如非必要，请勿使用这个类，最好使用 AutoGrownMemIO
class AutoGrownMemIOBuffer : public SeekableBuffer
{
public:
	explicit AutoGrownMemIOBuffer(size_t capacity = 8*1024);
	~AutoGrownMemIOBuffer();
};

//////////////////////////////////////////////////////////////////////////
//! 这个类是个 Stream 的哑实现
//!
//! 只维护一个位置指针来表达 stream 何时结束，并不真正读取或写入数据
//! 只要调用 theAutoGrownMemIOStub.attach(pStreamBuffer), 就构造了一个 AutoGrownMemIO 的等效类，效率可能比 AutoGrownMemIO 还要高一点
//! 为了方便也可以使用 AutoGrownMemIOBuffer 构造一个 SeekableBuffer, 不过 AutoGrownMemIOBuffer 的效率稍低
//! 这是因为 SeekableBuffer 同时支持读和写，本身效率就低一些(比其他四种 StreamBuffer)
//! 如果 attach 其它四种 StreamBuffer, 效率就会很高
//!
//! 经测试比 AutoGrownMemIO 还要高大约 20% ---- 之前 AutoGrownMemIO 的 pos 使用 size_t 存储
//! 应该是因为 AutoGrownMemIO 的 pos 是用 size_t 存储，读写时要多两次加法和两次内存读取操作
//!
//! 现在 AutoGrownMemIO 更新了，为了效率，也使用指针存储 pos
//! @note
//!  如非必要，请勿使用这个类，最好使用 AutoGrownMemIO
//!
class AutoGrownMemIOStub : public ISeekableStream
{
	IOBufferBase* m_buf;
	size_t m_stub_pos;

public:
	template<class StreamBufferT>
	explicit AutoGrownMemIOStub(StreamBufferT* buf = 0)
	{
		attach(buf);
	}

	template<class StreamBufferT>
	void attach(StreamBufferT* buf)
	{
		m_stub_pos = 0;
		m_buf = buf;
		buf->attach(this);
	}

	void seek(stream_position_t pos);
	void seek(stream_offset_t offset, int origin);
	stream_position_t tell();
	size_t read(void* vbuf, size_t length);
	size_t write(const void* vbuf, size_t length);
	void flush();
};

void AutoGrownMemIOStub::seek(stream_position_t newPos)
{
	if (newPos > m_buf->bufcapacity()) {
		string_appender<> oss;
		oss << "in " << BOOST_CURRENT_FUNCTION
			<< "[newPos=" << newPos << "(too large), size=" << m_buf->bufcapacity() << "]";
		throw std::invalid_argument(oss.str());
	}
	m_stub_pos = newPos;
}

void AutoGrownMemIOStub::seek(stream_offset_t offset, int origin)
{
	stream_position_t pos;
	switch (origin)
	{
		default:
		{
			string_appender<> oss;
			oss << "in " << BOOST_CURRENT_FUNCTION
				<< "[offset=" << offset << ", origin=" << origin << "(invalid)]";
			throw std::invalid_argument(oss.str().c_str());
		}
		case 0: pos = (size_t)(0 + offset); break;
		case 1: pos = (size_t)(m_stub_pos + offset); break;
		case 2: pos = (size_t)(m_buf->bufcapacity() + offset); break;
	}
	seek(pos);
}

stream_position_t AutoGrownMemIOStub::tell()
{
	return m_stub_pos;
}

size_t AutoGrownMemIOStub::read(void* vbuf, size_t length)
{
	assert(length != 0);

	if (m_stub_pos == m_buf->bufcapacity() && length)
		throw EndOfFileException(BOOST_CURRENT_FUNCTION);
	else
	{	// 假装读取，实际上不读任何数据
		using namespace std;
		size_t n = min(length, size_t(m_buf->bufcapacity() - m_stub_pos));
		m_stub_pos += n;
		return n;
	}
}

size_t AutoGrownMemIOStub::write(const void* vbuf, size_t length)
{
	assert(length != 0);

	if (m_stub_pos >= m_buf->bufcapacity() && length)
		throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
	else
	{   // 假装写入，实际上不写任何数据
		using namespace std;
		size_t n = min(length, size_t(m_buf->bufcapacity() - m_stub_pos));
		m_stub_pos += n;
		return n;
	}
}

void AutoGrownMemIOStub::flush()
{
	// do nothing...
}

//////////////////////////////////////////////////////////////////////////

AutoGrownMemIOBuffer::AutoGrownMemIOBuffer(size_t capacity)
	: SeekableBuffer(capacity)
{
	attach(new AutoGrownMemIOStub(this));
	m_end = m_beg + m_capacity;
}

AutoGrownMemIOBuffer::~AutoGrownMemIOBuffer()
{
	delete m_os;
	m_os = 0;
	m_is = 0;
}
*/

} // terark



