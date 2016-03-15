/* vim: set tabstop=4 : */
#include "ZeroCopy.hpp"
#include "IOException.hpp"
#include <assert.h>
#include <algorithm>
#include <stdexcept>

#if defined(_MSC_VER)
	#pragma warning(disable:4819)
	#include <intrin.h>
	#pragma intrinsic(_BitScanReverse)
	//#pragma intrinsic(_BitScanReverse64)
#endif

#include <boost/version.hpp>
#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
# include <boost/detail/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif

#include "var_int.hpp"

namespace terark {

////////////////////////////////////////////////////////////////////////////////////////////////////////
//

using namespace std; // for min, max

IZeroCopyInputStream::~IZeroCopyInputStream()
{

}

IZeroCopyOutputStream::~IZeroCopyOutputStream()
{

}


size_t ZcReader::fill_and_read(void* vbuf, size_t length)
{
	assert(NULL != m_is);
	assert(0 != m_bufsize);
	return do_fill_and_read(vbuf, length);
}

// this function should not inline
size_t ZcReader::do_fill_and_read(void* vbuf, size_t length)
{
//	assert(length != 0);
//	assert(m_pos + length > m_end);
	assert(NULL != m_is);
	assert(0 != m_bufsize);

	if (terark_unlikely(NULL == m_is))
	{
		throw std::invalid_argument("(NULL==m_is) in ZcReader::do_fill_and_read");
	}

	size_t n1 = m_end - m_pos; // remain bytes in buffer
	size_t n2 = length - n1;   // remain bytes to read

	memcpy(vbuf, m_pos, n1);   // read data remain inbuf

	while (n2 > 0)
	{
		size_t toRead = max(n2, m_bufsize);
		size_t readed;
	   	m_beg = (unsigned char*)m_is->zcRead(toRead, &readed);
		if (terark_unlikely(0 == readed)) {
			if (0 == n1) { // eof before this call `do_fill_and_read`
				char szMsg[128];
				sprintf(szMsg, "ZcReader::do_fill_and_read, 1 read 0 byte, n1=%lld, n2=%lld"
						, (long long)n1
						, (long long)n2
						);
				throw EndOfFileException(szMsg);
			}
			else {
				m_beg = m_pos = m_end = NULL;
				return n1;
			}
 		}
		size_t n3 = min(n2, readed);
		memcpy((byte*)vbuf + n1, m_beg, n3);
		n2 -= n3;
		n1 += n3;
		m_pos = m_beg + n3;
		m_end = m_beg + readed;
	}
	assert(n1 == length);
	return length;
}

// this function should not inline
void ZcReader::fill_and_ensureRead(void* vbuf, size_t length)
{
	assert(NULL != m_is);
	assert(0 != m_bufsize);
	size_t n = do_fill_and_read(vbuf, length);
	if (terark_unlikely(n != length))
	{
		char szbuf[256];
		sprintf(szbuf, "\"%s\", ReadBytes[want=%lld, read=%lld]"
				, BOOST_CURRENT_FUNCTION
				, (long long)length
				, (long long)n
				);
		throw EndOfFileException(szbuf);
	}
}

// this function should not inline
byte ZcReader::fill_and_read_byte()
{
	assert(NULL != m_is);
	assert(0 != m_bufsize);
	byte b;
	if (terark_likely(do_fill_and_read(&b, 1)))
		return b;
	else
		throw EndOfFileException(BOOST_CURRENT_FUNCTION);
}

/**
 @brief 检测是否真的到了 eof

  在 fileptr 到达文件末尾但之前的一次 fread(buf,1,len,fp)==len 时，
  feof(fp) 返回 false，并非应该预期的 true
  所以，在这里，做一次真实的判断，看是否的确到了 eof

 @note
  -# 只有当 m_pos == m_end 时，该函数才会被调用
 */
int ZcReader::test_eof()
{
	// 只有当 m_pos == m_end 时，该函数才会被调用
	assert(m_pos == m_end);
	assert(NULL != m_is);
	assert(0 != m_bufsize);

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

void ZcReader::getline(std::string& line, size_t maxlen)
{
	assert(NULL != m_is);
	assert(0 != m_bufsize);

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
				line.append((char*)m_pos, (char*)m_end);
				assert(line.size() == len);

				size_t nRead;
			   	m_beg = (unsigned char*)m_is->zcRead(m_bufsize, &nRead);
				m_end = m_beg + nRead;
				m_pos = m_beg;
				if (0 == nRead)
				{
					if (line.empty())
						throw EndOfFileException("ZcReader::getline, read 0 byte");
					else {
						m_pos = m_end = m_beg = NULL;
						return;
					}
				}
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
				try {
					int nextCh = readByte();
					if ('\n' != nextCh)
						// not line feed, push back the byte
						--m_pos;
				}
				catch (const EndOfFileException&) {
					// ignore
				}
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

//////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////

ZcWriter::~ZcWriter()
{
	flush();
}

size_t ZcWriter::flush_and_write(const void* vbuf, size_t length)
{
	assert(NULL != m_os);
	return do_flush_and_write(vbuf, length);
}

void ZcWriter::flush_and_ensureWrite(const void* vbuf, size_t length)
{
	assert(NULL != m_os);
	size_t n = do_flush_and_write(vbuf, length);
	if (n != length)
	{
		char szbuf[256];
		sprintf(szbuf, "\"%s\", WriteBytes[want=%lld, read=%lld]"
				, BOOST_CURRENT_FUNCTION
				, (long long)length
				, (long long)n
				);
		throw OutOfSpaceException(szbuf);
	}
}

void ZcWriter::flush_and_write_byte(byte b)
{
	assert(NULL != m_os);
	assert(m_pos == m_end);
	do_flush_and_write(&b, 1);
}

size_t ZcWriter::do_flush_and_write(const void* vbuf, size_t length)
{
	assert(NULL != m_os);

	size_t n1 = m_end - m_pos; // remain bytes in buffer
	size_t n2 = length - n1;   // remain bytes to read

	memcpy(m_pos, vbuf, n1);   // write data to available space remain inbuf

	while (n2 > 0)
	{
		size_t toWrite  = max(n2, m_bufsize);
		size_t writable;
	   	m_beg = (unsigned char*)m_os->zcWrite(toWrite, &writable);
		if (terark_unlikely(0 == writable)) {
			char szMsg[128];
			sprintf(szMsg, "ZcWriter::do_flush_and_write, 1 write 0 byte, n1=%u, n2=%u", (unsigned)n1, (unsigned)n2);
			throw DelayWriteException(szMsg);
		//	throw BrokenPipeException(szMsg);
 		}
		size_t n3 = min(n2, writable);
		memcpy(m_beg, (byte*)vbuf + n1, n3);
		n2 -= n3;
		n1 += n3;
		m_pos = m_beg + n3;
		m_end = m_beg + writable;
	}
	assert(n1 == length);
	return length;
}

void ZcWriter::flush()
{
	assert(NULL != m_os);
	m_os->zcFlush(m_pos - m_beg);
}

#define STREAM_READER ZcReader
#define STREAM_WRITER ZcWriter
#include "var_int_io.hpp"

} // namespace terark

