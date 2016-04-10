/* vim: set tabstop=4 : */
#ifndef __terark_io_byte_io_impl_h__
#define __terark_io_byte_io_impl_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

// #include "IOException.hpp"

#define BYTE_IO_EMPTY

#define TERARK_GEN_ensureRead(prefix)  \
	void prefix ensureRead(void* vbuf, size_t length) \
	{ \
		size_t n = this->read(vbuf, length); \
		if (n != length) \
		{ \
			string_appender<> oss; \
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\"" \
				<< ", ReadBytes[want=" << length << ", read=" << n << "]"; \
			throw EndOfFileException(oss.str().c_str()); \
		} \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define TERARK_GEN_ensureWrite(prefix)  \
	void prefix ensureWrite(const void* vbuf, size_t length) \
	{ \
		size_t n = this->write(vbuf, length); \
		if (n != length) \
		{ \
			string_appender<> oss; \
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\"" \
				<< ", WriteBytes[want=" << length << ", written=" << n << "]"; \
			throw OutOfSpaceException(oss.str().c_str()); \
		} \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define TERARK_GEN_getByte(prefix) \
	int prefix getByte() \
	{ \
		unsigned char b; \
		if (this->read(&b, 1) == 0) \
			return -1; \
		return b; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define TERARK_GEN_readByte(prefix) \
	unsigned char prefix readByte() \
	{ \
		unsigned char b; \
		if (this->read(&b, 1) == 0) \
			throw EndOfFileException(BOOST_CURRENT_FUNCTION); \
		return b; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define TERARK_GEN_writeByte(prefix) \
	void prefix writeByte(unsigned char b) \
	{ \
		if (this->write(&b, 1) == 0) \
			throw OutOfSpaceException(BOOST_CURRENT_FUNCTION); \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

/*
namespace terark {

template<class Stream>
class InputStream_Method_Impl
{
public:
	unsigned char readByte()
	{
		unsigned char b;
		if (static_cast<Stream&>(*this).read(&b, 1) == 0)
			throw EndOfFileException(BOOST_CURRENT_FUNCTION);
		return b;
	}

	void ensureRead(void* vbuf, size_t length)
	{
		size_t n = static_cast<Stream&>(*this).read(vbuf, length);
		if (n != length)
		{
			string_appender<> oss;
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
				<< ", ReadBytes[want=" << length << ", read=" << n << "]";
			throw EndOfFileException(oss.str().c_str());
		}
	}
};

template<class Stream>
class OutputStream_Method_Impl
{
public:
	void writeByte(unsigned char b)
	{
		if (static_cast<Stream&>(*this).write(&b, 1) == 0)
			throw OutOfSpaceException(BOOST_CURRENT_FUNCTION);
	}

	void ensureWrite(const void* vbuf, size_t length)
	{
		size_t n = static_cast<Stream&>(*this).write(vbuf, length);
		if (n != length)
		{
			string_appender<> oss;
			oss << "\"" << BOOST_CURRENT_FUNCTION << "\""
				<< ", WriteBytes[want=" << length << ", written=" << n << "]";
			throw OutOfSpaceException(oss.str().c_str());
		}
	}
};

template<class Stream>
class ByteIoImplement :
   	public  InputStream_Method_Impl<Stream>,
   	public OutputStream_Method_Impl<Stream>
{
public:
};

} // namespace terark
*/

#endif

