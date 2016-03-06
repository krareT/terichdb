/* vim: set tabstop=4 : */
#ifndef __terark_io_hole_stream_h__
#define __terark_io_hole_stream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <assert.h>
//#include <string.h> // for memcpy
//#include <boost/current_function.hpp>
//#include <boost/type_traits/integral_constant.hpp>

#include <terark/stdtypes.hpp>
//#include "IOException.hpp"

namespace terark {

class HoleStream
{
public:
	explicit HoleStream() : m_pos(0) {}

//	size_t read(void* vbuf, size_t length) { m_pos += length; return length; }
	size_t write(const void* vbuf, size_t length) { m_pos += length; return length; }

//	void ensureRead(void* vbuf, size_t length) { m_pos += length; }
	void ensureWrite(const void* vbuf, size_t length) { m_pos += length; }

//	byte readByte() { return 0; }

	void writeByte(unsigned char) { m_pos++; }

private:
	stream_position_t m_pos;
};

class SeekableHoleStream
{
public:
	explicit SeekableHoleStream(stream_position_t size)
	{
		m_pos  = 0;
		m_size = size;
	}

private:
	stream_position_t m_pos;
	stream_position_t m_size;
};

} // namespace terark

#endif

