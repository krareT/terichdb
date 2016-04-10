/* vim: set tabstop=4 : */
#ifndef __terark_io_HexCodingStream_h__
#define __terark_io_HexCodingStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <assert.h>
//#include <string.h>
#include <stdio.h>
#include <iostream>
#include <boost/current_function.hpp>
#include "IOException.hpp"
#include "MemStream.hpp"
#include <terark/util/DataBuffer.hpp>

namespace terark {

extern const unsigned char G_hex_val_hexTab[];

void invalid_hex_char(unsigned char ch, const char* func);

inline unsigned char hex_val(unsigned char ch, const char* func)
{
	if (ch < '0' || ch >= 'f')
		invalid_hex_char(ch, func);
	unsigned char hv = G_hex_val_hexTab[ch-'0'];
	if (255 == hv)
		invalid_hex_char(ch, func);
	return hv;
}

inline unsigned char hex_val(unsigned char ch)
{
	return hex_val(ch, BOOST_CURRENT_FUNCTION);
}

template<class ByteStream>
class HexCodingStream
{
	ByteStream* m_bs;

	unsigned char do_readByte(const char* func)
	{
		unsigned char h4 = m_bs->readByte();
		unsigned char l4 = m_bs->readByte();

		return hex_val(h4, func) << 4 | hex_val(l4, func);
	}

public:
	typedef typename ByteStream::is_seekable is_seekable;

	explicit HexCodingStream(ByteStream* bs) : m_bs(bs) {}

	bool eof()  const throw() { return m_bs->eof(); }
	void flush() { m_bs->flush(); }

	unsigned char readByte()
	{
		return do_readByte(BOOST_CURRENT_FUNCTION);
	}
	int  getByte()
	{
		try {
			return do_readByte(BOOST_CURRENT_FUNCTION);
		} catch (const EndOfFileException& exp) {
			return -1;
		}
	}
	void writeByte(unsigned char b)
	{
		const char hexChar[] = "0123456789ABCDEF";
		m_bs->writeByte(hexChar[b >> 4]);
		m_bs->writeByte(hexChar[b & 15]);
	}
	void ensureRead(void* data, size_t length)
	{
		unsigned char* pb = (unsigned char*)(data);
		while (length)
		{
			*pb = do_readByte(BOOST_CURRENT_FUNCTION);
			pb++; length--;
		}
	}
	void ensureWrite(const void* data, size_t length)
	{
		const unsigned char* pb = (const unsigned char*)(data);
		while (length)
		{
			writeByte(*pb);
			pb++; length--;
		}
	}
	size_t read(void* data, size_t length)
	{
		unsigned char* pb = (unsigned char*)(data);
		try {
			while (length)
			{
				*pb = do_readByte(BOOST_CURRENT_FUNCTION);
				pb++; length--;
			}
		} catch (const EndOfFileException& exp) {
			// ignore
		}
		return pb - (unsigned char*)(data);
	}
	size_t write(const void* data, size_t length)
	{
		const unsigned char* pb = (const unsigned char*)(data);
		try {
			while (length)
			{
				writeByte(*pb);
				pb++; length--;
			}
		} catch (const OutOfSpaceException&) {
			// ignore
		}
		return pb - (unsigned char*)(data);
	}
};

//class AutoGrownMemIO; // declare
typedef HexCodingStream<AutoGrownMemIO> HexAutoGrownMemIO;

template<class SrcStream, class DstStream>
DstStream& bin_dump_hex(SrcStream& src, DstStream& dst)
{
	const size_t buf_len = 4*1024;
	DataBufferPtr data_buf(buf_len);
	AutoGrownMemIO hex_buf(2*buf_len);
	HexCodingStream<AutoGrownMemIO> hexMS(&hex_buf);
	try {
		while (true)
		{
			size_t len = src.read(data_buf->data(), buf_len);
			if (len) {
				hex_buf.seek(0);
				hexMS.write(data_buf->data(), len);
				dst.write((char*)hex_buf.buf(), 2*len);
			} else
				break;
		}
	} catch (EndOfFileException&) {
		// ignore...
	}
	return dst;
}

template<class SrcStream>
class bin_dump_hex_manip
{
	SrcStream& m_src;

public:
	explicit bin_dump_hex_manip(SrcStream& src) : m_src(src) {}

	friend std::ostream& operator<<(std::ostream& os, bin_dump_hex_manip<SrcStream> x)
	{
		bin_dump_hex(x.m_src, os);
		return os;
	}
};

template<class SrcStream>
bin_dump_hex_manip<SrcStream>
bin_dump_hex(SrcStream& src)
{
	return bin_dump_hex_manip<SrcStream>(src);
}

} // namespace terark

#endif

