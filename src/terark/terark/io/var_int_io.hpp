// file var_int_io.hpp
//    for included by other cpp files
//
// input macro param:
//    STREAM_READER
//    STREAM_WRITER
// must at least define one of them
//
#if !defined(STREAM_READER) && !defined(STREAM_WRITER)
#  error "must define macro STREAM_READER or STREAM_WRITER"
#endif

#ifdef _MSC_VER
/* Always compile this module for speed, not size */
#pragma optimize("t", on)
#endif

#include "var_int_inline.hpp"

#ifdef STREAM_READER

uint32_t STREAM_READER::read_var_uint32()
{
	if (terark_likely(this->buf_remain_bytes() >= 5))
	{
		return gg_load_var_uint<uint32_t>(m_pos, (const unsigned char**)&m_pos, BOOST_CURRENT_FUNCTION);
	}
	else // slower branch
   	{
		return gg_load_var_uint_slow<STREAM_READER, uint32_t>(*this, BOOST_CURRENT_FUNCTION);
	}
}

uint64_t STREAM_READER::read_var_uint64()
{
	if (terark_likely(this->buf_remain_bytes() >= 10))
	{
		return gg_load_var_uint<uint64_t>(m_pos, (const unsigned char**)&m_pos, BOOST_CURRENT_FUNCTION);
	}
	else // slower branch
   	{
		return gg_load_var_uint_slow<STREAM_READER, uint64_t>(*this, BOOST_CURRENT_FUNCTION);
	}
}

int32_t STREAM_READER::read_var_int32()
{
	return var_int32_u2s(read_var_uint32());
}

int64_t STREAM_READER::read_var_int64()
{
	return var_int64_u2s(read_var_uint64());
}

///////////////////////////////////////////////////////////////////////////////////////////

uint32_t STREAM_READER::read_var_uint30()
{
	if (terark_likely(this->buf_remain_bytes() >= 4))
	{
		return gg_load_var_uint30(m_pos, (const unsigned char**)&m_pos);
	}
	else // slower branch
	{
		return gg_load_var_uint30_slow<STREAM_READER>(*this);
	}
}

uint64_t STREAM_READER::read_var_uint61()
{
	if (terark_likely(this->buf_remain_bytes() >= 8))
	{
		return gg_load_var_uint61(m_pos, (const unsigned char**)&m_pos);
	}
	else // slower branch
	{
		return gg_load_var_uint61_slow<STREAM_READER>(*this);
	}
}

int32_t STREAM_READER::read_var_int30()
{
	return var_int30_u2s(read_var_uint30());
}

int64_t STREAM_READER::read_var_int61()
{
	return var_int61_u2s(read_var_uint61());
}

void STREAM_READER::read_string(std::string& str)
{
	size_t len = read_var_uint32();
	str.resize(len);
	if (terark_likely(len))
		ensureRead(&str[0], len);
}

#endif // STREAM_READER

#ifdef STREAM_WRITER

void STREAM_WRITER::write_var_uint32(uint32_t x)
{
	if (this->buf_remain_bytes() >= 5)
	{
		unsigned char* endp = gg_save_var_uint<uint32_t>(m_pos, x);
		assert(endp - m_pos <= 5);
		m_pos = endp;
	}
	else
   	{
		unsigned char tmpbuf[5];
		ptrdiff_t len = gg_save_var_uint<uint32_t>(tmpbuf, x) - tmpbuf;
		assert(len <= 5);
		ensureWrite(tmpbuf, len);
	}
}

void STREAM_WRITER::write_var_uint64(uint64_t x)
{
	if (this->buf_remain_bytes() >= 10)
   	{
		unsigned char* endp = gg_save_var_uint<uint64_t>(m_pos, x);
		assert(endp - m_pos <= 10);
		m_pos = endp;
	}
	else {
		unsigned char tmpbuf[10];
		ptrdiff_t len = gg_save_var_uint<uint64_t>(tmpbuf, x) - tmpbuf;
		assert(len <= 10);
		ensureWrite(tmpbuf, len);
	}
}

void STREAM_WRITER::write_var_int32(int32_t x)
{
	write_var_uint32(var_int32_s2u(x));
}

void STREAM_WRITER::write_var_int64(int64_t x)
{
	write_var_uint64(var_int64_s2u(x));
}

///////////////////////////////////////////////////////////////////////////////////////////

void STREAM_WRITER::write_var_uint30(uint32_t x)
{
	if (this->buf_remain_bytes() >= 4)
	{
		unsigned char* endp = gg_save_var_uint30(m_pos, x);
		assert(endp - m_pos <= 4);
		m_pos = endp;
	}
	else
	{
		unsigned char tmpbuf[4];
		ptrdiff_t len = gg_save_var_uint30(tmpbuf, x) - tmpbuf;
		assert(len <= 4);
		ensureWrite(tmpbuf, len);
	}
}

void STREAM_WRITER::write_var_uint61(uint64_t x)
{
	if (this->buf_remain_bytes() >= 8)
	{
		unsigned char* endp = gg_save_var_uint61(m_pos, x);
		assert(endp - m_pos <= 8);
		m_pos = endp;
	}
	else {
		unsigned char tmpbuf[8];
		ptrdiff_t len = gg_save_var_uint61(tmpbuf, x) - tmpbuf;
		assert(len <= 8);
		ensureWrite(tmpbuf, len);
	}
}

void STREAM_WRITER::write_var_int30(int32_t x)
{
	write_var_uint30(var_int30_s2u(x));
}

void STREAM_WRITER::write_var_int61(int64_t x)
{
	write_var_uint61(var_int61_s2u(x));
}


void STREAM_WRITER::write_string(const std::string& str)
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
void STREAM_WRITER::write_string(const char* str, size_t len)
{
	write_var_uint32(len);
	ensureWrite(str, len);
}
#endif // 0

#endif // STREAM_WRITER

#undef STREAM_READER
#undef STREAM_WRITER


