/* vim: set tabstop=4 : */
#ifndef __terark_io_MemMapStream_h__
#define __terark_io_MemMapStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <stdio.h>
#include <assert.h>
#include <string.h> // for memcpy

#include <terark/stdtypes.hpp>
//#include "../io/MemStream.hpp"

//#include <boost/type_traits/integral_constant.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/iterator.hpp>
#include <boost/operators.hpp>

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#	include <sys/types.h>
#	include <sys/stat.h>
#	include <fcntl.h>
#   define WIN32_LEAN_AND_MEAN
#   define NOMINMAX
#   include <windows.h>
#else
#	include <sys/types.h>
#	include <sys/stat.h>
#   include <sys/mman.h>
#	include <unistd.h>
#	include <fcntl.h>
#	include <errno.h>
#endif

namespace terark {

/**
 * MemMapStream may be slower than buffered read/write ReadFile/WriteFile
 *    because MemMapStream may cause OS to close prefetch
 */
class TERARK_DLL_EXPORT MemMapStream
{
	DECLARE_NONE_COPYABLE_CLASS(MemMapStream)

public:
	typedef boost::mpl::true_ is_seekable;

public:
	MemMapStream(stream_position_t new_file_size, const std::string& fpath, int mode);
	MemMapStream();
	~MemMapStream();

	bool is_open() const throw();
	void open(stream_position_t new_file_size, const std::string& fpath, int mode);
	void clone(const MemMapStream& source);
	void close();

	bool eof() const throw();
	int  getByte();
	byte readByte();
	void writeByte(byte b);

	void probe(size_t size);
	void skip(size_t size);
	//! must call probe(size) before calling this function
	void unchecked_skip(size_t size);

	size_t read(void* buf, size_t size);
	size_t write(const void* buf, size_t size);

	void ensureRead(void* buf, size_t size);
	void ensureWrite(const void* buf, size_t size);

	void flush();

	void seek(stream_offset_t offset, int origin);
	void seek(stream_position_t pos);
	stream_position_t tell() const throw();
	stream_position_t size() const throw();

	//! 均指当前被 map 的部分
	unsigned char* current() const throw() { return m_pos; }
	unsigned char* begin() const throw() { return m_beg; }
	unsigned char* end() const throw() { return m_end; }
	size_t map_size() const throw() { return m_end - m_beg; }
	size_t map_tell() const throw() { return m_pos - m_beg; }
	size_t map_available() const throw() { return m_end - m_pos; }

	void unchecked_write(const void* data, size_t size) throw();
	void unchecked_read(void* data, size_t size) throw();

	void remap(stream_position_t aligned_fpos, size_t unaligned_size);
	bool try_remap(stream_position_t aligned_fpos, size_t unaligned_size);
	void unaligned_remap(stream_position_t unaligned_fpos, size_t unaligned_size);

	void set_fsize(stream_position_t fsize);

	void* map(stream_position_t fpos, size_t size);
	void* map(stream_position_t fpos, size_t size, int mode);
	void  unmap(void* base, size_t size);

	size_t page_size() const { return m_page_size; }
	size_t best_block_size() const { return m_best_block_size; }
	void set_best_block_size(size_t n) { m_best_block_size = n; }

	const std::string& fpath() const { return m_fpath; }

// 	int errcode() const throw() { return m_errno; }
// 	std::string errmsg() const throw();

	int file_handle() const { return (int)(size_t)m_hFile; }

	void align(stream_position_t* fpos, size_t* size)
	{
		stream_position_t base_off = align_down(*fpos, m_page_size);
		*size = align_up(*fpos - base_off + *size, m_page_size);
		*fpos = base_off;
	}

	int BinCompare(MemMapStream& y);

protected:
	unsigned char* m_beg;
	unsigned char* m_pos;
	unsigned char* m_end;

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	HANDLE m_hFile;
	HANDLE m_hMap;
#else
	int m_hFile;
#endif

	int  m_mode;
//	int  m_errno;

	stream_position_t m_file_size;
	stream_position_t m_file_pos;

	size_t m_best_block_size;
	size_t m_page_size;
	size_t m_AllocationGranularity;
	std::string m_fpath;

	void init();
	void init(stream_position_t new_file_size, const std::string& fpath, int mode);

	stream_position_t get_fsize();

	bool remap_impl(stream_position_t fpos, size_t size);

	void remap_and_skip(size_t size);
	void remap_and_probe(size_t size);

	size_t remap_and_read(void* buf, size_t size);
	size_t remap_and_write(const void* buf, size_t size);

	void remap_and_ensureRead(void* buf, size_t size);
	void remap_and_ensureWrite(const void* buf, size_t size);

	int  remap_and_getByte();
	byte remap_and_readByte();
	void remap_and_writeByte(unsigned char b);

	void cleanup_and_throw(const char* msg);
};

inline void MemMapStream::probe(size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		// do nothing ...
	} else
		remap_and_probe(size);
}

inline void MemMapStream::skip(size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		m_pos += size;
	} else
		remap_and_skip(size);
}

inline void MemMapStream::unchecked_skip(size_t size)
{
	// if this assert fails, caller maybe not call probe(size) first
	assert(m_pos+size <= m_end);

	m_pos += size;
}

inline size_t MemMapStream::read(void* buf, size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		memcpy(buf, m_pos, size);
		m_pos += size;
		return size;
	} else
		return remap_and_read(buf, size);
}

inline size_t MemMapStream::write(const void* buf, size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		memcpy(m_pos, buf, size);
		m_pos += size;
		return size;
	} else
		return remap_and_write(buf, size);
}

inline void MemMapStream::ensureRead(void* buf, size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		memcpy(buf, m_pos, size);
		m_pos += size;
	} else
		remap_and_ensureRead(buf, size);
}

inline void MemMapStream::ensureWrite(const void* buf, size_t size)
{
	if (terark_likely(m_pos+size <= m_end)) {
		memcpy(m_pos, buf, size);
		m_pos += size;
	} else
		remap_and_ensureWrite(buf, size);
}

inline byte MemMapStream::readByte()
{
	if (terark_likely(m_pos < m_end))
		return *m_pos++;
	else
		return remap_and_readByte();
}

inline int MemMapStream::getByte()
{
	if (terark_likely(m_pos < m_end))
		return *m_pos++;
	else
		return remap_and_getByte();
}

inline void MemMapStream::writeByte(byte b)
{
	if (terark_likely(m_pos < m_end))
		*m_pos++ = b;
	else
		remap_and_writeByte(b);
}

inline stream_position_t MemMapStream::tell() const throw()
{
	return m_file_pos + (m_pos - m_beg);
}

inline void MemMapStream::unchecked_write(const void* data, size_t size) throw()
{
	assert(m_pos <= m_end);
	memcpy(m_pos, data, size);
	m_pos += size;
}
inline void MemMapStream::unchecked_read(void* data, size_t size) throw()
{
	assert(m_pos <= m_end);
	memcpy(data, m_pos, size);
	m_pos += size;
}

inline bool MemMapStream::eof() const throw()
{
	return tell() == m_file_size;
}

template<class T>
class MemMap_Iterator :
	public boost::forward_iterator_helper<MemMap_Iterator<T>, T>
{
public:
	explicit MemMap_Iterator(MemMapStream* mms = 0) : mms(mms) {}

	T& operator*() const
	{
		mms->probe(sizeof(T));
		return *(T*)mms->current();
	}

	MemMap_Iterator& operator++()
	{
		mms->skip(sizeof(T));
		return *this;
	}

private:
	MemMapStream* mms;
};

class MMS_MapRegion
{
	DECLARE_NONE_COPYABLE_CLASS(MMS_MapRegion)

	stream_position_t m_fpos;
	size_t            m_size;
	MemMapStream*     m_mms;
	unsigned char*    m_base;

public:
	MMS_MapRegion(MemMapStream& mms, stream_position_t fpos, size_t size)
	{
		m_mms  = &mms;
		m_fpos = fpos;
		m_size = size;
		m_base = (unsigned char*)mms.map(fpos, size);
	}
	MMS_MapRegion(MemMapStream& mms, stream_position_t fpos, size_t size, int flag)
	{
		m_mms  = &mms;
		m_fpos = fpos;
		m_size = size;
		m_base = (unsigned char*)mms.map(fpos, size, flag);
	}
	~MMS_MapRegion()
	{
		m_mms->unmap(m_base, m_size);
	}

	stream_position_t fpos() const { return m_fpos; }
	size_t            size() const { return m_size; }
	unsigned char*    base() const { return m_base; }

	bool cover(stream_position_t pos, size_t length) const
	{
		return pos >= m_fpos && pos + length <= m_fpos + m_size;
	}

	void swap(MMS_MapRegion& y)
	{
		std::swap(m_fpos, y.m_fpos);
		std::swap(m_size, y.m_size);
		std::swap(m_mms , y.m_mms );
		std::swap(m_base, y.m_base);
	}
};

//! 可以映射不对齐的内存数据
class TERARK_DLL_EXPORT MMS_MapData
{
	DECLARE_NONE_COPYABLE_CLASS(MMS_MapData)

	stream_position_t m_base_pos;
	size_t            m_offset;
	size_t            m_size;
	MemMapStream*     m_mms;
	unsigned char*    m_base_ptr;

public:
	MMS_MapData(MemMapStream& mms, stream_position_t fpos, size_t size);
	MMS_MapData(MemMapStream& mms, stream_position_t fpos, size_t size, int flag);
	~MMS_MapData();

	stream_position_t fpos() const { return m_base_pos + m_offset; }
	size_t size() const { return m_size; }
	unsigned char* data() const { return m_base_ptr + m_offset; }

	void swap(MMS_MapData& y)
	{
		std::swap(m_base_pos, y.m_base_pos);
		std::swap(m_offset  , y.m_offset);
		std::swap(m_size    , y.m_size);
		std::swap(m_mms     , y.m_mms);
		std::swap(m_base_ptr, y.m_base_ptr);
	}
};

} // namespace terark

#endif

