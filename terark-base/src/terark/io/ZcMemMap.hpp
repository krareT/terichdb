/* vim: set tabstop=4 : */
#ifndef __terark_io_ZcMemMap_h__
#define __terark_io_ZcMemMap_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <stdio.h>
#include <assert.h>
#include <string.h> // for memcpy

#include <terark/stdtypes.hpp>
#include "IStream.hpp"
#include "ZeroCopy.hpp"

//#include <boost/type_traits/integral_constant.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/iterator.hpp>
#include <boost/operators.hpp>

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#	include <sys/types.h>
#	include <sys/stat.h>
#	include <fcntl.h>
#   define NOMINMAX
#   define WIN32_LEAN_AND_MEAN
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

class TERARK_DLL_EXPORT ZcMemMap
   	: public IZeroCopyInputStream
	, public IZeroCopyOutputStream
	, public ISeekable
{
	DECLARE_NONE_COPYABLE_CLASS(ZcMemMap)

public:
	typedef boost::mpl::true_ is_seekable;

	static void ThrowOpenFileException(const char* fpath, int mode);

public:
	ZcMemMap(stream_position_t new_file_size, const std::string& fpath, int mode);
	ZcMemMap();
	~ZcMemMap();

	bool is_open() const throw();
	void open(stream_position_t new_file_size, const std::string& fpath, int mode);
	void clone(const ZcMemMap& source);
	void close();

	bool eof() const;

	const void* zcRead(size_t length, size_t* readed) override;
	void* zcWrite(size_t length, size_t* writable) override;

	void zcFlush(size_t nWritten);

	void seek(stream_offset_t offset, int origin);
	void seek(stream_position_t pos);
	stream_position_t tell() const;
	stream_position_t size() const;

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

	void cleanup_and_throw(const char* msg);
};



} // namespace terark

#endif

