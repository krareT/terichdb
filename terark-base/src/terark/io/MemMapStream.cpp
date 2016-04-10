/* vim: set tabstop=4 : */

#include <assert.h>
#include <string.h>
#include <terark/num_to_str.hpp>
#include <stdexcept>

#include "MemMapStream.hpp"
#include "IOException.hpp"
#include "IStream.hpp"

namespace terark {

static void write_error_msg(string_appender<>& oss)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	int errCode = GetLastError();
	HLOCAL hLocal = NULL;
	DWORD dwTextLength = FormatMessageA(
		FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_ALLOCATE_BUFFER,
		NULL,
		errCode,
		0, //MAKELANGID(LANG_CHINESE, SUBLANG_CHINESE_SIMPLIFIED)
		(LPSTR)&hLocal,
		0,
		NULL
		);
	LPCSTR pszMsg = (LPCSTR)LocalLock(hLocal);
	oss << "error[code=" << errCode << ", message=" << pszMsg << "]";
	LocalFree(hLocal);

#else
	char szbuf[256];
	int  errCode = errno;
	if (strerror_r(errCode, szbuf, 256) != 0)
		snprintf(szbuf, sizeof(szbuf), "strerror_r failed");
	oss << "error[code=" << errCode << ", message=" << szbuf << "]";
#endif
}

static std::string error_info(const std::string& fpath, const char* uinfo)
{
	string_appender<> oss;
	oss << "file=\"" << fpath << "\"" << ", " << uinfo << " : ";
	write_error_msg(oss);
	return oss.str();
}

MMS_MapData::MMS_MapData(MemMapStream& mms, stream_position_t fpos, size_t size)
{
	m_mms = &mms;
	m_size = size;
	m_base_ptr = 0; // maybe failure, so first set to 0
	m_base_pos = align_down(fpos, mms.page_size());
	m_offset = fpos - m_base_pos;
	m_base_ptr = (unsigned char*)mms.map(m_base_pos, align_up(m_offset + size, mms.page_size()));
}

MMS_MapData::MMS_MapData(MemMapStream& mms, stream_position_t fpos, size_t size, int flag)
{
	m_mms = &mms;
	m_size = size;
	m_base_ptr = 0; // maybe failure, so first set to 0
	m_base_pos = align_down(fpos, mms.page_size());
	m_offset = fpos - m_base_pos;
	m_base_ptr = (unsigned char*)mms.map(m_base_pos, align_up(m_offset + size, mms.page_size()), flag);
}

MMS_MapData::~MMS_MapData()
{
	if (m_base_ptr)
		m_mms->unmap(m_base_ptr, align_up(m_offset + m_size, m_mms->page_size()));
}

//////////////////////////////////////////////////////////////////////////

MemMapStream::MemMapStream(stream_position_t new_file_size, const std::string& fpath, int mode)
{
	init();
   	open(new_file_size, fpath, mode);
}

MemMapStream::MemMapStream()
{
	init();
}

MemMapStream::~MemMapStream()
{
	close();
}

void MemMapStream::init(stream_position_t new_file_size, const std::string& fpath, int mode)
{
	init();
	m_file_size = new_file_size;
	m_fpath = fpath;
	m_mode = mode;
}

void MemMapStream::init()
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	m_hFile = INVALID_HANDLE_VALUE;
	m_hMap = 0;
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	m_page_size = si.dwPageSize;
	m_AllocationGranularity = si.dwAllocationGranularity;
#else
	m_hFile = -1;
	m_page_size = sysconf(_SC_PAGESIZE);
	m_AllocationGranularity = m_page_size;
#endif
	m_file_pos = 0;
	m_file_size = 0;
	m_beg = m_pos = m_end = 0;
	m_best_block_size = 256 * 1024;
	assert(m_best_block_size % m_AllocationGranularity == 0);
}

void MemMapStream::clone(const MemMapStream& source)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	HANDLE hProcess = GetCurrentProcess();
	BOOL bRet = DuplicateHandle(
		hProcess,
		source.m_hFile,
		hProcess,
		&m_hFile,
		0, //(m_mode & O_RDWR) ? GENERIC_ALL : GENERIC_READ,
		FALSE,
		DUPLICATE_SAME_ACCESS
		);
    if (m_hFile == INVALID_HANDLE_VALUE)
        cleanup_and_throw("failed on calling DuplicateHandle");
	m_hMap = 0;
#else
	m_hFile = ::dup(source.m_hFile);
#endif
	m_page_size = source.m_page_size;
	m_AllocationGranularity = source.m_AllocationGranularity;
	m_file_pos = 0;
	m_beg = m_pos = m_end = 0;
	m_best_block_size = source.m_best_block_size;
	m_fpath = source.m_fpath;
}

void* MemMapStream::map(stream_position_t fpos, size_t size)
{
	return map(fpos, size, m_mode);
}

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)

void* MemMapStream::map(stream_position_t fpos, size_t size, int mode)
{
	if ((mode & O_RDWR) && m_file_size < fpos + size)
	{
		set_fsize(fpos + size);
		m_file_size = fpos + size;
		if (NULL != m_hMap)
			CloseHandle(m_hMap), m_hMap = NULL;
	}
	if (NULL == m_hMap)
	{
		m_hMap = CreateFileMapping(m_hFile
			, NULL
			, !(m_mode & O_RDWR) ? PAGE_READONLY : PAGE_READWRITE
			, 0
			, 0
			, NULL
			);
		if (NULL == m_hMap)
			cleanup_and_throw("failed CreateFileMapping");
	}
	void* base =
        ::MapViewOfFileEx( m_hMap,
						   (mode & O_RDWR) ? FILE_MAP_WRITE : FILE_MAP_READ,
                           (DWORD) (fpos >> 32),
                           (DWORD) (fpos & 0xffffffff),
                           size,
						   (LPVOID) 0
						   );
	if (0 == base)
		throw IOException(error_info(m_fpath, "failed MapViewOfFileEx").c_str());
    return base;
}

void MemMapStream::unmap(void* base, size_t size)
{
	::UnmapViewOfFile(base);
}

void MemMapStream::open(stream_position_t new_file_size, const std::string& fpath, int mode)
{
    using namespace std;

    if (is_open())
        throw IOException("file already open");

	init(new_file_size, fpath, mode);

	bool readonly = !(m_mode & O_RDWR);
    m_hFile = ::CreateFileA(fpath.c_str(),
                       readonly ? GENERIC_READ : GENERIC_READ|GENERIC_WRITE,
                       FILE_SHARE_READ,
                       NULL,
                       (m_mode & O_CREAT) ?
                           OPEN_ALWAYS :
                           OPEN_EXISTING,
                       readonly ?
                           FILE_ATTRIBUTE_READONLY :
                           FILE_ATTRIBUTE_TEMPORARY,
                       NULL);

    if (m_hFile == INVALID_HANDLE_VALUE)
        cleanup_and_throw("failed opening file");

    if (m_mode & O_TRUNC)
		set_fsize(new_file_size);

	m_file_size = get_fsize();

	return;
}

stream_position_t MemMapStream::get_fsize()
{
    typedef BOOL (WINAPI *func)(HANDLE, PLARGE_INTEGER);
    HMODULE hmod = ::GetModuleHandleA("kernel32.dll");
    func fp_get_size =
        reinterpret_cast<func>(::GetProcAddress(hmod, "GetFileSizeEx"));

    if (fp_get_size) {
        LARGE_INTEGER info;
        if (fp_get_size(m_hFile, &info)) {
            boost::uintmax_t fsize =
                ( (static_cast<boost::intmax_t>(info.HighPart) << 32) |
                  info.LowPart );
            return fsize;
        } else {
            cleanup_and_throw("failed getting file size");
        }
    } else {
        DWORD hi;
        DWORD low;
        if ( (low = ::GetFileSize(m_hFile, &hi))
                 !=
             INVALID_FILE_SIZE )
        {
            boost::uintmax_t fsize =
                (static_cast<boost::intmax_t>(hi) << 32) | low;
            return fsize;
        } else {
            cleanup_and_throw("failed getting file size");
        }
    }
	assert(0); // would not goes here...
	return 0;  // trim warning..
}

void MemMapStream::set_fsize(stream_position_t fsize)
{
    LONG sizehigh = LONG(fsize >> 32);
    LONG sizelow = LONG(fsize & 0xffffffff);
    ::SetFilePointer(m_hFile, sizelow, &sizehigh, FILE_BEGIN);
    if (::GetLastError() != NO_ERROR || !::SetEndOfFile(m_hFile))
        cleanup_and_throw("failed setting file size");
	m_file_size = fsize;
}

bool MemMapStream::remap_impl(stream_position_t fpos, size_t map_size)
{
	bool readonly = !(m_mode & O_RDWR);

	if (m_beg)
		::UnmapViewOfFile(m_beg);

	if (!readonly && m_file_size < fpos + map_size)
	{
		set_fsize(fpos + map_size);
		if (NULL != m_hMap)
			CloseHandle(m_hMap), m_hMap = NULL;
	}
	if (NULL == m_hMap)
	{
		m_hMap = CreateFileMapping(m_hFile
			, NULL
			, readonly ? PAGE_READONLY : PAGE_READWRITE
			, 0
			, 0
			, NULL
			);
		if (NULL == m_hMap)
			cleanup_and_throw("failed CreateFileMapping");
	}
    m_beg = (unsigned char*)
        ::MapViewOfFileEx( m_hMap,
                           readonly ? FILE_MAP_READ : FILE_MAP_WRITE,
                           (DWORD) (fpos >> 32),
                           (DWORD) (fpos & 0xffffffff),
                           map_size != size_t(-1L) ? map_size : 0, (LPVOID) 0 );
    return 0 != m_beg;
}

bool MemMapStream::is_open() const throw()
{
	return m_hFile != INVALID_HANDLE_VALUE;
}

#else // not windows

void* MemMapStream::map(stream_position_t fpos, size_t size, int mode)
{
	assert(fpos % m_AllocationGranularity == 0);
	if ((mode & O_RDWR) && m_file_size < fpos + size)
	{
		set_fsize(fpos + size);
	}
//	int flags = (mode & O_RDWR) ? MAP_SHARED : MAP_PRIVATE;
	int flags = MAP_SHARED;
	void* base = ::mmap(NULL, size,
                         (mode & O_RDWR) ? (PROT_READ | PROT_WRITE) : PROT_READ,
                         flags,
                         m_hFile, fpos);
	if (MAP_FAILED == base)
	{
		throw IOException(error_info(m_fpath, "failed mmap").c_str());
	}
    return base;
}

void MemMapStream::unmap(void* base, size_t size)
{
	assert(0 != base && MAP_FAILED != base);
	assert(size % m_page_size == 0);
	::munmap(base, size);
}

void MemMapStream::open(stream_position_t new_file_size, const std::string& fpath, int mode)
{
    using namespace std;
#if defined(O_LARGEFILE)
	mode |= O_LARGEFILE;
#endif
	init(new_file_size, fpath, mode);

    if (is_open())
        throw IOException("file already open");

    m_hFile = ::open(fpath.c_str(), mode, S_IRWXU);
    if (-1 == m_hFile)
        cleanup_and_throw("failed opening file");
	assert(-1 != m_hFile);

    if (m_mode & O_TRUNC)
		set_fsize(new_file_size);

	m_file_size = get_fsize();
	return;
}

stream_position_t MemMapStream::get_fsize()
{
    struct stat info;
    bool success = ::fstat(m_hFile, &info) != -1;
    if (success)
		return info.st_size;
	else {
        cleanup_and_throw("failed getting file size");
		return 0; // remove compiler warning
	}
}

void MemMapStream::set_fsize(stream_position_t fsize)
{
	if (ftruncate(m_hFile, fsize) == -1)
	{
		string_appender<> oss;
		oss << "failed ftruncate(" << fsize << "), old_fsize=" << m_file_size;
        cleanup_and_throw(oss.str().c_str());
	}
	m_file_size = fsize;
}

bool MemMapStream::remap_impl(stream_position_t fpos, size_t map_size)
{
	if (m_beg)
	{
		if (0 != munmap(m_beg, align_up(m_end-m_beg, m_page_size)))
			cleanup_and_throw("failed unmapping in MemMapStream::remap_impl");
	}

	if (m_mode & O_RDWR && m_file_size < fpos + map_size)
	{
		set_fsize(fpos + map_size);
	}
//	int flags = m_mode & O_RDWR ? MAP_SHARED : MAP_PRIVATE;
	int flags = MAP_SHARED;
    m_beg = (unsigned char*)::mmap(NULL, map_size,
                         m_mode & O_RDWR ? (PROT_READ | PROT_WRITE) : PROT_READ,
                         flags,
                         m_hFile, fpos);
    return (m_beg != MAP_FAILED);
}

bool MemMapStream::is_open() const throw()
{
	return -1 != m_hFile;
}

#endif

/**
 @brief

 @note:
  - aligned_fpos must align at m_AllocationGranularity
  - unaligned_size may be not aligned with m_page_size
 */
bool MemMapStream::try_remap(stream_position_t aligned_fpos, size_t unaligned_size)
{
	using namespace std;
	assert(aligned_fpos % m_AllocationGranularity == 0);
	if (aligned_fpos % m_AllocationGranularity != 0)
		throw IOException("can not map file offset not aligned at page size");

	if (!(m_mode & O_RDWR)) // read only
	{
		if (m_file_size <= aligned_fpos)
		{
			string_appender<> oss;
			oss << "map readonly file out of region, at" << BOOST_CURRENT_FUNCTION
				;
			throw EndOfFileException(oss.str().c_str());
		}
		else if (aligned_fpos + unaligned_size > m_file_size)
		{
			unaligned_size = size_t(m_file_size - aligned_fpos);
		}
	}
	size_t aligned_size = align_up(unaligned_size, m_page_size);
	bool bRet = remap_impl(aligned_fpos, aligned_size);
	if (bRet) {
		m_file_pos = aligned_fpos;
		m_pos = m_beg;
		m_end = m_beg + min(aligned_size, size_t(m_file_size - aligned_fpos));
	} else {
		m_beg = m_pos = m_end = NULL;
	}
	return bRet;
}

void MemMapStream::remap(stream_position_t aligned_fpos, size_t unaligned_size)
{
	if (!try_remap(aligned_fpos, unaligned_size))
	{
		cleanup_and_throw("failed mapping view");
	}
}

void MemMapStream::unaligned_remap(stream_position_t fpos, size_t size)
{
	using namespace std; // for max

	stream_position_t aligned_fpos = align_down(fpos, m_AllocationGranularity);
	size_t page_offset = size_t(fpos - aligned_fpos);
	size_t map_size = max(m_best_block_size, page_offset + size);
	if (!(m_mode & O_RDWR))
	{ // mapped area can not beyond file size
		stream_position_t remain = m_file_size - this->tell();
		if (map_size > remain)
			map_size = size_t(remain);
	}
	remap(aligned_fpos, map_size);

	assert(m_pos == m_beg);

	m_pos = m_beg + page_offset;
}

void MemMapStream::cleanup_and_throw(const char* msg)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	if (m_hMap != NULL)
        ::CloseHandle(m_hMap);
    if (m_hFile != INVALID_HANDLE_VALUE)
        ::CloseHandle(m_hFile);
#else
    if (-1 != m_hFile)
        ::close(m_hFile);
#endif

	init();

	throw IOException(error_info(m_fpath, msg).c_str());
}

void MemMapStream::close()
{
    bool error = false;

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	if (m_beg)
		error = !::UnmapViewOfFile(m_beg) || error;
	if (m_hMap != NULL)
		error = !::CloseHandle(m_hMap) || error;
	if (m_hFile != INVALID_HANDLE_VALUE)
	    error = !::CloseHandle(m_hFile) || error;
#else
	if (m_beg)
		error = ::munmap(m_beg, align_up(m_end-m_beg, m_page_size)) != 0 || error;
	if (-1 != m_hFile)
		error = ::close(m_hFile) != 0 || error;
#endif

	init();

    if (error)
	{
		string_appender<> oss;
		oss << "file=\"" << m_fpath << "\"" << ", error closing mapped file";
        throw IOException(oss.str().c_str());
	}
}

void MemMapStream::flush()
{
	// do nothing...?
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	::FlushFileBuffers(m_hFile);
#else
#endif
}

void MemMapStream::remap_and_skip(size_t size)
{
	using namespace std;
	assert(m_end-m_pos < ptrdiff_t(size));

	size_t curr = m_end-m_pos;
	while (curr != size)
	{
		unaligned_remap(m_file_pos + (m_end-m_beg), m_best_block_size);

		size_t nbytes = min(size_t(size-curr), size_t(m_end-m_pos));
		m_pos += nbytes;
		curr += nbytes;
	}
}

void MemMapStream::remap_and_probe(size_t size)
{
	using namespace std;
    // 如果 size 跨越了当前 map 的区域，
    // 就必须将当前 map 区域的最后部分也包括在 remap 区域中
    // 所以是 (m_pos-m_beg), 而非 (m_end-m_beg)

	stream_position_t old_pos = tell();
	unaligned_remap(m_file_pos + (m_pos-m_beg), max(m_best_block_size, size));
	m_pos = m_beg + (old_pos - m_file_pos);
}

size_t MemMapStream::remap_and_read(void* vbuf, size_t size)
{
	using namespace std;
	assert(m_end-m_pos < ptrdiff_t(size));

	unsigned char* bbuf = (unsigned char*)vbuf;
	size_t curr = m_end-m_pos;
	memcpy(bbuf, m_pos, curr);
	while (curr != size)
	{
		stream_position_t fpos = m_file_pos + (m_end-m_beg);
		if (fpos > m_file_size)
		{
			string_appender<> oss;
			oss << "this[fpos=" << m_file_pos
				<< ", map_size=" << (m_end-m_beg)
				<< ", fsize=" << m_file_size
				<< "], at: " << BOOST_CURRENT_FUNCTION
				;
			throw EndOfFileException(oss.str().c_str());
		}
		unaligned_remap(fpos, m_best_block_size);
		size_t nbytes = min(size_t(size-curr), size_t(m_end-m_pos));
		memcpy(bbuf+curr, m_pos, nbytes);
		m_pos += nbytes;
		curr += nbytes;
	}
	return curr;
}

size_t MemMapStream::remap_and_write(const void* vbuf, size_t size)
{
	using namespace std;
	assert(m_end-m_pos < ptrdiff_t(size));

	const unsigned char* bbuf = (const unsigned char*)vbuf;
	size_t curr = m_end-m_pos;
	memcpy(m_pos, bbuf, curr);
	while (curr != size)
	{
		unaligned_remap(m_file_pos + (m_pos-m_beg), m_best_block_size);

		size_t nbytes = min(size_t(size-curr), size_t(m_end-m_pos));
		memcpy(m_pos, bbuf+curr, nbytes);
		m_pos += nbytes;
		curr += nbytes;
	}
	return curr;
}

void MemMapStream::remap_and_ensureRead(void* buf, size_t size)
{
	size_t n = remap_and_read(buf, size);
	if (n != size)
	{
		string_appender<> oss;
		oss << BOOST_CURRENT_FUNCTION;
		throw IOException(oss.str().c_str());
	}
}

void MemMapStream::remap_and_ensureWrite(const void* buf, size_t size)
{
	size_t n = remap_and_write(buf, size);
	if (n != size)
	{
		string_appender<> oss;
		oss << BOOST_CURRENT_FUNCTION;
		throw IOException(oss.str().c_str());
	}
}

unsigned char MemMapStream::remap_and_readByte()
{
	unsigned char b;
	remap_and_ensureRead(&b, 1);
	return b;
}

int MemMapStream::remap_and_getByte()
{
	try {
		return remap_and_readByte();
	} catch (const std::exception&) {
		return -1;
	}
}

void MemMapStream::remap_and_writeByte(unsigned char b)
{
	remap_and_ensureWrite(&b, 1);
}

void MemMapStream::seek(stream_offset_t offset, int origin)
{
	switch (origin)
	{
	default:
		throw std::invalid_argument(BOOST_CURRENT_FUNCTION);
		break;
	case 0:
		seek(offset);
		break;
	case 1:
		seek(tell() + offset);
		break;
	case 2:
		seek(size() + offset);
		break;
	}
}

void MemMapStream::seek(stream_position_t fpos)
{
	stream_position_t fpos_end = m_file_pos + (m_end - m_beg);
	if (m_file_pos <= fpos && fpos <= fpos_end)
	{
		m_pos = m_beg + (fpos - m_file_pos);
	}
	else // seek out of region
	{
		stream_position_t new_fpos_beg = align_down(fpos, m_best_block_size);
		stream_position_t new_fpos_end = align_up  (fpos, m_best_block_size);

		if (!(m_mode & O_RDWR) && fpos > m_file_size)
		{
			string_appender<> oss;
			oss << "out of file region, fpos[cur=" << m_file_pos
				<< ", new=" << fpos
				<< "], eofpos[cur=" << fpos_end << ", new=" << new_fpos_end
				<< "], at: " << BOOST_CURRENT_FUNCTION;
			throw IOException(oss.str().c_str());
		}
		remap(new_fpos_beg, m_best_block_size);
		m_pos = m_beg + (fpos - new_fpos_beg);
	}
}

stream_position_t MemMapStream::size() const throw()
{
	return m_file_size;
}

int MemMapStream::BinCompare(MemMapStream& y)
{
	if (0 == m_file_size && 0 == y.m_file_size)
		return 0;

	if (m_best_block_size == y.m_best_block_size && m_file_size == y.m_file_size)
	{
		stream_position_t pos = 0;
		for (; pos + m_best_block_size <= m_file_size; pos += m_best_block_size)
		{
			MMS_MapRegion rx(*this, pos, m_best_block_size, O_RDONLY);
			MMS_MapRegion ry(y,     pos, m_best_block_size, O_RDONLY);
			int ret = ::memcmp(rx.base(), ry.base(), m_best_block_size);
			if (0 != ret) return ret;
		}
		if (pos == m_file_size)
			return 0;
		size_t n = m_file_size - pos;
		MMS_MapRegion rx(*this, pos, n, O_RDONLY);
		MMS_MapRegion ry(y,     pos, n, O_RDONLY);
		int ret = ::memcmp(rx.base(), ry.base(), n);
		return ret;
	}
	else
	{
		this->seek(0);
		y.seek(0);
		using namespace std;
		unsigned char bx, by;
		stream_position_t n = min(m_file_size, y.m_file_size);
		assert(n > 0);
		while (n--)
		{
			bx = readByte(), by = y.readByte();
			if (bx != by)
				return bx - by;
		}
		return	m_file_size < y.m_file_size ? -1 :
				m_file_size > y.m_file_size ? +1 : 0;
	}
	assert(0); // would not go here
}


//////////////////////////////////////////////////////////////////////////
// std::string MemMapStream::errmsg() const throw()
// {
// 	IOException excep(m_errno, "MemMapStream");
// 	return excep.what();
// }


} // namespace terark

