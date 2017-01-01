/* vim: set tabstop=4 : */
#if defined(_MSC_VER)

#if !defined(_WINDOWS_) && !defined(_INC_WINDOWS)
#   define NOMINMAX
#   define WIN32_LEAN_AND_MEAN
#	include <windows.h>
#endif

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(push)
# pragma warning(disable: 4267)
#endif

#include <assert.h>
#include "WinFileStream.hpp"

namespace terark {

WinFileStream::~WinFileStream()
{
	if (m_bAutoClose && INVALID_HANDLE_VALUE != m_hFile)
		CloseHandle(m_hFile);
}

WinFileStream::WinFileStream(
		HANDLE hFile,
		bool bAutoClose,
		const std::string& strFile
) : m_hFile(hFile), m_bAutoClose(false), m_strFile(strFile)
{}

WinFileStream::WinFileStream(
		LPCSTR szFile,
		DWORD dwDesiredAccess,
		DWORD dwShareMode,
		LPSECURITY_ATTRIBUTES lpSecurityAttributes)
{
	if (!open(szFile, dwDesiredAccess, dwShareMode, lpSecurityAttributes))
	{
		throw OpenFileException(szFile, ErrorText(GetLastError()).c_str());
	}
}

WinFileStream::WinFileStream(
		LPCSTR szFile,
		DWORD dwDesiredAccess,
		DWORD dwShareMode,
		LPSECURITY_ATTRIBUTES lpSecurityAttributes,
		DWORD dwCreationDisposition,
		DWORD dwFlagsAndAttributes,
		HANDLE hTemplateFile)
{
	if (!open(szFile, dwDesiredAccess, dwShareMode, lpSecurityAttributes,
				dwCreationDisposition, dwFlagsAndAttributes, hTemplateFile))
	{
		throw OpenFileException(szFile, ErrorText(GetLastError()).c_str());
	}
}

bool WinFileStream::open(
		LPCSTR szFile,
		DWORD dwDesiredAccess,
		DWORD dwShareMode,
		LPSECURITY_ATTRIBUTES lpSecurityAttributes)
{
	return open(szFile, dwDesiredAccess, dwShareMode, lpSecurityAttributes,
		DefaultCreateDisposition(dwDesiredAccess), 0, NULL
		);
}

bool WinFileStream::open(
		LPCSTR szFile,
		DWORD dwDesiredAccess,
		DWORD dwShareMode,
		LPSECURITY_ATTRIBUTES lpSecurityAttributes,
		DWORD dwCreationDisposition,
		DWORD dwFlagsAndAttributes,
		HANDLE hTemplateFile)
{
	m_strFile = szFile;
	m_hFile = CreateFile(szFile, dwDesiredAccess, dwShareMode,
		lpSecurityAttributes, dwCreationDisposition, dwFlagsAndAttributes, hTemplateFile);
	m_bAutoClose = true;

	return INVALID_HANDLE_VALUE != m_hFile;
}

void WinFileStream::attach(HANDLE	hFile,
						   bool	bAutoClose,
						   const std::string& strFile)
{
	assert(INVALID_HANDLE_VALUE == m_hFile);
	m_hFile = hFile;
	m_bAutoClose = bAutoClose;
	m_strFile  = strFile;
}
HANDLE WinFileStream::detach()
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	HANDLE hFile = m_hFile;
	m_hFile = INVALID_HANDLE_VALUE;
	return hFile;
}

size_t WinFileStream::read(void* vbuf, size_t length)
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	DWORD nReaded;
	if (ReadFile(m_hFile, vbuf, length, &nReaded, 0))
	{
		return nReaded;
	}
	throw IOException(ErrorText(GetLastError()).c_str());
}

size_t WinFileStream::write(const void* vbuf, size_t length)
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	DWORD nWritten;
	if (WriteFile(m_hFile, vbuf, length, &nWritten, 0))
	{
		return nWritten;
	}
	throw IOException(ErrorText(GetLastError()).c_str());
}

uint64_t WinFileStream::do_seek(int64_t offset, int origin)
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	LONG hi32 = LONG(uint64_t(offset) >> 32);
	LONG lo32 = LONG(offset & 0xFFFFFFFF);
	lo32 = SetFilePointer(m_hFile, lo32, &hi32, origin);
	DWORD err;
	if (INVALID_SET_FILE_POINTER == lo32 && (err = GetLastError()) != NO_ERROR)
	{
		throw IOException(ErrorText(err).c_str());
	}
	return uint64_t(hi32) << 32 | lo32;
}

void WinFileStream::seek(int64_t offset, int origin)
{
	(void)do_seek(offset, origin);
}

uint64_t WinFileStream::tell() const
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	LONG hi32 = 0;
	LONG lo32 = 0;
	lo32 = SetFilePointer(m_hFile, lo32, &hi32, FILE_CURRENT);
	DWORD err;
	if (INVALID_SET_FILE_POINTER == lo32 && (err = GetLastError()) != NO_ERROR)
	{
		throw IOException(ErrorText(err).c_str());
	}
	return uint64_t(hi32) << 32 | lo32;
}

void WinFileStream::flush()
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	if (!FlushFileBuffers(m_hFile))
		throw IOException(ErrorText(GetLastError()).c_str());
}

void WinFileStream::close()
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	CloseHandle(m_hFile);
}

uint64_t WinFileStream::size()
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	DWORD  hi32;
	DWORD  lo32 = GetFileSize(m_hFile, &hi32);
	DWORD  err;
	if (INVALID_FILE_SIZE == lo32 && NO_ERROR != (err = GetLastError()))
	{
		throw IOException(ErrorText(err).c_str());
	}
	return uint64_t(hi32) << 32 | lo32;
}

void WinFileStream::setEof(uint64_t newSize)
{
	assert(INVALID_HANDLE_VALUE != m_hFile);
	uint64_t oldPos = do_seek(newSize, FILE_BEGIN);
	BOOL bRet = SetEndOfFile(m_hFile);
	seek(oldPos, FILE_BEGIN);
	if (!bRet)
		throw IOException(ErrorText(GetLastError()).c_str());
}

DWORD WinFileStream::DefaultCreateDisposition(DWORD dwDesiredAccess) const
{
	if (dwDesiredAccess & (GENERIC_READ|GENERIC_WRITE))
	{
		return OPEN_ALWAYS;
	}
	else if (dwDesiredAccess & GENERIC_READ)
	{
		return OPEN_EXISTING;
	}
	else if (dwDesiredAccess & GENERIC_WRITE)
	{
		return CREATE_ALWAYS;
	}
	else
	{
		return OPEN_ALWAYS;
	}
}

std::string WinFileStream::ErrorText(DWORD errCode) const
{
	HLOCAL hLocal = NULL;
	DWORD dwTextLength = FormatMessage(
		FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_ALLOCATE_BUFFER,
		NULL,
		errCode,
		0, //MAKELANGID(LANG_CHINESE, SUBLANG_CHINESE_SIMPLIFIED)
		(LPTSTR)&hLocal,
		0,
		NULL
		);
	std::string strText = "file: \"" + m_strFile + "\" error: " + (LPCSTR)LocalLock(hLocal);
	LocalFree(hLocal);

	return strText;
}

}

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma warning(pop)
#endif

#endif
