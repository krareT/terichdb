/* vim: set tabstop=4 : */
#include "IOException.hpp"
#include <terark/num_to_str.hpp>

#include <string.h>

#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#   define NOMINMAX
#   define WIN32_LEAN_AND_MEAN
#	include <windows.h>
#else
#	include <errno.h>
#endif

namespace terark {

IOException::IOException(const char* szMsg)
  : m_message(szMsg), m_errCode(lastError())
{
	m_message += ": ";
	m_message += errorText(m_errCode);
}

IOException::IOException(const std::string& msg)
  : m_message(msg), m_errCode(lastError())
{
	m_message += ": ";
	m_message += errorText(m_errCode);
}

IOException::IOException(int errCode, const char* szMsg)
  : m_message(szMsg), m_errCode(errCode)
{
	m_message += ": ";
	m_message += errorText(m_errCode);
}

int IOException::lastError()
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
	return ::GetLastError();
#else
	return errno;
#endif
}

std::string IOException::errorText(int errCode)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
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
	string_appender<> oss;
	LPCSTR pszMsg = (LPCSTR)LocalLock(hLocal);
	oss << "error[code=" << errCode << ", message=" << pszMsg << "]";
	LocalFree(hLocal);
#else
	string_appender<> oss;
	oss << "error[code=" << errCode << ", message=" << ::strerror(errCode) << "]";
#endif
	return oss.str();
}

//////////////////////////////////////////////////////////////////////////

OpenFileException::OpenFileException(const char* path, const char* szMsg)
	: IOException(szMsg), m_path(path)
{
	m_message += ": ";
	m_message += m_path;
}

} // namespace terark
