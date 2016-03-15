/* vim: set tabstop=4 : */
#ifndef __terark_io_IOException_h__
#define __terark_io_IOException_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <exception>
#include <string>
#include <terark/config.hpp>

namespace terark {

#if defined(_MSC_VER)
// non dll-interface class 'std::exception' used as base for dll-interface
#pragma warning(push)
#pragma warning(disable:4275)
#endif
class TERARK_DLL_EXPORT IOException : public std::exception
{
protected:
	std::string m_message;
	int m_errCode;
public:
	explicit IOException(const char* szMsg = "terark::IOException");
	explicit IOException(const std::string& msg);
	explicit IOException(int errCode, const char* szMsg = "terark::IOException");
	virtual ~IOException() throw() {}

	const char* what() const throw() { return m_message.c_str(); }
	int errCode() const throw() { return m_errCode; }

	static int lastError();
	static std::string errorText(int errCode);
};
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

class TERARK_DLL_EXPORT OpenFileException : public IOException
{
	std::string m_path;
public:
	explicit OpenFileException(const char* path, const char* szMsg = "terark::OpenFileException");
	explicit OpenFileException(const std::string& msg) : IOException(msg) {}
	~OpenFileException() throw() {}
};

// blocked streams read 0 bytes will cause this exception
// other streams read not enough maybe cause this exception
// all streams read 0 bytes will cause this exception
class TERARK_DLL_EXPORT EndOfFileException : public IOException
{
public:
	explicit EndOfFileException(const char* szMsg = "terark::EndOfFileException")
		: IOException(szMsg)
	{ }
	explicit EndOfFileException(const std::string& msg) : IOException(msg) {}
};

class TERARK_DLL_EXPORT OutOfSpaceException : public IOException
{
public:
	explicit OutOfSpaceException(const char* szMsg = "terark::OutOfSpaceException")
		: IOException(szMsg)
	{ }
	explicit OutOfSpaceException(const std::string& msg) : IOException(msg) {}
};

class TERARK_DLL_EXPORT DelayWriteException : public IOException
{
public:
	DelayWriteException(const char* szMsg = "terark::DelayWriteException")
		: IOException(szMsg)
	{ }
	DelayWriteException(const std::string& msg) : IOException(msg) {}
//	size_t streamPosition;
};

class TERARK_DLL_EXPORT BrokenPipeException : public IOException
{
public:
	BrokenPipeException(const char* szMsg = "terark::BrokenPipeException")
		: IOException(szMsg)
	{ }
	BrokenPipeException(const std::string& msg) : IOException(msg) {}
};


} // namespace terark

#endif // __terark_io_IOException_h__
