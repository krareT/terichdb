/* vim: set tabstop=4 : */
#ifndef __terark_io_DataIO_Exception_h__
#define __terark_io_DataIO_Exception_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/config.hpp>
#include <stdexcept>
#include <string>

namespace terark {

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
// non dll-interface class 'std::exception' used as base for dll-interface
#pragma warning(push)
#pragma warning(disable:4275)
#endif
class TERARK_DLL_EXPORT DataFormatException : public std::exception
{
protected:
	std::string m_message;
public:
	explicit DataFormatException(const char* szMsg = "terark::DataFormatException");
	explicit DataFormatException(const std::string& strMsg);
	virtual ~DataFormatException() throw();

	const char* what() const throw() { return m_message.c_str(); }
};
#if defined(_MSC_VER) && (_MSC_VER >= 1020)
#pragma warning(pop)
#endif

class TERARK_DLL_EXPORT InvalidObjectException : public DataFormatException
{
public:
	explicit InvalidObjectException(const char* szMsg = "terark::InvalidObjectException");
	explicit InvalidObjectException(const std::string& strMsg);
};

// a size value is too large, such as container's size
//
class TERARK_DLL_EXPORT SizeValueTooLargeException : public DataFormatException
{
public:
	static void checkSizeValue(size_t value, size_t maxValue);
	SizeValueTooLargeException(size_t value, size_t maxValue, const char* szMsg = "terark::SizeValueTooLargeException");
	explicit SizeValueTooLargeException(const std::string& strMsg);
};

class TERARK_DLL_EXPORT BadVersionException : public DataFormatException
{
public:
	explicit BadVersionException(unsigned loaded_version, unsigned curr_version, const char* className);
};

class TERARK_DLL_EXPORT NotFoundFactoryException : public DataFormatException
{
public:
	explicit NotFoundFactoryException(const char* szMsg = "terark::NotFoundFactoryException");
	explicit NotFoundFactoryException(const std::string& strMsg);
};


} // namespace terark

#endif // __terark_io_DataIO_Exception_h__
