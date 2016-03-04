/* vim: set tabstop=4 : */
#ifndef __nark_io_DataIO_Exception_h__
#define __nark_io_DataIO_Exception_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <nark/config.hpp>
#include <stdexcept>
#include <string>

namespace nark {

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
// non dll-interface class 'std::exception' used as base for dll-interface
#pragma warning(push)
#pragma warning(disable:4275)
#endif
class NARK_DLL_EXPORT DataFormatException : public std::exception
{
protected:
	std::string m_message;
public:
	explicit DataFormatException(const char* szMsg = "nark::DataFormatException");
	explicit DataFormatException(const std::string& strMsg);
	virtual ~DataFormatException() throw();

	const char* what() const throw() { return m_message.c_str(); }
};
#if defined(_MSC_VER) && (_MSC_VER >= 1020)
#pragma warning(pop)
#endif

class NARK_DLL_EXPORT InvalidObjectException : public DataFormatException
{
public:
	explicit InvalidObjectException(const char* szMsg = "nark::InvalidObjectException");
	explicit InvalidObjectException(const std::string& strMsg);
};

// a size value is too large, such as container's size
//
class NARK_DLL_EXPORT SizeValueTooLargeException : public DataFormatException
{
public:
	static void checkSizeValue(size_t value, size_t maxValue);
	SizeValueTooLargeException(size_t value, size_t maxValue, const char* szMsg = "nark::SizeValueTooLargeException");
	explicit SizeValueTooLargeException(const std::string& strMsg);
};

class NARK_DLL_EXPORT BadVersionException : public DataFormatException
{
public:
	explicit BadVersionException(unsigned loaded_version, unsigned curr_version, const char* className);
};

class NARK_DLL_EXPORT NotFoundFactoryException : public DataFormatException
{
public:
	explicit NotFoundFactoryException(const char* szMsg = "nark::NotFoundFactoryException");
	explicit NotFoundFactoryException(const std::string& strMsg);
};


} // namespace nark

#endif // __nark_io_DataIO_Exception_h__
