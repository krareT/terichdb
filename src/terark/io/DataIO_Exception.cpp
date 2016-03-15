/* vim: set tabstop=4 : */

#include "DataIO_Exception.hpp"
#include <stdio.h> // for sprintf
#include <terark/num_to_str.hpp>

namespace terark {

DataFormatException::DataFormatException(const char* szMsg)
	: m_message(szMsg)
{ }

DataFormatException::DataFormatException(const std::string& strMsg)
	: m_message(strMsg)
{ }

DataFormatException::~DataFormatException() throw()
{}

InvalidObjectException::InvalidObjectException(const char* szMsg)
	: DataFormatException(szMsg)
{ }

InvalidObjectException::InvalidObjectException(const std::string& strMsg)
	: DataFormatException(strMsg)
{ }

// a size value is too large, such as container's size
//
void SizeValueTooLargeException::checkSizeValue(size_t value, size_t maxValue)
{
	if (value > maxValue)
		throw SizeValueTooLargeException(value, maxValue);
}
SizeValueTooLargeException::SizeValueTooLargeException(size_t value, size_t maxValue, const char* szMsg)
	: DataFormatException(szMsg)
{
	char szBuf[256];
	sprintf(szBuf, "[value=%zd(0x%zX), maxValue=%zd(0x%zX)]", value, value, maxValue, maxValue);
	m_message.append(szBuf);
}
SizeValueTooLargeException::SizeValueTooLargeException(const std::string& strMsg)
	: DataFormatException(strMsg)
{ }

BadVersionException::BadVersionException(unsigned loaded_version, unsigned curr_version, const char* className)
	: DataFormatException("")
{
	static_cast<string_appender<>&>(m_message = "")
		<< "class=\"" << className << "\", version[loaded=" << loaded_version << ", current=" << curr_version << "]";
}

NotFoundFactoryException::NotFoundFactoryException(const char* szMsg)
	: DataFormatException(szMsg)
{ }
NotFoundFactoryException::NotFoundFactoryException(const std::string& strMsg)
	: DataFormatException(strMsg)
{ }


} // namespace terark

