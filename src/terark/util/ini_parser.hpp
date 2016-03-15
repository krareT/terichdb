/* vim: set tabstop=4 : */
#ifndef __terark_ini_parser_h__
#define __terark_ini_parser_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

/**
 config file, ini file BNF grammar

 config  = {line}
 line    = [key_val|section]
 section = {sp} '[' {sp} section_name {sp} ']' {sp}
 key_val = {sp} key {sp} '=' {sp} val {sp}
 comment = ('#'|';') *[^\r\n]
 */

#include <string>
#include <memory>

#include "../config.hpp" // only for TERARK_DLL_EXPORT
#include "../num_to_str.hpp"
#include <sstream>

namespace terark {

class TERARK_DLL_EXPORT ini_parser
{
	ini_parser(const ini_parser&);
	const ini_parser& operator=(const ini_parser&);
public:
	ini_parser(const std::string& configFile, bool writable = false);
	~ini_parser();

	void open(const std::string& configFile, bool writable = false);
	void close();
	void flush();

	const char* getcstr(const std::string& section, const std::string& key, const std::string& defaultVal = std::string()) const;
	std::string getstr(const std::string& section, const std::string& key, const std::string& defaultVal = std::string()) const;
	void setstr(const std::string& section, const std::string& key, const std::string& val);

	int  getint(const std::string& section, const std::string& key, int defaultVal = 0) const;
	void setint(const std::string& section, const std::string& key, int val);

	size_t section_count() const;
	size_t key_val_count(const std::string& section) const;

	template<class T>
	void set_val(const std::string& section, const std::string& key, const T& val)
	{
		string_appender<> oss;
		oss << val;
		setstr(section, key, oss.str());
	}

	template<class T>
	bool get_val(const std::string& section, const std::string& key, T& val)
	{
		std::string sVal = getstr(section, key);
		if (sVal.empty()) return false;
		std::istringstream iss(sVal);
		iss >> val;
		return true;
	}

private:
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L || \
	defined(_MSC_VER) && _MSC_VER >= 1700
	std::unique_ptr<class ini_parser_impl> m_parser;
#else
	std::auto_ptr<class ini_parser_impl> m_parser;
#endif
	mutable std::string m_tmpcstr; //!< used for getcstr()
};

} // namespace terark

#endif // __terark_ini_parser_h__

