#pragma once
#include <terark/config.hpp>
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>
#include <stdexcept>

namespace terark {

class TERARK_DLL_EXPORT BadChecksumException : public std::logic_error {
	typedef std::logic_error super;
public:
	uint64_t m_old;
	uint64_t m_new;
	~BadChecksumException();
	BadChecksumException(fstring msg, uint64_t Old, uint64_t New);
};

} // terark
