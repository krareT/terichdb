#ifndef __terark_util_crc_hpp__
#define __terark_util_crc_hpp__

#include <terark/config.hpp>
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>
#include <stdexcept>

namespace terark {

TERARK_DLL_EXPORT
uint32_t Crc32c_update(uint32_t inCrc32, const void *buf, size_t bufLen);

// If crc32 does not match, user should throw this exception
class TERARK_DLL_EXPORT BadCrc32cException : public std::logic_error {
	typedef std::logic_error super;
	uint32_t m_old_crc32;
	uint32_t m_new_crc32;
public:
	~BadCrc32cException();
	BadCrc32cException(fstring msg, uint32_t old_crc32, uint32_t new_crc32);
};

} // terark

#endif // __terark_util_crc_hpp__

