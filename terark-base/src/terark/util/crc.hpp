#ifndef __terark_util_crc_hpp__
#define __terark_util_crc_hpp__

#include <terark/util/checksum_exception.hpp>

namespace terark {

TERARK_DLL_EXPORT
uint32_t Crc32c_update(uint32_t inCrc32, const void *buf, size_t bufLen);

class TERARK_DLL_EXPORT BadCrc32cException : public BadChecksumException {
public:
	BadCrc32cException(fstring msg, uint32_t Old, uint32_t New)
		: BadChecksumException(msg, Old, New) {}
	~BadCrc32cException();
};

} // terark

#endif // __terark_util_crc_hpp__

