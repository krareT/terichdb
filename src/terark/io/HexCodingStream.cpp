/* vim: set tabstop=4 : */
#include "HexCodingStream.hpp"
#include "DataInput.hpp"
#include <terark/num_to_str.hpp>

namespace terark {

// '0' == 0x30
// 'a' == 0x61
// 'A' == 0x41
const unsigned char G_hex_val_hexTab[] =
{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	255, 255, 255, 255, 255, 255,
	// below, begin with '0' + 16 = 0x40
	255,
	0xA, 0xB, 0xC, 0xD, 0xE, 0xF,
	255, 255, 255, 255, 255,
	255, 255, 255, 255, 255,
	// below, begin with 'A' + 16 = 0x51
	255, 255, 255, 255,
	255, 255, 255, 255,
	255, 255, 255, 255,
	255, 255, 255, 255,
	// below, begin with 'a' = 0x61
	0xA, 0xB, 0xC, 0xD, 0xE, 0xF,
};

void invalid_hex_char(unsigned char ch, const char* func)
{
	string_appender<> oss;
	oss << "invalid hex char(ch=" << char(ch) << ",ascii=" << int(ch) << ") in func: " << func;
	throw DataFormatException(oss.str());
}

} // namespace terark
