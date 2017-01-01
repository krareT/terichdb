#include "checksum_exception.hpp"

namespace terark {

BadChecksumException::~BadChecksumException() {}

BadChecksumException::
BadChecksumException(fstring msg, uint64_t Old, uint64_t New)
  : super(msg.c_str()), m_old(Old), m_new(New) {}

} // terark

