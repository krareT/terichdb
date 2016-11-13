#include "objbox.hpp"

namespace terark { namespace objbox {

void intrusive_ptr_release(obj* p) {
	assert(p->refcnt > 0);
	if (0 == --p->refcnt)
		delete p;
}

obj::~obj() {}

obj_ptr::obj_ptr(fstring str)
	: super(new obj_string(str.data(), str.size())) {}

// just for enable COW, if std::string has no COW
// this constructor has the same efficiency with obj_ptr(fstring)
obj_ptr::obj_ptr(const std::string& str)
	: super(new obj_string(str)) {}

TERARK_BOXING_OBJECT_IMPL(obj_bool   ,   bool)
TERARK_BOXING_OBJECT_IMPL(obj_short  ,   signed short)
TERARK_BOXING_OBJECT_IMPL(obj_ushort , unsigned short)
TERARK_BOXING_OBJECT_IMPL(obj_int    ,   signed int)
TERARK_BOXING_OBJECT_IMPL(obj_uint   , unsigned int)
TERARK_BOXING_OBJECT_IMPL(obj_long   ,   signed long)
TERARK_BOXING_OBJECT_IMPL(obj_ulong  , unsigned long)
TERARK_BOXING_OBJECT_IMPL(obj_llong  ,   signed long long)
TERARK_BOXING_OBJECT_IMPL(obj_ullong , unsigned long long)
TERARK_BOXING_OBJECT_IMPL(obj_float  , float)
TERARK_BOXING_OBJECT_IMPL(obj_double , double)
TERARK_BOXING_OBJECT_IMPL(obj_ldouble, long double)

TERARK_BOXING_OBJECT_DERIVE_IMPL(obj_array , valvec<obj_ptr>)

obj_string::~obj_string() {}
obj_string::obj_string() {}
obj_string::obj_string(fstring y) : std::string(y.data(), y.size()) {}
obj_string::obj_string(const char* y) : std::string(y, strlen(y)) {}

obj_string::obj_string(const char* str, size_t len) : std::string(str, len) {}

// g++ std::string is copy on write
obj_string::obj_string(const std::string& y) : std::string(y) {}

} } // terark::objbox

