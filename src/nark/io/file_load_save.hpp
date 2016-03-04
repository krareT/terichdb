#ifndef __nark_io_file_util_hpp__
#define __nark_io_file_util_hpp__

#include <nark/io/DataIO.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>

namespace nark {

template<class Object>
void native_load_file(const char* fname, Object* obj) {
	assert(NULL != fname);
	assert(NULL != obj);
	FileStream file(fname, "rb");
	NativeDataInput<InputBuffer> dio; dio.attach(&file);
	Object tmp;
	dio >> tmp;
	obj->swap(tmp);
}

template<class Object>
void native_save_file(const char* fname, const Object& obj) {
	assert(NULL != fname);
	FileStream file(fname, "wb");
	NativeDataOutput<OutputBuffer> dio; dio.attach(&file);
	dio << obj;
}

} // namespace nark

#endif // __nark_io_file_util_hpp__

