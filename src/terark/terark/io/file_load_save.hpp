#ifndef __terark_io_file_util_hpp__
#define __terark_io_file_util_hpp__

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace terark {

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

} // namespace terark

#endif // __terark_io_file_util_hpp__

