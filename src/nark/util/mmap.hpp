#ifndef __nark_util_mmap_hpp__
#define __nark_util_mmap_hpp__

#include <stddef.h>
#include "../config.hpp"

namespace nark {

NARK_DLL_EXPORT void  mmap_close(void* base, size_t size);

NARK_DLL_EXPORT
void* mmap_load(const char* fname, size_t* size,
				bool writable = false,
				bool populate = false);

template<class String>
void* mmap_load(const String& fname, size_t* size,
				bool writable = false,
				bool populate = false) {
	return mmap_load(fname.c_str(), size, writable, populate);
}

class MmapWholeFile {
	MmapWholeFile(const MmapWholeFile&);
	MmapWholeFile& operator=(const MmapWholeFile&);

public:
	void*  base;
	size_t size;

	~MmapWholeFile() {
		if (base) {
			mmap_close(base, size);
		}
	}
	MmapWholeFile() { base = NULL; size = 0; }
	explicit MmapWholeFile(const char* fname,
						   bool writable = false,
						   bool populate = false) {
		base = mmap_load(fname, &size, writable, populate);
	}
	template<class String>
	explicit MmapWholeFile(const String& fname,
						   bool writable = false,
						   bool populate = false) {
		base = mmap_load(fname, &size, writable, populate);
	}
};

} // namespace nark

#endif // __nark_util_mmap_hpp__

