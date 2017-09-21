#ifndef __terark_util_mmap_hpp__
#define __terark_util_mmap_hpp__

#include <stddef.h>
#include <utility>
#include "../config.hpp"
#include "../fstring.hpp"

namespace terark {

TERARK_DLL_EXPORT void  mmap_close(void* base, size_t size);

TERARK_DLL_EXPORT
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

	void swap(MmapWholeFile& y) {
		std::swap(base, y.base);
		std::swap(size, y.size);
	}

	fstring memory() const {
		return fstring{(const char*)base, (ptrdiff_t)size};
	}
};

} // namespace terark

#endif // __terark_util_mmap_hpp__

