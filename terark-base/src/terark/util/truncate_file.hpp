#ifndef __terark_util_truncate_file_hpp__
#define __terark_util_truncate_file_hpp__

#pragma once

#include <terark/config.hpp>

namespace terark {
	TERARK_DLL_EXPORT
	void truncate_file(const char* fpath, unsigned long long size);

	template<class String>
	inline
	void truncate_file(const String& fpath, unsigned long long size) {
		truncate_file(fpath.c_str(), size);
	}
}

#endif // __terark_util_truncate_file_hpp__
