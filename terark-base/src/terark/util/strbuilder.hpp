// strbuilder.hpp

#include <stdio.h>
#include <string>

namespace terark {

#if defined(__GLIBC__) || defined(__CYGWIN__) || \
	defined(__DARWIN_C_LEVEL) && defined(__DARWIN_C_FULL) && __DARWIN_C_LEVEL >= __DARWIN_C_FULL
	// This class is more simple to use, but it should be used for one-time printf
	// This class is about 50% faster than StrBuilder on one-time printf
	class StrPrintf {
		StrPrintf(const StrPrintf&);
		StrPrintf& operator=(const StrPrintf&);
	public:
		char* s; // for easy access
		int   n;
		StrPrintf(const char* format, ...);
		StrPrintf(std::string& dest, const char* format, ...);
		~StrPrintf();
		operator std::string() const;
	};
#else
  #pragma message("StrPrintf is skiped because not in glibc")
#endif

#if defined(__GNUC__) || defined(__CYGWIN__)
	// This class should be used for multiple-time append by printf
	// This class is about 30% faster than StrPrintf on building big strings
	// This class is about 50% slower than std::ostingstream on building big strings
	class StrBuilder {
		StrBuilder(const StrBuilder&);
		StrBuilder& operator=(const StrBuilder&);
		FILE* memFile;
		char*  s;
		size_t n;
	public:
		~StrBuilder();
		StrBuilder();
		StrBuilder& printf(const char* format, ...);
		void clear();
		StrBuilder& flush();
		size_t size() const { return n; }
		int ilen() const { return (int)n; }
		const char* c_str();
		operator std::string() const;
		void setEof(int end_offset); // assert(end_offset < 0)
		void setEof(int end_offset, const char* endmark); // assert(end_offset < 0)
	};
#else
  #pragma message("strbuilder skiped because not in glibc")
#endif

} // namespace terark

