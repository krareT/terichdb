#ifndef __nark_util_linebuf_hpp__
#define __nark_util_linebuf_hpp__

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h> // strchr
#include <boost/noncopyable.hpp>
#include "../config.hpp"
#include "../stdtypes.hpp"

namespace nark {

struct NARK_DLL_EXPORT LineBuf : boost::noncopyable {
	size_t capacity;
	size_t n;
	char*  p;

	typedef char* iterator;
	typedef char* const_iterator;

	LineBuf();
	~LineBuf();

	ptrdiff_t getline(FILE* f);

	bool  empty() const { return 0 == n; }
	size_t size() const { return n; }
	char* begin() const { return p; }
	char* end()   const { return p + n; }

	///@{
	///@return removed bytes
	size_t trim();  // remove all trailing spaces, including '\r' and '\n'
	size_t chomp(); // remove all trailing '\r' and '\n', just as chomp in perl
	///@}

	operator char*() const { return p; }

	/// split into fields
	template<class Vec>
	size_t split(const char* delims, Vec* F, size_t max_fields = ~size_t(0)) {
		size_t dlen = strlen(delims);
		if (0 == dlen) // empty delims redirect to blank delim
			return split(' ', F);
		if (1 == dlen)
			return split(delims[0], F);
		F->resize(0);
		char *col = p, *end = p + n;
		while (col <= end && F->size()+1 < max_fields) {
			char* next = (char*)memmem(col, end-col, delims, dlen);
			if (NULL == next) next = end;
			F->push_back(typename Vec::value_type(col, next));
			*next = 0;
			col = next + dlen;
		}
		if (col <= end)
			F->push_back(typename Vec::value_type(col, end));
		return F->size();
	}
	template<class Vec>
	size_t split_by_any(const char* delims, Vec* F, size_t max_fields = ~size_t(0)) {
		size_t dlen = strlen(delims);
		if (0 == dlen) // empty delims redirect to blank delim
			return split(' ', F);
		if (1 == dlen)
			return split(delims[0], F);
		F->resize(0);
		char *col = p, *end = p + n;
		while (col <= end && F->size()+1 < max_fields) {
			char* next = col;
			while (next < end && memchr(delims, *next, dlen) == NULL) ++next;
			F->push_back(typename Vec::value_type(col, next));
			*next = 0;
			col = next + 1;
		}
		if (col <= end)
			F->push_back(typename Vec::value_type(col, end));
		return F->size();
	}
	template<class Vec>
	size_t split(const char delim, Vec* F, size_t max_fields = ~size_t(0)) {
		F->resize(0);
		if (' ' == delim) {
		   	// same as awk, skip first blank field, and skip dup blanks
			char *col = p, *end = p + n;
			while (col < end && isspace(*col)) ++col; // skip first blank field
			while (col < end && F->size()+1 < max_fields) {
				char* next = col;
				while (next < end && !isspace(*next)) ++next;
				F->push_back(typename Vec::value_type(col, next));
				while (next < end &&  isspace(*next)) *next++ = 0; // skip blanks
				col = next;
			}
			if (col < end)
				F->push_back(typename Vec::value_type(col, end));
		} else {
			char *col = p, *end = p + n;
			while (col <= end && F->size()+1 < max_fields) {
				char* next = col;
				while (next < end && delim != *next) ++next;
				F->push_back(typename Vec::value_type(col, next));
				*next = 0;
				col = next + 1;
			}
			if (col <= end)
				F->push_back(typename Vec::value_type(col, end));
		}
		return F->size();
	}

	/// @params[out] offsets: length of offsets must >= arity+1
	///  on return, offsets[arity] saves tuple data length
	bool read_binary_tuple(int32_t* offsets, size_t arity, FILE* f);

	void read_all(FILE*);
	void read_all(const char* fname);
};

} // namespace nark

#endif // __nark_util_linebuf_hpp__

