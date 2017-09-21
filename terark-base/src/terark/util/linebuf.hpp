#ifndef __terark_util_linebuf_hpp__
#define __terark_util_linebuf_hpp__

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h> // strchr
#include <boost/noncopyable.hpp>
#include <terark/config.hpp>
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>

namespace terark {

struct TERARK_DLL_EXPORT LineBuf : boost::noncopyable {
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
	char*  data() const { return p; }
	char* begin() const { return p; }
	char*   end() const { return p + n; }

	void swap(LineBuf& y) {
		std::swap(capacity, y.capacity);
		std::swap(n, y.n);
		std::swap(p, y.p);
	}

	void clear();
	void erase_all() { n = 0; }

	///@{
	///@return removed bytes
	size_t trim();  // remove all trailing spaces, including '\r' and '\n'
	size_t chomp(); // remove all trailing '\r' and '\n', just as chomp in perl
	///@}

	void push_back(char ch) {
		if (n + 1 < capacity) {
			p[n++] = ch;
			p[n] = '\0';
		}
		else {
			push_back_slow_path(ch);
		}
	}
private:
	void push_back_slow_path(char ch);
public:

	operator char*() const { return p; }
	operator fstring() const { return fstring(p, n); }

	/// split into fields
	template<class Vec>
	size_t split(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		size_t dlen = delims.size();
		if (0 == dlen) // empty delims redirect to blank delim
			return split(' ', F);
		if (1 == dlen)
			return split(delims[0], F);
		F->resize(0);
		char *col = p, *end = p + n;
		while (col <= end && F->size()+1 < max_fields) {
			char* next = (char*)memmem(col, end-col, delims.data(), dlen);
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
	size_t split_by_any(fstring delims, Vec* F, size_t max_fields = ~size_t(0)) {
		size_t dlen = delims.size();
		if (0 == dlen) // empty delims redirect to blank delim
			return split(' ', F);
		if (1 == dlen)
			return split(delims[0], F);
		F->resize(0);
		char *col = p, *end = p + n;
		while (col <= end && F->size()+1 < max_fields) {
			char* next = col;
			while (next < end && memchr(delims.data(), *next, dlen) == NULL) ++next;
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

	LineBuf& read_all(FILE*, size_t align = 0);
	LineBuf& read_all(fstring fname, size_t align = 0);
};

} // namespace terark

#endif // __terark_util_linebuf_hpp__

