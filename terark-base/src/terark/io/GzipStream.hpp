/* vim: set tabstop=4 : */
#ifndef __terark_io_GzipStream_h__
#define __terark_io_GzipStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/stdtypes.hpp>
#include <terark/util/refcount.hpp>
#include "IOException.hpp"
#include "IStream.hpp"

namespace terark {

class TERARK_DLL_EXPORT GzipStreamBase : public RefCounter
{
protected:
	void* m_fp;
	void ThrowOpenFileException(const char* fpath, const char* mode);

public:
	GzipStreamBase() : m_fp(0) {}
	virtual ~GzipStreamBase();

	bool isOpen() const { return 0 != m_fp; }

	void open(const char* fpath, const char* mode);
	bool xopen(const char* fpath, const char* mode);
	void dopen(int fd, const char* mode);
	void close();
};

class TERARK_DLL_EXPORT GzipInputStream	: public IInputStream, public GzipStreamBase
{
	DECLARE_NONE_COPYABLE_CLASS(GzipInputStream)

public:
	explicit GzipInputStream(const char* fpath, const char* mode = "rb");
	explicit GzipInputStream(int fd, const char* mode = "rb");
	GzipInputStream() throw() {}

	bool eof() const;

	void ensureRead(void* vbuf, size_t length);
	size_t read(void* buf, size_t size) throw();
};

class TERARK_DLL_EXPORT GzipOutputStream : public IOutputStream, public GzipStreamBase
{
	DECLARE_NONE_COPYABLE_CLASS(GzipOutputStream)

public:
	explicit GzipOutputStream(const char* fpath, const char* mode = "wb");
	explicit GzipOutputStream(int fd, const char* mode = "wb");
	GzipOutputStream() throw() {}

	void ensureWrite(const void* vbuf, size_t length);
	size_t write(const void* buf, size_t size) throw();
	void flush();
};

} // namespace terark

#endif

