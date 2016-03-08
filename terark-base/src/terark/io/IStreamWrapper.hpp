/* vim: set tabstop=4 : */
#ifndef __terark_io_IStreamWrapper_h__
#define __terark_io_IStreamWrapper_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <boost/preprocessor/iteration/local.hpp>
#include <boost/preprocessor/enum.hpp>
#include <boost/preprocessor/enum_params.hpp>
#include <boost/type_traits.hpp>

#include "IStream.hpp"
/*
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#if _MSC_VER < 1500
#  define _POSIX_
#endif
#  include <io.h>
#  include <fcntl.h>

#else

#endif
*/

namespace terark {

#define CONS_LIMITS (1, 10)

#define CONS_ARG(z, n, _) const Arg##n& a##n

#define TEMPLATE_CONS(WrapperClass, n) template<BOOST_PP_ENUM_PARAMS(n, class Arg)> \
	explicit WrapperClass(BOOST_PP_ENUM(n, CONS_ARG, ~)) : m_stream(BOOST_PP_ENUM_PARAMS(n, a)) {}

template<class SeekableClass>
class SeekableWrapper : public ISeekable
{
	DECLARE_NONE_COPYABLE_CLASS(SeekableWrapper)
protected:
	SeekableClass m_stream;

public:
	SeekableWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(SeekableWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual void seek(stream_position_t pos) { m_stream.seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream.seek(offset, origin); }
	virtual stream_position_t tell() const { return m_stream.tell(); }
	virtual stream_position_t size() const { return m_stream.size(); }
};
template<class SeekableClass>
class SeekableWrapper<SeekableClass*> : public ISeekable
{
protected:
	SeekableClass* m_stream;

public:
	SeekableWrapper(SeekableClass* stream) : m_stream(stream) {}

	virtual void seek(stream_position_t pos) { m_stream->seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream->seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream->tell(); }
	virtual stream_position_t size() const { return m_stream->size(); }
};

template<class InputStream>
class InputStreamWrapper : public IInputStream
{
	DECLARE_NONE_COPYABLE_CLASS(InputStreamWrapper)
protected:
	InputStream m_stream;

public:
	InputStreamWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(InputStreamWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual bool eof() const { return m_stream.eof(); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream.read(vbuf, length); }
};
template<class InputStream>
class InputStreamWrapper<InputStream*> : public IInputStream
{
protected:
	InputStream* m_stream;

public:
	InputStreamWrapper(InputStream* stream) : m_stream(stream) {}

	virtual bool eof() const { return m_stream->eof(); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream->read(vbuf, length); }
};

template<class OutputStream>
class OutputStreamWrapper : public IOutputStream
{
	DECLARE_NONE_COPYABLE_CLASS(OutputStreamWrapper)
protected:
	OutputStream m_stream;

public:
	OutputStreamWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(OutputStreamWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual size_t write(const void* vbuf, size_t length) { return m_stream.write(vbuf, length); }
	virtual void flush() { m_stream.flush(); }
};
template<class OutputStream>
class OutputStreamWrapper<OutputStream*> : public IOutputStream
{
protected:
	OutputStream* m_stream;

public:
	OutputStreamWrapper(OutputStream* stream) : m_stream(stream) {}

	virtual size_t write(const void* vbuf, size_t length) { return m_stream->write(vbuf, length); }
	virtual void flush() { m_stream->flush(); }
};

//////////////////////////////////////////////////////////////////////////

template<class InputStream>
class SeekableInputStreamWrapper : public ISeekableInputStream
{
	DECLARE_NONE_COPYABLE_CLASS(SeekableInputStreamWrapper)
protected:
	InputStream m_stream;

public:
	SeekableInputStreamWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(SeekableInputStreamWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual bool eof() const { return m_stream.eof(); }

	virtual void seek(stream_position_t pos) { m_stream.seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream.seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream.tell(); }
	virtual stream_position_t size() const { return m_stream.size(); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream.read(vbuf, length); }
	virtual size_t pread(stream_position_t pos, void* vbuf, size_t length) { return m_stream.pread(pos, vbuf, length); }
};
template<class InputStream>
class SeekableInputStreamWrapper<InputStream*> : public ISeekableInputStream
{
protected:
	InputStream* m_stream;

public:
	SeekableInputStreamWrapper(IInputStream* stream) : m_stream(stream) {}

	virtual bool eof() const { return m_stream->eof(); }

	virtual void seek(stream_position_t pos) { m_stream->seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream->seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream->tell(); }
	virtual stream_position_t size() const { return m_stream->size(); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream->read(vbuf, length); }
	virtual size_t pread(stream_position_t pos, void* vbuf, size_t length) { return m_stream->pread(pos, vbuf, length); }
};

template<class OutputStream>
class SeekableOutputStreamWrapper : public ISeekableOutputStream
{
	DECLARE_NONE_COPYABLE_CLASS(SeekableOutputStreamWrapper)
protected:
	OutputStream m_stream;

public:
	SeekableOutputStreamWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(SeekableOutputStreamWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual void seek(stream_position_t pos) { m_stream.seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream.seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream.tell(); }
	virtual stream_position_t size() const { return m_stream.size(); }
	virtual size_t pwrite(stream_position_t pos, const void* vbuf, size_t length) { return m_stream.pwrite(pos, vbuf, length); }

	virtual size_t write(const void* vbuf, size_t length) { return m_stream.write(vbuf, length); }
	virtual void flush() { m_stream.flush(); }
};
template<class OutputStream>
class SeekableOutputStreamWrapper<OutputStream*> : public ISeekableOutputStream
{
protected:
	OutputStream* m_stream;

public:
	SeekableOutputStreamWrapper(OutputStream* stream) : m_stream(stream) {}

	virtual void seek(stream_position_t pos) { m_stream->seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream->seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream->tell(); }
	virtual stream_position_t size() const { return m_stream->size(); }
	virtual size_t pwrite(stream_position_t pos, const void* vbuf, size_t length) { return m_stream->pwrite(pos, vbuf, length); }

	virtual size_t write(const void* vbuf, size_t length) { return m_stream->write(vbuf, length); }
	virtual void flush() { m_stream->flush(); }
};

template<class Stream>
class SeekableStreamWrapper : public ISeekableStream
{
	DECLARE_NONE_COPYABLE_CLASS(SeekableStreamWrapper)
protected:
	Stream m_stream;

public:
	SeekableStreamWrapper() {}
#define BOOST_PP_LOCAL_LIMITS CONS_LIMITS
#define BOOST_PP_LOCAL_MACRO(n)  TEMPLATE_CONS(SeekableStreamWrapper, n)
#include BOOST_PP_LOCAL_ITERATE()

	virtual bool eof() const { return m_stream.eof(); }

	virtual void seek(stream_position_t pos) { m_stream.seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream.seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream.tell(); }
	virtual stream_position_t size() const { return m_stream.size(); }
	virtual size_t pread(stream_position_t pos, void* vbuf, size_t length) { return m_stream.pread(pos, vbuf, length); }
	virtual size_t pwrite(stream_position_t pos, const void* vbuf, size_t length) { return m_stream.pwrite(pos, vbuf, length); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream.read(vbuf, length); }

	virtual size_t write(const void* vbuf, size_t length) { return m_stream.write(vbuf, length); }
	virtual void flush() { m_stream.flush(); }
};
template<class Stream>
class SeekableStreamWrapper<Stream*> : public ISeekableStream
{
protected:
	Stream* m_stream;

public:
	SeekableStreamWrapper(Stream* stream) : m_stream(stream) {}

	virtual bool eof() const { return m_stream->eof(); }

	virtual void seek(stream_position_t pos) { m_stream->seek(pos); }
	virtual void seek(stream_offset_t offset, int origin) { m_stream->seek(offset, origin); }

	virtual stream_position_t tell() const { return m_stream->tell(); }
	virtual stream_position_t size() const { return m_stream->size(); }
	virtual size_t pread(stream_position_t pos, void* vbuf, size_t length) { return m_stream->pread(pos, vbuf, length); }
	virtual size_t pwrite(stream_position_t pos, const void* vbuf, size_t length) { return m_stream->pwrite(pos, vbuf, length); }

	virtual size_t read(void* vbuf, size_t length) { return m_stream->read(vbuf, length); }
	virtual size_t write(const void* vbuf, size_t length) { return m_stream->write(vbuf, length); }
	virtual void flush() { m_stream->flush(); }
};

#undef CONS_LIMITS
#undef CONS_ARG
#undef TEMPLATE_CONS

} // namespace terark

#endif

