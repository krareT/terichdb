/* vim: set tabstop=4 : */
#include "IStream.hpp"
#include <assert.h>

namespace terark {

ISeekable::~ISeekable()
{
}
/*
stream_position_t ISeekable::size()
{
	stream_position_t old_pos = tell();
	seek(0, 2); // seek to end
	stream_position_t nsize = tell();
	seek(old_pos);
	return nsize;
}
*/
void ISeekable::rewind()
{
	this->seek(0);
}

void ISeekable::seek(stream_position_t position)
{
	seek(stream_offset_t(position), 0);
}


IAcceptor::~IAcceptor()
{

}

/////////////////////////////////////////////////////
//

IInputStream::~IInputStream()
{
}

IOutputStream::~IOutputStream()
{
}

////////////////////////////////////////////////////////////////////////////////////////////

size_t ISeekableInputStream::pread(stream_position_t pos, void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t n = this->read(vbuf, length);
	this->seek(old);
	return n;
}

size_t ISeekableOutputStream::pwrite(stream_position_t pos, const void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t n = this->write(vbuf, length);
	this->seek(old);
	return n;
}

size_t ISeekableStream::pread(stream_position_t pos, void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t nread = this->read(vbuf, length);
	this->seek(old);
	return nread;
}
size_t ISeekableStream::pwrite(stream_position_t pos, const void* vbuf, size_t length)
{
	stream_position_t old = this->tell();
	this->seek(pos);
	size_t n = this->write(vbuf, length);
	this->seek(old);
	return n;
}


///////////////////////////////////////////////////////
//
#if defined(__GLIBC__) || defined(__CYGWIN__)

ssize_t
OutputStream_write(void *cookie, const char *buf, size_t size)
{
	IOutputStream* output = (IOutputStream*)cookie;
	return output->write(buf, size);
}

ssize_t
InputStream_read(void *cookie, char *buf, size_t size)
{
	IInputStream* input = (IInputStream*)cookie;
	return input->read(buf, size);
}

/**
 * @note must call fclose after use of returned FILE
 */
FILE* IInputStream::forInputFILE()
{
	cookie_io_functions_t func = {
		InputStream_read,
		NULL,
		NULL,
		NULL
	};
	assert(this);
	void* cookie = this;
	FILE* fp = fopencookie(cookie,"r", func);
	if (fp == NULL) {
		perror("fopencookie@IInputStream::forInputFILE");
		return NULL;
	}
	return fp;
}

/**
 * @note must call fclose after use of returned FILE
 */
FILE* IOutputStream::forOutputFILE()
{
	cookie_io_functions_t func = {
		NULL,
		OutputStream_write,
		NULL,
		NULL
	};
	assert(this);
	void* cookie = this;
	FILE* fp = fopencookie(cookie,"w", func);
	if (fp == NULL) {
		perror("fopencookie@IOutputStream::forOutputFILE");
		return NULL;
	}
	return fp;
}

#endif


} // namespace terark

