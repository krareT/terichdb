/* vim: set tabstop=4 : */
#include "BzipStream.hpp"

#include <assert.h>
#include <string.h>

#ifdef _MSC_VER
#   pragma comment(lib, "libbz2.lib")
#endif
/*
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
#	include <io.h>
#else
#	include <unistd.h>
#	include <sys/stat.h>
#	include <sys/types.h>
#	include <fcntl.h>
#	include <errno.h>
#endif
*/

#include <bzlib.h>

#include "byte_io_impl.hpp"
#include <terark/num_to_str.hpp>

static const char* strbzerr(int err)
{
	switch (err)
	{
	default:
		{
			static char szbuf[64];
			sprintf(szbuf, "unknow err=%d", err);
			return szbuf;
		}
	case BZ_OK:				return "BZ_OK";
	case BZ_RUN_OK:			return "BZ_RUN_OK";
	case BZ_FLUSH_OK:		return "BZ_FLUSH_OK";
	case BZ_FINISH_OK:		return "BZ_FINISH_OK";
	case BZ_STREAM_END:		return "BZ_STREAM_END";

	case BZ_CONFIG_ERROR:	return "BZ_CONFIG_ERROR";
	case BZ_SEQUENCE_ERROR: return "BZ_SEQUENCE_ERROR";
	case BZ_PARAM_ERROR:	return "BZ_PARAM_ERROR";
	case BZ_MEM_ERROR:		return "BZ_MEM_ERROR";
	case BZ_DATA_ERROR:		return "BZ_DATA_ERROR";
	case BZ_DATA_ERROR_MAGIC:return "BZ_DATA_ERROR_MAGIC";
	case BZ_UNEXPECTED_EOF: return "BZ_UNEXPECTED_EOF";
	case BZ_OUTBUFF_FULL:	return "BZ_OUTBUFF_FULL";
	case BZ_IO_ERROR:		return "BZ_IO_ERROR";
	}
}

namespace terark {

// only can call on unopened BzipInputStream
void BzipInputStream::open(const char* fpath, const char* mode)
{
	assert(0 == m_fp);

	int err;
	m_cf = fopen(fpath, mode);
	if (0 == m_cf)
	{
		string_appender<> oss;
		oss << "mode=" << mode;
		throw OpenFileException(fpath, oss.str().c_str());
	}
	m_fp = BZ2_bzReadOpen(&err, m_cf
		, 0    // verbosity, 0 will not print msg
		, 0    // small
		, NULL // unused
		, 0    // nUnused
		);
	if (0 == m_fp)
	{
		string_appender<> oss;
		oss << "mode=" << mode << ", err=" << strbzerr(err);
		throw OpenFileException(fpath, oss.str().c_str());
	}
}

void BzipInputStream::dopen(int fd, const char* mode)
{
	assert(0 == m_fp);

	int err;
#ifdef _MSC_VER
	m_cf = _fdopen(fd, mode);
#else
	m_cf = fdopen(fd, mode);
#endif
	if (0 == m_cf)
	{
		string_appender<> oss;
		oss << "fd=" << fd << ", mode=" << mode;
		throw OpenFileException("<fd>", oss.str().c_str());
	}
	m_fp = BZ2_bzReadOpen(&err, m_cf
		, 0    // verbosity, 0 will not print msg
		, 0    // small
		, NULL // unused
		, 0    // nUnused
		);
	if (0 == m_fp)
	{
		string_appender<> oss;
		oss << "fd=" << fd << ", mode=" << mode << ", err=" << strbzerr(err);
		throw OpenFileException("<fd>", oss.str().c_str());
	}
}

void BzipInputStream::close()
{
	assert(m_fp);
	assert(m_cf);
	int err;
	BZ2_bzReadClose(&err, m_fp);
	::fclose(m_cf);
	m_fp = 0;
	m_cf = 0;
	if (BZ_OK != err)
	{
		string_appender<> oss;
		oss << "BZ2_bzReadClose err=" << strbzerr(err)
			<< ", in " << BOOST_CURRENT_FUNCTION;
		throw OpenFileException("<fd>", oss.str().c_str());
	}
}

BzipInputStream::~BzipInputStream()
{
	if (m_fp)
		this->close();
}

BzipInputStream::BzipInputStream(const char* fpath, const char* mode)
{
	m_fp = 0;
	m_cf = 0;
   	open(fpath, mode);
}

BzipInputStream::BzipInputStream(int fd, const char* mode)
{
	m_fp = 0;
 	m_cf = 0;
  	dopen(fd, mode);
}

bool BzipInputStream::eof() const
{
	assert(m_cf);
	assert(m_fp);
	return !!feof(m_cf);
}

size_t BzipInputStream::read(void* buf, size_t size)
{
	assert(m_cf);
	assert(m_fp);
	int err = BZ_OK;
	int nRead = BZ2_bzRead(&err, m_fp, buf, size);
	if (BZ_OK != err)
	{
		string_appender<> oss;
		oss << "BZ2_bzRead err=" << strbzerr(err)
			<< ", in " << BOOST_CURRENT_FUNCTION;
		throw OpenFileException("<fd>", oss.str().c_str());
	}
	assert(nRead <= (int)size);
	return (size_t)nRead;
}

TERARK_GEN_ensureRead (BzipInputStream::)

///////////////////////////////////////////////////////

// only can call on unopened BzipOutputStream
void BzipOutputStream::open(const char* fpath, const char* mode)
{
	assert(0 == m_fp);

	int err;
	m_cf = fopen(fpath, mode);
	if (0 == m_cf)
	{
		string_appender<> oss;
		oss << "mode=" << mode;
		throw OpenFileException(fpath, oss.str().c_str());
	}
	m_fp = BZ2_bzWriteOpen(&err, m_cf
		,  9   // blocksize100k
		,  0   // verbosity
		, 30   // workFactor, default=30
		);
	if (0 == m_fp)
	{
		string_appender<> oss;
		oss << "mode=" << mode << ", err=" << strbzerr(err);
		throw OpenFileException(fpath, oss.str().c_str());
	}
}

void BzipOutputStream::dopen(int fd, const char* mode)
{
	assert(0 == m_fp);

	int err;
#ifdef _MSC_VER
	m_cf = _fdopen(fd, mode);
#else
	m_cf = fdopen(fd, mode);
#endif
	if (0 == m_cf)
	{
		string_appender<> oss;
		oss << "fd=" << fd << ", mode=" << mode;
		throw OpenFileException("<fd>", oss.str().c_str());
	}
	m_fp = BZ2_bzWriteOpen(&err, m_cf
		,  9   // blocksize100k
		,  0   // verbosity
		, 30   // workFactor, default=30
		);
	if (0 == m_fp)
	{
		string_appender<> oss;
		oss << "fd=" << fd << ", mode=" << mode << ", err=" << strbzerr(err);
		throw OpenFileException("<fd>", oss.str().c_str());
	}
}

void BzipOutputStream::close()
{
	assert(m_fp);
	assert(m_cf);
	int err = BZ_OK;
	int abandon = 0;
	unsigned in_lo32, in_hi32, out_lo32, out_hi32;
	BZ2_bzWriteClose64(&err, m_fp, abandon, &in_lo32, &in_hi32, &out_lo32, &out_hi32);
	::fclose(m_cf);
	m_fp = 0;
	m_cf = 0;
	if (BZ_OK != err)
	{
		string_appender<> oss;
		oss << "BZ2_bzWriteClose64 err=" << strbzerr(err)
			<< ", in " << BOOST_CURRENT_FUNCTION;
		throw IOException(oss.str().c_str());
	}
}

BzipOutputStream::~BzipOutputStream()
{
	if (m_fp)
		this->close();
}

BzipOutputStream::BzipOutputStream(const char* fpath, const char* mode)
{
	m_fp = 0;
	m_cf = 0;
   	open(fpath, mode);
}

BzipOutputStream::BzipOutputStream(int fd, const char* mode)
{
	m_fp = 0;
	m_cf = 0;
   	dopen(fd, mode);
}

void BzipOutputStream::flush()
{
	assert(m_fp);
	assert(m_cf);
	if (fflush(m_cf) == EOF)
		throw DelayWriteException(BOOST_CURRENT_FUNCTION);
}

size_t BzipOutputStream::write(const void* buf, size_t size)
{
	assert(m_fp);
	assert(m_cf);
	int err = BZ_OK;
	BZ2_bzWrite(&err, m_fp, (void*)buf, size);
	if (BZ_OK != err)
	{
		string_appender<> oss;
		oss << "BZ2_bzWrite err=" << strbzerr(err)
			<< ", in " << BOOST_CURRENT_FUNCTION;
		throw IOException(oss.str().c_str());
	}
	return size;
}
TERARK_GEN_ensureWrite(BzipOutputStream::)

} // namespace terark

