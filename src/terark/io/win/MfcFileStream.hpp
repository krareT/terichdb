/* vim: set tabstop=4 : */
#ifndef __MfcFileStream_h__
#define __MfcFileStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/util/refcount.hpp>
#include "IOException.hpp"
#include "IStream.hpp"

#include <afx.h>

namespace terark {
	class MfcFileStream
		: public RefCounter
		, public IInputStream
		, public IOutputStream
		, public ISeekable
	{
		CFile* m_fp;

	public:
		typedef boost::mpl::true_ is_seekable;

		MfcFileStream(CFile* fp) : m_fp(fp) {}

		size_t read(void* vbuf, size_t length);
		size_t write(const void* vbuf, size_t length);
		bool seek(stream_offset_t offset, int origin);
		void flush();

		void ensureWrite(const void* vbuf, size_t length);
		void ensureRead(void* vbuf, size_t length);
		void writeByte(unsigned char b);
		int getByte();
		unsigned char readByte();
	};

	class MfcArchiveStream
		: public IInputStream
		, public IOutputStream
		, public ISeekable
	{
		CArchive* m_fp;

	public:
		typedef boost::mpl::false_ is_seekable;

		MfcArchiveStream(CArchive* fp) : m_fp(fp) {}

		size_t read(void* vbuf, size_t length);
		size_t write(const void* vbuf, size_t length);
		bool seek(stream_offset_t offset, int origin);
		void flush();

		void ensureWrite(const void* vbuf, size_t length);
		void ensureRead(void* vbuf, size_t length);
		void writeByte(unsigned char b);
		int getByte();
		unsigned char readByte();
	};

	//! @{
	//! serialize CString
	template<class Input>
	void DataIO_loadObject(Input& input, CString& x)
	{
		var_size_t size;
		input >> size;
		LPTSTR buf = x.GetBufferSetLength(size.t);
		buf[size] = 0;
		uint32_t size2 = input.read(buf, size.t);

		if (size2 != size.t)
		{
			throw EndOfFileException("when load CString");
		}
		x.ReleaseBuffer(size.t);
	}
	template<class Output>
	void DataIO_saveObject(Output& output, const CString& x)
	{
		var_size_t size(x.GetLength());
		output << size;
		uint32_t size2 = output.write(static_cast<LPCTSTR>(x), size.t);

		if (size2 != size.t)
		{
			throw OutOfSpaceException("when save CString");
		}
	}
	//! @}
}

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma warning(pop)
#endif

#endif // __MfcFileStream_h__
