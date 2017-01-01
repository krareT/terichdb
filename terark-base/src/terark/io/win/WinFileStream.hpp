/* vim: set tabstop=4 : */
#ifndef __WinFileStream_h__
#define __WinFileStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(push)
# pragma warning(disable: 4267)
#endif

#include <terark/util/refcount.hpp>
#include <terark/io/IOException.hpp>
#include <terark/io/IStream.hpp>

#if !defined(_WINDOWS_) && !defined(_INC_WINDOWS)
#   define NOMINMAX
#   define WIN32_LEAN_AND_MEAN
#	include <windows.h>
#endif

namespace terark {

	class WinFileStream : public RefCounter, public ISeekableStream
	{
	public:
		typedef boost::mpl::true_ is_seekable;

		~WinFileStream();

		explicit WinFileStream(
				HANDLE hFile = INVALID_HANDLE_VALUE,
				bool bAutoClose = false,
				const std::string& strFile = ""
		);
		explicit WinFileStream(
				LPCSTR szFile,
				DWORD dwDesiredAccess,
				DWORD dwShareMode = FILE_SHARE_READ,
				LPSECURITY_ATTRIBUTES lpSecurityAttributes = 0 // optional
				);
		explicit WinFileStream(
				LPCSTR szFile,
				DWORD dwDesiredAccess,
				DWORD dwShareMode,
				LPSECURITY_ATTRIBUTES lpSecurityAttributes, // optional
				DWORD dwCreationDisposition,
				DWORD dwFlagsAndAttributes = 0,
				HANDLE hTemplateFile = NULL
				);
		bool open(
				LPCSTR szFile,
				DWORD dwDesiredAccess,
				DWORD dwShareMode = FILE_SHARE_READ,
				LPSECURITY_ATTRIBUTES lpSecurityAttributes = 0 // optional
				);
		bool open(
				LPCSTR szFile,
				DWORD dwDesiredAccess,
				DWORD dwShareMode,
				LPSECURITY_ATTRIBUTES lpSecurityAttributes, // optional
				DWORD dwCreationDisposition,
				DWORD dwFlagsAndAttributes = 0,
				HANDLE hTemplateFile = NULL
				);
		void attach(HANDLE	hFile,
					bool	bAutoClose = false,
					const std::string& strFile = "");

		HANDLE detach();

		size_t read(void* vbuf, size_t length);

		size_t write(const void* vbuf, size_t length);

		void seek(int64_t offset, int origin);
		uint64_t tell() const;

		void flush();

		void close();

		uint64_t size();

		void setEof(uint64_t newSize);

	protected:
		HANDLE		m_hFile;
		bool		m_bAutoClose;
		std::string m_strFile;

		uint64_t do_seek(int64_t offset, int origin);
		DWORD DefaultCreateDisposition(DWORD dwDesiredAccess) const;
		std::string ErrorText(DWORD errCode) const;
	};
}

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma warning(pop)
#endif

#endif // __WinFileStream_h__
