/* vim: set tabstop=4 : */
#ifndef __ConcurrentStream_h__
#define __ConcurrentStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <terark/stdtypes.hpp>
#include <boost/thread/mutex.hpp>

namespace terark {

template<class StreamClass>
class ConcurrentStreamWrapper : public StreamClass
{
	ConcurrentStreamWrapper(const ConcurrentStreamWrapper&);
	ConcurrentStreamWrapper& operator=(const ConcurrentStreamWrapper&);
protected:
	boost::mutex m_mutex;
public:
	ConcurrentStreamWrapper() {}

	template<class T1>
	ConcurrentStreamWrapper(T1 p1) : StreamClass(p1) {}
	template<class T1, class T2>
	ConcurrentStreamWrapper(T1 p1, T2 p2) : StreamClass(p1, p2) {}
	template<class T1, class T2, class T3>
	ConcurrentStreamWrapper(T1 p1, T2 p2, T3 p3) : StreamClass(p1, p2, p3) {}

	int getByte()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::getByte();
	}
	unsigned char readByte()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::readByte();
	}
	void writeByte(unsigned char b)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::writeByte(b);
	}
	size_t read(void* data, size_t length)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::read(data, length);
	}
	size_t write(const void* data, size_t length)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::write(data, length);
	}
	void flush()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		StreamClass::flush();
	}
	bool eof()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::eof();
	}

	bool seek(size_t newPos)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seek(newPos);
	}
	bool seek(long offset, int origin)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seek(offset, origin);
	}
	size_t tell()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::tell();
	}

// for divided dual IO
	bool seekp(size_t newPos)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seekp(newPos);
	}
	bool seekp(long offset, int origin)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seekp(offset, origin);
	}
	size_t tellp()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::tellp();
	}
	bool seekg(size_t newPos)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seekg(newPos);
	}
	bool seekg(long offset, int origin)
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::seekg(offset, origin);
	}
	size_t tellg()
	{
		boost::mutex::scoped_lock lock(m_mutex)
		return StreamClass::tellg();
	}
};

} // namespace terark

#endif // __ConcurrentStream_h__

