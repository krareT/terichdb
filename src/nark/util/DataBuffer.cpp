#include "DataBuffer.hpp"

namespace nark {

inline
DataBuffer::DataBuffer(size_t size)
  : m_refcount(0), m_size(size)
{}

DataBuffer* DataBuffer::create(size_t size)
{
	DataBuffer* p = (DataBuffer*)new char[sizeof(DataBuffer) + size];
	new (p) DataBuffer(size); // placement new...
	return p;
}
void DataBuffer::destroy(DataBuffer* p)
{
	char* pb = (char*)p;
	delete [] pb;
}

DataBufferPtr::DataBufferPtr(size_t size)
  : MyBase(DataBuffer::create(size))
{}

// SmartBuffer

SmartBuffer::SmartBuffer(size_t size)
{
	m_data = size ? new byte[size] : 0;
	m_size = size;
	m_refcountp = new boost::detail::atomic_count(1);
}

SmartBuffer::~SmartBuffer()
{
	if (m_refcountp && 0 == --*m_refcountp)
	{
		delete m_refcountp;
		delete [] m_data;
	}
}

SmartBuffer::SmartBuffer(const SmartBuffer& rhs)
  : m_data(rhs.m_data)
  , m_size(rhs.m_size)
  , m_refcountp(rhs.m_refcountp)
{
	if (m_refcountp)
		++*m_refcountp;
}

const SmartBuffer& SmartBuffer::operator=(const SmartBuffer& rhs)
{
	SmartBuffer(rhs).swap(*this);
	return *this;
}





} // namespace nark

