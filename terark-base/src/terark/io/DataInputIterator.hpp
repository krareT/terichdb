/* vim: set tabstop=4 : */
#ifndef __terark_io_DataInputIterator_h__
#define __terark_io_DataInputIterator_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#ifndef __terark_io_DataInput_h__
#include "DataInput.hpp"
#endif

namespace terark {

template<class StreamClass>
LittleEndianDataInput<StreamClass*> LittleEndianDataInputer(StreamClass* stream)
{
	return LittleEndianDataInput<StreamClass*>(stream);
}

template<class StreamClass>
PortableDataInput<StreamClass*> PortableDataInputer(StreamClass* stream)
{
	return PortableDataInput<StreamClass*>(stream);
}
//////////////////////////////////////////////////////////////////////////

template<class DataInput, class T>
class DataInputIterator :
	public boost::input_iterator_helper<DataInputIterator<DataInput, T>, T>
{
	DataInput m_input;
	size_t m_count;

public:
	//! 序列的 count 已知，构造这个序列 iterator
	DataInputIterator(DataInput input, size_t count)
		: m_input(input), m_count(count)
	{
		assert(m_count > 0);
	}

	//! 序列的 count 还在 stream 中，构造时读取它(var_uint32_t 的 count)
	DataInputIterator(DataInput input)
		: m_input(input)
	{
		var_uint32_t x;  input >> x;
		m_count = x.t;
	}

	DataInputIterator()
		: m_count(0) {}

	//! 读取之后立即往前走，所以，同一个位置只能读取一次
	T operator*()
	{
		assert(m_count > 0);
		--m_count;

		T x; m_input >> x;
		return x;
	}

	//! 无操作
	DataInputIterator& operator++()
	{
		assert(m_count >= 0);
		return *this;
	}

	bool operator==(const DataInputIterator& r) const
	{
		return r.m_count == this->m_count;
	}

	bool is_end() const { return 0 == m_count; }

	size_t count() const { return m_count; }
};

//////////////////////////////////////////////////////////////////////////

}

#endif // __terark_io_DataInputIterator_h__

