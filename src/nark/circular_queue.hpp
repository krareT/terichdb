/* vim: set tabstop=4 : */
/********************************************************************
	@file circular_queue.hpp
	@brief 循环队列的实现

	@date	2006-9-28 12:07
	@author	Lei Peng
	@{
*********************************************************************/
#ifndef __circular_queue_hpp_
#define __circular_queue_hpp_

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <vector>
#include <boost/swap.hpp>

namespace nark {

/**
 @brief 使用一个用户指定的 vector 类容器实现循环队列

  - 循环队列的大小是固定的，其结构是一个环状数组，最少会浪费一个元素的存储空间

  - 循环队列可以提供 serial/real_index 序列号，可用于元素的标识（如网络协议中的帧序号）

  - 所有输入参数均需要满足其前条件，如果不满足，
    Debug 版会引发断言失败，Release 版会导致未定义行为
 */
template<class ElemT>
class circular_queue
{
	// valid element count is (m_vec.size() - 1)
	ElemT*    m_vec;
	ptrdiff_t m_head; // point to next pop_front position
	ptrdiff_t m_tail; // point to next push_back position
	ptrdiff_t m_nlen;

	ptrdiff_t prev(ptrdiff_t current) const throw()
	{
		ptrdiff_t i = current - 1;
		return i >=0 ? i : i + m_nlen; // == (i + m_nlen) % m_nlen, but more fast
	//	return (current + m_vec.size() - 1) % m_vec.size();
	}
	ptrdiff_t next(ptrdiff_t current) const throw()
	{
		ptrdiff_t i = current + 1;
		return i < m_nlen ? i : i - m_nlen; // == i % m_nlen, but more fast
	//	return (current + 1) % m_vec.size();
	}
	ptrdiff_t prev_n(ptrdiff_t current, ptrdiff_t n) const throw()
	{
		assert(n < m_nlen);
		ptrdiff_t i = current - n;
		return i >=0 ? i : i + m_nlen; // == i % c, but more fast
//		return (current + m_vec.size() - n) % m_vec.size();
	}
	ptrdiff_t next_n(ptrdiff_t current, ptrdiff_t n) const throw()
	{
		assert(n < m_nlen);
		ptrdiff_t c = m_nlen;
		ptrdiff_t i = current + n;
		return i < c ? i : i - c; // == i % c, but more fast
	//	return (current + n) % m_vec.size();
	}

public:
	typedef ElemT			value_type;

	typedef       ElemT&          reference;
	typedef const ElemT&    const_reference;
	typedef       ElemT*	      pointer;
	typedef const ElemT*    const_pointer;

	typedef uintptr_t	size_type;

	class const_iterator
	{
		friend class circular_queue;
		const ElemT* p;
		const circular_queue* queue;
		const ElemT* base() const throw() { return &queue->m_vec[0]; }
		typedef const_iterator my_type;

	public:
		my_type operator++()    const throw()
		{ p = base() + queue->next(p - base()); return *this; }
		my_type operator++(int) const throw()
		{ my_type temp = ++(*this); return temp; }

		my_type operator--()    const throw()
		{ p = base() + queue->prev(p - base()); return *this; }
		my_type operator--(int) const throw()
		{ my_type temp = --(*this); return temp; }

		my_type& operator+=(ptrdiff_t distance) throw()
		{ p = base() + queue->next_n(p - base()); return *this; }
		my_type& operator-=(ptrdiff_t distance) throw()
		{ p = base() + queue->prev_n(p - base()); return *this; }

		my_type operator+(ptrdiff_t distance) throw()
		{ my_type temp = *this; temp += distance; return temp; }
		my_type operator-(ptrdiff_t distance) throw()
		{ my_type temp = *this; temp -= distance; return temp; }

		bool operator==(const my_type& r) const throw() { return p == r.p; }
		bool operator!=(const my_type& r) const throw() { return p != r.p; }
		bool operator< (const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) <
				queue->virtual_index(r.p - base());
		}
		bool operator> (const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) >
				queue->virtual_index(r.p - base());
		}
		bool operator<=(const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) <=
				queue->virtual_index(r.p - base());
		}
		bool operator>=(const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) >=
				queue->virtual_index(r.p - base());
		}

		const ElemT& operator *() const throw() { return *p; }
		const ElemT* operator->() const throw() { return  p; }
	};

	class iterator
	{
		friend class circular_queue;
		ElemT* p;
		circular_queue* queue;
		ElemT* base() const throw() { return &queue->m_vec[0]; }
		typedef iterator my_type;

	public:
		my_type operator++()    const throw()
		{ p = base() + queue->next(p - base()); return *this; }
		my_type operator++(int) const throw()
		{ my_type temp = ++(*this); return temp; }

		my_type operator--()    const throw()
		{ p = base() + queue->prev(p - base()); return *this; }
		my_type operator--(int) const throw()
		{ my_type temp = --(*this); return temp; }

		my_type& operator+=(ptrdiff_t distance) throw()
		{ p = base() + queue->next_n(p - base()); return *this; }
		my_type& operator-=(ptrdiff_t distance) throw()
		{ p = base() + queue->prev_n(p - base()); return *this; }

		my_type operator+(ptrdiff_t distance) throw()
		{ my_type temp = *this; temp += distance; return temp; }
		my_type operator-(ptrdiff_t distance) throw()
		{ my_type temp = *this; temp -= distance; return temp; }

		bool operator==(const my_type& r) const throw() { return p == r.p; }
		bool operator!=(const my_type& r) const throw() { return p != r.p; }
		bool operator< (const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) <
				   queue->virtual_index(r.p - base());
		}
		bool operator> (const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) >
				   queue->virtual_index(r.p - base());
		}
		bool operator<=(const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) <=
				   queue->virtual_index(r.p - base());
		}
		bool operator>=(const my_type& r) const throw()
		{
			return queue->virtual_index(p - base()) >=
				   queue->virtual_index(r.p - base());
		}

		ElemT& operator *() const throw() { return *p; }
		ElemT* operator->() const throw() { return  p; }

		operator const const_iterator&() const throw()
		{ return *reinterpret_cast<const const_iterator*>(this); }
	};
	friend class const_iterator;
	friend class       iterator;

	/**
	 @brief 构造最多能容纳 capacity 个有效元素的循环队列
	 */
	explicit circular_queue(ptrdiff_t capacity) : m_nlen(capacity + 1)
	{
		assert(capacity != 0);
		m_vec = (ElemT*)malloc(sizeof(ElemT) * m_nlen);
		if (NULL == m_vec) throw std::bad_alloc();
		m_head = m_tail = 0;
	}
	circular_queue() {
		m_vec = NULL;
		m_nlen = m_head = m_tail = 0;
	}

	void init(ptrdiff_t capacity) {
		assert(0 == m_nlen);
		new(this)circular_queue(capacity);
	}

	~circular_queue() {
	   	clear();
		if (m_vec) ::free(m_vec);
   	}

	/**
	 @brief 清除队列中的有效元素

	 - 前条件：无
	 - 操作结果：队列为空
	 */
	void clear()
	{
		while (!empty())
			pop_front();
		m_head = m_tail = 0;
	}

	/**
	 @brief 测试队列是否为空

	 - 前条件：无
	 @return true 表示队列为空，false 表示非空
	 */
	bool empty() const throw() { return m_head == m_tail; }

	/**
	 @brief 测试队列是否已满

	 - 前条件：无
	 @return true 表示队列已满，false 表示未满
	 */
	bool full() const throw() { return next(m_tail) == m_head; }
//	bool full() const throw() { return m_tail+1==m_head || (m_head+m_nlen-1==m_tail); }

	/**
	 @brief 返回队列当前尺寸

	 - 前条件：无
	 @return 队列中有效元素的个数，总小于等于 capacity
	 */
	size_type size() const throw() { return m_head <= m_tail ? m_tail - m_head : m_tail + m_nlen - m_head; }

	/**
	 @brief 返回队列容量

	 - 前条件：无
	 @return 即构造该对象时传入的参数，或者 resize 后的新容量
	 */
	size_type capacity() const throw() { return m_nlen - 1; }

	/**
	 @brief 在队列尾部加入一个新元素

	 - 前条件：队列不满
	 - 操作结果：新元素被添加到队列尾部
	 */
	void push_back(const ElemT& val)
	{
		assert(!full());
		new(&m_vec[m_tail])ElemT(val);
		m_tail = next(m_tail);
	}

    //@{
	/**
	 @brief 返回队列头部的那个元素

	 - 前条件：队列不空
	 */
	const ElemT& front() const throw()
	{
		assert(!empty());
		return m_vec[m_head];
	}
	ElemT& front() throw()
	{
		assert(!empty());
		return m_vec[m_head];
	}
	//@}

	/**
	 @brief 弹出队列头部的那个元素并通过 out 参数 val 返回

	 - 前条件：队列不空
	 @param[out] val 队列头部元素将被复制进 val
	 */
	void pop_front(ElemT& val)
	{
		assert(!empty());
		boost::swap(val, m_vec);
		m_vec[m_head].~ElemT();
		m_head = next(m_head);
	}

	/**
	 @brief 弹出队列头部的那个元素

	 - 前条件：队列不空
	 该函数与 stl vector/list/deque 兼容
	 */
	void pop_front()
	{
		assert(!empty());
		m_vec[m_head].~ElemT();
		m_head = next(m_head);
	}

	/**
	 @brief 弹出序列号小于等于输入参数 real_index 的所有元素

	  pop all elements which real_index is earlier or equal than real_index
	 - 前条件：队列不空，且参数 real_index 代表的元素必须在队列中
	 */
	void pop_lower(ptrdiff_t real_index)
	{
		assert(!empty());
		// because poped elements can be equal with real_index
		// poped elements count is (virtual_index(real_index) + 1)
		ptrdiff_t count = virtual_index(real_index) + 1;
		assert(count <= size());
		while (count-- > 0)
		{
			pop_front();
		}
	}

	/**
	 @name 不是队列操作的成员
	  none queue operations.
	  只是为了功能扩展
     @{
	 */

	/**
	 @brief 在队列头部加入新元素

	 - 前条件：队列不满
	 - 操作结果：队列中现存元素的 real_index 均增一，新元素 val 成为新的队头
	 */
	void push_front(const ElemT& val)
	{
		assert(!full());
		m_head = prev(m_head);
		new(&m_vec[m_head])ElemT(val);
	}

	//@{
	/** 返回队列尾部元素 前条件：队列不空 */
	ElemT& back() throw()
	{
		assert(!empty());
		return m_vec[prev(m_tail)];
	}
	const ElemT& back() const throw()
	{
		assert(!empty());
		return m_vec[prev(m_tail)];
	}
	//@}

	/**
	 @brief 弹出队列尾部元素

	 - 前条件：队列不空
	 */
	void pop_back(ElemT& val)
	{
		assert(!empty());
		m_tail = prev(m_tail);
		boost::swap(val, m_vec[m_tail]);
		m_vec[m_tail].~ElemT();
	}

	/**
	 @brief 弹出队列尾部元素

	 - 前条件：队列不空
	 */
	void pop_back()
	{
		assert(!empty());
		m_tail = prev(m_tail);
		m_vec[m_tail].~ElemT();
	}

	/**
	 @brief 弹出序列号比大于等于输入参数 real_index 的所有元素

	  pop elements which real_index is later or equal than real_index
	 - 前条件：队列不空，且参数 real_index 代表的元素必须在队列中
	 */
	void pop_upper(ptrdiff_t real_index)
	{
		assert(!empty());
		// because poped elements can be equal with real_index
		// if not include the equal one, count is (size() - virtual_index(real_index) - 1);
		ptrdiff_t count = size() - virtual_index(real_index);
		assert(count <= size());
		while (count-- > 0)
		{
			pop_back();
		}
	}
	//@} // name 不是队列操作的成员

	/**
	 @name iterator 相关成员
	 @{
	 */
	iterator begin() throw()
	{
		iterator iter;
		iter.queue = this;
		iter.p = m_vec + m_head;
		return iter;
	}
	const_iterator begin() const throw()
	{
		iterator iter;
		iter.queue = this;
		iter.p = m_vec + m_head;
		return iter;
	}
	iterator end() throw()
	{
		iterator iter;
		iter.queue = this;
		iter.p = m_vec + m_tail;
		return iter;
	}
	const_iterator end() const throw()
	{
		iterator iter;
		iter.queue = this;
		iter.p = m_vec + m_tail;
		return iter;
	}
	//@}

	/**
	 @brief 通过real_index取得元素相对于队头的偏移
	 */
	ptrdiff_t virtual_index(ptrdiff_t real_index) const throw()
	{
		assert(real_index >= 0);
		assert(real_index < (ptrdiff_t)size());
		ptrdiff_t i = real_index - m_head;
		return i >= 0 ? i : i + m_nlen;
	//	return (m_vec.size() + real_index - m_head) % m_vec.size();
	}

	/**
	 @brief 通过virtual_index取得元素的序列号
	 */
	ptrdiff_t real_index(ptrdiff_t virtual_index) const throw()
	{
		assert(virtual_index >= 0);
		assert(virtual_index < size());
		ptrdiff_t i = virtual_index + m_head;
		return i < m_nlen ? i : i - m_nlen;
	//	return (virtual_index + m_head) % m_vec.size();
	}

	/**
	 @brief 队头的序列号
	 */
	ptrdiff_t head_real_index() const throw() { return m_head; }
	/**
	 @brief 队尾的序列号
	 */
	ptrdiff_t tail_real_index() const throw() { return m_tail; }

	/**
	 @brief 通过 virtual_index 取得元素
	 @{
	 */
	ElemT& operator[](ptrdiff_t virtual_index)
	{
		assert(virtual_index >= 0);
		assert(virtual_index < size());
		ptrdiff_t c = m_nlen;
		ptrdiff_t i = m_head + virtual_index;
		ptrdiff_t j = i < c ? i : i - c;
		return m_vec[j];
	//	return m_vec[(m_head + virtual_index) % m_vec.size()];
	}
	const ElemT& operator[](ptrdiff_t virtual_index) const
	{
		assert(virtual_index >= 0);
		assert(virtual_index < size());
		ptrdiff_t c = m_nlen;
		ptrdiff_t i = m_head + virtual_index;
		ptrdiff_t j = i < c ? i : i - c;
		return m_vec[j];
	//	return m_vec[(m_head + virtual_index) % m_vec.size()];
	}
	//@}
};

/*
template<class ElemT, class VectorT>
inline
circular_queue<ElemT, VectorT>::const_iterator
operator+(ptrdiff_t n, const circular_queue<ElemT, VectorT>::const_iterator& iter)
{
	return iter + n;
}

template<class ElemT, class VectorT>
inline
circular_queue<ElemT, VectorT>::iterator
operator+(ptrdiff_t n, const circular_queue<ElemT, VectorT>::iterator& iter)
{
	return iter + n;
}
*/

} // namespace nark


#endif



// @} end file circular_queue.hpp

