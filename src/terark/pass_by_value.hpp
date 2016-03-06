/* vim: set tabstop=4 : */
#ifndef __terark_pass_by_value_h__
#define __terark_pass_by_value_h__

//#if defined(_MSC_VER) && (_MSC_VER >= 1020)
//# pragma once
//#endif

namespace terark {

//! 当 T 是一个人造的引用时，使用这个类来转发调用
//!
//!	input >> t 实际调用的是 void DataIO_loadObject(Input& input, T t)
//! 这里 pass_by_value 和 T 都是传值调用的
//!
//! T 中包含一个真实的引用，例如当 T 是 load_as_var_int_proxy<IntT> 时
//! 这样，就不需要将每个类似 load_as_var_int_proxy<IntT> 的 Class 都写到 DataInput 接口中
//! 从而 DataInput 接口只需要一个 pass_by_value
//!
//! 如此，实际上是使用了两个中间层一个是 load_as_var_int_proxy<IntT>，用来做真实的 proxy
//! 另一个就是 pass_by_value 了，只用来适配 DataInput 接口，
//! 因为作为 T& 不能绑定到临时变量
//! ---- Add this line for Microsoft C++ 2013 brain dead compiler error ----
template<class T> class pass_by_value
{
public:
	T val;

    typedef T type;

	pass_by_value(const T& val) : val(val) {}

	T& operator=(const T& y) { val = y; return val; }

	operator T&() { return val; }

	T& get() { return val; }
};

}

#endif // __terark_pass_by_value_h__

