
typedef boost::mpl::true_  is_loading;
typedef boost::mpl::false_ is_saving;
typedef var_uint32_t my_var_uint32_t; // for decompose & argument dependent lookup
typedef var_uint64_t my_var_uint64_t;
typedef var_size_t   my_var_size_t;
typedef DataFormatException my_DataFormatException;
typedef BadVersionException my_BadVersionException;

// endian of float types are same as corresponding size integer's
MyType& operator>>(float& x) {
	BOOST_STATIC_ASSERT(sizeof(float) == 4);
	*this >> *(uint32_t*)(&x);
	return *this;
}
MyType& operator>>(double& x) {
	BOOST_STATIC_ASSERT(sizeof(double) == 8);
	*this >> *(uint64_t*)(&x);
	return *this;
}
MyType& operator>>(long double& x) {
	// wire format of 'long double' is same as double
	double y;
	*this >> y;
	x = y;
	return *this;
}

MyType& operator>>(var_size_t& x) {
	// default same with 'var_size_t_base'
	// can be overload and hidden by derived
	*this >> static_cast<var_size_t_base&>(x);
	return *this;
}

//////////////////////////////////////////////////////////////////////////
//! dual operator, for auto dispatch single serialize proc...
//!
//! for DataInput , operator& is input,
//! for DataOutput, operator& is output.
template<class T> MyType& operator&(T& x) { return operator>>(x); }

//-------------------------------------------------------------------------------------------------
//! can not bound temp object to non-const reference,
//! so use pass_by_value object in this case
//! @{
template<class T> MyType& operator& (pass_by_value<T> x) { return (*this) >> x.val; }
template<class T> MyType& operator>>(pass_by_value<T> x) { return (*this) >> x.val; }
//@}
//-------------------------------------------------------------------------------------------------

template<class T> MyType& operator& (reference_wrapper<T> x) { return (*this) >> x.get(); }
template<class T> MyType& operator>>(reference_wrapper<T> x) { return (*this) >> x.get(); }

template<int Dim> MyType& operator>>(char (&x)[Dim]) { return this->load(x, Dim); }
template<int Dim> MyType& operator>>(unsigned char (&x)[Dim]) { return this->load(x, Dim); }
template<int Dim> MyType& operator>>(  signed char (&x)[Dim]) { return this->load(x, Dim); }

#ifdef DATA_IO_SUPPORT_SERIALIZE_PTR
template<class T> MyType& operator>>(T*& x)
{
	x = new T;
	*this >> *x;
	return *this;
}
#else
template<class T> MyType& operator>>(T*&)
{
	T::NotSupportSerializePointer();
	return *this;
}
#endif

//!@{
//! standard container this->....

template<class First, class Second>
MyType& operator>>(std::pair<First, Second>& x)
{
	return *this >> x.first >> x.second;
}

template<class KeyT, class ValueT, class Compare, class Alloc>
MyType& operator>>(std::map<KeyT, ValueT, Compare, Alloc>& x)
{
	var_size_t size; *this >> size;
	x.clear();
	for (size_t i = 0; i < size.t; ++i)
	{
		std::pair<KeyT, ValueT> e;
		*this >> e;
		x.insert(x.end(), e); // x.end() as hint, time complexity is O(1)
	}
	return *this;
}

template<class KeyT, class ValueT, class Compare, class Alloc>
MyType& operator>>(std::multimap<KeyT, ValueT, Compare, Alloc>& x)
{
	var_size_t size; *this >> size;
	x.clear();
	for (size_t i = 0; i < size.t; ++i)
	{
		std::pair<KeyT, ValueT> e;
		*this >> e;
		x.insert(x.end(), e); // x.end() as hint, time complexity is O(1)
	}
	return *this;
}

template<class ValueT, class Compare, class Alloc>
MyType& operator>>(std::set<ValueT, Compare, Alloc>& x)
{
	x.clear();
	var_size_t size;
	*this >> size;
	for (size_t i = 0; i < size.t; ++i)
	{
		ValueT e;
		*this >> e;
		x.insert(x.end(), e); // x.end() as hint, time complexity is O(1)
	}
	return *this;
}

template<class ValueT, class Compare, class Alloc>
MyType& operator>>(std::multiset<ValueT, Compare, Alloc>& x)
{
	x.clear();
	var_size_t size;
	*this >> size;
	for (size_t i = 0; i < size.t; ++i)
	{
		ValueT e;
		*this >> e;
		x.insert(x.end(), e); // x.end() as hint, time complexity is O(1)
	}
	return *this;
}

template<class ValueT, class Alloc>
MyType& operator>>(std::list<ValueT, Alloc>& x)
{
	x.clear();
	var_size_t size;
	*this >> size;
	for (size_t i = 0; i < size.t; ++i)
	{
		x.push_back(ValueT());
		*this >> x.back();
	}
	return *this;
}

template<class ValueT, class Alloc>
MyType& operator>>(std::deque<ValueT, Alloc>& x)
{
	x.clear();
	var_size_t size;
	*this >> size;
	for (size_t i = 0; i < size.t; ++i)
	{
		x.push_back(ValueT());
		*this >> x.back();
	}
	return *this;
}
//!@}

template<class T>
T load_as() {
	T x;
	*this >> x;
	return x;
}

template<class T>
void skip_obj() {
	T x;
	*this >> x;
}
