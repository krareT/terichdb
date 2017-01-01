	typedef boost::mpl::false_ is_loading;
	typedef boost::mpl::true_  is_saving;
	typedef var_uint32_t my_var_uint32_t; // for decompose & argument dependent lookup
	typedef var_uint64_t my_var_uint64_t;
	typedef var_size_t   my_var_size_t;

	//////////////////////////////////////////////////////////////////////////
	//! dual operator, for auto dispatch single serialize proc...
	//!
	//! for PrimitiveOutputImpl, operator& is output,
	//! for DataOutput, operator& is output.
	template<class T> MyType& operator&(const T& x) { return operator<<(x); }

	template<class T> MyType& operator& (pass_by_value<T> x) { return operator<<(x.val); }
	template<class T> MyType& operator<<(pass_by_value<T> x) { return operator<<(x.val); }

	template<class T> MyType& operator& (reference_wrapper<T> x) { return operator<<(x.get()); }
	template<class T> MyType& operator<<(reference_wrapper<T> x) { return operator<<(x.get()); }

	template<int Dim> MyType& operator<<(const char (&x)[Dim]) { return this->save(x, Dim); }
	template<int Dim> MyType& operator<<(const byte (&x)[Dim]) { return this->save(x, Dim); }

#ifdef DATA_IO_SUPPORT_SERIALIZE_PTR
	template<class T> MyType& operator<<(T*& x)
	{
		*this << *x;
		return *this;
	}
	template<class T> MyType& operator<<(const T*& x)
	{
		*this << *x;
		return *this;
	}
#else
	template<class T> MyType& operator<<(T*&)
	{
		T::NotSupportSerializePointer();
		return *this;
	}
	template<class T> MyType& operator<<(const T*&)
	{
		T::NotSupportSerializePointer();
		return *this;
	}
#endif

	template<class Container>
	MyType& container_save(const Container& x)
	{
		var_size_t size(x.size());
		*this << size;
		typename Container::const_iterator i = x.begin();
		for (; i != x.end(); ++i) *this << *i;
		return *this;
	}

	//!@{
	//! standard container output.....
	template<class Key, class Value, class Compare, class Alloc>
	MyType& operator<<(const std::map<Key, Value, Compare, Alloc>& x)
	{
		return container_save(x);
	}
	template<class Key, class Value, class Compare, class Alloc>
	MyType& operator<<(const std::multimap<Key, Value, Compare, Alloc>& x)
	{
		return container_save(x);
	}
	template<class Value, class Compare, class Alloc>
	MyType& operator<<(const std::set<Value, Compare, Alloc>& x)
	{
		return container_save(x);
	}
	template<class Value, class Compare, class Alloc>
	MyType& operator<<(const std::multiset<Value, Compare, Alloc>& x)
	{
		return container_save(x);
	}
	template<class Value, class Alloc>
	MyType& operator<<(const std::list<Value, Alloc>& x)
	{
		return container_save(x);
	}
	template<class Value, class Alloc>
	MyType& operator<<(const std::deque<Value, Alloc>& x)
	{
		return container_save(x);
	}

	// endian of float types are same as corresponding size integer's
	MyType& operator<<(const float x) {
		BOOST_STATIC_ASSERT(sizeof(float) == 4);
		uint32_t ix;
		memcpy(&ix, &x, 4);
		*this << ix;
		return *this;
	}
	MyType& operator<<(const double x) {
		BOOST_STATIC_ASSERT(sizeof(double) == 8);
		uint64_t ix;
		memcpy(&ix, &x, 8);
		*this << ix;
		return *this;
	}
	MyType& operator<<(const long double x) {
		// wire format of 'long double' is same as double
		*this << double(x);
		return *this;
	}
	MyType& operator<<(const var_size_t x) {
		// default same with 'var_size_t_base'
		// can be overload and hidden by derived
		*this << static_cast<var_size_t_base>(x);
		return *this;
	}


