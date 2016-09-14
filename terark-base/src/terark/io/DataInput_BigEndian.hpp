public:

	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(short)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(unsigned short)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(int)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(unsigned int)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(long)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(long long)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(__int64)
	DATA_IO_GEN_BIG_ENDIAN_INT_INPUT(unsigned __int64)
#endif

	MyType& load(wchar_t* s, size_t n)
	{
		this->ensureRead(s, sizeof(wchar_t)*n);
#ifdef BOOST_LITTLE_ENDIAN
		byte_swap(s, n);
#endif
		return *this;
	}
#ifndef BOOST_NO_INTRINSIC_WCHAR_T
	MyType& operator>>(wchar_t& x)
	{
		this->ensureRead(&x, sizeof(x));
#ifdef BOOST_LITTLE_ENDIAN
		x = byte_swap(x);
#endif
		return *this;
	}
#endif

	template<class T> MyType& operator>>(T& x)
	{
		DataIO_load_elem(*this, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T, int Dim>
	MyType& operator>>(T (&x)[Dim])
	{
		DataIO_load_array(*this, x, Dim, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T>
	MyType& operator>>(valvec<T>& x)
	{
		DataIO_load_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T, class Alloc>
	MyType& operator>>(std::vector<T, Alloc>& x)
	{
		DataIO_load_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T>
	MyType& load_add(valvec<T>& x) {
		DataIO_load_add_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T, class Alloc>
	MyType& load_add(std::vector<T>& x) {
		DataIO_load_add_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}
