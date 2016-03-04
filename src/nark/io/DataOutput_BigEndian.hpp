public:

	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(short)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(unsigned short)

	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(int)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(unsigned int)

	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(long)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(long long)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(__int64)
	DATA_IO_GEN_BIG_ENDIAN_INT_OUTPUT(unsigned __int64)
#endif

	MyType& save(const wchar_t* s, size_t n)
	{
#ifdef BOOST_BIG_ENDIAN
		this->ensureWrite(s, sizeof(wchar_t)*n);
#else
		std::vector<wchar_t> tempv(s, s + n);
		byte_swap(&*tempv.begin(), n);
		this->ensureWrite(&*tempv.begin(), sizeof(wchar_t)*n);
#endif
		return *this;
	}

#ifndef BOOST_NO_INTRINSIC_WCHAR_T
	MyType& operator<<(wchar_t x)
	{
#ifdef BOOST_LITTLE_ENDIAN
		x = byte_swap(x);
#endif
		this->ensureWrite(&x, sizeof(x));
		return *this;
	}
#endif

	template<class T> MyType& operator<<(const T& x)
	{
		DataIO_save_elem(*this, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T, int Dim>
	MyType& operator<<(const T (&x)[Dim])
	{
		DataIO_save_array(*this, x, Dim, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T>
	MyType& operator<<(const valvec<T>& x)
	{
		DataIO_save_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

	template<class T, class Alloc>
	MyType& operator<<(const std::vector<T, Alloc>& x)
	{
		DataIO_save_vector(*this, (T*)NULL, x, DATA_IO_BSWAP_FOR_BIG(T)());
		return *this;
	}

