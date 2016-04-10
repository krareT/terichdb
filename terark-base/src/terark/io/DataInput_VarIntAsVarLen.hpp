#ifdef TERARK_DATA_IO_SLOW_VAR_INT
protected:
	template<class UInt>
	UInt load_var_uint()
	{
		const int maxshift = sizeof(UInt) == 4 ? 28 : 63;
		UInt x = 0;
		for (int shift = 0; shift <= maxshift; shift += 7)
		{
			byte b; *this >> b;
			x |= UInt(b & 0x7F) << shift;
			if ((b & 0x80) == 0)
				return x;
		}
		assert(0); // should not get here
		throw std::runtime_error(BOOST_CURRENT_FUNCTION);
	}
public:
	MyType& operator>>(var_int32_t& x)
	{
		x.t = var_int32_u2s(load_var_uint<uint32_t>());
		return *this;
	}
	MyType& operator>>(var_uint32_t& x)
   	{
	   	x.t = load_var_uint<uint32_t>();
	   	return *this;
   	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator>>(var_int64_t& x)
	{
		x.t = var_int64_u2s(load_var_uint<uint64_t>());
		return *this;
	}
	MyType& operator>>(var_uint64_t& x)
   	{
	   	x.t = load_var_uint<uint64_t>();
	   	return *this;
   	}
#endif
	MyType& operator>>(serialize_version_t& x)
   	{
	   	x.t = load_var_uint<uint32_t>();
	   	return *this;
   	}

#else // TERARK_DATA_IO_SLOW_VAR_INT

// fast var_*int*
	MyType& operator>>(var_int32_t& x)
	{
		x.t = this->getStream()->read_var_int32();
		return *this;
	}
	MyType& operator>>(var_uint32_t& x)
   	{
	   	x.t = this->getStream()->read_var_uint32();
	   	return *this;
   	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator>>(var_int64_t& x)
	{
		x.t = this->getStream()->read_var_int64();
		return *this;
	}
	MyType& operator>>(var_uint64_t& x)
   	{
		x.t = this->getStream()->read_var_uint64();
	   	return *this;
   	}
#endif
	MyType& operator>>(serialize_version_t& x)
   	{
	   	x.t = this->getStream()->read_var_uint32();
	   	return *this;
   	}
#endif // TERARK_DATA_IO_SLOW_VAR_INT

//-------------------------------------------------------------
	MyType& operator>>(var_int30_t& x)
	{
		x.t = this->getStream()->read_var_int30();
		return *this;
	}
	MyType& operator>>(var_uint30_t& x)
	{
		x.t = this->getStream()->read_var_uint30();
		return *this;
	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator>>(var_int61_t& x)
	{
		x.t = this->getStream()->read_var_int61();
		return *this;
	}
	MyType& operator>>(var_uint61_t& x)
	{
		x.t = this->getStream()->read_var_uint61();
		return *this;
	}
#endif

