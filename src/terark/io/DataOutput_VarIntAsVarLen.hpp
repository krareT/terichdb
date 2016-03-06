public:
#ifdef TERARK_DATA_IO_SLOW_VAR_INT

	MyType& operator<<(var_int32_t x)
	{
		byte buf[5];
		this->ensureWrite(buf, save_var_int32(buf, x.t) - buf);
		return *this;
	}

	MyType& operator<<(var_uint32_t x)
	{
		byte buf[5];
		this->ensureWrite(buf, save_var_uint32(buf, x.t) - buf);
		return *this;
	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator<<(var_int64_t x)
	{
		byte buf[9];
		this->ensureWrite(buf, save_var_int64(buf, x.t) - buf);
		return *this;
	}
	MyType& operator<<(var_uint64_t x)
	{
		byte buf[9];
		this->ensureWrite(buf, save_var_uint64(buf, x.t) - buf);
		return *this;
	}
#endif
	MyType& operator<<(serialize_version_t x)
	{
		byte buf[5];
		this->ensureWrite(buf, save_var_uint32(buf, x.t) - buf);
		return *this;
	}
#else // TERARK_DATA_IO_SLOW_VAR_INT
// fast var_*int*
	MyType& operator<<(var_int32_t x)
	{
		this->getStream()->write_var_int32(x);
		return *this;
	}

	MyType& operator<<(var_uint32_t x)
	{
		this->getStream()->write_var_uint32(x);
		return *this;
	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator<<(var_int64_t x)
	{
		this->getStream()->write_var_int64(x);
		return *this;
	}
	MyType& operator<<(var_uint64_t x)
	{
		this->getStream()->write_var_uint64(x);
		return *this;
	}
#endif
	MyType& operator<<(serialize_version_t x)
	{
		this->getStream()->write_var_uint32(x.t);
		return *this;
	}
#endif // TERARK_DATA_IO_SLOW_VAR_INT

//--------------------------------------------------------
	MyType& operator<<(var_int30_t x)
	{
		this->getStream()->write_var_int30(x);
		return *this;
	}

	MyType& operator<<(var_uint30_t x)
	{
		this->getStream()->write_var_uint30(x);
		return *this;
	}

#if !defined(BOOST_NO_INT64_T)
	MyType& operator<<(var_int61_t x)
	{
		this->getStream()->write_var_int61(x);
		return *this;
	}
	MyType& operator<<(var_uint61_t x)
	{
		this->getStream()->write_var_uint61(x);
		return *this;
	}
#endif

