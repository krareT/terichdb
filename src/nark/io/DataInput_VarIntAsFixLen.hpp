	MyType& operator>>(var_int32_t & x) { return *this >> x.t; }
	MyType& operator>>(var_uint32_t& x) { return *this >> x.t; }

#if !defined(BOOST_NO_INT64_T)
	MyType& operator>>(var_int64_t & x) { return *this >> x.t; }
	MyType& operator>>(var_uint64_t& x) { return *this >> x.t; }
#endif
	MyType& operator>>(serialize_version_t& x) { return *this >> x.t; }

	MyType& operator>>(var_int30_t & x) { return *this >> x.t; }
	MyType& operator>>(var_uint30_t& x) { return *this >> x.t; }

#if !defined(BOOST_NO_INT64_T)
	MyType& operator>>(var_int61_t & x) { return *this >> x.t; }
	MyType& operator>>(var_uint61_t& x) { return *this >> x.t; }
#endif

