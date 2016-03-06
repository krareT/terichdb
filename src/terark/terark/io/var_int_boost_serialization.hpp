/* vim: set tabstop=4 : */
#ifndef __terark_io_var_int_for_boost_serialization_h__
#define __terark_io_var_int_for_boost_serialization_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <assert.h>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/split_free.hpp>

#include "var_int.hpp"

namespace boost { namespace serialization {

template<class Archive>
uint32_t load_var_uint32(Archive& ar)
{
	uint32_t v = 0;
	for (int shift = 0; shift < 35; shift += 7)
	{
		unsigned char b; ar >> b;
		v |= uint32_t(b & 0x7F) << shift;
		if ((b & 0x80) == 0)
			return v;
	}
	assert(0); // should not get here
	return v; // avoid compile warning
}

template<class Archive>
uint64_t load_var_uint64(Archive& ar)
{
	uint64_t v = 0;
	unsigned char b;
	for (int shift = 0; shift < 56; shift += 7)
	{
		ar >> b;
		v |= uint64_t(b & 0x7F) << shift;
		if ((b & 0x80) == 0)
			return v;
	}
	ar >> b;
	v |= uint64_t(b) << 56;
	return v;
}

//////////////////////////////////////////////////////////////////////////

template<class Archive>
void load(Archive& ar, terark::var_uint32_t& x, unsigned/*version*/)
{
	x.t = load_var_uint32(ar);
}

template<class Archive>
void load(Archive& ar, terark::var_int32_t& x, unsigned/*version*/)
{
	x.t = var_int32_u2s(load_var_uint32(ar));
}

template<class Archive>
void save(Archive& ar, terark::var_uint32_t x, unsigned/*version*/)
{
	unsigned char buf[5];
	ar.save_binary(buf, terark::save_var_uint32(buf, x.t) - buf);
}

template<class Archive>
void save(Archive& ar, terark::var_int32_t x, unsigned/*version*/)
{
	unsigned char buf[5];
	ar.save_binary(buf, terark::save_var_int32(buf, x.t) - buf);
}

//////////////////////////////////////////////////////////////////////////
template<class Archive>
void load(Archive& ar, terark::var_uint64_t& x, unsigned/*version*/)
{
	x.t = terark::load_var_uint64(ar);
}

template<class Archive>
void load(Archive& ar, terark::var_int64_t& x, unsigned/*version*/)
{
	x.t = terark::var_int64_u2s(load_var_uint64(ar));
}

template<class Archive>
void save(Archive& ar, terark::var_uint64_t x, unsigned/*version*/)
{
	unsigned char buf[9];
	ar.save_binary(buf, terark::save_var_uint64(buf, x.t) - buf);
}

template<class Archive>
void save(Archive& ar, terark::var_int64_t x, unsigned/*version*/)
{
	unsigned char buf[9];
	ar.save_binary(buf, terark::save_var_int64(buf, x.t) - buf);
}

} } // namespace boost::serialization

BOOST_SERIALIZATION_SPLIT_FREE(terark::var_uint32_t)
BOOST_SERIALIZATION_SPLIT_FREE(terark::var_int32_t)
BOOST_SERIALIZATION_SPLIT_FREE(terark::var_uint64_t)
BOOST_SERIALIZATION_SPLIT_FREE(terark::var_int64_t)


#endif // __terark_io_var_int_for_boost_serialization_h__

