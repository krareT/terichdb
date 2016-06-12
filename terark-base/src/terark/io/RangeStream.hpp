#pragma once
#ifndef __terark_io_stream_range_hpp__
#define __terark_io_stream_range_hpp__

#include <stddef.h>
#include <string>
#include <terark/stdtypes.hpp>
#include <boost/static_assert.hpp>
//#include <boost/enable_shared_from_this.hpp>
#include <boost/core/enable_if.hpp>
#include "var_int.hpp"
#include "var_int_inline.hpp"

namespace terark {

	template<class Stream>
	class RangeStream : public Stream {
		size_t m_len;

	public:
		RangeStream() { m_len = 0; }
		void setRangeLen(size_t len) { m_len = len; }
		bool eof() const { return 0 == m_len; }
		int  getByte() {
			assert(m_len > 0);
			m_len--;
			return Stream::getByte();
		}
		byte readByte() {
			assert(m_len > 0);
			m_len--;
			return Stream::readByte();
		}
		void ensureRead(void* vbuf, size_t length) {
			assert(length <= m_len);
			Stream::ensureRead(vbuf, length);
			m_len -= length;
		}
		size_t read(void* vbuf, size_t length) {
			size_t ret = Stream::read(vbuf, length);
			m_len -= ret;
			return ret;
		}

		template<class ByteArray>
		void readAll(ByteArray& ba) {
			BOOST_STATIC_ASSERT(sizeof(ba[0]) == 1);
			ba.resize(m_len);
			if (m_len) {
				// must be a continuous memory block
				assert(&*ba.begin() + m_len == &*(ba.end()-1) + 1);
				Stream::ensureRead(&*ba.begin(), ba.size());
				m_len = 0;
			}
		}

		uint32_t read_var_uint32() { return gg_load_var_uint_slow<RangeStream, uint32_t>(*this, BOOST_CURRENT_FUNCTION); }
		uint32_t read_var_uint30() { return gg_load_var_uint30_slow<RangeStream>(*this); }
		uint64_t read_var_uint64() { return gg_load_var_uint_slow<RangeStream, uint64_t>(*this, BOOST_CURRENT_FUNCTION);  }
		uint64_t read_var_uint61() { return gg_load_var_uint61_slow<RangeStream>(*this); }
		int32_t read_var_int32() { return var_int32_u2s(read_var_uint32()); }
		int32_t read_var_int30() { return var_int30_u2s(read_var_uint30()); }
		int64_t read_var_int64() { return var_int64_u2s(read_var_uint64()); }
		int64_t read_var_int61() { return var_int61_u2s(read_var_uint61()); }

		void read_string(std::string& s) {
			size_t len = TERARK_IF_WORD_BITS_64(read_var_uint64, read_var_uint32)();
			s.resize(len);
			if (len)
				this->ensureRead(&*s.begin(), len);
		}
	};

	template<class T>
	class RestAllTpl {
	public:
		RestAllTpl(T& x) : p(&x) {}

	private:
		T* p;

		template<class DataIO, class ByteArray>
		static
		typename boost::enable_if_c<sizeof(*((ByteArray*)0)->begin()) != 0, void>::type
		dioLoad(DataIO& dio, ByteArray& ba) {
			BOOST_STATIC_ASSERT(sizeof(ba[0]) == 1);
			dio.readAll(ba);
			assert(dio.eof());
		}
		template<class DataIO, class U>
		static
		typename boost::disable_if_c<sizeof(*((U*)0)->begin()) != 0, void>::type
		dioLoad(DataIO& dio, U& x) { dio >> x; }

		template<class DataIO, class ByteArray>
		static
		typename boost::enable_if_c<sizeof(*((ByteArray*)0)->begin()) != 0, void>::type
		dioSave(DataIO& dio, const ByteArray& ba) {
			BOOST_STATIC_ASSERT(sizeof(ba[0]) == 1);
			dio.ensureWrite(ba.data(), ba.size());
		}
		template<class DataIO, class U>
		static
		typename boost::disable_if_c<sizeof(*((U*)0)->begin()) != 0, void>::type
		dioSave(DataIO& dio, const U& x) { dio << x; }

		template<class DataIO>
		friend void DataIO_loadObject(DataIO& dio, RestAllTpl x) {
			dioLoad(dio, *x.p);
		}

		template<class DataIO>
		friend void DataIO_saveObject(DataIO& dio, RestAllTpl x) {
			dioSave(dio, *x.p);
		}
	};

	template<class T> RestAllTpl<const T> RestAll(const T& t) { return t; }
	template<class T> pass_by_value<RestAllTpl<T> > RestAll(T& t) { return RestAllTpl<T>(t); }

} // namespace terark

#endif // __terark_io_stream_range_hpp__
