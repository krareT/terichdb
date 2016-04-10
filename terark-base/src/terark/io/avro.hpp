/* vim: set tabstop=4 : */
#ifndef __terark_io_avro_h__
#define __terark_io_avro_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "DataIO.hpp"
#include "../hash_strmap.hpp"

namespace terark {

template<class StreamT>
class AvroDataInput : public StreamT {
public:
	AvroDataInput& operator>>(var_size_t& x) {
		var_int64_t y;
		*this >> y;
		x.t = y.t;
		return *this;
	}
	AvroDataInput& operator>>(std::string& x) {
		var_int64_t len;
		*this >> len;
		load(&x[0], len.t);
		return *this;
	}
	template<class Alloc>
	AvroDataInput& operator>>(std::vector<byte, Alloc>& x) {
		var_int64_t len;
		*this >> len;
		x.resize(len.t);
		load(&x[0], len.t);
		return *this;
	}
	template<class T, class Alloc>
	AvroDataInput& operator>>(std::vector<T, Alloc>& x) {
		var_int64_t len;
		size_t pos = 0;
		x.resize(0);
		do {
			*this >> len;
			load(&x[0], len.t);
			if (len < 0) {
				len.t = -len.t;
				var_int64_t bytes_of_block;
				*this >> bytes_of_block; // skip bytes_of_block
			}
			x.resize(pos + len.t);
			DataIO_load_array(*this, &x[pos], len.t, DATA_IO_BSWAP_FOR_LITTLE(T));
			pos += len.t;
		} while (len.t);
		return *this;
	}
	template<class Value>
	AvroDataInput& operator>>(hash_strmap<Value>& x) {
		x.clear();
		std::string key;
		Value val;
		var_int64_t len;
		do {
			*this >> len;
			if (len < 0) {
				len = -len;
				var_int64_t bytes_of_block;
				*this >> bytes_of_block; // skip bytes_of_block
			}
			for (size_t i = 0, n = len.t; i != n; ++i) {
				*this >> key;
				*this >> val;
				std::pair<size_t, bool> ib = x.insert_i(key, val);
				if (!ib.second) {
					throw std::runtime_error("duplicate key");
				}
			}
		} while (len.t);
		return *this;
	}
};

template<class StreamT>
class AvroDataOutput : public StreamT {
public:
	AvroDataOutput& operator<<(const var_size_t x) {
		var_int64_t y(x.t);
		*this << y;
		return *this;
	}
	AvroDataOutput& operator<<(const std::string& x) {
		fstring y(x);
		return *this << y;
	}
	AvroDataOutput& operator<<(const fstring x) {
		var_int64_t len(x.n);
		*this << len;
		save(x.p, len.t);
		return *this;
	}
	template<class T, class Alloc>
	AvroDataInput& operator<<(const std::vector<T, Alloc>& x) {
		var_int64_t len(x.size());
		*this << len;
		load(&x[0], len.t);
		return *this;
	}
	template<class T, class Alloc>
	AvroDataInput& operator<<(const std::vector<T, Alloc>& x) {
		var_int64_t len(x.size());
		*this << len;
		DataIO_save_array(*this, &x[0], len.t, DATA_IO_BSWAP_FOR_LITTLE(T));
		*this << byte(0); // var_int64_t(0)
		return *this;
	}
	template<class Value>
	AvroDataOutput& operator<<(const hash_strmap<Value>& x) {
		var_int64_t len(x.size);
		*this << len;
		if (x.delcnt()) {
			for (size_t i = 0, n = x.end_i(); i != n; i = x.next_i(i)) {
				if (!x.is_deleted()) {
					*this << x.key(i);
					*this << x.val(i);
				}
			}
		}
		else {
			for (size_t i = 0, n = x.end_i(); i != n; ++i) {
				*this << x.key(i);
				*this << x.val(i);
			}
		}
		*this << byte(0); // var_int64_t(0)
		return *this;
	}
};

} // namespace terark::avro

#endif // __terark_io_avro_h__

