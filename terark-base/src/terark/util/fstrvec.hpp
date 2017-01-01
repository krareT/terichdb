#ifndef __terark_fstrvec_hpp__
#define __terark_fstrvec_hpp__

#include <terark/valvec.hpp>
#include <terark/util/throw.hpp>
#include <string>
#include <utility>
#include <vector>

#if defined(__GNUC__)
#include <stdint.h> // for uint16_t
#endif

namespace terark {

// 'Offset' could be a struct which contains offset as a field
template<class Offset>
struct default_offset_op {
	size_t get(const Offset& x) const { return x; }
	void   set(Offset& x, size_t y) const { x = static_cast<Offset>(y); }
	void   inc(Offset& x, ptrdiff_t d = 1) const {
		assert(d >= 0);
		x += Offset(d);
	}
	Offset make(size_t y) const { return Offset(y); }
	static const Offset maxpool = Offset(-1);
// msvc compile error:
//	BOOST_STATIC_ASSERT(Offset(0) < Offset(-1)); // must be unsigned
};

// just allow operations at back
//
template< class Char
		, class Offset = unsigned
		, class OffsetOp = default_offset_op<Offset>
		>
class basic_fstrvec : private OffsetOp {
	template<class> struct void_ { typedef void type; };
public:
	valvec<Char>   strpool;
	valvec<Offset> offsets;
	static const size_t maxpool = OffsetOp::maxpool;

	explicit basic_fstrvec(const OffsetOp& oop = OffsetOp())
	  : OffsetOp(oop) {
		offsets.push_back(OffsetOp::make(0));
	}

	void reserve(size_t capacity) {
		offsets.reserve(capacity+1);
	}
	void reserve_strpool(size_t capacity) {
		strpool.reserve(capacity);
	}

	void erase_all() {
		strpool.erase_all();
		offsets.resize_no_init(1);
		offsets[0] = OffsetOp::make(0);
	}

	// push_back an empty string
	// offten use with back_append
   	void push_back() {
		offsets.push_back(OffsetOp::make(strpool.size()));
	}

#define basic_fstrvec_check_overflow(StrLen) \
	assert(strpool.size() + StrLen < maxpool); \
	if (maxpool <= UINT32_MAX) { \
		if (strpool.size() + StrLen > maxpool) { \
			THROW_STD(length_error \
				, "strpool.size() = %zd, StrLen = %zd" \
				, strpool.size(), size_t(StrLen)); \
		} \
	}
#define basic_fstrvec_check_range(range, ExtraLen) \
	if (range.first > range.second) { \
		THROW_STD(invalid_argument, \
			"invalid ptr_range{ %p , %p }", range.first, range.second); \
	} \
	basic_fstrvec_check_overflow(size_t(range.second + ExtraLen - range.first))
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	template<class String>
	typename void_<typename String::iterator>::type
   	push_back(const String& str) {
		basic_fstrvec_check_overflow(str.size());
		strpool.append(str.begin(), str.end());
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void push_back(std::pair<const Char*, const Char*> range) {
		basic_fstrvec_check_range(range, 0);
		strpool.append(range.first, range.second);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	template<class String>
	typename void_<typename String::iterator>::type
   	push_back(const String& str, Char lastChar) {
		basic_fstrvec_check_overflow(str.size()+1);
		strpool.append(str.begin(), str.end());
		strpool.push_back(lastChar);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void push_back(std::pair<const Char*, const Char*> range, Char lastChar) {
		basic_fstrvec_check_range(range, 1);
		strpool.append(range.first, range.second);
		strpool.push_back(lastChar);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	void emplace_back(const Char* str, size_t len) {
		basic_fstrvec_check_overflow(len);
		strpool.append(str, len);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}
	template<class ForwardIterator>
	void emplace_back(ForwardIterator first, ForwardIterator last) {
		size_t len = std::distance(first, last);
		basic_fstrvec_check_overflow(len);
		strpool.append(first, len);
		offsets.push_back(OffsetOp::make(strpool.size()));
	}

	void back_append(std::pair<const Char*, const Char*> range) {
		assert(range.first <= range.second);
		basic_fstrvec_check_range(range, 0);
		assert(offsets.size() >= 2);
		strpool.append(range.first, range.second);
		OffsetOp::inc(offsets.back(), range.second - range.first);
	}
	template<class ForwardIterator>
	void back_append(ForwardIterator first, ForwardIterator last) {
		assert(offsets.size() >= 2);
		size_t len = std::distance(first, last);
		basic_fstrvec_check_overflow(len);
		strpool.append(first, len);
		OffsetOp::inc(offsets.back(), last - first);
	}
	template<class String>
	typename void_<typename String::iterator>::type
	back_append(const String& str) {
		assert(offsets.size() >= 2);
		basic_fstrvec_check_overflow(str.size());
		strpool.append(str.data(), str.size());
		OffsetOp::inc(offsets.back(), str.size());
	}
	void back_append(const Char* str, size_t len) {
		basic_fstrvec_check_overflow(len);
		assert(offsets.size() >= 2);
		strpool.append(str, len);
		OffsetOp::inc(offsets.back(), len);
	}
	void back_append(Char ch) {
		assert(offsets.size() >= 2);
		basic_fstrvec_check_overflow(1);
		strpool.push_back(ch);
		OffsetOp::inc(offsets.back(), 1);
	}

	void pop_back() {
		assert(offsets.size() >= 2);
		offsets.pop_back();
		strpool.resize(OffsetOp::get(offsets.back()));
	}

	void resize(size_t n) {
		assert(n < offsets.size() && "basic_fstrvec::resize just allow shrink");
		if (n >= offsets.size()) {
		   	throw std::logic_error("basic_fstrvec::resize just allow shrink");
		}
		offsets.resize(n+1);
		strpool.resize(OffsetOp::get(offsets.back()));
	}

	std::pair<const Char*, const Char*> front() const {
	   	assert(offsets.size() >= 2);
		return (*this)[0];
   	}
	std::pair<const Char*, const Char*> back() const {
	   	assert(offsets.size() >= 2);
	   	return (*this)[offsets.size()-2];
	}

	size_t used_mem_size() const { return offsets.used_mem_size() + strpool.used_mem_size(); }
	size_t full_mem_size() const { return offsets.full_mem_size() + strpool.full_mem_size(); }
	size_t free_mem_size() const { return offsets.free_mem_size() + strpool.free_mem_size(); }

	size_t size() const { return offsets.size() - 1; }
	bool  empty() const { return offsets.size() < 2; }

	std::pair<Char*, Char*> operator[](size_t idx) {
		assert(idx < offsets.size()-1);
		Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx+0]);
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 <= off1);
		return std::pair<Char*, Char*>(base + off0, base + off1);
	}
	std::pair<const Char*, const Char*> operator[](size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx+0]);
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 <= off1);
		return std::pair<const Char*, const Char*>(base + off0, base + off1);
	}
	std::pair<Char*, Char*> at(size_t idx) {
		if (idx >= offsets.size()-1) {
			throw std::out_of_range("basic_fstrvec: at");
		}
		return (*this)[idx];
	}
	std::pair<const Char*, const Char*> at(size_t idx) const {
		if (idx >= offsets.size()-1) {
			throw std::out_of_range("basic_fstrvec: at");
		}
		return (*this)[idx];
	}
	std::basic_string<Char> str(size_t idx) const {
		assert(idx < offsets.size()-1);
		std::pair<const Char*, const Char*> x = (*this)[idx];
		return std::basic_string<Char>(x.first, x.second);
	}

	size_t slen(size_t idx) const {
		assert(idx < offsets.size()-1);
		size_t off0 = OffsetOp::get(offsets[idx+0]);
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 <= off1);
		return off1 - off0;
	}
	int ilen(size_t idx) const { return (int)slen(idx); }

	Char* beg_of(size_t idx) {
		assert(idx < offsets.size()-1);
		Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
		assert(off0 <= strpool.size());
		return base + off0;
	}
	const Char* beg_of(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
		assert(off0 <= strpool.size());
		return base + off0;
	}
	Char* end_of(size_t idx) {
		assert(idx < offsets.size()-1);
		Char* base = strpool.data();
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off1 <= strpool.size());
		return base + off1;
	}
	const Char* end_of(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off1 <= strpool.size());
		return base + off1;
	}

	const Char* c_str(size_t idx) const {
		assert(idx < offsets.size()-1);
		const Char* base = strpool.data();
		size_t off0 = OffsetOp::get(offsets[idx]);
	#if !defined(NDEBUG)
		size_t off1 = OffsetOp::get(offsets[idx+1]);
		assert(off0 < off1);
		assert(off1 <= strpool.size());
	//	assert(off1 >= 1); // off0 < off1 implies this assert
		assert('\0' == base[off1-1]);
	#endif
		return base + off0;
	}
	void shrink_to_fit() {
		strpool.shrink_to_fit();
		offsets.shrink_to_fit();
	}

	void swap(basic_fstrvec& y) {
		std::swap(static_cast<OffsetOp&>(*this), static_cast<OffsetOp&>(y));
		strpool.swap(y.strpool);
		offsets.swap(y.offsets);
	}

	void to_stdstrvec(std::vector<std::basic_string<Char> >* stdstrvec) const {
		assert(offsets.size() >= 1);
		stdstrvec->resize(offsets.size()-1);
		const Char* base = strpool.data();
		for(size_t i = 0; i < offsets.size()-1; ++i) {
			size_t off0 = OffsetOp::get(offsets[i+0]);
			size_t off1 = OffsetOp::get(offsets[i+1]);
			assert(off0 <= off1);
			assert(off1 <= strpool.size());
			(*stdstrvec)[i].assign(base + off0, base + off1);
		}
	}

	std::vector<std::basic_string<Char> > to_stdstrvec() const {
		std::vector<std::basic_string<Char> > res;
		to_stdstrvec(&res);
		return res;
	}

	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const basic_fstrvec& x) {
		dio << x.strpool;
		dio << x.offsets;
	}
	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, basic_fstrvec& x) {
		dio >> x.strpool;
		dio >> x.offsets;
	}
};

typedef basic_fstrvec<char, unsigned int > fstrvec;
typedef basic_fstrvec<char, unsigned long> fstrvecl;
typedef basic_fstrvec<char, unsigned long long> fstrvecll;

typedef basic_fstrvec<wchar_t, unsigned int > wfstrvec;
typedef basic_fstrvec<wchar_t, unsigned long> wfstrvecl;
typedef basic_fstrvec<wchar_t, unsigned long long> wfstrvecll;

typedef basic_fstrvec<uint16_t, unsigned int > fstrvec16;
typedef basic_fstrvec<uint16_t, unsigned long> fstrvec16l;
typedef basic_fstrvec<uint16_t, unsigned long long> fstrvec16ll;

} // namespace terark

namespace std {
	template<class Char, class Offset>
	void swap(terark::basic_fstrvec<Char, Offset>& x,
			  terark::basic_fstrvec<Char, Offset>& y)
   	{ x.swap(y); }
}


#endif // __terark_fstrvec_hpp__

