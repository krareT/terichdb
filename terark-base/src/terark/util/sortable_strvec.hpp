#ifndef __terark_util_sortable_strvec_hpp__
#define __terark_util_sortable_strvec_hpp__

#include <terark/valvec.hpp>
#include <terark/fstring.hpp>
#include <terark/int_vector.hpp>

namespace terark {

class TERARK_DLL_EXPORT SortableStrVec {
public:
#pragma pack(push, 4)
#if TERARK_WORD_BITS == 64
	struct OffsetLength {
		uint64_t offset : 40; //  1T
		uint64_t length : 24; // 16M, but limit to 1M
	};
  #if defined(_MSC_VER)
    struct SEntry {
        uint64_t offset : 40; //  1T
        uint64_t length : 24; // 16M, but limit to 1M
        uint32_t seq_id;
        size_t endpos() const { return offset + length; }
    };
    static const size_t MAX_STR_NUM = UINT32_MAX;
  #else
	struct SEntry {
		uint64_t offset : 40; //  1T
		uint64_t length : 20; //  1M
		uint64_t seq_id : 36; // 64G, avg 16 byte reaches offset 1T
		size_t endpos() const { return offset + length; }
	};
    static const size_t MAX_STR_NUM = (size_t(1) << 36) - 1; // 64G-1
#endif
	static const size_t MAX_STR_POOL = (size_t(1) << 40) - 1; // 1T-1
#else
	struct OffsetLength {
		uint32_t offset;
		uint32_t length;
	};
	struct SEntry {
		uint32_t offset;
		uint32_t length;
		uint32_t seq_id;
		size_t endpos() const { return offset + length; }
	};
	static const size_t MAX_STR_POOL = (size_t(1) << 31) - 1; // 2G-1
#endif
	static const size_t MAX_STR_LEN = (size_t(1) << 20) - 1; // 1M-1

#pragma pack(pop)
	valvec<byte_t> m_strpool;
	valvec<SEntry> m_index;
	size_t m_real_str_size;
	size_t sync_real_str_size();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();
	double avg_size() const { return double(m_strpool.size()) / m_index.size(); }
    size_t mem_cap () const { return m_index.full_mem_size() + m_strpool.full_mem_size(); }
	size_t mem_size() const { return sizeof(SEntry) * m_index.size() + m_strpool.size(); }
	size_t str_size() const { return m_strpool.size(); }
	size_t size() const { return m_index.size(); }
	fstring operator[](size_t idx) const;
	byte_t* mutable_nth_data(size_t idx) { return m_strpool.data() + m_index[idx].offset; }
	const
	byte_t* nth_data(size_t idx) const { return m_index[idx].offset + m_strpool.data(); }
    size_t  nth_size(size_t idx) const { return m_index[idx].length; }
    size_t  nth_offset(size_t idx) const { return m_index[idx].offset; }
    size_t  nth_seq_id(size_t idx) const { return m_index[idx].seq_id; }
    size_t  nth_endpos(size_t idx) const { return m_index[idx].endpos(); }
    fstring back() const { return (*this)[m_index.size()-1]; }
	void swap(SortableStrVec&);
	void push_back(fstring str);
	void pop_back();
	void back_append(fstring str);
	void back_shrink(size_t nShrink);
	void back_grow_no_init(size_t nGrow);
	void reverse_keys();
	void sort();
	void sort_by_offset();
	void sort_by_seq_id();
	void clear();
	void build_subkeys();
	void build_subkeys(valvec<SEntry>& subkeys);
	void compact();
	void compress_strpool(int compressLevel);
	void make_ascending_offset();
	size_t lower_bound_by_offset(size_t offset) const;
	size_t upper_bound_by_offset(size_t offset) const;
	size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
	size_t lower_bound(fstring) const;
	size_t upper_bound(fstring) const;
private:
	void compress_strpool_level_1();
	void compress_strpool_level_2();
	void compress_strpool_level_3();

	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, SortableStrVec& x) {
		uint64_t indexSize, poolSize;
		dio >> indexSize;
		dio >> poolSize;
		x.m_index.resize_no_init(size_t(indexSize));
		x.m_strpool.resize_no_init(size_t(poolSize));
		dio.ensureRead(x.m_index.data(), x.m_index.used_mem_size());
		dio.ensureRead(x.m_strpool.data(), x.m_strpool.used_mem_size());
	}
	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const SortableStrVec& x) {
		dio << uint64_t(x.m_index.size());
		dio << uint64_t(x.m_strpool.size());
		dio.ensureWrite(x.m_index.data(), x.m_index.used_mem_size());
		dio.ensureWrite(x.m_strpool.data(), x.m_strpool.used_mem_size());
	}
};

class TERARK_DLL_EXPORT FixedLenStrVec {
public:
    size_t m_fixlen;
    size_t m_size;
    valvec<byte_t> m_strpool;

    explicit FixedLenStrVec(size_t fixlen = 0);
    ~FixedLenStrVec();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();

    double avg_size() const { return m_fixlen; }
    size_t mem_cap () const { return m_strpool.capacity(); }
    size_t mem_size() const { return m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    size_t size() const { return m_size; }
    fstring operator[](size_t idx) const {
        assert(idx < m_size);
        assert(m_fixlen * m_size == m_strpool.size());
        size_t fixlen = m_fixlen;
        size_t offset = fixlen * idx;
        return fstring(m_strpool.data() + offset, fixlen);
    }
    byte_t* mutable_nth_data(size_t idx) { return m_strpool.data() + m_fixlen * idx; }
    const
    byte_t* nth_data(size_t idx) const { return m_strpool.data() + m_fixlen * idx; }
    size_t  nth_size(size_t idx) const { return m_fixlen; }
    size_t  nth_offset(size_t idx) const { return m_fixlen * idx; }
    size_t  nth_seq_id(size_t idx) const { return idx; }
    size_t  nth_endpos(size_t idx) const { return m_fixlen * (idx + 1); }
    fstring back() const { return (*this)[m_size-1]; }
    void swap(FixedLenStrVec&);
    void push_back(fstring str);
    void pop_back();
    void reverse_keys();
    void sort();
    void clear();
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring) const;
    size_t upper_bound(fstring) const;
};

class TERARK_DLL_EXPORT SortedStrVec {
public:
    UintVecMin0    m_offsets;
    valvec<byte_t> m_strpool;

    explicit SortedStrVec();
    ~SortedStrVec();
    void reserve(size_t strNum, size_t maxStrPool);
    void finish() { shrink_to_fit(); }
    void shrink_to_fit();

    double avg_size() const { return m_strpool.size() / double(m_offsets.size()-1); }
    size_t mem_cap () const { return m_offsets.mem_size() + m_strpool.capacity(); }
    size_t mem_size() const { return m_offsets.mem_size() + m_strpool.size(); }
    size_t str_size() const { return m_strpool.size(); }
    size_t size() const {
        assert(m_offsets.size() >= 1);
        return m_offsets.size()-1;
    }
    fstring operator[](size_t idx) const {
        assert(idx + 1 < m_offsets.size());
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return fstring(m_strpool.data() + BegEnd[0], BegEnd[1] - BegEnd[0]);
    }
    byte_t* mutable_nth_data(size_t idx) { return m_strpool.data() + m_offsets[idx]; }
    const
    byte_t* nth_data(size_t idx) const { return m_strpool.data() + m_offsets[idx]; }
    size_t  nth_size(size_t idx) const {
        size_t BegEnd[2];  m_offsets.get2(idx, BegEnd);
        return BegEnd[1] - BegEnd[0];
    }
    size_t  nth_offset(size_t idx) const { return m_offsets[idx]; }
    size_t  nth_seq_id(size_t idx) const { return idx; }
    size_t  nth_endpos(size_t idx) const { return m_offsets[idx+1]; }
    fstring back() const { return (*this)[m_offsets.size()-1]; }
    void swap(SortedStrVec&);
    void push_back(fstring str);
    void pop_back();
    void back_append(fstring str);
    void back_shrink(size_t nShrink);
    void back_grow_no_init(size_t nGrow);
    void reverse_keys();
    void sort();
    void clear();
    size_t lower_bound_by_offset(size_t offset) const;
    size_t upper_bound_by_offset(size_t offset) const;
    size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const;
    size_t lower_bound(fstring) const;
    size_t upper_bound(fstring) const;
};

} // namespace terark

#endif // __terark_util_sortable_strvec_hpp__
