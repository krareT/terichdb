#ifndef __nark_util_sortable_strvec_hpp__
#define __nark_util_sortable_strvec_hpp__

#include <nark/valvec.hpp>
#include <nark/fstring.hpp>

namespace nark {

class NARK_DLL_EXPORT SortableStrVec {
public:
#pragma pack(push, 4)
#if NARK_WORD_BITS == 64
	struct SEntry {
		uint64_t offset : 40; //  1T
		uint64_t length : 24; // 16M
		uint32_t seq_id;
		size_t endpos() const { return offset + length; }
	};
	static const size_t MAX_STR_POOL = (size_t(1) << 40) - 1; // 1T-1
#else
	struct SEntry {
		uint32_t offset;
		uint32_t length;
		uint32_t seq_id;
		size_t endpos() const { return offset + length; }
	};
	static const size_t MAX_STR_POOL = (size_t(1) << 31) - 1; // 2G-1
#endif
	static const size_t MAX_STR_LEN = (size_t(1) << 24) - 1; // 16M-1

#pragma pack(pop)
	valvec<byte_t> m_strpool;
	valvec<SEntry> m_index;
	size_t m_real_str_size;
	size_t sync_real_str_size();
	double avg_size() const { return double(m_strpool.size()) / m_index.size(); }
	size_t mem_size() const { return sizeof(SEntry) * m_index.size() + m_strpool.size(); }
	size_t str_size() const { return m_strpool.size(); }
	size_t size() const { return m_index.size(); }
	fstring operator[](size_t idx) const;
	byte_t* mutable_nth_data(size_t idx) { return m_strpool.data() + m_index[idx].offset; }
	const
	byte_t* nth_data(size_t idx) const { return m_index[idx].offset + m_strpool.data(); }
	size_t  nth_size(size_t idx) const { return m_index[idx].length; }
	size_t  nth_offset(size_t idx) const { return m_index[idx].offset; }
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
	void build_subkeys(valvec<SEntry>& subkeys);
	void compact();
	void compress_strpool(int compressLevel);
	void make_ascending_offset();
	size_t lower_bound_by_offset(size_t offset) const;
	size_t upper_bound_by_offset(size_t offset) const;
	size_t lower_bound_at_pos(size_t lo, size_t hi, size_t pos) const;
	size_t upper_bound_at_pos(size_t lo, size_t hi, size_t pos) const;
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

class NARK_DLL_EXPORT DataStore {
protected:
	size_t m_numRecords;
	void risk_swap(DataStore& y);
public:
	static DataStore* load_from(fstring fpath);
	DataStore();
	virtual ~DataStore();
	size_t num_records() const { return m_numRecords; }
	virtual size_t mem_size() const = 0;
	virtual long long total_data_size() const = 0;
	virtual void get_record_append(size_t recID, valvec<byte_t>* recData) const = 0;
	void get_record(size_t recID, valvec<byte_t>* recData) const;
	valvec<byte_t> get_record(size_t recID) const;
};

} // namespace nark

#endif // __nark_util_sortable_strvec_hpp__
