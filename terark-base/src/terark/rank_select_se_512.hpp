#ifndef __terark_rank_select_se_512_hpp__
#define __terark_rank_select_se_512_hpp__

#include "rank_select.hpp"
#if defined(__BMI2__)
#include <bmiintrin.h>
  #if defined(__GNUC__) && __GNUC__*1000 + __GNUC_MINOR__ < 4008 \
	&& !defined(__clang__) && !defined(__INTEL_COMPILER)
	/* Intel-specified, single-leading-underscore version of BEXTR */
	static __inline__ unsigned int __attribute__((__always_inline__))
	_bextr_u32(unsigned int __X, unsigned int __Y, unsigned int __Z) {
	  return __builtin_ia32_bextr_u32 (__X, ((__Y & 0xff) | ((__Z & 0xff) << 8)));
	}
	/* Intel-specified, single-leading-underscore version of BEXTR */
	static __inline__ unsigned long long __attribute__((__always_inline__))
	_bextr_u64(unsigned long long __X, unsigned int __Y, unsigned int __Z)
	{
	  return __builtin_ia32_bextr_u64 (__X, ((__Y & 0xff) | ((__Z & 0xff) << 8)));
	}
  #endif
#endif

namespace terark {

// rank1   use 2-level cache, time is O(1), 2 memory access
// select0 use 1-level cache, time is O(1+loglog(n))
// select1 use binary search, slower than select0
// rank_select_se, "_se" means "separated"
// rank index is separated from bits
class TERARK_DLL_EXPORT rank_select_se_512
	: public RankSelectConstants<512>, public febitvec {
public:
	rank_select_se_512();
	rank_select_se_512(size_t n, bool val = false);
	rank_select_se_512(size_t n, valvec_no_init);
	rank_select_se_512(size_t n, valvec_reserve);
	rank_select_se_512(const rank_select_se_512&);
	rank_select_se_512& operator=(const rank_select_se_512&);
#if defined(HSM_HAS_MOVE)
	rank_select_se_512(rank_select_se_512&& y) noexcept;
	rank_select_se_512& operator=(rank_select_se_512&& y) noexcept;
#endif
	~rank_select_se_512();
	void clear();
	void risk_release_ownership();
	void risk_mmap_from(unsigned char* base, size_t length);
	void shrink_to_fit();

	void swap(rank_select_se_512&);
	void build_cache(bool speed_select0, bool speed_select1);
	size_t mem_size() const;
	size_t rank1(size_t bitpos) const;
	size_t rank0(size_t bitpos) const;
	size_t select0(size_t id) const;
	size_t select1(size_t id) const;
	size_t max_rank1() const { return m_max_rank1; }
	size_t max_rank0() const { return m_max_rank0; }
protected:
	void nullize_cache();
#pragma pack(push,4)
	struct RankCache512 {
		uint32_t  base;
		uint64_t  rela;
		explicit RankCache512(uint32_t l1);
		operator size_t() const { return base; }
	};
#pragma pack(pop)
	RankCache512* m_rank_cache;
	uint32_t*  m_sel0_cache;
	uint32_t*  m_sel1_cache;
	size_t     m_max_rank0;
	size_t     m_max_rank1;
public:
	const RankCache512* get_rank_cache() const { return m_rank_cache; }
	const uint32_t* get_sel0_cache() const { return m_sel0_cache; }
	const uint32_t* get_sel1_cache() const { return m_sel1_cache; }
	static size_t fast_rank0(const bm_uint_t* bits, const RankCache512* rankCache, size_t bitpos);
	static size_t fast_rank1(const bm_uint_t* bits, const RankCache512* rankCache, size_t bitpos);
	static size_t fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const RankCache512* rankCache, size_t id);
	static size_t fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const RankCache512* rankCache, size_t id);

	size_t excess1(size_t bp) const { return 2*rank1(bp) - bp; }
	static size_t fast_excess1(const bm_uint_t* bits, const RankCache512* rankCache, size_t bitpos)
		{ return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos; }
};

} // namespace terark

#endif // __terark_rank_select_se_512_hpp__

