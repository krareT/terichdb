/*
 * rank_select_inline.hpp
 *
 *  Created on: Sep 1, 2015
 *      Author: leipeng
 */

#ifndef TERARK_RANK_SELECT_INLINE_HPP_
#define TERARK_RANK_SELECT_INLINE_HPP_

#ifdef __BMI2__
	#include "rank_select_inline_bmi2.hpp"
#else
	#include "rank_select_inline_slow.hpp"
#endif

namespace terark {

/////////////////////////////////////////////////////////////////////////////

inline size_t rank_select::
fast_rank1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
	size_t line_wordpos = (bitpos & ~(LineBits - 1)) / WordBits;
	size_t line_word_idxupp = bitpos / WordBits;
	size_t rank = rankCache[bitpos / LineBits];
	for (size_t i = line_wordpos; i < line_word_idxupp; ++i)
		rank += fast_popcount(bits[i]);
	if (bitpos % WordBits != 0)
		rank += fast_popcount_trail(bits[line_word_idxupp], bitpos % WordBits);
	return rank;
}
inline size_t rank_select::
fast_rank0(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
	return bitpos - fast_rank1(bits, rankCache, bitpos);
}
inline size_t rank_select::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const uint32_t* rankCache, size_t rank) {
	THROW_STD(invalid_argument, "not supported");
}
inline size_t rank_select::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const uint32_t* rankCache, size_t rank) {
	THROW_STD(invalid_argument, "not supported");
}

/////////////////////////////////////////////////////////////////////////////

inline size_t rank_select_se::
fast_rank1(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos) {
	RankCache rc = rankCache[bitpos / LineBits];
	return rc.lev1 + rc.lev2[(bitpos / 64) % 4] +
		fast_popcount_trail(
			((const uint64_t*)bits)[bitpos / 64], bitpos % 64);
}
inline size_t rank_select_se::
fast_rank0(const bm_uint_t* bits, const RankCache* rankCache, size_t bitpos) {
	return bitpos - fast_rank1(bits, rankCache, bitpos);
}

inline size_t rank_select_se::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const RankCache* rankCache, size_t Rank0) {
	size_t lo = sel0[Rank0 / LineBits];
	size_t hi = sel0[Rank0 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - rankCache[mid].lev1;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - rankCache[lo].lev1);
	size_t hit = LineBits * lo - rankCache[lo].lev1;
	size_t line_bitpos = (lo-1) * LineBits;
	RankCache rc = rankCache[lo-1];
	hit = LineBits * (lo-1) - rc.lev1;
	const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

	if (Rank0 < hit + 64*2 - rc.lev2[2]) {
		if (Rank0 < hit + 64*1 - rc.lev2[1]) { // rc.lev2[0] is always 0
			return line_bitpos + UintSelect1(~pBit64[0], Rank0 - hit);
		}
		return line_bitpos + 64*1 +
			UintSelect1(~pBit64[1], Rank0 - (hit + 64*1 - rc.lev2[1]));
	}
	if (Rank0 < hit + 64*3 - rc.lev2[3]) {
		return line_bitpos + 64*2 +
			UintSelect1(~pBit64[2], Rank0 - (hit + 64*2 - rc.lev2[2]));
	}
   	else {
		return line_bitpos + 64 * 3 +
			UintSelect1(~pBit64[3], Rank0 - (hit + 64*3 - rc.lev2[3]));
	}
}

inline size_t rank_select_se::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const RankCache* rankCache, size_t Rank1) {
	size_t lo = sel1[Rank1 / LineBits];
	size_t hi = sel1[Rank1 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = rankCache[mid].lev1;
		if (mid_val <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank1 < rankCache[lo].lev1);
	size_t hit = rankCache[lo].lev1;
	size_t line_bitpos = (lo-1) * LineBits;
	RankCache rc = rankCache[lo-1];
	hit = rc.lev1;
	const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

	if (Rank1 < hit + rc.lev2[2]) {
		if (Rank1 < hit + rc.lev2[1]) { // rc.lev2[0] is always 0
			return line_bitpos + UintSelect1(pBit64[0], Rank1 - hit);
		}
		return line_bitpos + 64*1 +
			 UintSelect1(pBit64[1], Rank1 - (hit + rc.lev2[1]));
	}
	if (Rank1 < hit + rc.lev2[3]) {
		return line_bitpos + 64*2 +
			 UintSelect1(pBit64[2], Rank1 - (hit + rc.lev2[2]));
	}
   	else {
		return line_bitpos + 64*3 +
			 UintSelect1(pBit64[3], Rank1 - (hit + rc.lev2[3]));
	}
}

/////////////////////////////////////////////////////////////////////////////

inline size_t rank_select_se_512::
fast_rank1(const bm_uint_t* bits, const RankCache512* rankCache, size_t bitpos) {
	const RankCache512& rc = rankCache[bitpos / 512];
	const uint64_t* pu64 = (const uint64_t*)bits;
	int tail = fast_popcount_trail(pu64[bitpos / 64], bitpos % 64);
	int k = bitpos % 512 / 64;
	return rc.base + tail + TERARK_GET_BITS_64(rc.rela, k, 9);
}
inline size_t rank_select_se_512::
fast_rank0(const bm_uint_t* bits, const RankCache512* rankCache, size_t bitpos) {
	return bitpos - fast_rank1(bits, rankCache, bitpos);
}

#if defined(__BMI2__) && TERARK_WORD_BITS == 64
	#define rank512(bm64, i) TERARK_GET_BITS_64(bm64, i, 9)
#else
	#define rank512(bm64, i) ((bm64 >> (i-1)*9) & 511)
#endif

inline size_t rank_select_se_512::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const RankCache512* rankCache, size_t Rank0) {
	size_t lo = sel0[Rank0 / LineBits];
	size_t hi = sel0[Rank0 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - rankCache[mid].base;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - rankCache[lo].base);
	size_t hit = LineBits * lo - rankCache[lo].base;
	size_t line_bitpos = (lo-1) * LineBits;
	uint64_t rcRela = rankCache[lo-1].rela;
	hit = LineBits * (lo-1) - rankCache[lo-1].base;
	const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

#define select0_nth64(n) line_bitpos + 64*n + \
	UintSelect1(~pBit64[n], Rank0 - (hit + 64*n - rank512(rcRela, n)))

	if (Rank0 < hit + 64*4 - rank512(rcRela, 4)) {
		if (Rank0 < hit + 64*2 - rank512(rcRela, 2))
			if (Rank0 < hit + 64*1 - rank512(rcRela, 1))
				return line_bitpos + UintSelect1(~pBit64[0], Rank0 - hit);
			else
				return select0_nth64(1);
		else
			if (Rank0 < hit + 64*3 - rank512(rcRela, 3))
				return select0_nth64(2);
			else
				return select0_nth64(3);
	} else {
		if (Rank0 < hit + 64*6 - rank512(rcRela, 6))
			if (Rank0 < hit + 64*5 - rank512(rcRela, 5))
				return select0_nth64(4);
			else
				return select0_nth64(5);
		else
			if (Rank0 < hit + 64*7 - rank512(rcRela, 7))
				return select0_nth64(6);
			else
				return select0_nth64(7);
	}
#undef select0_nth64
}

inline size_t rank_select_se_512::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const RankCache512* rankCache, size_t Rank1) {
	size_t lo = sel1[Rank1 / LineBits];
	size_t hi = sel1[Rank1 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = rankCache[mid].base;
		if (mid_val <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank1 < rankCache[lo].base);
	size_t hit = rankCache[lo].base;
	size_t line_bitpos = (lo-1) * LineBits;
	uint64_t rcRela = rankCache[lo-1].rela;
	hit = rankCache[lo-1].base;
	const uint64_t* pBit64 = (const uint64_t*)(bits + LineWords * (lo-1));

#define select1_nth64(n) line_bitpos + 64*n + \
	 UintSelect1(pBit64[n], Rank1 - (hit + rank512(rcRela, n)))

	if (Rank1 < hit + rank512(rcRela, 4)) {
		if (Rank1 < hit + rank512(rcRela, 2))
			if (Rank1 < hit + rank512(rcRela, 1))
				return line_bitpos + UintSelect1(pBit64[0], Rank1 - hit);
			else
				return select1_nth64(1);
		else
			if (Rank1 < hit + rank512(rcRela, 3))
				return select1_nth64(2);
			else
				return select1_nth64(3);
	} else {
		if (Rank1 < hit + rank512(rcRela, 6))
			if (Rank1 < hit + rank512(rcRela, 5))
				return select1_nth64(4);
			else
				return select1_nth64(5);
		else
			if (Rank1 < hit + rank512(rcRela, 7))
				return select1_nth64(6);
			else
				return select1_nth64(7);
	}
#undef select1_nth64
}

/////////////////////////////////////////////////////////////////////////////

inline size_t rank_select_il::
fast_rank1(const Line* lines, const Line*, size_t bitpos) {
	const Line& line = lines[bitpos / LineBits];
	return line.rlev1 + line.rlev2[bitpos%LineBits / 64]
		+ fast_popcount_trail(
			line.bit64[bitpos%LineBits / 64], bitpos % 64);
}
inline size_t rank_select_il::
fast_rank0(const Line* lines, const Line*, size_t bitpos) {
	return bitpos - fast_rank1(lines, lines, bitpos);
}

inline size_t rank_select_il::
fast_select0(const Line* lines, const uint32_t* sel0, const Line*, size_t Rank0) {
	size_t lo = sel0[Rank0 / LineBits];
	size_t hi = sel0[Rank0 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		size_t mid_val = LineBits * mid - lines[mid].rlev1;
		if (mid_val <= Rank0) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	assert(Rank0 < LineBits * lo - lines[lo].rlev1);
	size_t hit = LineBits * lo - lines[lo].rlev1;
	const Line& xx = lines[lo - 1];
	hit = LineBits * (lo - 1) - xx.rlev1;
	size_t index = (lo-1) * LineBits; // base bit index
	if (Rank0 < hit + 64*2 - xx.rlev2[2]) {
		if (Rank0 < hit + 64*1 - xx.rlev2[1]) { // xx.rlev2[0] is always 0
			return index + 64*0 + UintSelect1(~xx.bit64[0], Rank0 - hit);
		}
		return index + 64*1 + UintSelect1(
				~xx.bit64[1], Rank0 - (hit + 64*1 - xx.rlev2[1]));
	}
	if (Rank0 < hit + 64*3 - xx.rlev2[3]) {
		return index + 64*2 + UintSelect1(
				~xx.bit64[2], Rank0 - (hit + 64*2 - xx.rlev2[2]));
	}
   	else {
		return index + 64*3 + UintSelect1(
				~xx.bit64[3], Rank0 - (hit + 64*3 - xx.rlev2[3]));
	}
}

inline size_t rank_select_il::
fast_select1(const Line* lines, const uint32_t* sel1, const Line*, size_t Rank1) {
	size_t lo = sel1[Rank1 / LineBits];
	size_t hi = sel1[Rank1 / LineBits + 1];
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (lines[mid].rlev1 <= Rank1) // upper_bound
			lo = mid + 1;
		else
			hi = mid;
	}
	size_t hit = lines[lo].rlev1;
	assert(Rank1 < hit);
	const Line& xx = lines[lo - 1];
	hit = xx.rlev1;
	assert(Rank1 >= hit);
	size_t index = (lo-1) * LineBits; // base bit index
	if (Rank1 < hit + xx.rlev2[2]) {
		if (Rank1 < hit + xx.rlev2[1]) { // xx.rlev2[0] is always 0
			return index + UintSelect1(xx.bit64[0], Rank1 - hit);
		}
		return index + 64*1 + UintSelect1(
				 xx.bit64[1], Rank1 - (hit + xx.rlev2[1]));
	}
	if (Rank1 < hit + xx.rlev2[3]) {
		return index + 64*2 + UintSelect1(
				 xx.bit64[2], Rank1 - (hit + xx.rlev2[2]));
	}
   	else {
		return index + 64*3 + UintSelect1(
				 xx.bit64[3], Rank1 - (hit + xx.rlev2[3]));
	}
}

}



#endif /* TERARK_RANK_SELECT_INLINE_HPP_ */
