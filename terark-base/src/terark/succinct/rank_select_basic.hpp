#ifndef __terark_rank_select_basic_hpp__
#define __terark_rank_select_basic_hpp__

#include <terark/bitmap.hpp>
#include <terark/util/throw.hpp>

#ifdef __BMI2__
#   include "rank_select_inline_bmi2.hpp"
#else
#   include "rank_select_inline_slow.hpp"
#endif

#if defined(__BMI2__) && TERARK_WORD_BITS == 64
#   define rank512(bm64, i) TERARK_GET_BITS_64(bm64, i, 9)
#else
#   define rank512(bm64, i) ((bm64 >> (i-1)*9) & 511)
#endif

#define rank_select_check_overflow(SIZE, OP, TYPE)                                  \
    do {                                                                            \
        if ((SIZE) OP size_t(std::numeric_limits<uint32_t>::max()))                 \
            THROW_STD(length_error, #TYPE" overflow , size = %zd", size_t(SIZE));   \
    } while (false)

namespace terark {

template<size_t iLineBits>
struct RankSelectConstants {
    static const size_t LineBits = iLineBits;
    static const size_t LineWords = LineBits / WordBits;

    static size_t BitsToLines(size_t nbits)
      { return (nbits + LineBits - 1) / LineBits; }
};

} // namespace terark

#endif // __terark_rank_select_basic_hpp__

