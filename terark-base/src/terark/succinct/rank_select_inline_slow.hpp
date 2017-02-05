/*
 * rank_select_inline.hpp
 *
 *  Created on: Sep 1, 2015
 *      Author: leipeng
 */

#ifndef TERARK_RANK_SELECT_INLINE_SLOW_HPP_
#define TERARK_RANK_SELECT_INLINE_SLOW_HPP_

#include <terark/util/throw.hpp>

namespace terark {

///@param r rank range is [0, 64), more exctly: [0, popcnt(x))
///@returns [0, popcnt(x)), the bitpos of r'th 1
inline unsigned UintSelect1(uint64_t x, unsigned r) {
    assert(0 != x);
#if defined(NDEBUG)
    if (terark_unlikely(r >= (unsigned)fast_popcount(x))) {
        fprintf(stderr
            , "%s:%d: assert(r < popcnt(x)) fail: r=%u, popcnt(x)=%d"
            , __FILE__, __LINE__
            , r, (int)fast_popcount(x));
        abort();
    }
#else
    unsigned nPopCnt = (unsigned)fast_popcount(x);
    assert(r < nPopCnt);
#endif

    unsigned s, t;
    uint64_t a, b, c, d;

    a =  x - ((x >> 1) & 0x5555555555555555);
    b = (a & 0x3333333333333333) + ((a >> 2) & 0x3333333333333333);
    c = (b + (b >> 4)) & 0x0F0F0F0F0F0F0F0F;
    d = (c + (c >> 8)) & 0x00FF00FF00FF00FF;

    s = 0;
    t = ((d >> 16) + d) & 255; // popcnt(lo32)
    if (r >= t) {s += 32; r -= t;}

    t = (d >> s) & 0xFF;
    if (r >= t) {s += 16; r -= t;}

    t = (c >> s) & 0xF;
    if (r >= t) {s += 8; r -= t;}

    t = (b >> s) & 0x7;
    if (r >= t) {s += 4; r -= t;}

    t = (a >> s) & 0x3;
    if (r >= t) {s += 2; r -= t;}

    t = (x >> s) & 0x1;
    if (r >= t) s++;

    return s;
}

// 'k' may be 0
#define TERARK_GET_BITS_64(u64,k,width) ( k ? (u64 >> (k-1)*width) & ((1<<width)-1) : 0 )

} // namespace terark



#endif /* TERARK_RANK_SELECT_INLINE_SLOW_HPP_ */
