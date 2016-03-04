#ifndef __nark_radix_sort__
#define __nark_radix_sort__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4127)
#endif

#include <stddef.h>
#include <assert.h>
#include <vector>
#include "config.hpp"
#include "valvec.hpp"
#include "util/autofree.hpp"

namespace nark {

	class NARK_DLL_EXPORT radix_sorter
	{
	public:
		typedef int (*codefun_t)(int ch_, const void* codetab_as_locale);

		struct radix_sort_elem
		{
			void* obj;
			const unsigned char* key;
			ptrdiff_t link;
			ptrdiff_t addr;
		};
	private:
		std::vector<radix_sort_elem> ov;
		const unsigned char* codetab;
		codefun_t codefun;
		ptrdiff_t max_key_len;
		const int radix;
		const int ValueSize;
		const int CharSize;

	public:
		/**
		 * @param first sequence start
		 * @param last  sequence end
		 * @param getpn requires:
		 * 			integral getpn.size(*first), return size in bytes!
		 * 			const unsigned char* getpn.data(*first)
		 * @param codetab if NOT NULL, sorted by lexical codetab[getpn.data(..)[*]]
		 * @param radix	  radix of alpha set
		 * @param codefun if NOT NULL, code = codefun(getpn.data(..)[*], codetab_as_locale)
		 */
		template<class RandIter, class GetPn, class CharType>
		radix_sorter(RandIter first, RandIter last, GetPn getpn, const CharType* codetab
					, int radix = 256 // don't default large (1 << sizeof(CharType)*8)
					, codefun_t codefun = NULL
					)
	       		: ov(std::distance(first, last))
				, codetab((const unsigned char*)codetab)
				, radix(radix)
				, ValueSize(sizeof(typename std::iterator_traits<RandIter>::value_type))
				, CharSize(sizeof(CharType))
		{
			// when sizeof(CharType) == 4, must provide codetab
			// assert(sizeof(CharType) == 4 && codetab != NULL || sizeof(CharType) <= 2);
			assert(sizeof(CharType) == 4 || sizeof(CharType) <= 2);
			ptrdiff_t j = 0;
			max_key_len = 0;
			for (RandIter iter = first; iter != last; ++iter, ++j)
			{
				ptrdiff_t keylen = getpn.size(*iter);
				radix_sort_elem& e = ov[j];
				e.obj = &*iter;
				e.key = getpn.data(*iter);
				e.link = j + 1; // link to next
				e.addr = keylen;
				assert(keylen % CharSize == 0);
				if (max_key_len < keylen)
					max_key_len = keylen;
			}
			this->codefun = codefun;
			max_key_len /= sizeof(CharType);
		}

		void sort();

		static const unsigned char* get_case_table();
	}; // radix_sorter

	// 先排序最长的 Key, 后排序较短的 Key
	//
	template<class RandIter, class GetPn>
	void radix_sort(RandIter first, RandIter last, GetPn getpn,
		const unsigned char* codetab = NULL,
		int radix = 256,
		radix_sorter::codefun_t codefun = NULL)
	{
		radix_sorter sorter(first, last, getpn, codetab, radix, codefun);
		sorter.sort();
	}

	namespace radix_sort_tpl_impl {
		static const unsigned nil = unsigned(-1);
		struct RadixSortMeta {
			unsigned head;
			unsigned tail;
			RadixSortMeta() : head(nil), tail(nil) {}
		};
		NARK_DLL_EXPORT
		size_t collect(unsigned* next_p, size_t next_n,
					   RadixSortMeta* radix_p, size_t radix_n);
	}

	/// uint getChar(const Value&, size_t charIdx)
	/// uint getSize(const Value&)
	template<class Value, class GetChar, class GetSize>
	void radix_sort_tpl(Value* vec, size_t vlen,
						GetChar getChar,
						GetSize getSize,
						size_t radix = 256)
	{
		using namespace radix_sort_tpl_impl;
		if (0 == vlen)
			return;
		AutoFree<unsigned> next(vlen);
		size_t maxkeylen = 0;
		for(size_t i = 0; i < vlen; ++i) {
			size_t keylen = getSize(vec[i]);
			next.p[i] = unsigned(i + 1);
			maxkeylen = std::max(maxkeylen, keylen);
		}
		if (0 == maxkeylen) {
			assert(0); // may be invalid call
			return;
		}
		next.p[vlen - 1] = nil;

		size_t all_head = nil;
		{
			AutoFree<RadixSortMeta> lenlink(maxkeylen + 1, RadixSortMeta());
			AutoFree<RadixSortMeta> keylink(radix);

			// 1. length distribute
			for(size_t i = 0; i < vlen; ++i) {
				size_t keylen = getSize(vec[i]);
				if (nark_unlikely(nil == lenlink.p[keylen].head)) {
					lenlink.p[keylen].head = unsigned(i);
				} else {
					next.p[lenlink.p[keylen].tail] = unsigned(i);
				}
				lenlink.p[keylen].tail = unsigned(i);
			}

			// 2. length collect
			size_t len_head = collect(next.p, vlen, lenlink.p, maxkeylen + 1);

			// 3. radix sort char data
			size_t head = nil;
			for(size_t keylen = maxkeylen; keylen > 0; --keylen) {
				assert(keylen > 0);
				head = lenlink.p[keylen].head;

				// 3.1 distribute
				std::fill_n(keylink.p, radix, RadixSortMeta());
				size_t p = head;
				do {
					size_t ch = getChar(vec[p], keylen - 1);
					assert(ch < radix);
					if (nark_unlikely(nil == keylink.p[ch].head)) {
						keylink.p[ch].head = unsigned(p);
					} else {
						next.p[keylink.p[ch].tail] = unsigned(p);
					}
					keylink[ch].tail = unsigned(p);
					p = next.p[p];
				} while (nil != p);

				// 3.2 collect
				head = collect(next.p, vlen, keylink.p, radix);
				assert(head < vlen);

				// link prev chunk to current sorted chunk
				for (; keylen > 1; --keylen) {
					if (nil != lenlink[keylen - 1].head) {
						next[lenlink[keylen - 1].tail] = unsigned(head);
						break;
					}
				}
			}
			if (nil == lenlink.p[0].head) { // has no empty keys
				all_head = head;
			} else {
				// has empty keys, empty keys are the mins
				// link the empty keys to the min non-empty keys.
				next.p[lenlink.p[0].tail] = unsigned(head);
				all_head = len_head;
			}
		}
		// ReArrange:
		{
			AutoFree<unsigned> addr(vlen);
			{
				size_t i = all_head, j = 0;
				for (; nil != i; i = next[i], ++j) addr[j] = unsigned(i);
				assert(j == vlen);
				next.free(); // early free
			}
			using std::swap;
			for(size_t i = 0; i < vlen; ++i) {
				if (nark_unlikely(addr[i] != i)) {
				//	Value temp(std::move(vec[i]));
					size_t j = i, k;
					do {
						k = addr[j];
					//	vec[j] = vec[k];
						swap(vec[j], vec[k]);
						addr[j] = unsigned(j);
						j = k;
					} while (addr[k] != i);
				//	vec[j] = temp;
					addr[j] = unsigned(j);
				}
			}
		}
	}

} // namespace nark

#endif

