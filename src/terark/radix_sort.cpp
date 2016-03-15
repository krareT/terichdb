#include "radix_sort.hpp"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static const ptrdiff_t nil = -1;

namespace terark {

namespace { // anonymous namespace

struct lnode
{
	ptrdiff_t head;
	ptrdiff_t tail;

	lnode() : head(-1), tail(-1) {}
};

class Impl
{
	std::vector<radix_sorter::radix_sort_elem> ov;
	const unsigned char* codetab;
	const radix_sorter::codefun_t codefun;
	const ptrdiff_t max_key_len;
	const int ValueSize;
	const int CharSize;
	const int radix;
	ptrdiff_t head_link;

	std::vector<lnode> keylink;
	std::vector<lnode> lenlink;
	std::vector<ptrdiff_t> lencnt;

public:
	Impl(std::vector<radix_sorter::radix_sort_elem>& ov,
		 const unsigned char* codetab,
		 const radix_sorter::codefun_t codefun,
		 const ptrdiff_t max_key_len,
		 const int ValueSize,
		 const int CharSize,
		 const int radix)
		: codetab(codetab)
		, codefun(codefun)
		, max_key_len(max_key_len)
		, ValueSize(ValueSize)
		, CharSize(CharSize)
		, radix(radix)
		, head_link(-1)
		, keylink(radix)
		, lenlink(max_key_len + 1)
		, lencnt(max_key_len + 1)
	{
		this->ov.swap(ov);
	}

/**
 * 实际上, 长度可以看成最低有效 key, 使用 sort_at 算法, 但细节实现不同
 */
template<int CCharSize>
void len_distribute()
{
#if defined(_DEBUG) || !defined(NDEBUG)
	for (ptrdiff_t i=0,n=lenlink.size(); i < n; ++i) {
		assert(lenlink[i].head == nil);
		assert(lenlink[i].tail == nil);
	}
#endif
	ptrdiff_t p = 0;
	ov.back().link = nil;
	do {
		// only this line different with sort_at
		// addr is keylen, and 'keylink' in sort_at and sort, is 'lenlink' here
		ptrdiff_t k = ov[p].addr / CCharSize; // will be optimized by shift
		assert(ov[p].addr % CCharSize == 0);

		if (terark_unlikely(nil == lenlink[k].head))
			lenlink[k].head = p;
		else
			ov[lenlink[k].tail].link = p;
		lenlink[k].tail = p;
		lencnt[k]++;
		p = ov[p].link;

	} while (nil != p);
}

ptrdiff_t sort_keylen()
{
	switch (CharSize)
	{
	default:
		fprintf(stderr, "%s:%d: fatal: CharSize=%d\n", __FILE__, __LINE__, CharSize);
		abort();
		break;
	// 常数的 CharSize, 通过编译器优化除法, 速度更快
	case 1:	len_distribute<1>(); break;
	case 2:	len_distribute<2>(); break;
	case 4:	len_distribute<4>(); break;
	}
	ptrdiff_t first = collect(lenlink);
#if 0 // #if defined(_DEBUG) || !defined(NDEBUG)
	printw(first, -1);
#endif
	return first;
}

ptrdiff_t collect(const std::vector<lnode>& vlink)
{
	ptrdiff_t j = 0, N = vlink.size(), first, last;
	while (nil == vlink[j].head) ++j;
	first = vlink[j].head;
	last  = vlink[j].tail;
	for (++j; j < N; ++j)
	{
		if (nil != vlink[j].head) {
			ov[last].link = vlink[j].head;
			last = vlink[j].tail;
		}
	}
	ov[last].link = nil;
	return first;
}

template<class CharType>
void distribute(ptrdiff_t first, ptrdiff_t kpos, ptrdiff_t cnt)
{
	ptrdiff_t p = first;
#if defined(_DEBUG) || !defined(NDEBUG)
	ptrdiff_t cnt2 = 0;
	assert(nil != first);
#endif
	for (ptrdiff_t i=0; i < radix; ++i) keylink[i].head = nil;

	if (codefun) {
		do {
			const CharType* skey = (const CharType*)ov[p].key;
			ptrdiff_t k = codefun(skey[kpos], codetab);
			assert(k < radix);
			if (terark_unlikely(nil == keylink[k].head))
				keylink[k].head = p;
			else
				ov[keylink[k].tail].link = p;
			keylink[k].tail = p;
			p = ov[p].link;
	#if defined(_DEBUG) || !defined(NDEBUG)
			cnt2++;
	#endif
		} while (nil != p);
	}
	else if (codetab) {
		const CharType* tab = (const CharType*)codetab;
		do {
			const CharType* skey = (const CharType*)ov[p].key;
			ptrdiff_t k = tab[skey[kpos]];
			assert(k < radix);
			if (terark_unlikely(nil == keylink[k].head))
				keylink[k].head = p;
			else
				ov[keylink[k].tail].link = p;
			keylink[k].tail = p;
			p = ov[p].link;
	#if defined(_DEBUG) || !defined(NDEBUG)
			cnt2++;
	#endif
		} while (nil != p);
	}
	else {
		// no codetab
		do {
			const CharType* skey = (const CharType*)ov[p].key;
			ptrdiff_t k = skey[kpos];
			assert(k < radix);
			if (terark_unlikely(nil == keylink[k].head))
				keylink[k].head = p;
			else
				ov[keylink[k].tail].link = p;
			keylink[k].tail = p;
			p = ov[p].link;
	#if defined(_DEBUG) || !defined(NDEBUG)
			cnt2++;
	#endif
		} while (nil != p);
	}//if
#if defined(_DEBUG) || !defined(NDEBUG)
	assert(cnt2 == cnt);
#endif
}

ptrdiff_t sort_at(ptrdiff_t first, ptrdiff_t kpos, ptrdiff_t cnt)
{
	assert(nil != first);
	switch (CharSize)
	{
	default:
		assert(0);
		break;
	case 1: distribute<unsigned char >(first, kpos, cnt); break;
	case 2: distribute<unsigned short>(first, kpos, cnt); break;
	case 4: distribute<unsigned int  >(first, kpos, cnt); break;
	}
	ptrdiff_t new_first = collect(keylink);
#if 0 // #if defined(_DEBUG) || !defined(NDEBUG)
	printw(new_first, kpos);
	printf("sort_at: first[old=%ld, new=%ld], kpos=%ld, cnt=%ld\n"
		  , (long)first, (long)new_first, (long)kpos, (long)cnt);
#endif
	return new_first;
}

void sort()
{
	if (terark_unlikely(0 == max_key_len))
		// not need sort
		return;

	assert(max_key_len >= 1);

	ptrdiff_t initial_first = sort_keylen();
	ptrdiff_t cnt = 0;
	for (ptrdiff_t klen = max_key_len; ; --klen)
	{
		assert(klen >= 1);
		for (ptrdiff_t first = lenlink[klen].head;; --klen)
		{
			assert(nil != first);
			cnt += lencnt[klen];
			first = sort_at(first, klen-1, cnt);
			assert(nil != first);
			if (terark_unlikely(1 == klen)) {
				if (nil == lenlink[0].head) {
					// has no empty key
					this->head_link = first;
				} else {
					// has empty keys, empty keys are the mins
					// link the empty keys to the min non-empty keys.
					ov[lenlink[0].tail].link = first;
					this->head_link = initial_first;
				}
				// completed
				goto Done;
			} else if (nil != lenlink[klen-1].head) {
				// link prev chunk to current sorted chunk
				ov[lenlink[klen-1].tail].link = first;
				break;
			} else {
				// continue loop
			}
		}//for
	}//for
Done:
	rearrange();
}

// use memcpy and malloc for temp object, so slow
void rearrange_slow(int ValueSize)
{
	const ptrdiff_t N = ov.size();
	for (ptrdiff_t i = head_link, j = 0; nil != i; i = ov[i].link, ++j)
		ov[j].addr = i;
	unsigned char* temp = (unsigned char*)malloc(ValueSize);
	for (ptrdiff_t i=0; i < N; ++i)
	{
		if (terark_likely(ov[i].addr != i))
		{
			ptrdiff_t j = i, k;
			memcpy(temp, ov[i].obj, ValueSize);
			do {
				k = ov[j].addr;
				memcpy(ov[j].obj, ov[k].obj, ValueSize);
				ov[j].addr = j;
				j = k;
			} while (ov[k].addr != i);

			memcpy(ov[j].obj, temp, ValueSize);
			ov[j].addr = j;
		}
	}
	free(temp);
}

// use assign on raw memory for temp object, so fast
template<class Value>
void rearrange_fast(Value* /*dummy*/)
{
	const ptrdiff_t N = ov.size();
	ptrdiff_t i, j;
	assert(sizeof(Value) == ValueSize);
#if 0 // #if defined(_DEBUG) || !defined(NDEBUG)
	printf("%s, ValueSize=%d, head_link=%ld\n", "rearrange_fast", sizeof(Value), (long)head_link);
#endif
	for (i = head_link, j = 0; nil != i; i = ov[i].link, ++j)
		ov[j].addr = i;
#if 0 // #if defined(_DEBUG) || !defined(NDEBUG)
	printf("gen addr completed: j=%ld, N=%ld\n", (long)j, (long)N);
	assert(N == j);
	for (i = 0; i < N; ++i)
	{
		const char* keystr = (const char*)ov[ov[i].addr].key;
		int keylen = strlen(keystr);
		printf(">>%-30.*s %2d\n", keylen, keystr, keylen);
	}
#endif
	for (i=0; i < N; ++i)
	{
		if (terark_unlikely(ov[i].addr != i))
		{
			const Value temp = *(const Value*)ov[i].obj;
			ptrdiff_t k;
			j = i;
			do {
				k = ov[j].addr;
				*(Value*)ov[j].obj = *(const Value*)ov[k].obj;
				ov[j].addr = j;
				j = k;
			} while (ov[k].addr != i);

			*(Value*)ov[j].obj = temp;
			ov[j].addr = j;
		}
	}
}

template<int Size>
struct block
{
	unsigned char data[Size];
};

void rearrange()
{
	switch (ValueSize)
	{
	default: rearrange_slow(ValueSize); break;
#define CASE_OF_N_PTR(N) case sizeof(void*)*N: rearrange_fast((block<sizeof(void*)*N>*)0); break;
	CASE_OF_N_PTR(1);
	CASE_OF_N_PTR(2);
	CASE_OF_N_PTR(3);
	CASE_OF_N_PTR(4);
	CASE_OF_N_PTR(5);
	CASE_OF_N_PTR(6);
	CASE_OF_N_PTR(7);
	CASE_OF_N_PTR(8);
	}
}

void printw(ptrdiff_t first, ptrdiff_t kpos)
{
#if defined(_DEBUG) || !defined(NDEBUG)
	if (CharSize > 1) return;

	printf("++++first=%ld, kpos=%ld, ov.size=%ld +++++++++++++++++++++++++++++++++\n", (long)first, (long)kpos, (long)ov.size());
	if (kpos >= 0) {
		for (ptrdiff_t i = 0; i < kpos; ++i) fputc(' ', stdout);
		printf("+\n");
	}
	for (ptrdiff_t i = first; nil != i; i = ov[i].link)
	{
		int keylen = (int)strlen((const char*)ov[i].key);
		printf("%-30.*s %2d\n", keylen, ov[i].key, keylen);
	}
	if (kpos >= 0) {
		for (ptrdiff_t i = 0; i < kpos; ++i) fputc(' ', stdout);
		printf("+\n");
	}
	printf("----first=%ld, kpos=%ld, ov.size=%ld --------------------------------------\n", (long)first, (long)kpos, (long)ov.size());
#endif
}

}; // class Impl

} // anonymous namespace

void radix_sorter::sort()
{
	Impl x(ov, codetab, codefun, max_key_len, ValueSize, CharSize, radix);
	x.sort();
}

const unsigned char* radix_sorter::get_case_table()
{
	static unsigned char casetab[256] =
	{
		  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
		 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
		 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
		 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
		 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
		 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
		 96, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
		 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,123,124,125,126,127,
		128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
		144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,
		160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,
		176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,
		192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,
		208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,
		224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,
		240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255
	};
	return casetab;
}

namespace radix_sort_tpl_impl{
	size_t collect(unsigned* next_p, size_t next_n,
				 RadixSortMeta* radix_p, size_t radix_n) {
		size_t j = 0;
		while (nil == radix_p[j].head) ++j;
		assert(j < radix_n);
		size_t head = radix_p[j].head;
		size_t tail = radix_p[j].tail;
		for (++j; j < radix_n; ++j) {
			if (nil != radix_p[j].head) {
				next_p[tail] = radix_p[j].head;
				tail = radix_p[j].tail;
			}
		}
		next_p[tail] = nil;
		return head;
	}
}

} // namespace terark

