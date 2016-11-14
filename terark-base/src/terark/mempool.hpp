#ifndef __penglei_mempool_hpp__
#define __penglei_mempool_hpp__

#include "valvec.hpp"
#include <boost/integer/static_log2.hpp>

namespace terark {

/// mempool which alloc mem block identified by
/// integer offset(relative address), not pointers(absolute address)
/// integer offset could be 32bit even in 64bit hardware.
///
/// the returned offset is aligned to align_size, this allows 32bit
/// integer could address up to 4G*align_size memory
///
/// when memory exhausted, valvec can realloc memory without memcpy
/// @see valvec
template<int AlignSize>
class MemPool : private valvec<unsigned char> {
	BOOST_STATIC_ASSERT((AlignSize & (AlignSize-1)) == 0);
	BOOST_STATIC_ASSERT(AlignSize >= 4);
    typedef valvec<unsigned char> mem;
    typedef typename boost::mpl::if_c<AlignSize == 4, uint32_t, uint64_t>::type link_size_t;
    
    static const size_t huge_list_tail = ~size_t(0);
    static const link_size_t free_list_tail = ~link_size_t(0);
    static const size_t offset_shift = AlignSize == 4 ? boost::static_log2<AlignSize>::value : 0;

    struct link_t { // just for readable
        link_size_t next;
        explicit link_t(link_size_t l) : next(l) {}
    };
	struct HugeLink {
		size_t next;
		size_t size;
	};
    link_t* flarr; // free list array
    size_t  fllen; // free list length
	size_t  nFree; // number of free bytes
	size_t  hugelist;

	void destroy_and_clean() {
		mem::clear();
		if (flarr) {
			free(flarr);
			flarr = NULL;
		}
		fllen = 0;
		nFree = 0;
		hugelist = list_tail;
	}

public:
	      mem& get_data_byte_vec()       { return *this; }
	const mem& get_data_byte_vec() const { return *this; }

    static size_t align_to(size_t len) {
        return (len + align_size - 1) & ~size_t(align_size - 1);
    }
    enum { align_size = AlignSize };

    explicit MemPool(size_t maxBlockSize) {
        assert(maxBlockSize >= align_size);
        assert(maxBlockSize >= sizeof(HugeLink));
        maxBlockSize = align_to(maxBlockSize);
        fllen = maxBlockSize / align_size;
        flarr = (link_t*)malloc(sizeof(link_t) * fllen);
        if (NULL == flarr) {
            throw std::bad_alloc();
        }
		nFree = 0;
        std::uninitialized_fill_n(flarr, fllen, link_t(free_list_tail));
		hugelist = huge_list_tail;
    }
    MemPool(const MemPool& y) : mem(y) {
        fllen = y.fllen;
        flarr = (link_t*)malloc(sizeof(link_t) * fllen);
        if (NULL == flarr) {
            throw std::bad_alloc();
        }
		nFree = y.nFree;
        memcpy(flarr, y.flarr, sizeof(link_t) * fllen);
		hugelist = y.hugelist;
    }
    MemPool& operator=(const MemPool& y) {
        if (&y == this)
            return *this;
		destroy_and_clean();
        MemPool(y).swap(*this);
        return *this;
    }
    ~MemPool() {
        if (flarr) {
            free(flarr);
			flarr = NULL;
		}
    }

#ifdef HSM_HAS_MOVE
    MemPool(MemPool&& y) noexcept : mem(y) {
		assert(y.data() == NULL);
		assert(y.size() == 0);
        fllen = y.fllen;
        flarr = y.flarr;
		nFree = y.nFree;
		hugelist = y.hugelist;
		y.fllen = 0;
		y.flarr = NULL;
		y.nFree = 0;
		y.hugelist = list_tail;
    }
    MemPool& operator=(MemPool&& y) noexcept {
        if (&y == this)
            return *this;
        this->~MemPool();
        new(this)MemPool(y);
        return *this;
    }
#endif

	using mem::data;
    using mem::size; // bring to public...
//  using mem::shrink_to_fit;
    using mem::reserve;
    using mem::capacity;

	unsigned char byte_at(size_t pos) const {
		assert(pos < n);
		return p[pos];
	}

	// keep flarr
    void clear() {
		hugelist = huge_list_tail;
		nFree = 0;
        std::uninitialized_fill_n(flarr, fllen, link_t(free_list_tail));
		mem::clear();
	}

    void erase_all() {
		hugelist = huge_list_tail;
		nFree = 0;
        std::uninitialized_fill_n(flarr, fllen, link_t(free_list_tail));
		mem::erase_all();
	}

    void resize_no_init(size_t newsize) {
        assert(newsize % align_size == 0);
		assert(newsize >= mem::size());
        mem::resize_no_init(newsize);
    }

	void shrink_to_fit() {
		mem::shrink_to_fit();
	}

    template<class U> const U& at(size_t pos) const {
        assert((pos << offset_shift) < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + (pos << offset_shift));
    }
    template<class U> U& at(size_t pos) {
        assert((pos << offset_shift) < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + (pos << offset_shift));
    }

    // param request must be aligned by align_size
    size_t alloc(size_t request) {
        assert(request % align_size == 0);
        assert(request > 0);
		request = std::max(sizeof(link_t), request);
        size_t res = huge_list_tail;
        if (request <= fllen * align_size) {
			size_t idx = request / align_size - 1;
			if (free_list_tail != flarr[idx].next) {
				assert(nFree >= request);
			    res = size_t(flarr[idx].next) << offset_shift;
				assert(res + request <= this->n);
				flarr[idx] = at<link_t>(flarr[idx].next);
				nFree -= request;
			}
		}
		else { // find in freelist, use first match
			res = hugelist;
			size_t* prev = &hugelist;
			while (huge_list_tail != res) {
			   	HugeLink* h = (HugeLink*)(p + res);
				assert(res + h->size <= this->n);
				if (h->size >= request) {
					size_t remain = h->size - request;
					if (remain) {
						size_t free_pos = res + request;
						if (res + h->size == this->n) {
							// this is the top most block, shrink the heap
							this->n = free_pos;
							*prev = h->next; // remove from hugelist
							nFree -= remain; // not in freelist
						} else if (remain <= fllen * align_size) {
							assert(remain >= sizeof(link_t));
							assert(remain >= align_size);
							size_t idx = remain / align_size - 1;
                            free_pos >>= offset_shift;
							at<link_t>(free_pos) = flarr[idx];
							flarr[idx].next = link_size_t(free_pos);
							*prev = h->next; // remove from hugelist
						} else {
							// replace h with h2, the 2nd part of h
							HugeLink* h2 = (HugeLink*)(p + free_pos);
							h2->next = h->next;
							h2->size = remain;
							*prev = free_pos; // replace linked in pointer
						}
					} else {
						*prev = h->next; // remove from hugelist
					}
					nFree -= request;
					break;
				}
				res = h->next;
				prev = &h->next;
			}
		}
		if (huge_list_tail == res) {
			ensure_capacity(n + request);
			res = n;
			n += request;
		}
		return res >> offset_shift;
	}

    size_t alloc3(size_t pos, size_t oldlen, size_t newlen) {
        assert(newlen % align_size == 0);
        assert(newlen > 0);
        assert(oldlen % align_size == 0);
        assert(oldlen > 0);
        pos <<= offset_shift;
		oldlen = std::max(sizeof(link_t), oldlen);
		newlen = std::max(sizeof(link_t), newlen);
        assert(pos < n);
        assert(pos + oldlen <= n);
        if (pos + oldlen == n) {
            ensure_capacity(pos + newlen);
            n = pos + newlen;
            return pos >> offset_shift;
        }
        else if (newlen < oldlen) {
			assert(oldlen - newlen >= sizeof(link_t));
			assert(oldlen - newlen >= align_size);
			sfree(pos + newlen, oldlen - newlen);
            return pos >> offset_shift;
		}
		else if (newlen == oldlen) {
			// do nothing
            return pos >> offset_shift;
		}
		else {
            size_t newpos = alloc(newlen);
            memcpy(p + newpos, p + pos, std::min(oldlen, newlen));
            sfree(pos, oldlen);
            return newpos >> offset_shift;
        }
    }

    void sfree(size_t pos, size_t len) {
        assert(len % align_size == 0);
        assert(len > 0);
        pos <<= offset_shift;
        assert(pos < n);
		len = std::max(sizeof(link_t), len);
        assert(pos + len <= n);
        if (pos + len == n) {
            n = pos;
        }
	   	else if (len <= fllen * align_size) {
            size_t idx = len / align_size - 1;
            at<link_t>(pos >> offset_shift) = flarr[idx];
            flarr[idx].next = link_size_t(pos >> offset_shift);
			nFree += len;
        }
		else {
			HugeLink* h = (HugeLink*)(p + pos);
			h->next = hugelist;
			h->size = len;
			hugelist = pos;
			nFree += len;
		}
    }

	size_t free_size() const { return nFree; }

    void swap(MemPool& y) {
        mem::swap(y);
        std::swap(flarr, y.flarr);
        std::swap(fllen, y.fllen);
		std::swap(nFree, y.nFree);
		std::swap(hugelist, y.hugelist);
    }

	template<class DataIO>
	friend void DataIO_loadObject(DataIO& dio, MemPool& self) {
		typename DataIO::my_var_size_t var;
		self.clear();
		if (self.flarr)
			::free(self.flarr);
		self.flarr = NULL;
		self.fllen = 0;
		self.nFree = 0;
		self.hugelist = list_tail;
		dio >> var;  self.hugelist = var.t;
		dio >> var;  self.nFree = var.t;
		dio >> var;  self.fllen = var.t;
		self.flarr = (link_t*)malloc(sizeof(link_t) * self.fllen);
		if (NULL == self.flarr) {
			self.flarr = NULL;
			self.fllen = 0;
			self.nFree = 0;
			self.hugelist = list_tail;
			throw std::bad_alloc();
		}
		for (size_t i = 0, n = self.fllen; i < n; ++i) {
			dio >> var;
			self.flarr[i].next = var.t;
		}
		dio >> static_cast<mem&>(self);
	}

	template<class DataIO>
	friend void DataIO_saveObject(DataIO& dio, const MemPool& self) {
		typename DataIO::my_var_size_t var;
		dio << typename DataIO::my_var_size_t(self.hugelist);
		dio << typename DataIO::my_var_size_t(self.nFree);
		dio << typename DataIO::my_var_size_t(self.fllen);
		for (size_t i = 0, n = self.fllen; i < n; ++i)
			dio << typename DataIO::my_var_size_t(self.flarr[i].next);
		dio << static_cast<const mem&>(self);
	}
};

} // namespace terark

#endif

