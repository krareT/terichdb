#ifndef __penglei_mempool_hpp__
#define __penglei_mempool_hpp__

#include "valvec.hpp"
#include <boost/integer/static_log2.hpp>
#include <boost/mpl/if.hpp>

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
    
    static const size_t skip_list_level_max = 8;    // data io depend on this, don't modify this value
    static const size_t list_tail = ~link_size_t(0);
    static const size_t offset_shift = AlignSize == 4 ? boost::static_log2<AlignSize>::value : 0;

    struct link_t { // just for readable
        link_size_t next;
        explicit link_t(link_size_t l) : next(l) {}
    };
    struct huge_link_t {
        link_size_t size;
        link_size_t next[skip_list_level_max];
    };
    link_t* free_list_arr;
    size_t  free_list_len;
    size_t  fragment_size;
    huge_link_t huge_list;

    size_t random_level() {
        size_t level = 1;
        while (rand() % 4 == 0 && level < skip_list_level_max)
            ++level;
        return level - 1;
    }

    void destroy_and_clean() {
        mem::clear();
        if (free_list_arr) {
            free(free_list_arr);
            free_list_arr = NULL;
        }
        free_list_len = 0;
        fragment_size = 0;
        huge_list.size = 0;
        for(auto& next : huge_list.next)
            next = list_tail;
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
        assert(maxBlockSize >= sizeof(huge_link_t));
        maxBlockSize = align_to(maxBlockSize);
        free_list_len = maxBlockSize / align_size;
        free_list_arr = (link_t*)malloc(sizeof(link_t) * free_list_len);
        if (NULL == free_list_arr) {
            throw std::bad_alloc();
        }
        fragment_size = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, link_t(list_tail));
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
    }
    MemPool(const MemPool& y) : mem(y) {
        free_list_len = y.free_list_len;
        free_list_arr = (link_t*)malloc(sizeof(link_t) * free_list_len);
        if (NULL == free_list_arr) {
            throw std::bad_alloc();
        }
        fragment_size = y.fragment_size;
        memcpy(free_list_arr, y.free_list_arr, sizeof(link_t) * free_list_len);
        huge_list = y.huge_list;
    }
    MemPool& operator=(const MemPool& y) {
        if (&y == this)
            return *this;
        destroy_and_clean();
        MemPool(y).swap(*this);
        return *this;
    }
    ~MemPool() {
        if (free_list_arr) {
            free(free_list_arr);
            free_list_arr = NULL;
        }
    }

#ifdef HSM_HAS_MOVE
    MemPool(MemPool&& y) noexcept : mem(y) {
        assert(y.data() == NULL);
        assert(y.size() == 0);
        free_list_len = y.free_list_len;
        free_list_arr = y.free_list_arr;
        fragment_size = y.fragment_size;
        huge_list = y.huge_list;
        y.free_list_len = 0;
        y.free_list_arr = NULL;
        y.fragment_size = 0;
        y.huge_list.size = 0;
        for(auto& next : y.huge_list.next) next = list_tail;
    }
    MemPool& operator=(MemPool&& y) noexcept {
        if (&y == this)
            return *this;
        this->~MemPool();
        ::new(this) MemPool(y);
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

    // keep free_list_arr
    void clear() {
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
        fragment_size = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, link_t(list_tail));
        mem::clear();
    }

    void erase_all() {
        huge_list.size = 0;
        for(auto& next : huge_list.next) next = list_tail;
        fragment_size = 0;
        std::uninitialized_fill_n(free_list_arr, free_list_len, link_t(list_tail));
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
        assert(pos < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + pos);
    }
    template<class U> U& at(size_t pos) {
        assert(pos < n);
    //  assert(pos + sizeof(U) < n);
        return *(U*)(p + pos);
    }

    // param request must be aligned by align_size
    size_t alloc(size_t request) {
        assert(request % align_size == 0);
        assert(request > 0);
        request = std::max(sizeof(link_t), request);
        size_t res = list_tail;
        if (request <= free_list_len * align_size) {
            size_t idx = request / align_size - 1;
            if (list_tail != free_list_arr[idx].next) {
                assert(fragment_size >= request);
                res = size_t(free_list_arr[idx].next) << offset_shift;
                assert(res + request <= this->n);
                free_list_arr[idx] = at<link_t>(res);
                fragment_size -= request;
            }
        }
        else { // find in freelist, use first match
            assert(request >= sizeof(huge_link_t));
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2 = nullptr;
            size_t k = huge_list.size;
            while (k-- > 0) {
                while (n1->next[k] != list_tail && (n2 = &at<huge_link_t>(size_t(n1->next[k]) << offset_shift))->size < request)
                    n1 = n2;
                update[k] = n1;
            }
            if (n2 != nullptr && n2->size >= request) {
                assert((byte*)n2 >= p);
                size_t remain = n2->size - request;
                res = size_t((byte*)n2 - p);
                size_t res_shift = res >> offset_shift;
                for (k = 0; k < huge_list.size; ++k)
                    if ((n1 = update[k])->next[k] == res_shift)
                        n1->next[k] = n2->next[k];
                while (huge_list.next[huge_list.size - 1] == list_tail && --huge_list.size > 0)
                    ;
                if (remain)
                    sfree(res + request, remain);
                fragment_size -= request;
            }
        }
        if (list_tail == res) {
            ensure_capacity(n + request);
            res = n;
            n += request;
        }
        return res;
    }

    size_t alloc3(size_t pos, size_t oldlen, size_t newlen) {
        assert(newlen % align_size == 0);
        assert(newlen > 0);
        assert(oldlen % align_size == 0);
        assert(oldlen > 0);
        oldlen = std::max(sizeof(link_t), oldlen);
        newlen = std::max(sizeof(link_t), newlen);
        assert(pos < n);
        assert(pos + oldlen <= n);
        if (pos + oldlen == n) {
            ensure_capacity(n = pos + newlen);
            return pos;
        }
        else if (newlen < oldlen) {
            assert(oldlen - newlen >= sizeof(link_t));
            assert(oldlen - newlen >= align_size);
            sfree(pos + newlen, oldlen - newlen);
            return pos;
        }
        else if (newlen == oldlen) {
            // do nothing
            return pos;
        }
        else {
            size_t newpos = alloc(newlen);
            memcpy(p + newpos, p + pos, std::min(oldlen, newlen));
            sfree(pos, oldlen);
            return newpos;
        }
    }

    void sfree(size_t pos, size_t len) {
        assert(len % align_size == 0);
        assert(len > 0);
        assert(pos < n);
        len = std::max(sizeof(link_t), len);
        assert(pos + len <= n);
        if (pos + len == n) {
            n = pos;
        }
        else if (len <= free_list_len * align_size) {
            size_t idx = len / align_size - 1;
            at<link_t>(pos) = free_list_arr[idx];
            free_list_arr[idx].next = link_size_t(pos >> offset_shift);
            fragment_size += len;
        }
        else {
            assert(len >= sizeof(huge_link_t));
            huge_link_t* update[skip_list_level_max];
            huge_link_t* n1 = &huge_list;
            huge_link_t* n2;
            size_t k = huge_list.size;
            while (k-- > 0) {
                while (n1->next[k] != list_tail && (n2 = &at<huge_link_t>(size_t(n1->next[k]) << offset_shift))->size < len)
                    n1 = n2;
                update[k] = n1;
            }
            k = random_level();  
            if (k >= huge_list.size) {
                k = huge_list.size++;
                update[k] = &huge_list;
            };
            n2 = &at<huge_link_t>(pos);
            size_t pos_shift = pos >> offset_shift;
            do {
                n1 = update[k];
                n2->next[k] = n1->next[k];
                n1->next[k] = pos_shift;
            } while(k-- > 0);
            n2->size = len;
            fragment_size += len;
        }
    }

    size_t free_size() const { return fragment_size; }

    void swap(MemPool& y) {
        mem::swap(y);
        std::swap(free_list_arr, y.free_list_arr);
        std::swap(free_list_len, y.free_list_len);
        std::swap(fragment_size, y.fragment_size);
        std::swap(huge_list, y.huge_list);
    }

    template<class DataIO>
    friend void DataIO_loadObject(DataIO& dio, MemPool& self) {
        typename DataIO::my_var_size_t var;
        self.clear();
        if (self.free_list_arr)
            ::free(self.free_list_arr);
        self.free_list_arr = NULL;
        self.free_list_len = 0;
        self.fragment_size = 0;
        self.huge_list.size = 0;
        for (auto& next : self.huge_list.next) next = list_tail;
        dio >> var; self.huge_list.size = var.t;
        for (auto& next : self.huge_list.next) {
            dio >> var;
            next = var.t;
        }
        dio >> var;  self.fragment_size = var.t;
        dio >> var;  self.free_list_len = var.t;
        self.free_list_arr = (link_t*)malloc(sizeof(link_t) * self.free_list_len);
        if (NULL == self.free_list_arr) {
            self.free_list_arr = NULL;
            self.free_list_len = 0;
            self.fragment_size = 0;
            self.huge_list.size = 0;
            for (auto& next : self.huge_list.next) next = list_tail;
            throw std::bad_alloc();
        }
        for (size_t i = 0, n = self.free_list_len; i < n; ++i) {
            dio >> var;
            self.free_list_arr[i].next = var.t;
        }
        dio >> static_cast<mem&>(self);
    }

    template<class DataIO>
    friend void DataIO_saveObject(DataIO& dio, const MemPool& self) {
        typename DataIO::my_var_size_t var;
        dio << typename DataIO::my_var_size_t(self.huge_list.size);
        for (auto& next : self.huge_list.next)
            dio << typename DataIO::my_var_size_t(next);
        dio << typename DataIO::my_var_size_t(self.fragment_size);
        dio << typename DataIO::my_var_size_t(self.free_list_len);
        for (size_t i = 0, n = self.free_list_len; i < n; ++i)
            dio << typename DataIO::my_var_size_t(self.free_list_arr[i].next);
        dio << static_cast<const mem&>(self);
    }
};

} // namespace terark

#endif

