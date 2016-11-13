#pragma once

#include "threaded_rbtree.h"
#include <stdexcept>
#include <cstdint>
#include <utility>
#include <iterator>
#include <algorithm>
#include <cmath>
#include <cassert>
#include <new>
#include <cstring>

namespace threaded_rbtree_hash_detail
{
    class move_trivial_tag
    {
    };
    class move_assign_tag
    {
    };
    template<class T>
    struct is_trivial_expand : public std::is_trivial<T>
    {
    };
    template<class K, class V>
    struct is_trivial_expand<std::pair<K, V>> : public std::conditional<std::is_trivial<K>::value && std::is_trivial<V>::value,
                                                                        std::true_type,
                                                                        std::false_type>::type
    {
    };
    template<class iterator_t>
    struct get_tag
    {
        typedef typename std::conditional<is_trivial_expand<typename std::iterator_traits<iterator_t>::value_type>::value,
                                          move_trivial_tag,
                                          move_assign_tag>::type type;
    };

    template<class iterator_t, class tag_t, class ...args_t> void construct_one(iterator_t where, tag_t, args_t &&...args)
    {
        typedef typename std::iterator_traits<iterator_t>::value_type iterator_value_t;
        ::new(std::addressof(*where)) iterator_value_t(std::forward<args_t>(args)...);
    }

    template<class iterator_t> void destroy_one(iterator_t where, move_trivial_tag)
    {
    }
    template<class iterator_t> void destroy_one(iterator_t where, move_assign_tag)
    {
        typedef typename std::iterator_traits<iterator_t>::value_type iterator_value_t;
        where->~iterator_value_t();
    }

    template<class iterator_from_t, class iterator_to_t>
    void move_construct_and_destroy(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin, move_trivial_tag)
    {
        std::ptrdiff_t count = move_end - move_begin;
        std::memmove(&*to_begin, &*move_begin, count * sizeof(*move_begin));
    }
    template<class iterator_from_t, class iterator_to_t>
    void move_construct_and_destroy(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin, move_assign_tag)
    {
        for(; move_begin != move_end; ++move_begin)
        {
            construct_one(to_begin++, move_assign_tag(), std::move(*move_begin));
            destroy_one(move_begin, move_assign_tag());
        }
    }
}

template<class config_t>
class threaded_rbtree_hash
{
public:
    typedef typename config_t::key_type key_type;
    typedef typename config_t::mapped_type mapped_type;
    typedef typename config_t::value_type value_type;
    typedef typename config_t::storage_type storage_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef typename config_t::hasher hasher;
    typedef typename config_t::key_compare key_compare;
    typedef typename config_t::allocator_type allocator_type;
    typedef typename config_t::offset_type offset_type;
    typedef typename config_t::hash_value_type hash_value_type;
    typedef value_type &reference;
    typedef value_type const &const_reference;
    typedef value_type *pointer;
    typedef value_type const *const_pointer;


protected:
    static size_type constexpr max_stack_depth = 2 * (sizeof(offset_type) * 8 - 1);

    typedef threaded_rbtree_node_t<offset_type> node_t;
    typedef threaded_rbtree_stack_t<node_t, max_stack_depth> stack_t;
    typedef threaded_rbtree_root_t<node_t, std::false_type, std::false_type> trb_root_t;

    static size_type constexpr offset_empty = node_t::nil_sentinel;

    struct pod_value_t
    {
        typename std::aligned_storage<sizeof(storage_type), alignof(storage_type)>::type value_pod;

        storage_type *value()
        {
            return reinterpret_cast<storage_type *>(&value_pod);
        }
        storage_type const *value() const
        {
            return reinterpret_cast<storage_type const *>(&value_pod);
        }
    };

    typedef typename allocator_type::template rebind<trb_root_t>::other bucket_allocator_t;
    typedef typename allocator_type::template rebind<node_t>::other node_allocator_t;
    typedef typename allocator_type::template rebind<pod_value_t>::other value_allocator_t;
    struct root_t : public hasher, public key_compare, public bucket_allocator_t, public node_allocator_t, public value_allocator_t
    {
        template<class any_hasher, class any_key_compare, class any_allocator_type>
        root_t(any_hasher &&hash, any_key_compare &&compare, any_allocator_type &&alloc)
            : hasher(std::forward<any_hasher>(hash))
            , key_compare(std::forward<any_key_compare>(compare))
            , bucket_allocator_t(alloc)
            , node_allocator_t(alloc)
            , value_allocator_t(std::forward<any_allocator_type>(alloc))
        {
            static_assert(std::is_unsigned<offset_type>::value && std::is_integral<offset_type>::value, "offset_type must be unsighed integer");
            static_assert(sizeof(offset_type) <= sizeof(threaded_rbtree_hash::size_type), "offset_type too big");
            static_assert(std::is_integral<hash_value_type>::value, "hash_value_type must be integer");
            bucket_count = 0;
            capacity = 0;
            size = 0;
            free_count = 0;
            free_list = offset_empty;
            setting_load_factor = 1;
            bucket = nullptr;
            node = nullptr;
            value = nullptr;
        }
        typename threaded_rbtree_hash::size_type bucket_count;
        typename threaded_rbtree_hash::size_type capacity;
        typename threaded_rbtree_hash::size_type size;
        typename threaded_rbtree_hash::size_type free_count;
        typename threaded_rbtree_hash::size_type free_list;
        float setting_load_factor;
        trb_root_t *bucket;
        node_t *node;
        pod_value_t *value;
    };
    template<class k_t, class v_t> struct get_key_select_t
    {
        key_type const &operator()(key_type const &value) const
        {
            return value;
        }
        key_type const &operator()(storage_type const &value) const
        {
            return config_t::get_key(value);
        }
        template<class first_t, class second_t> key_type operator()(std::pair<first_t, second_t> const &pair)
        {
            return key_type(pair.first);
        }
        template<class in_t, class ...args_t> key_type operator()(in_t const &in, args_t const &...args) const
        {
            return key_type(in);
        }
    };
    template<class k_t> struct get_key_select_t<k_t, k_t>
    {
        key_type const &operator()(key_type const &value) const
        {
            return config_t::get_key(value);
        }
        template<class in_t, class ...args_t> key_type operator()(in_t const &in, args_t const &...args) const
        {
            return key_type(in, args...);
        }
    };
    typedef get_key_select_t<key_type, storage_type> get_key_t;

    struct deref_node_t
    {
        node_t &operator()(size_type index) const
        {
            return root_ptr->node[index];
        }
        root_t const *root_ptr;
    };
    struct const_deref_node_t
    {
        node_t const &operator()(size_type index) const
        {
            return root_ptr->node[index];
        }
        root_t const *root_ptr;
    };
    struct deref_key_t
    {
        key_type const &operator()(size_type index) const
        {
            return config_t::get_key(*root_ptr->value[index].value());
        }
        root_t const *root_ptr;
    };
    struct offset_compare
    {
        bool operator()(size_type left, size_type right) const
        {
            key_compare const &compare = *root_ptr;
            auto const &left_key = config_t::get_key(*root_ptr->value[left].value());
            auto const &right_key = config_t::get_key(*root_ptr->value[right].value());
            if(compare(left_key, right_key))
            {
                return true;
            }
            else if(compare(right_key, left_key))
            {
                return false;
            }
            else
            {
                return left < right;
            }
        }
        root_t const *root_ptr;
    };

public:
    class iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename threaded_rbtree_hash::value_type value_type;
        typedef typename threaded_rbtree_hash::difference_type difference_type;
        typedef typename threaded_rbtree_hash::reference reference;
        typedef typename threaded_rbtree_hash::pointer pointer;
    public:
        iterator(size_type _offset, threaded_rbtree_hash *_self) : offset(_offset), self(_self)
        {
        }
        iterator(iterator const &) = default;
        iterator &operator++()
        {
            offset = self->advance_next_(offset);
            return *this;
        }
        iterator operator++(int)
        {
            iterator save(*this);
            ++*this;
            return save;
        }
        reference operator *() const
        {
            return reinterpret_cast<reference>(*self->root_.value[offset].value());
        }
        pointer operator->() const
        {
            return reinterpret_cast<pointer>(self->root_.value[offset].value());
        }
        bool operator == (iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
    protected:
        friend class threaded_rbtree_hash;
        size_type offset;
        threaded_rbtree_hash *self;
    };
    class const_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename threaded_rbtree_hash::value_type value_type;
        typedef typename threaded_rbtree_hash::difference_type difference_type;
        typedef typename threaded_rbtree_hash::reference reference;
        typedef typename threaded_rbtree_hash::const_reference const_reference;
        typedef typename threaded_rbtree_hash::pointer pointer;
        typedef typename threaded_rbtree_hash::const_pointer const_pointer;
    public:
        const_iterator(size_type _offset, threaded_rbtree_hash const *_self) : offset(_offset), self(_self)
        {
        }
        const_iterator(const_iterator const &) = default;
        const_iterator(iterator const &it) : offset(it.offset), self(it.self)
        {
        }
        const_iterator &operator++()
        {
            offset = self->advance_next_(offset);
            return *this;
        }
        const_iterator operator++(int)
        {
            const_iterator save(*this);
            ++*this;
            return save;
        }
        const_reference operator *() const
        {
            return reinterpret_cast<const_reference>(*self->root_.value[offset].value());
        }
        const_pointer operator->() const
        {
            return reinterpret_cast<const_pointer>(self->root_.value[offset].value());
        }
        bool operator == (const_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (const_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
    protected:
        friend class threaded_rbtree_hash;
        size_type offset;
        threaded_rbtree_hash const *self;
    };
    class local_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename threaded_rbtree_hash::value_type value_type;
        typedef typename threaded_rbtree_hash::difference_type difference_type;
        typedef typename threaded_rbtree_hash::reference reference;
        typedef typename threaded_rbtree_hash::pointer pointer;
    public:
        local_iterator(size_type _offset, threaded_rbtree_hash *_self) : offset(_offset), self(_self)
        {
        }
        local_iterator(local_iterator const &) = default;
        local_iterator &operator++()
        {
            offset = self->local_advance_next_(offset);
            return *this;
        }
        local_iterator operator++(int)
        {
            local_iterator save(*this);
            ++*this;
            return save;
        }
        reference operator *() const
        {
            return reinterpret_cast<reference>(*self->root_.value[offset].value());
        }
        pointer operator->() const
        {
            return reinterpret_cast<pointer>(self->root_.value[offset].value());
        }
        bool operator == (local_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (local_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
    protected:
        friend class threaded_rbtree_hash;
        size_type offset;
        threaded_rbtree_hash *self;
    };
    class const_local_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename threaded_rbtree_hash::value_type value_type;
        typedef typename threaded_rbtree_hash::difference_type difference_type;
        typedef typename threaded_rbtree_hash::reference reference;
        typedef typename threaded_rbtree_hash::const_reference const_reference;
        typedef typename threaded_rbtree_hash::pointer pointer;
        typedef typename threaded_rbtree_hash::const_pointer const_pointer;
    public:
        const_local_iterator(size_type _offset, threaded_rbtree_hash const *_self) : offset(_offset), self(_self)
        {
        }
        const_local_iterator(const_local_iterator const &) = default;
        const_local_iterator(local_iterator const &it) : offset(it.offset), self(it.self)
        {
        }
        const_local_iterator &operator++()
        {
            offset = self->local_advance_next_(offset);
            return *this;
        }
        const_local_iterator operator++(int)
        {
            const_local_iterator save(*this);
            ++*this;
            return save;
        }
        const_reference operator *() const
        {
            return reinterpret_cast<const_reference>(*self->root_.value[offset].value());
        }
        const_pointer operator->() const
        {
            return reinterpret_cast<const_pointer>(self->root_.value[offset].value());
        }
        bool operator == (const_local_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (const_local_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
    protected:
        friend class threaded_rbtree_hash;
        size_type offset;
        threaded_rbtree_hash const *self;
    };
    typedef typename std::conditional<config_t::unique_type::value, std::pair<iterator, bool>, iterator>::type insert_result_t;
    typedef std::pair<iterator, bool> pair_ib_t;
protected:
    typedef std::pair<size_type, bool> pair_posb_t;
    template<class unique_type> typename std::enable_if<unique_type::value, insert_result_t>::type result_(pair_posb_t posb)
    {
        return std::make_pair(iterator(posb.first, this), posb.second);
    }
    template<class unique_type> typename std::enable_if<!unique_type::value, insert_result_t>::type result_(pair_posb_t posb)
    {
        return iterator(posb.first, this);
    }

public:
    //empty
    threaded_rbtree_hash() : root_(hasher(), key_compare(), allocator_type())
    {
    }
    //empty
    explicit threaded_rbtree_hash(size_type bucket_count,
                                  hasher const &hash = hasher(),
                                  key_compare const &compare = key_compare(),
                                  allocator_type const &alloc = allocator_type()
    ) : root_(hash, compare, alloc)
    {
        rehash(bucket_count);
    }
    //empty
    explicit threaded_rbtree_hash(allocator_type const &alloc) : root_(hasher(), key_compare(), alloc)
    {
    }
    //empty
    threaded_rbtree_hash(size_type bucket_count, allocator_type const &alloc) : root_(hasher(), key_compare(), alloc)
    {
        rehash(bucket_count);
    }
    //empty
    threaded_rbtree_hash(size_type bucket_count, hasher const &hash, allocator_type const &alloc) : root_(hash, key_compare(), alloc)
    {
        rehash(bucket_count);
    }
    //range
    template<class iterator_t> threaded_rbtree_hash(iterator_t begin,
                                                    iterator_t end,
                                                    size_type bucket_count = 8,
                                                    hasher const &hash = hasher(),
                                                    key_compare const &compare = key_compare(),
                                                    allocator_type const &alloc = allocator_type()
    ) : root_(hash, compare, alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //range
    template<class iterator_t> threaded_rbtree_hash(iterator_t begin,
                                                    iterator_t end,
                                                    size_type bucket_count,
                                                    allocator_type const &alloc
    ) : root_(hasher(), key_compare(), alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //range
    template<class iterator_t> threaded_rbtree_hash(iterator_t begin,
                                                    iterator_t end,
                                                    size_type bucket_count,
                                                    hasher const &hash,
                                                    allocator_type const &alloc
    ) : root_(hash, key_compare(), alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //copy
    threaded_rbtree_hash(threaded_rbtree_hash const &other)
        : root_(other.get_hasher(), other.get_key_comp(), other.get_value_allocator_())
    {
        copy_all_<false>(&other.root_);
    }
    //copy
    threaded_rbtree_hash(threaded_rbtree_hash const &other,
                         allocator_type const &alloc
    ) : root_(other.get_hasher(), other.get_key_comp(), alloc)
    {
        copy_all_<false>(&other.root_);
    }
    //move
    threaded_rbtree_hash(threaded_rbtree_hash &&other) : root_(hasher(), key_compare(), value_allocator_t())
    {
        swap(other);
    }
    //move
    threaded_rbtree_hash(threaded_rbtree_hash &&other,
                         allocator_type const &alloc
    ) : root_(std::move(other.get_hasher()), std::move(other.get_key_comp()), alloc)
    {
        copy_all_<true>(&other.root_);
    }
    //initializer list
    threaded_rbtree_hash(std::initializer_list<value_type> il,
                         size_type bucket_count = 8,
                         hasher const &hash = hasher(),
                         key_compare const &compare = key_compare(),
                         allocator_type const &alloc = allocator_type()
    ) : threaded_rbtree_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), hash, compare, alloc)
    {
    }
    //initializer list
    threaded_rbtree_hash(std::initializer_list<value_type> il,
                         size_type bucket_count,
                         allocator_type const &alloc
    ) : threaded_rbtree_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), alloc)
    {
    }
    //initializer list
    threaded_rbtree_hash(std::initializer_list<value_type> il,
                         size_type bucket_count,
                         hasher const &hash,
                         allocator_type const &alloc
    ) : threaded_rbtree_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), hash, alloc)
    {
    }
    //destructor
    ~threaded_rbtree_hash()
    {
        dealloc_all_();
    }
    //copy
    threaded_rbtree_hash &operator = (threaded_rbtree_hash const &other)
    {
        if(this == &other)
        {
            return *this;
        }
        dealloc_all_();
        get_hasher() = other.get_hasher();
        get_key_comp() = other.get_key_comp();
        get_bucket_allocator_() = other.get_bucket_allocator_();
        get_node_allocator_() = other.get_node_allocator_();
        get_value_allocator_() = other.get_value_allocator_();
        copy_all_<false>(&other.root_);
        return *this;
    }
    //move
    threaded_rbtree_hash &operator = (threaded_rbtree_hash &&other)
    {
        if(this == &other)
        {
            return *this;
        }
        swap(other);
        return *this;
    }
    //initializer list
    threaded_rbtree_hash &operator = (std::initializer_list<value_type> il)
    {
        clear();
        rehash(std::distance(il.begin(), il.end()));
        insert(il.begin(), il.end());
        return *this;
    }

    allocator_type get_allocator() const
    {
        return *static_cast<value_allocator_t const *>(&root_);
    }
    hasher hash_function() const
    {
        return *static_cast<hasher const *>(&root_);
    }
    key_compare key_comp() const
    {
        return *static_cast<key_compare const *>(&root_);
    }

    void swap(threaded_rbtree_hash &other)
    {
        std::swap(root_, other.root_);
    }

    typedef std::pair<iterator, iterator> pair_ii_t;
    typedef std::pair<const_iterator, const_iterator> pair_cici_t;
    typedef std::pair<local_iterator, local_iterator> pair_lili_t;
    typedef std::pair<const_local_iterator, const_local_iterator> pair_clicli_t;

    //single element
    insert_result_t insert(value_type const &value)
    {
        return result_<typename config_t::unique_type>(insert_value_(value));
    }
    //single element
    template<class in_value_t>
    typename std::enable_if<std::is_convertible<in_value_t, value_type>::value, insert_result_t>::type insert(in_value_t &&value)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<in_value_t>(value)));
    }
    //with hint
    iterator insert(const_iterator hint, value_type const &value)
    {
        return iterator(insert_value_(value).first, this);
    }
    //with hint
    template<class in_value_t>
    typename std::enable_if<std::is_convertible<in_value_t, value_type>::value, insert_result_t>::type insert(const_iterator hint, in_value_t &&value)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<in_value_t>(value)));
    }
    //range
    template<class iterator_t> void insert(iterator_t begin, iterator_t end)
    {
        for(; begin != end; ++begin)
        {
            emplace_hint(cend(), *begin);
        }
    }
    //initializer list
    void insert(std::initializer_list<value_type> il)
    {
        insert(il.begin(), il.end());
    }

    //single element
    template<class ...args_t> insert_result_t emplace(args_t &&...args)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<args_t>(args)...));
    }
    //with hint
    template<class ...args_t> insert_result_t emplace_hint(const_iterator hint, args_t &&...args)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<args_t>(args)...));
    }

    template<class in_key_t> iterator find(in_key_t const &key)
    {
        if(root_.size == 0)
        {
            return end();
        }
        return iterator(find_value_(key), this);
    }
    template<class in_key_t> const_iterator find(in_key_t const &key) const
    {
        if(root_.size == 0)
        {
            return cend();
        }
        return const_iterator(find_value_(key), this);
    }

    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value
                                                             && config_t::unique_type::value
                                                             && !std::is_same<key_type, storage_type>::value
                                                             , void>::type> mapped_type &at(in_key_t const &key)
    {
        offset_type offset = root_.size;
        if(root_.size != 0)
        {
            offset = find_value_(key);
        }
        if(offset == root_.size)
        {
            throw std::out_of_range("threaded_rbtree_hash out of range");
        }
        return root_.value[offset].value()->second;
    }
    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value
                                                             && config_t::unique_type::value
                                                             && !std::is_same<key_type, storage_type>::value
                                                             , void>::type> mapped_type const &at(in_key_t const &key) const
    {
        offset_type offset = root_.size;
        if(root_.size != 0)
        {
            offset = find_value_(key);
        }
        if(offset == root_.size)
        {
            throw std::out_of_range("threaded_rbtree_hash out of range");
        }
        return root_.value[offset].value()->second;
    }

    iterator erase(const_iterator it)
    {
        if(root_.size == 0)
        {
            return end();
        }
        remove_offset_(it.offset);
        return iterator(advance_next_(it.offset), this);
    }
    local_iterator erase(const_local_iterator it)
    {
        if(root_.size == 0)
        {
            return local_iterator(offset_empty, this);
        }
        size_type next = local_advance_next_(it.offset);
        remove_offset_(it.offset);
        return local_iterator(next, this);
    }
    size_type erase(key_type const &key)
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return remove_value_(typename config_t::unique_type(), key);
    }
    iterator erase(const_iterator erase_begin, const_iterator erase_end)
    {
        if(erase_begin == cbegin() && erase_end == cend())
        {
            clear();
            return end();
        }
        else
        {
            while(erase_begin != erase_end)
            {
                erase(erase_begin++);
            }
            return iterator(erase_begin.offset, this);
        }
    }
    local_iterator erase(const_local_iterator erase_begin, const_local_iterator erase_end)
    {
        while(erase_begin != erase_end)
        {
            erase(erase_begin++);
        }
        return local_iterator(erase_begin.offset, this);
    }

    size_type count(key_type const &key) const
    {
        return find(key) == end() ? 0 : 1;
    }

    pair_lili_t equal_range(key_type const &key)
    {
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        size_type lower, upper;
        threaded_rbtree_equal_range(root_.bucket[bucket],
                                    const_deref_node_t{&root_},
                                    key,
                                    deref_key_t{&root_},
                                    get_key_comp(),
                                    lower,
                                    upper
        );
        return std::make_pair(local_iterator(lower, this), local_iterator(upper, this));
    }
    pair_clicli_t equal_range(key_type const &key) const
    {
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        size_type lower, upper;
        threaded_rbtree_equal_range(root_.bucket[bucket],
                                    const_deref_node_t{&root_},
                                    key,
                                    deref_key_t{&root_},
                                    get_key_comp(),
                                    lower,
                                    upper
        );
        return std::make_pair(const_local_iterator(lower, this), const_local_iterator(upper, this));
    }

    iterator begin()
    {
        return iterator(find_begin_(), this);
    }
    iterator end()
    {
        return iterator(root_.size, this);
    }
    const_iterator begin() const
    {
        return const_iterator(find_begin_(), this);
    }
    const_iterator end() const
    {
        return const_iterator(root_.size, this);
    }
    const_iterator cbegin() const
    {
        return const_iterator(find_begin_(), this);
    }
    const_iterator cend() const
    {
        return const_iterator(root_.size, this);
    }

    bool empty() const
    {
        return root_.size == root_.free_count;
    }
    void clear()
    {
        clear_all_();
    }
    size_type size() const
    {
        return root_.size - root_.free_count;
    }
    size_type max_size() const
    {
        return offset_empty - 1;
    }

    local_iterator begin(size_type n)
    {
        return local_iterator(root_.bucket[n].root.root, this);
    }
    local_iterator end(size_type n)
    {
        return local_iterator(offset_empty, this);
    }
    const_local_iterator begin(size_type n) const
    {
        return const_local_iterator(root_.bucket[n].root.root, this);
    }
    const_local_iterator end(size_type n) const
    {
        return const_local_iterator(offset_empty, this);
    }
    const_local_iterator cbegin(size_type n) const
    {
        return const_local_iterator(root_.bucket[n].root.root, this);
    }
    const_local_iterator cend(size_type n) const
    {
        return const_local_iterator(offset_empty, this);
    }

    size_type bucket_count() const
    {
        return root_.bucket_count;
    }
    size_type max_bucket_count() const
    {
        return max_size();
    }

    size_type bucket_size(size_type n) const
    {
        return std::distance(begin(n), end(n));
    }

    size_type bucket(key_type const &key) const
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return get_hasher()(key) % root_.bucket_count;
    }

    void reserve(size_type count)
    {
        rehash(size_type(std::ceil(count / root_.setting_load_factor)));
        if(count > root_.capacity && root_.capacity <= max_size())
        {
            realloc_(size_type(std::ceil(std::max<float>(count, root_.bucket_count * root_.setting_load_factor))));
        }
    }
    void rehash(size_type count)
    {
        rehash_(std::max<size_type>({8, count, size_type(std::ceil(size() / root_.setting_load_factor))}));
    }

    void max_load_factor(float ml)
    {
        if(ml <= 0)
        {
            return;
        }
        root_.setting_load_factor = ml;
        if(root_.size != 0)
        {
            rehash_(size_type(std::ceil(size() / root_.setting_load_factor)));
        }
    }
    float max_load_factor() const
    {
        return root_.setting_load_factor;
    }
    float load_factor() const
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return size() / float(root_.bucket_count);
    }

protected:
    root_t root_;

protected:

    hasher &get_hasher()
    {
        return root_;
    }
    hasher const &get_hasher() const
    {
        return root_;
    }

    key_compare &get_key_comp()
    {
        return root_;
    }
    key_compare const &get_key_comp() const
    {
        return root_;
    }

    offset_compare get_offset_comp()
    {
        return offset_compare{&root_};
    }
    offset_compare get_offset_comp() const
    {
        return offset_compare{&root_};
    }

    bucket_allocator_t &get_bucket_allocator_()
    {
        return root_;
    }
    bucket_allocator_t const &get_bucket_allocator_() const
    {
        return root_;
    }
    node_allocator_t &get_node_allocator_()
    {
        return root_;
    }
    node_allocator_t const &get_node_allocator_() const
    {
        return root_;
    }
    value_allocator_t &get_value_allocator_()
    {
        return root_;
    }
    value_allocator_t const &get_value_allocator_() const
    {
        return root_;
    }

    size_type advance_next_(size_type i) const
    {
        for(++i; i < root_.size; ++i)
        {
            if(root_.node[i].is_used())
            {
                break;
            }
        }
        return i;
    }

    size_type find_begin_() const
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.node[i].is_used())
            {
                return i;
            }
        }
        return root_.size;
    }

    size_type local_advance_next_(size_type i) const
    {
        return threaded_rbtree_move_next(i, const_deref_node_t{&root_});
    }

    template<class iterator_t, class ...args_t> static void construct_one_(iterator_t where, args_t &&...args)
    {
        threaded_rbtree_hash_detail::construct_one(where,
                                                   typename threaded_rbtree_hash_detail::get_tag<iterator_t>::type(),
                                                   std::forward<args_t>(args)...
        );
    }

    template<class iterator_t> static void destroy_one_(iterator_t where)
    {
        threaded_rbtree_hash_detail::destroy_one(where, typename threaded_rbtree_hash_detail::get_tag<iterator_t>::type());
    }

    template<class iterator_from_t, class iterator_to_t>
    static void move_construct_and_destroy_(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin)
    {
        threaded_rbtree_hash_detail::move_construct_and_destroy(move_begin,
                                                                move_end,
                                                                to_begin,
                                                                typename threaded_rbtree_hash_detail::get_tag<iterator_from_t>::type()
        );
    }

    void dealloc_all_()
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.node[i].is_used())
            {
                destroy_one_(root_.value[i].value());
            }
        }
        if(root_.bucket_count != 0)
        {
            get_bucket_allocator_().deallocate(root_.bucket, root_.bucket_count);
        }
        if(root_.capacity != 0)
        {
            get_node_allocator_().deallocate(root_.node, root_.capacity);
            get_value_allocator_().deallocate(root_.value, root_.capacity);
        }
    }

    void clear_all_()
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.node[i].is_used())
            {
                destroy_one_(root_.value[i].value());
            }
        }
        if(root_.bucket_count != 0)
        {
            std::fill_n(root_.bucket, root_.bucket_count, trb_root_t());
        }
        if(root_.capacity != 0)
        {
            std::memset(root_.node, 0xFFFFFFFF, sizeof(node_t) * root_.capacity);

        }
        root_.size = 0;
        root_.free_count = 0;
        root_.free_list = offset_empty;
    }

    template<bool move> void copy_all_(root_t const *other)
    {
        root_.bucket_count = 0;
        root_.capacity = 0;
        root_.size = 0;
        root_.free_count = 0;
        root_.free_list = offset_empty;
        root_.setting_load_factor = other->setting_load_factor;
        root_.bucket = nullptr;
        root_.node = nullptr;
        root_.value = nullptr;
        size_type size = other->size - other->free_count;
        if(size > 0)
        {
            rehash_(size);
            realloc_(size);
            for(size_type other_i = 0; other_i < other->size; ++other_i)
            {
                if(other->node[other_i].is_used())
                {
                    size_type i = root_.size;
                    if(move)
                    {
                        construct_one_(root_.value[i].value(), std::move(*other->value[other_i].value()));
                    }
                    else
                    {
                        construct_one_(root_.value[i].value(), *other->value[other_i].value());
                    }
                    size_type bucket = get_hasher()(get_key_t()(*other->value[i].value())) % root_.bucket_count;
                    stack_t stack;
                    threaded_rbtree_find_path_for_multi(root_.bucket[bucket], stack, deref_node_t{&root_}, i, get_offset_comp());
                    threaded_rbtree_insert(root_.bucket[bucket], stack, deref_node_t{&root_}, i);
                }
            }
        }
    }

    static uint32_t get_prime_(std::integral_constant<size_t, 4>, size_type size)
    {
        static uint32_t const prime_array[] =
        {
            5,11,19,37,   53UL,         97UL,         193UL,       389UL,
            769UL,        1543UL,       3079UL,       6151UL,      12289UL,
            24593UL,      49157UL,      98317UL,      196613UL,    393241UL,
            786433UL,     1572869UL,    3145739UL,    6291469UL,   12582917UL,
            25165843UL,   50331653UL,   100663319UL,  201326611UL, 402653189UL,
            805306457UL,  1610612741UL, 3221225473UL, 4294967291UL,
        };
        for(auto prime : prime_array)
        {
            if(prime >= size)
            {
                return prime;
            }
        }
        return *std::prev(std::end(prime_array));
    }

    static uint64_t get_prime_(std::integral_constant<size_t, 8>, size_type size)
    {
        static uint64_t const prime_array[] =
        {
            5,11,19,37,   53UL,         97UL,         193UL,       389UL,
            769UL,        1543UL,       3079UL,       6151UL,      12289UL,
            24593UL,      49157UL,      98317UL,      196613UL,    393241UL,
            786433UL,     1572869UL,    3145739UL,    6291469UL,   12582917UL,
            25165843UL,   50331653UL,   100663319UL,  201326611UL, 402653189UL,
            805306457UL,  1610612741UL, 3221225473UL, 4294967291UL,
            /* 30 */ 8589934583ULL,
            /* 31 */ 17179869143ULL,
            /* 32 */ 34359738337ULL,
            /* 33 */ 68719476731ULL,
            /* 34 */ 137438953447ULL,
            /* 35 */ 274877906899ULL,
            /* 36 */ 549755813881ULL,
            /* 37 */ 1099511627689ULL,
            /* 38 */ 2199023255531ULL,
            /* 39 */ 4398046511093ULL,
            /* 40 */ 8796093022151ULL,
            /* 41 */ 17592186044399ULL,
            /* 42 */ 35184372088777ULL,
            /* 43 */ 70368744177643ULL,
            /* 44 */ 140737488355213ULL,
            /* 45 */ 281474976710597ULL,
            /* 46 */ 562949953421231ULL,
            /* 47 */ 1125899906842597ULL,
            /* 48 */ 2251799813685119ULL,
            /* 49 */ 4503599627370449ULL,
            /* 50 */ 9007199254740881ULL,
            /* 51 */ 18014398509481951ULL,
            /* 52 */ 36028797018963913ULL,
            /* 53 */ 72057594037927931ULL,
            /* 54 */ 144115188075855859ULL,
            /* 55 */ 288230376151711717ULL,
            /* 56 */ 576460752303423433ULL,
            /* 57 */ 1152921504606846883ULL,
            /* 58 */ 2305843009213693951ULL,
            /* 59 */ 4611686018427387847ULL,
            /* 60 */ 9223372036854775783ULL,
            /* 61 */ 18446744073709551557ULL,
        };
        for(auto prime : prime_array)
        {
            if(prime >= size)
            {
                return prime;
            }
        }
        return *std::prev(std::end(prime_array));
    }

    void rehash_(size_type size)
    {
        size = std::min<size_type>(get_prime_(std::integral_constant<size_t, sizeof(size_type)>(), size), max_size());
        trb_root_t *new_bucket = get_bucket_allocator_().allocate(size);
        std::fill_n(new_bucket, size, trb_root_t());

        if(root_.bucket_count != 0)
        {
            for(size_type i = 0; i < root_.size; ++i)
            {
                if(root_.node[i].is_used())
                {
                    size_type bucket = get_hasher()(get_key_t()(*root_.value[i].value())) % size;
                    stack_t stack;
                    threaded_rbtree_find_path_for_multi(new_bucket[bucket], stack, const_deref_node_t{&root_}, i, get_offset_comp());
                    threaded_rbtree_insert(new_bucket[bucket], stack, deref_node_t{&root_}, i);
                }
            }
            get_bucket_allocator_().deallocate(root_.bucket, root_.bucket_count);
        }
        root_.bucket_count = size;
        root_.bucket = new_bucket;
    }

    void realloc_(size_type size)
    {
        size_type constexpr value_size = sizeof(value_size);
        if(size * value_size > 0x1000)
        {
            size = ((size * value_size + std::max<size_type>(value_size, 0x1000) - 1) & (~size_type(0) ^ 0xFFF)) / value_size;
        }
        else if(size * value_size > 0x100)
        {
            size = ((size * value_size + std::max<size_type>(value_size, 0x100) - 1) & (~size_type(0) ^ 0xFF)) / value_size;
        }
        else
        {
            size = ((size * value_size + std::max<size_type>(value_size, 0x10) - 1) & (~size_type(0) ^ 0xF)) / value_size;
        }
        size = std::min(size, max_size());
        node_t *new_node = get_node_allocator_().allocate(size);
        pod_value_t *new_value = get_value_allocator_().allocate(size);

        std::memset(new_node + root_.capacity, 0xFFFFFFFF, sizeof(node_t) * (size - root_.capacity));
        if(root_.capacity != 0)
        {
            std::memcpy(new_node, root_.node, sizeof(node_t) * root_.capacity);
            move_construct_and_destroy_(root_.value->value(), root_.value->value() + root_.capacity, new_value->value());
            get_node_allocator_().deallocate(root_.node, root_.capacity);
            get_value_allocator_().deallocate(root_.value, root_.capacity);
        }
        root_.capacity = size;
        root_.node = new_node;
        root_.value = new_value;
    }

    void check_grow_()
    {
        size_type new_size = size() + 1;
        if(new_size > root_.bucket_count * root_.setting_load_factor)
        {
            if(root_.bucket_count >= max_size())
            {
                throw std::length_error("threaded_rbtree_hash too long");
            }
            rehash_(size_type(std::ceil(root_.bucket_count * config_t::grow_proportion(root_.bucket_count))));
        }
        if(new_size > root_.capacity)
        {
            if(root_.capacity >= max_size())
            {
                throw std::length_error("threaded_rbtree_hash too long");
            }
            realloc_(size_type(std::ceil(std::max<float>(root_.capacity * config_t::grow_proportion(root_.capacity),
                                                         root_.bucket_count * root_.setting_load_factor))));
        }
    }

    template<class ...args_t> pair_posb_t insert_value_(args_t &&...args)
    {
        check_grow_();
        return insert_value_uncheck_(typename config_t::unique_type(), std::forward<args_t>(args)...);
    }

    template<class in_t, class ...args_t>
    typename std::enable_if<std::is_same<key_type, storage_type>::value
                            && !std::is_same<typename std::remove_reference<in_t>::type
            , key_type>::value, pair_posb_t>::type insert_value_uncheck_(std::true_type, in_t &&in, args_t &&...args)
    {
        key_type key = get_key_t()(in, args...);
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        stack_t stack;
        if(threaded_rbtree_find_path_for_unique(root_.bucket[bucket],
                                                stack,
                                                deref_node_t{&root_},
                                                key,
                                                deref_key_t{&root_},
                                                get_key_comp())
           )
        {
            return {stack.get_index(stack.height - 1), false};
        }
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::move(key));
        if(offset == root_.free_list)
        {
            root_.free_list = root_.node[offset].left_get_link();
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        threaded_rbtree_insert(root_.bucket[bucket], stack, deref_node_t{&root_}, offset);
        return {offset, true};
    }
    template<class in_t, class ...args_t>
    typename std::enable_if<!std::is_same<key_type, storage_type>::value
                            || std::is_same<typename std::remove_reference<in_t>::type
                            , key_type>::value, pair_posb_t>::type insert_value_uncheck_(std::true_type, in_t &&in, args_t &&...args)
    {
        key_type key = get_key_t()(in, args...);
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        stack_t stack;
        if(threaded_rbtree_find_path_for_unique(root_.bucket[bucket],
                                                stack,
                                                deref_node_t{&root_},
                                                key,
                                                deref_key_t{&root_},
                                                get_key_comp())
           )
        {
            return {stack.get_index(stack.height - 1), false};
        }
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::forward<in_t>(in), std::forward<args_t>(args)...);
        if(offset == root_.free_list)
        {
            root_.free_list = root_.node[offset].left_get_link();
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        threaded_rbtree_insert(root_.bucket[bucket], stack, deref_node_t{&root_}, offset);
        return{offset, true};
    }

    template<class in_t, class ...args_t> pair_posb_t insert_value_uncheck_(std::false_type, in_t &&in, args_t &&...args)
    {
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::forward<in_t>(in), std::forward<args_t>(args)...);
        if(offset == root_.free_list)
        {
            root_.free_list = root_.node[offset].left_get_link();
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        size_type bucket = get_hasher()(get_key_t()(*root_.value[offset].value())) % root_.bucket_count;
        stack_t stack;
        threaded_rbtree_find_path_for_multi(root_.bucket[bucket], stack, deref_node_t{&root_}, offset, get_offset_comp());
        threaded_rbtree_insert(root_.bucket[bucket], stack, deref_node_t{&root_}, offset);
        return {offset, true};
    }

    size_type find_value_(key_type const &key) const
    {
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        size_type offset = threaded_rbtree_lower_bound(root_.bucket[bucket],
                                                       deref_node_t{&root_},
                                                       key,
                                                       deref_key_t{&root_},
                                                       get_key_comp()
        );
        return (offset == offset_empty || get_key_comp()(key, get_key_t()(*root_.value[offset].value()))) ? root_.size : offset;
    }

    size_type remove_value_(std::true_type, key_type const &key)
    {
        size_type bucket = get_hasher()(key) % root_.bucket_count;
        stack_t stack;
        if(threaded_rbtree_find_path_for_unique(root_.bucket[bucket],
                                                stack,
                                                deref_node_t{&root_},
                                                key,
                                                deref_key_t{&root_},
                                                get_key_comp())
           )
        {
            size_type offset = stack.get_index(stack.height - 1);
            threaded_rbtree_remove(root_.bucket[bucket], stack, deref_node_t{&root_});
            destroy_one_(root_.value[offset].value());
            root_.node[offset].set_empty();
            root_.node[offset].left_set_link(root_.free_list);
            root_.free_list = offset_type(offset);
            ++root_.free_count;
            return 1;
        }
        else
        {
            return 0;
        }
    }

    size_type remove_value_(std::false_type, key_type const &key)
    {
        size_type count = 0;
        while(remove_value_(std::true_type(), key) == 1)
        {
            ++count;
        }
        return count;
    }

    void remove_offset_(size_type offset)
    {
        size_type bucket = get_hasher()(get_key_t()(*root_.value[offset].value())) % root_.bucket_count;
        stack_t stack;
        threaded_rbtree_find_path_for_remove(root_.bucket[bucket], stack, deref_node_t{&root_}, offset, get_offset_comp());
        threaded_rbtree_remove(root_.bucket[bucket], stack, deref_node_t{&root_});
        destroy_one_(root_.value[offset].value());
        root_.node[offset].set_empty();
        root_.node[offset].left_set_link(root_.free_list);
        root_.free_list = offset_type(offset);
        ++root_.free_count;
    }
};

template<class key_t, class unique_t, class hasher_t, class key_compare_t, class allocator_t>
struct threaded_rbtree_hash_set_config_t
{
    typedef key_t key_type;
    typedef key_t const mapped_type;
    typedef key_t const value_type;
    typedef key_t storage_type;
    typedef hasher_t hasher;
    typedef key_compare_t key_compare;
    typedef allocator_t allocator_type;
    typedef std::uintptr_t offset_type;
    typedef typename std::result_of<hasher(key_type)>::type hash_value_type;
    typedef unique_t unique_type;
    static float grow_proportion(std::size_t)
    {
        return 2;
    }
    template<class in_type> static key_type const &get_key(in_type &&value)
    {
        return value;
    }
};

template<class key_t, class value_t, class unique_t, class hasher_t, class key_compare_t, class allocator_t>
struct threaded_rbtree_hash_map_config_t
{
    typedef key_t key_type;
    typedef value_t mapped_type;
    typedef std::pair<key_t const, value_t> value_type;
    typedef std::pair<key_t, value_t> storage_type;
    typedef hasher_t hasher;
    typedef key_compare_t key_compare;
    typedef allocator_t allocator_type;
    typedef std::uintptr_t offset_type;
    typedef typename std::result_of<hasher(key_type)>::type hash_value_type;
    typedef unique_t unique_type;
    static float grow_proportion(std::size_t)
    {
        return 2;
    }
    template<class in_type> static key_type const &get_key(in_type &&value)
    {
        return value.first;
    }
};

template<class key_t, class hasher_t = std::hash<key_t>, class key_compare_t = std::less<key_t>, class allocator_t = std::allocator<key_t>>
using trb_hash_set = threaded_rbtree_hash<threaded_rbtree_hash_set_config_t<key_t, std::true_type, hasher_t, key_compare_t, allocator_t>>;

template<class key_t, class hasher_t = std::hash<key_t>, class key_compare_t = std::less<key_t>, class allocator_t = std::allocator<key_t>>
using trb_hash_multiset = threaded_rbtree_hash<threaded_rbtree_hash_set_config_t<key_t, std::false_type, hasher_t, key_compare_t, allocator_t>>;

template<class key_t, class value_t, class hasher_t = std::hash<key_t>, class key_compare_t = std::less<key_t>, class allocator_t = std::allocator<std::pair<key_t const, value_t>>>
class trb_hash_map : public threaded_rbtree_hash<threaded_rbtree_hash_map_config_t<key_t, value_t, std::true_type, hasher_t, key_compare_t, allocator_t>>
{
    typedef threaded_rbtree_hash<threaded_rbtree_hash_map_config_t<key_t, value_t, std::true_type, hasher_t, key_compare_t, allocator_t>> base_t;
public:
    //explicit
    explicit trb_hash_map(typename base_t::size_type bucket_count,
                          typename base_t::hasher const &hash = typename base_t::hasher(),
                          typename base_t::key_compare const &compare = typename base_t::key_compare(),
                          typename base_t::allocator_type const &alloc = typename base_t::allocator_type()
    ) : base_t(bucket_count, hash, compare, alloc)
    {
    }
    explicit trb_hash_map(typename base_t::allocator_type const &alloc) : base_t(alloc)
    {
    }
    template<class ...args_t>
    trb_hash_map(args_t &&...args) : base_t(std::forward<args_t>(args)...)
    {
    }
    template<class ...args_t>
    trb_hash_map(std::initializer_list<typename base_t::value_type> il, args_t &&...args)
        : base_t(il, std::forward<args_t>(args)...)
    {
    }

    typename base_t::mapped_type &operator[](typename base_t::key_type const &key)
    {
        typename base_t::offset_type offset = base_t::root_.size;
        if(base_t::root_.size != 0)
        {
            offset = base_t::find_value_(key);
        }
        if(offset == base_t::root_.size)
        {
            offset = base_t::insert_value_(key, typename base_t::mapped_type()).first;
        }
        return base_t::root_.value[offset].value()->second;
    }
};

template<class key_t, class value_t, class hasher_t = std::hash<key_t>, class key_compare_t = std::less<key_t>, class allocator_t = std::allocator<std::pair<key_t const, value_t>>>
using trb_hash_multimap = threaded_rbtree_hash<threaded_rbtree_hash_map_config_t<key_t, value_t, std::false_type, hasher_t, key_compare_t, allocator_t>>;
