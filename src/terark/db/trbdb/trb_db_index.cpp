#include "trb_db_index.hpp"
#include "trb_db_context.hpp"
#include <terark/util/fstrvec.hpp>
#include <terark/io/var_int.hpp>
#include <type_traits>
#include <tbb/spin_rw_mutex.h>
#include <terark/threaded_rbtree.h>
#include <terark/mempool.hpp>
#include <terark/lcast.hpp>
#include <boost/filesystem.hpp>


namespace fs = boost::filesystem;
using namespace terark;
using namespace terark::db;

namespace terark { namespace db { namespace trbdb {

typedef tbb::spin_rw_mutex TrbIndexRWLock;

typedef std::true_type TrbLockWrite;
typedef std::false_type TrbLockRead;

template<class Key, class Fixed>
class TrbIndexIterForward;
template<class Key, class Fixed>
class TrbIndexIterBackward;

template<class Key, class Fixed>
class TrbIndexStoreIterForward;
template<class Key, class Fixed>
class TrbIndexStoreIterBackward;


// var length
// fixed length
// fixed length aligned
// numeric

template<class Key, class Fixed>
class TrbWritableIndexTemplate : public TrbWritableIndex
{
    friend class TrbIndexIterForward<Key, Fixed>;
    friend class TrbIndexIterBackward<Key, Fixed>;
    friend class TrbIndexStoreIterForward<Key, Fixed>;
    friend class TrbIndexStoreIterBackward<Key, Fixed>;

    typedef size_t size_type;
    typedef threaded_rbtree_node_t<uint32_t> node_type;
    typedef threaded_rbtree_root_t<node_type, std::true_type, std::true_type> root_type;
    static size_type constexpr max_stack_depth = 2 * (sizeof(uint32_t) * 8 - 1);

    template<class Storage>
    struct mutable_deref_node_t
    {
        node_type &operator()(size_type index) const
        {
            return s.node(index);
        }
        Storage &s;
    };
    template<class Storage>
    static mutable_deref_node_t<Storage> mutable_deref_node(Storage &s)
    {
        return mutable_deref_node_t<Storage>{s};
    }
    template<class Storage>
    struct const_deref_node_t
    {
        node_type const &operator()(size_type index) const
        {
            return s.node(index);
        }
        Storage const &s;
    };
    template<class Storage>
    static const_deref_node_t<Storage> const_deref_node(Storage const &s)
    {
        return const_deref_node_t<Storage>{s};
    }
    template<class Storage>
    struct deref_key_t
    {
        fstring operator()(size_type index) const
        {
            return s.key(index);
        }
        Storage const &s;
    };
    template<class Storage>
    static deref_key_t<Storage> deref_key(Storage const &s)
    {
        return deref_key_t<Storage>{s};
    }
    template<class Storage>
    struct deref_pair_key_t
    {
        std::pair<fstring, size_type> operator()(size_type index) const
        {
            return {s.key(index), index};
        }
        Storage const &s;
    };
    template<class Storage>
    static deref_pair_key_t<Storage> deref_pair_key(Storage const &s)
    {
        return deref_pair_key_t<Storage>{s};
    }

    template<class Storage>
    struct normal_key_compare_type
    {
        bool operator()(std::pair<fstring, size_type> left, std::pair<fstring, size_type> right) const
        {
            int c = fstring_func::compare3()(left.first, right.first);
            if(c == 0)
            {
                return left.second > right.second;
            }
            return c < 0;
        }
        bool operator()(fstring left, fstring right) const
        {
            return left < right;
        }
        bool operator()(size_type left, size_type right) const
        {
            int c = fstring_func::compare3()(storage.key(left), storage.key(right));
            if(c == 0)
            {
                return left > right;
            }
            return c < 0;
        }
        int compare(std::pair<fstring, size_type> left, std::pair<fstring, size_type> right) const
        {
            int c = fstring_func::compare3()(left.first, right.first);
            if(c == 0)
            {
                if(left.second == right.second)
                {
                    return 0;
                }
                return left.second > right.second ? -1 : 1;
            }
            return c;
        }
        int compare(fstring left, fstring right) const
        {
            return fstring_func::compare3()(left, right);
        }
        int compare(size_type left, size_type right) const
        {
            int c = fstring_func::compare3()(storage.key(left), storage.key(right));
            if(c == 0)
            {
                if(left == right)
                {
                    return 0;
                }
                return left > right ? -1 : 1;
            }
            return c;
        }
        Storage const &storage;
    };
    template<class Storage>
    struct numeric_key_compare_type
    {
        bool operator()(std::pair<fstring, size_type> left, std::pair<fstring, size_type> right) const
        {
            assert(reinterpret_cast<size_type>(left.first.data()) % sizeof(Key) == 0);
            assert(reinterpret_cast<size_type>(right.first.data()) % sizeof(Key) == 0);

            assert(left.first.size() == sizeof(Key));
            assert(right.first.size() == sizeof(Key));

            auto left_key = *reinterpret_cast<Key const *>(left.first.data());
            auto right_key = *reinterpret_cast<Key const *>(right.first.data());

            if(left_key < right_key)
            {
                return true;
            }
            else if(right_key < left_key)
            {
                return false;
            }
            else
            {
                return left.second > right.second;
            }
        }
        bool operator()(fstring left, fstring right) const
        {
            assert(reinterpret_cast<size_type>(left.data()) % sizeof(Key) == 0);
            assert(reinterpret_cast<size_type>(right.data()) % sizeof(Key) == 0);

            assert(left.size() == sizeof(Key));
            assert(right.size() == sizeof(Key));

            auto left_key = *reinterpret_cast<Key const *>(left.data());
            auto right_key = *reinterpret_cast<Key const *>(right.data());

            return left_key < right_key;
        }
        bool operator()(size_type left, size_type right) const
        {
            assert(reinterpret_cast<size_type>(storage.key_ptr(left)) % sizeof(Key) == 0);
            assert(reinterpret_cast<size_type>(storage.key_ptr(right)) % sizeof(Key) == 0);

            assert(storage.key_len(left) == sizeof(Key));
            assert(storage.key_len(right) == sizeof(Key));

            auto left_key = *reinterpret_cast<Key const *>(storage.key_ptr(left));
            auto right_key = *reinterpret_cast<Key const *>(storage.key_ptr(right));

            if(left_key < right_key)
            {
                return true;
            }
            else if(right_key < left_key)
            {
                return false;
            }
            else
            {
                return left > right;
            }
        }
        Storage const &storage;
    };

    struct normal_storage_type
    {
        typedef terark::MemPool<4> pool_type;
        struct element_type
        {
            node_type node;
            uint32_t offset;
        };
        struct data_object
        {
            byte data[1];
        };
        root_type root;
        valvec<element_type> index;
        pool_type data;
        size_type total;

        normal_storage_type(size_type fixed_length) : data(256), total()
        {
            assert(fixed_length == 0);
        }

        size_type key_count() const
        {
            return root.get_count();
        }
        size_type total_length() const
        {
            return total;
        }
        size_type max_index() const
        {
            return index.size();
        }
        size_type memory_size() const
        {
            return sizeof(*this) + data.size() + index.size() * sizeof(element_type);
        }

        node_type &node(size_type i)
        {
            return index[i].node;
        }
        node_type const &node(size_type i) const
        {
            return index[i].node;
        }

        fstring key(size_type i) const
        {
            byte const *pos = data.at<data_object>(index[i].offset).data;
            uint32_t len;
            FAST_READ_VAR_UINT32(pos, len);
            return fstring(pos, len);
        }
        byte const *key_ptr(size_type i) const
        {
            byte const *pos = data.at<data_object>(index[i].offset).data;
            uint32_t len;
            FAST_READ_VAR_UINT32(pos, len);
            return pos;
        }
        size_type key_len(size_type i) const
        {
            byte const *pos = data.at<data_object>(index[i].offset).data;
            uint32_t len;
            FAST_READ_VAR_UINT32(pos, len);
            return len;
        }

        template<class IsWrite, class Compare, class Lock>
        bool store_check(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_unique(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               d,
                                                               deref_key(*this),
                                                               Compare{*this}
            );
            if(exists)
            {
                return stack.get_index(stack.height - 1) == i;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_unique(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  d,
                                                                  deref_key(*this),
                                                                  Compare{*this}
                    );
                    if(exists)
                    {
                        return stack.get_index(stack.height - 1) == i;
                    }
                }
            }
            if(terark_likely(i >= index.size()))
            {
                index.resize(i + 1, element_type{{0xFFFFFFFFU, 0xFFFFFFFFU}, std::numeric_limits<uint32_t>::max()});
            }
            if(terark_unlikely(node(i).is_used()))
            {
                bool resule = remove<TrbLockWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                threaded_rbtree_find_path_for_unique(root,
                                                     stack,
                                                     const_deref_node(*this),
                                                     d,
                                                     deref_key(*this),
                                                     Compare{*this}
                );
            }
            assert(node(i).is_empty());
            byte len_data[8];
            byte *end_ptr = save_var_uint32(len_data, uint32_t(d.size()));
            size_type len_len = size_type(end_ptr - len_data);
            size_type dst_len = pool_type::align_to(d.size() + len_len);
            index[i].offset = data.alloc(dst_len);
            byte *dst_ptr = data.at<data_object>(index[i].offset).data;
            std::memcpy(dst_ptr, len_data, len_len);
            std::memcpy(dst_ptr + len_len, d.data(), d.size());
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            total += d.size();
            ++v;
            return true;
        }
        template<class IsWrite, class Compare, class Lock>
        void store_cover(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            if(terark_unlikely(i < index.size() && node(i).is_used()))
            {
                bool resule = remove<IsWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                if(terark_likely(i >= index.size()))
                {
                    index.resize(i + 1, element_type{{0xFFFFFFFFU, 0xFFFFFFFFU}, 0xFFFFFFFFU});
                }
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    std::make_pair(d, uint32_t(i)),
                                                    deref_pair_key(*this),
                                                    Compare{*this}
                );
            }
            else
            {
                uint32_t version = v;
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    std::make_pair(d, uint32_t(i)),
                                                    deref_pair_key(*this),
                                                    Compare{*this}
                );
                if(!IsWrite::value)
                {
                    l.upgrade_to_writer();
                    if(version != v)
                    {
                        threaded_rbtree_find_path_for_multi(root,
                                                            stack,
                                                            const_deref_node(*this),
                                                            std::make_pair(d, uint32_t(i)),
                                                            deref_pair_key(*this),
                                                            Compare{*this}
                        );
                    }
                }
            }
            if(terark_likely(i >= index.size()))
            {
                index.resize(i + 1, element_type{{0xFFFFFFFFU, 0xFFFFFFFFU}, 0xFFFFFFFFU});
            }
            byte len_data[8];
            byte *end_ptr = save_var_uint32(len_data, uint32_t(d.size()));
            size_type len_len = size_type(end_ptr - len_data);
            size_type dst_len = pool_type::align_to(d.size() + len_len);
            index[i].offset = data.alloc(dst_len);
            byte *dst_ptr = data.at<data_object>(index[i].offset).data;
            std::memcpy(dst_ptr, len_data, len_len);
            std::memcpy(dst_ptr + len_len, d.data(), d.size());
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            size_type c;
            if(false
               || (
                   i != root.get_most_left(const_deref_node(*this))
                   &&
                   !Compare{*this}(c = threaded_rbtree_move_prev(i, const_deref_node(*this)), i)
                   )
               || (
                   i != root.get_most_right(const_deref_node(*this))
                   &&
                   !Compare{*this}(i, c = threaded_rbtree_move_next(i, const_deref_node(*this)))
                   )
               )
            {
                data.sfree(index[i].offset, dst_len);
                index[i].offset = index[c].offset;
            }
            total += d.size();
            ++v;
        }
        template<class IsWrite, class Compare, class Lock>
        bool remove(Lock &l, uint32_t volatile &v, size_type i)
        {
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_remove(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               i,
                                                               Compare{*this}
            );
            if(!exists)
            {
                return false;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_remove(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  i,
                                                                  Compare{*this}
                    );
                    if(!exists)
                    {
                        return false;
                    }
                }
            }
            byte const *ptr = data.at<data_object>(index[i].offset).data, *end_ptr;
            size_type len = load_var_uint32(ptr, &end_ptr);
            if(true
               && (
                   i == root.get_most_left(const_deref_node(*this))
                   ||
                   Compare{*this}(threaded_rbtree_move_prev(i, const_deref_node(*this)), i)
                   )
               && (
                   i == root.get_most_right(const_deref_node(*this))
                   ||
                   Compare{*this}(i, threaded_rbtree_move_next(i, const_deref_node(*this)))
                   )
               )
            {
                data.sfree(index[i].offset, pool_type::align_to(end_ptr - ptr + len));
            }
            threaded_rbtree_remove(root,
                                   stack,
                                   mutable_deref_node(*this)
            );
            total -= len;
            ++v;
            return true;
        }

        void clear()
        {
            root = root_type();
            index.clear();
            data.clear();
        }
        void shrink_to_fit()
        {
            index.shrink_to_fit();
            data.shrink_to_fit();
        }
    };
    struct fixed_storage_type
    {
        root_type root;
        valvec<node_type> index;
        valvec<byte> data;
        size_type key_length;

        fixed_storage_type(size_type fixed_length) : key_length(fixed_length)
        {
            assert(fixed_length > 0);
        }

        size_type key_count() const
        {
            return root.get_count();
        }
        size_type total_length() const
        {
            return root.get_count() * key_length;
        }
        size_type max_index() const
        {
            return index.size();
        }
        size_type memory_size() const
        {
            return sizeof(*this) + data.size() + index.size() * sizeof(node_type);
        }

        node_type &node(size_type i)
        {
            return index[i];
        }
        node_type const &node(size_type i) const
        {
            return index[i];
        }

        fstring key(size_type i) const
        {
            return fstring(data.data() + i * key_length, key_length);
        }
        byte const *key_ptr(size_type i) const
        {
            return data.data() + i * key_length;
        }
        size_type key_len(size_type i) const
        {
            return key_length;
        }

        template<class IsWrite, class Compare, class Lock>
        bool store_check(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            assert(d.size() == key_length);
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_unique(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               d,
                                                               deref_key(*this),
                                                               Compare{*this}
            );
            if(exists)
            {
                return stack.get_index(stack.height - 1) == i;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_unique(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  d,
                                                                  deref_key(*this),
                                                                  Compare{*this}
                    );
                    if(exists)
                    {
                        return stack.get_index(stack.height - 1) == i;
                    }
                }
            }
            if(terark_likely(i >= index.size()))
            {
                index.resize(i + 1, node_type{0xFFFFFFFFU, 0xFFFFFFFFU});
                data.resize_no_init(index.size() * key_length);
            }
            if(terark_unlikely(node(i).is_used()))
            {
                bool resule = remove<TrbLockWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                std::memcpy(data.data() + i * key_length, d.data(), d.size());
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    i,
                                                    Compare{*this}
                );
            }
            else
            {
                std::memcpy(data.data() + i * key_length, d.data(), d.size());
            }
            assert(node(i).is_empty());
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            ++v;
            return true;
        }
        template<class IsWrite, class Compare, class Lock>
        void store_cover(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            assert(d.size() == key_length);
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            if(terark_unlikely(i < index.size() && node(i).is_used()))
            {
                bool resule = remove<IsWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                if(terark_likely(i >= index.size()))
                {
                    index.resize(i + 1, node_type{0xFFFFFFFFU, 0xFFFFFFFFU});
                    data.resize_no_init(index.size() * key_length);
                }
                std::memcpy(data.data() + i * key_length, d.data(), d.size());
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    i,
                                                    Compare{*this}
                );
            }
            else
            {
                uint32_t version = v;
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    std::make_pair(d, uint32_t(i)),
                                                    deref_pair_key(*this),
                                                    Compare{*this}
                );
                if(!IsWrite::value)
                {
                    l.upgrade_to_writer();
                    if(version != v)
                    {
                        threaded_rbtree_find_path_for_multi(root,
                                                            stack,
                                                            const_deref_node(*this),
                                                            std::make_pair(d, uint32_t(i)),
                                                            deref_pair_key(*this),
                                                            Compare{*this}
                        );
                    }
                }
                if(terark_likely(i >= index.size()))
                {
                    index.resize(i + 1, node_type{0xFFFFFFFFU, 0xFFFFFFFFU});
                    data.resize_no_init(index.size() * key_length);
                }
                std::memcpy(data.data() + i * key_length, d.data(), d.size());
            }
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            ++v;
        }
        template<class IsWrite, class Compare, class Lock>
        bool remove(Lock &l, uint32_t volatile &v, size_type i)
        {
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_remove(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               i,
                                                               Compare{*this}
            );
            if(!exists)
            {
                return false;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_remove(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  i,
                                                                  Compare{*this}
                    );
                    if(!exists)
                    {
                        return false;
                    }
                }
            }
            threaded_rbtree_remove(root,
                                   stack,
                                   mutable_deref_node(*this)
            );
            ++v;
            return true;
        }

        void clear()
        {
            root = root_type();
            index.clear();
            data.clear();
        }
        void shrink_to_fit()
        {
            index.shrink_to_fit();
            data.shrink_to_fit();
        }
    };
    struct aligned_fixed_storage_type
    {
        struct element_type
        {
            node_type node;
            byte data[4];
        };
        root_type root;
        valvec<byte> index;
        size_type element_length;

        aligned_fixed_storage_type(size_type fixed_length) : element_length(fixed_length + sizeof(node_type))
        {
            assert(fixed_length > 0 && fixed_length % 4 == 0);
        }

        size_type key_count() const
        {
            return root.get_count();
        }
        size_type total_length() const
        {
            return root.get_count() * (element_length - sizeof(node_type));
        }
        size_type max_index() const
        {
            return index.size() / element_length;
        }
        size_type memory_size() const
        {
            return sizeof(*this) + index.size();
        }

        node_type &node(size_type i)
        {
            element_type *ptr = reinterpret_cast<element_type *>(index.data() + i * element_length);
            return ptr->node;
        }
        node_type const &node(size_type i) const
        {
            element_type const *ptr = reinterpret_cast<element_type const *>(index.data() + i * element_length);
            return ptr->node;
        }

        fstring key(size_type i) const
        {
            element_type const *ptr = reinterpret_cast<element_type const *>(index.data() + i * element_length);
            return fstring(ptr->data, element_length - sizeof(node_type));
        }
        byte const *key_ptr(size_type i) const
        {
            return reinterpret_cast<element_type const *>(index.data() + i * element_length)->data;
        }
        size_type key_len(size_type i) const
        {
            return element_length - sizeof(node_type);
        }

        template<class IsWrite, class Compare, class Lock>
        bool store_check(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            assert(d.size() + sizeof(node_type) == element_length);
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_unique(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               d,
                                                               deref_key(*this),
                                                               Compare{*this}
            );
            if(exists)
            {
                return stack.get_index(stack.height - 1) == i;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_unique(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  d,
                                                                  deref_key(*this),
                                                                  Compare{*this}
                    );
                    if(exists)
                    {
                        return stack.get_index(stack.height - 1) == i;
                    }
                }
            }
            if(terark_likely(i * element_length >= index.size()))
            {
                index.resize((i + 1) * element_length, 0xFFU);
            }
            element_type *ptr = reinterpret_cast<element_type *>(index.data() + i * element_length);
            if(terark_unlikely(node(i).is_used()))
            {
                bool resule = remove<TrbLockWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                std::memcpy(ptr->data, d.data(), d.size());
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    i,
                                                    Compare{*this}
                );
            }
            else
            {
                std::memcpy(ptr->data, d.data(), d.size());
            }
            assert(node(i).is_empty());
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            ++v;
            return true;
        }
        template<class IsWrite, class Compare, class Lock>
        void store_cover(Lock &l, uint32_t volatile &v, size_type i, fstring d)
        {
            assert(d.size() + sizeof(node_type) == element_length);
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            if(terark_unlikely(i < index.size() && node(i).is_used()))
            {
                bool resule = remove<IsWrite, Compare>(l, v, i);
                assert(resule);
                (void)resule;
                if(terark_likely(i * element_length >= index.size()))
                {
                    index.resize((i + 1) * element_length, 0xFFU);
                }
                assert(node(i).is_empty());
                element_type *ptr = reinterpret_cast<element_type *>(index.data() + i * element_length);
                std::memcpy(ptr->data, d.data(), d.size());
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    i,
                                                    Compare{*this}
                );
            }
            else
            {
                uint32_t version = v;
                threaded_rbtree_find_path_for_multi(root,
                                                    stack,
                                                    const_deref_node(*this),
                                                    std::make_pair(d, uint32_t(i)),
                                                    deref_pair_key(*this),
                                                    Compare{*this}
                );
                if(!IsWrite::value)
                {
                    l.upgrade_to_writer();
                    if(version != v)
                    {
                        threaded_rbtree_find_path_for_multi(root,
                                                            stack,
                                                            const_deref_node(*this),
                                                            std::make_pair(d, uint32_t(i)),
                                                            deref_pair_key(*this),
                                                            Compare{*this}
                        );
                    }
                }
                if(terark_likely(i * element_length >= index.size()))
                {
                    index.resize((i + 1) * element_length, 0xFFU);
                }
                element_type *ptr = reinterpret_cast<element_type *>(index.data() + i * element_length);
                std::memcpy(ptr->data, d.data(), d.size());
            }
            threaded_rbtree_insert(root,
                                   stack,
                                   mutable_deref_node(*this),
                                   i
            );
            ++v;
        }
        template<class IsWrite, class Compare, class Lock>
        bool remove(Lock &l, uint32_t volatile &v, size_type i)
        {
            uint32_t version = v;
            threaded_rbtree_stack_t<node_type, max_stack_depth> stack;
            bool exists = threaded_rbtree_find_path_for_remove(root,
                                                               stack,
                                                               const_deref_node(*this),
                                                               i,
                                                               Compare{*this}
            );
            if(!exists)
            {
                return false;
            }
            if(!IsWrite::value)
            {
                l.upgrade_to_writer();
                if(version != v)
                {
                    exists = threaded_rbtree_find_path_for_remove(root,
                                                                  stack,
                                                                  const_deref_node(*this),
                                                                  i,
                                                                  Compare{*this}
                    );
                    if(!exists)
                    {
                        return false;
                    }
                }
            }
            threaded_rbtree_remove(root,
                                   stack,
                                   mutable_deref_node(*this)
            );
            ++v;
            return true;
        }

        void clear()
        {
            root = root_type();
            index.clear();
        }
        void shrink_to_fit()
        {
            index.shrink_to_fit();
        }
    };

    template<class T, class Unused>
    struct is_aligned : public std::conditional<
        sizeof(T) % sizeof(uint32_t) == 0
        , std::true_type
        , std::false_type
    >::type
    {
    };
    template<class Unused>
    struct is_aligned<void, Unused> : public std::false_type
    {
    };

    typedef typename std::conditional<Fixed::value
        || std::is_arithmetic<Key>::value
        , std::true_type
        , std::false_type
    >::type fixed_type;
    typedef typename std::conditional<!fixed_type::value
        , normal_storage_type,
        typename std::conditional<
        is_aligned<Key, void>::value
        , aligned_fixed_storage_type
        , fixed_storage_type
        >::type
    >::type storage_type;
    typedef typename std::conditional<
        std::is_arithmetic<Key>::value
        , numeric_key_compare_type<storage_type>
        , normal_key_compare_type<storage_type>
    >::type key_compare_type;

    storage_type m_storage;
    uint32_t m_version;
    mutable TrbIndexRWLock m_rwMutex;

public:
    explicit TrbWritableIndexTemplate(size_type fixedLen, bool isUnique)
        : m_storage(fixedLen)
        , m_version()
    {
        ReadableIndex::m_isUnique = isUnique;
    }
    void save(PathRef) const override
    {
        //nothing todo ...
    }
    void load(PathRef) override
    {
        //nothing todo ...
    }

    IndexIterator* createIndexIterForward(DbContext*) const override
    {
        return new TrbIndexIterForward<Key, Fixed>(this, ReadableIndex::m_isUnique);
    }
    IndexIterator* createIndexIterBackward(DbContext*) const override
    {
        return new TrbIndexIterBackward<Key, Fixed>(this, ReadableIndex::m_isUnique);
    }

    llong indexStorageSize() const override
    {
        //TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        return m_storage.memory_size();
    }

    bool remove(fstring key, llong id, DbContext*) override
    {
        TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        assert(m_storage.key(id) == key);
        return m_storage.template remove<TrbLockRead, key_compare_type>(l, m_version, id);
    }
    bool insert(fstring key, llong id, DbContext*) override
    {
        TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        if(m_isUnique)
        {
            if(!m_storage.template store_check<TrbLockRead, key_compare_type>(l, m_version, id, key))
            {
                return false;
            }
        }
        else
        {
            m_storage.template store_cover<TrbLockRead, key_compare_type>(l, m_version, id, key);
        }
        return true;
    }
    bool replace(fstring key, llong oldId, llong newId, DbContext*) override
    {
        TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        assert(key == m_storage.key(oldId));
        m_storage.template store_cover<TrbLockRead, key_compare_type>(l, m_version, newId, key);
        bool success = m_storage.template remove<TrbLockWrite, key_compare_type>(l, m_version, oldId);
        assert(success);
        (void)success;
        return true;
    }

    void clear() override
    {
        TrbIndexRWLock::scoped_lock l(m_rwMutex);
        m_storage.clear();
    }

    void searchExactAppend(fstring key, valvec<llong>* recIdvec, DbContext*) const override
    {
        if(m_isFreezed)
        {
            size_type lower, upper;
            threaded_rbtree_equal_range(m_storage.root,
                                        const_deref_node(m_storage),
                                        key,
                                        deref_key(m_storage),
                                        key_compare_type{m_storage},
                                        lower,
                                        upper
            );
            while(lower != upper)
            {
                recIdvec->emplace_back(lower);
                lower = threaded_rbtree_move_next(lower, const_deref_node(m_storage));
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
            size_type lower, upper;
            threaded_rbtree_equal_range(m_storage.root,
                                        const_deref_node(m_storage),
                                        key,
                                        deref_key(m_storage),
                                        key_compare_type{m_storage},
                                        lower,
                                        upper
            );
            while(lower != upper)
            {
                recIdvec->emplace_back(lower);
                lower = threaded_rbtree_move_next(lower, const_deref_node(m_storage));
            }
        }
    }


    llong dataStorageSize() const override
    {
        //TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        return m_storage.memory_size();
    }
    llong dataInflateSize() const override
    {
        //TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        return m_storage.total_length();
    }
    llong numDataRows() const override
    {
        //TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
        return m_storage.max_index();
    }
    void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override
    {
        if(m_isFreezed)
        {
            fstring key = m_storage.key(size_t(id));
            val->append(key.data(), key.size());
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
            fstring key = m_storage.key(size_t(id));
            val->append(key.data(), key.size());
        }
    }

    StoreIterator* createStoreIterForward(DbContext*) const override
    {
        return new TrbIndexStoreIterForward<Key, Fixed>(this);
    }
    StoreIterator* createStoreIterBackward(DbContext*) const override
    {
        return new TrbIndexStoreIterBackward<Key, Fixed>(this);
    }

    llong append(fstring row, DbContext*) override
    {
        bool success;
        size_t id;
        {
            TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
            id = m_storage.max_index();
            if(m_isUnique)
            {
                success = m_storage.template store_check<TrbLockRead, key_compare_type>(l, m_version, id, row);
            }
            else
            {
                m_storage.template store_cover<TrbLockRead, key_compare_type>(l, m_version, id, row);
                success = true;
            }
        }
        if(!success)
        {
            TERARK_THROW(StoreInternalException,
                         "StoreInternalException: TrbWritableIndex::append"
            );
        }
        return llong(id);
    }
    void update(llong id, fstring row, DbContext*) override
    {
        bool success;
        {
            TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
            if(m_isUnique)
            {
                success = m_storage.template store_check<TrbLockRead, key_compare_type>(l, m_version, id, row);
            }
            else
            {
                m_storage.template store_cover<TrbLockRead, key_compare_type>(l, m_version, id, row);
                success = true;
            }
        }
        if(!success)
        {
            TERARK_THROW(StoreInternalException,
                         "StoreInternalException: TrbWritableIndex::update = %lld",
                         id
            );
        }
    }
    void remove(llong id, DbContext*) override
    {
        bool success;
        {
            TrbIndexRWLock::scoped_lock l(m_rwMutex, false);
            success = m_storage.template remove<TrbLockRead, key_compare_type>(l, m_version, id);
        }
        if(!success)
        {
            TERARK_THROW(StoreInternalException,
                         "StoreInternalException: TrbWritableIndex::remove = %lld",
                         id
            );
        }
    }

    void shrinkToFit() override
    {
        TrbIndexRWLock::scoped_lock l(m_rwMutex);
        m_storage.shrink_to_fit();
    }

    ReadableIndex* getReadableIndex() override
    {
        return this;
    }
    WritableIndex* getWritableIndex() override
    {
        return this;
    }
    ReadableStore* getReadableStore() override
    {
        return this;
    }
    AppendableStore* getAppendableStore() override
    {
        return this;
    }
    UpdatableStore* getUpdatableStore() override
    {
        return this;
    }
    WritableStore* getWritableStore() override
    {
        return this;
    }
};

template<class Key, class Fixed>
class TrbIndexIterForward : public IndexIterator
{
    typedef TrbWritableIndexTemplate<Key, Fixed> owner_t;
    boost::intrusive_ptr<owner_t> owner;
    size_t where;
    valvec<byte> data;

protected:
    void init()
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = o->m_storage.root.get_most_left(owner_t::const_deref_node(o->m_storage));
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = o->m_storage.root.get_most_left(owner_t::const_deref_node(o->m_storage));
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                data.assign(storage_key.begin(), storage_key.end());
            }
        }
    }

public:
    TrbIndexIterForward(owner_t const *o, bool u)
    {
        m_isUniqueInSchema = u;
        owner.reset(const_cast<owner_t *>(o));
        init();
    }

    void reset() override
    {
        init();
    }
    bool increment(llong* id, valvec<byte>* key) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            if(terark_likely(where != owner_t::node_type::nil_sentinel))
            {
                if(o->m_storage.node(where).is_empty())
                {
                    where = threaded_rbtree_lower_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        std::make_pair(fstring(data), where),
                                                        owner_t::deref_pair_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
                    );
                }
                auto storage_key = o->m_storage.key(where);
                *id = where;
                key->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                return true;
            }
        }
        else
        {
            if(terark_likely(where != owner_t::node_type::nil_sentinel))
            {
                TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
                if(o->m_storage.node(where).is_empty())
                {
                    where = threaded_rbtree_lower_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        std::make_pair(fstring(data), where),
                                                        owner_t::deref_pair_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
                    );
                }
                auto storage_key = o->m_storage.key(where);
                *id = where;
                key->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                return true;
            }
        }
        return false;
    }

    int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = threaded_rbtree_lower_bound(o->m_storage.root,
                                                owner_t::const_deref_node(o->m_storage),
                                                key,
                                                owner_t::deref_key(o->m_storage),
                                                typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                if(typename owner_t::key_compare_type{o->m_storage}(storage_key, key))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = threaded_rbtree_lower_bound(o->m_storage.root,
                                                owner_t::const_deref_node(o->m_storage),
                                                key,
                                                owner_t::deref_key(o->m_storage),
                                                typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                if(typename owner_t::key_compare_type{o->m_storage}(storage_key, key))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
        return -1;
    }
    int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = threaded_rbtree_upper_bound(o->m_storage.root,
                                                owner_t::const_deref_node(o->m_storage),
                                                key,
                                                owner_t::deref_key(o->m_storage),
                                                typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                return 1;
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = threaded_rbtree_upper_bound(o->m_storage.root,
                                                owner_t::const_deref_node(o->m_storage),
                                                key,
                                                owner_t::deref_key(o->m_storage),
                                                typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_next(where, owner_t::const_deref_node(o->m_storage));
                return 1;
            }
        }
        return -1;
    }
};

template<class Key, class Fixed>
class TrbIndexIterBackward : public IndexIterator
{
    typedef TrbWritableIndexTemplate<Key, Fixed> owner_t;
    boost::intrusive_ptr<owner_t> owner;
    size_t where;
    valvec<byte> data;

protected:
    void init()
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = o->m_storage.root.get_most_right(owner_t::const_deref_node(o->m_storage));
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = o->m_storage.root.get_most_right(owner_t::const_deref_node(o->m_storage));
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                data.assign(storage_key.begin(), storage_key.end());
            }
        }
    }

public:
    TrbIndexIterBackward(owner_t const *o, bool u)
    {
        m_isUniqueInSchema = u;
        owner.reset(const_cast<owner_t *>(o));
        init();
    }


    void reset() override
    {
        init();
    }
    bool increment(llong* id, valvec<byte>* key) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            if(terark_likely(where != owner_t::node_type::nil_sentinel))
            {
                if(o->m_storage.node(where).is_empty())
                {
                    where = threaded_rbtree_reverse_lower_bound(o->m_storage.root,
                                                                owner_t::const_deref_node(o->m_storage),
                                                                std::make_pair(fstring(data), where),
                                                                owner_t::deref_pair_key(o->m_storage),
                                                                typename owner_t::key_compare_type{o->m_storage}
                    );
                }
                auto storage_key = o->m_storage.key(where);
                *id = where;
                key->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                return true;
            }
        }
        else
        {
            if(terark_likely(where != owner_t::node_type::nil_sentinel))
            {
                TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
                if(o->m_storage.node(where).is_empty())
                {
                    where = threaded_rbtree_reverse_lower_bound(o->m_storage.root,
                                                                owner_t::const_deref_node(o->m_storage),
                                                                std::make_pair(fstring(data), where),
                                                                owner_t::deref_pair_key(o->m_storage),
                                                                typename owner_t::key_compare_type{o->m_storage}
                    );
                }
                auto storage_key = o->m_storage.key(where);
                *id = where;
                key->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                return true;
            }
        }
        return false;
    }

    int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = threaded_rbtree_reverse_lower_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        key,
                                                        owner_t::deref_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                if(typename owner_t::key_compare_type{o->m_storage}(key, storage_key))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = threaded_rbtree_reverse_lower_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        key,
                                                        owner_t::deref_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                if(typename owner_t::key_compare_type{o->m_storage}(key, storage_key))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
        return -1;
    }
    int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override
    {
        auto const *o = owner.get();
        if(o->m_isFreezed)
        {
            where = threaded_rbtree_reverse_upper_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        key,
                                                        owner_t::deref_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                return 1;
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            where = threaded_rbtree_reverse_upper_bound(o->m_storage.root,
                                                        owner_t::const_deref_node(o->m_storage),
                                                        key,
                                                        owner_t::deref_key(o->m_storage),
                                                        typename owner_t::key_compare_type{o->m_storage}
            );
            if(where != owner_t::node_type::nil_sentinel)
            {
                auto storage_key = o->m_storage.key(where);
                *id = where;
                retKey->assign(storage_key.data(), storage_key.size());
                data.assign(storage_key.data(), storage_key.size());
                where = threaded_rbtree_move_prev(where, owner_t::const_deref_node(o->m_storage));
                return 1;
            }
        }
        return -1;
    }
};

template<class Key, class Fixed>
class TrbIndexStoreIterForward : public StoreIterator
{
    typedef TrbWritableIndexTemplate<Key, Fixed> owner_t;
    size_t m_where;
public:
    TrbIndexStoreIterForward(owner_t const *o)
    {
        m_store.reset(const_cast<owner_t *>(o));
        m_where = 0;
    }
    bool increment(llong* id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(o->m_isFreezed)
        {
            size_t max = o->m_storage.max_index();
            while(m_where < max)
            {
                size_t k = m_where++;
                if(o->m_storage.node(k).is_used())
                {
                    fstring key = o->m_storage.key(k);
                    *id = k;
                    val->assign(key.data(), key.size());
                    return true;
                }
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            size_t max = o->m_storage.max_index();
            while(m_where < max)
            {
                size_t k = m_where++;
                if(o->m_storage.node(k).is_used())
                {
                    fstring key = o->m_storage.key(k);
                    *id = k;
                    val->assign(key.data(), key.size());
                    return true;
                }
            }
        }
        return false;
    }
    bool seekExact(llong id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(o->m_isFreezed)
        {
            if(id < 0 || id >= llong(o->m_storage.max_index()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_storage.max_index());
            }
            if(o->m_storage.node(id).is_used())
            {
                fstring key = o->m_storage.key(size_t(id));
                val->assign(key.data(), key.size());
                return true;
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            if(id < 0 || id >= llong(o->m_storage.max_index()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_storage.max_index());
            }
            if(o->m_storage.node(id).is_used())
            {
                fstring key = o->m_storage.key(size_t(id));
                val->assign(key.data(), key.size());
                return true;
            }
        }
        return false;
    }
    void reset() override
    {
        m_where = 0;
    }
};

template<class Key, class Fixed>
class TrbIndexStoreIterBackward : public StoreIterator
{
    typedef TrbWritableIndexTemplate<Key, Fixed> owner_t;
    size_t m_where;
public:
    TrbIndexStoreIterBackward(owner_t const *o)
    {
        m_store.reset(const_cast<owner_t *>(o));
        //TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
        m_where = o->m_storage.max_index();
    }
    bool increment(llong* id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(o->m_isFreezed)
        {
            while(m_where > 0)
            {
                size_t k = --m_where;
                if(o->m_storage.node(k).is_used())
                {
                    fstring key = o->m_storage.key(k);
                    *id = k;
                    val->assign(key.data(), key.size());
                    return true;
                }
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            while(m_where > 0)
            {
                size_t k = --m_where;
                if(o->m_storage.node(k).is_used())
                {
                    fstring key = o->m_storage.key(k);
                    *id = k;
                    val->assign(key.data(), key.size());
                    return true;
                }
            }
        }
        return false;
    }
    bool seekExact(llong id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(o->m_isFreezed)
        {
            if(id < 0 || id >= llong(o->m_storage.max_index()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_storage.max_index());
            }
            if(o->m_storage.node(id).is_used())
            {
                fstring key = o->m_storage.key(size_t(id));
                val->assign(key.data(), key.size());
                return true;
            }
        }
        else
        {
            TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
            if(id < 0 || id >= llong(o->m_storage.max_index()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_storage.max_index());
            }
            if(o->m_storage.node(id).is_used())
            {
                fstring key = o->m_storage.key(size_t(id));
                val->assign(key.data(), key.size());
                return true;
            }
        }
        return false;
    }
    void reset() override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        //TrbIndexRWLock::scoped_lock l(o->m_rwMutex, false);
        m_where = o->m_storage.max_index();
    }
};

TrbWritableIndex *TrbWritableIndex::createIndex(Schema const &schema)
{
    if(schema.columnNum() == 1)
    {
        ColumnMeta cm = schema.getColumnMeta(0);
#define CASE_COL_TYPE(Enum, Type) \
    case ColumnType::Enum: return new TrbWritableIndexTemplate<Type, std::false_type>(schema.getFixedRowLen(), schema.m_isUnique);
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        switch(cm.type)
        {
        default: break;
            CASE_COL_TYPE(Uint08, uint8_t);
            CASE_COL_TYPE(Sint08, int8_t);
            CASE_COL_TYPE(Uint16, uint16_t);
            CASE_COL_TYPE(Sint16, int16_t);
            CASE_COL_TYPE(Uint32, uint32_t);
            CASE_COL_TYPE(Sint32, int32_t);
            CASE_COL_TYPE(Uint64, uint64_t);
            CASE_COL_TYPE(Sint64, int64_t);
            CASE_COL_TYPE(Float32, float);
            CASE_COL_TYPE(Float64, double);
        }
#undef CASE_COL_TYPE
    }
    if(schema.getFixedRowLen() != 0)
    {
        return new TrbWritableIndexTemplate<void, std::true_type>(schema.getFixedRowLen(), schema.m_isUnique);
    }
    else
    {
        return new TrbWritableIndexTemplate<void, std::false_type>(schema.getFixedRowLen(), schema.m_isUnique);
    }
}

}}} //namespace terark { namespace db { namespace trbdb {