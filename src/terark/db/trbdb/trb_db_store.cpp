
#define NOMINMAX
#include "trb_db_store.hpp"
#include "trb_db_context.hpp"
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/num_to_str.hpp>
#include <boost/filesystem.hpp>
#include <terark/io/var_int.hpp>


namespace fs = boost::filesystem;
using namespace terark;
using namespace terark::db;

namespace terark { namespace db { namespace trbdb {

class TrbStoreIterForward : public StoreIterator
{
    typedef TrbWritableStore owner_t;
    size_t m_where;
public:
    TrbStoreIterForward(owner_t const *o)
    {
        m_store.reset(const_cast<owner_t *>(o));
        m_where = 0;
    }
    bool increment(llong* id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        size_t max = o->m_index.size();
        while(m_where < max)
        {
            size_t k = m_where++;
            if(o->m_index[k] != 0x80000000U)
            {
                fstring item = o->readItem(k);
                *id = k;
                val->assign(item.begin(), item.end());
                return true;
            }
        }
        return false;
    }
    bool seekExact(llong id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(id < 0 || id >= llong(o->m_index.size()))
        {
            THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                        , id, o->m_index.size());
        }
        if(o->m_index[id] != 0x80000000U)
        {
            fstring item = o->readItem(size_t(id));
            val->assign(item.begin(), item.end());
            return true;
        }
        return false;
    }
    void reset() override
    {
        m_where = 0;
    }
};

class TrbStoreIterBackward : public StoreIterator
{
    typedef TrbWritableStore owner_t;
    size_t m_where;
public:
    TrbStoreIterBackward(owner_t const *o)
    {
        m_store.reset(const_cast<owner_t *>(o));
        m_where = o->m_index.size();
    }
    bool increment(llong* id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        while(m_where > 0)
        {
            size_t k = --m_where;
            if(o->m_index[k] != 0x80000000U)
            {
                fstring item = o->readItem(k);
                *id = k;
                val->assign(item.begin(), item.end());
                return true;
            }
        }
        return false;
    }
    bool seekExact(llong id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(id < 0 || id >= llong(o->m_index.size()))
        {
            THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                      , id, o->m_index.size());
        }
        if(o->m_index[id] != 0x80000000U)
        {
            fstring item = o->readItem(size_t(id));
            val->assign(item.begin(), item.end());
            return true;
        }
        return false;
    }
    void reset() override
    {
        m_where = m_store->numDataRows();
    }
};


TrbWritableStore::TrbWritableStore(PathRef fpath)
    : m_data(256)
    , m_fp(fixFilePath(fpath).c_str(), "wb")
{
    m_fp.disbuf();
    NativeDataInput<InputBuffer> in; in.attach(&m_fp);
    uint32_t index_remove;
    std::string key;

    try
    {
        while(true)
        {
            // |   1  |  31 |
            // |remove|index|
            // index_remove_replace
            //     if remove , remove item at index
            //     otherwise , read key , insert or update key at index

            in >> index_remove;
            if((index_remove & 0x80000000) == 0)
            {
                in >> key;
                storeItem(index_remove, key);
            }
            else
            {
                //TODO check index exists !!!
                bool success = removeItem(index_remove & 0x7FFFFFFFU);
                if(!success)
                {
                    //TODO WTF ? bad storage file +1 ???
                }
            }
        }
    }
    catch(EndOfFileException const &e)
    {
        (void)e;//shut up !
    }
    m_out.attach(&m_fp);
}

fstring TrbWritableStore::readItem(size_type i) const
{
    if(terark_unlikely(i >= m_index.size()))
    {
        return fstring();
    }
    byte const *ptr;
    size_type len = load_var_uint32(m_data.at<data_object>(m_index[i]).data, &ptr);
    return fstring(ptr, len);
}

void TrbWritableStore::storeItem(size_type i, fstring d)
{
    if(terark_likely(i >= m_index.size()))
    {
        m_index.resize(i + 1, 0x80000000U);
    }
    byte len_data[8];
    byte *end_ptr = save_var_uint32(len_data, uint32_t(d.size()));
    size_type len_len = size_type(end_ptr - len_data);
    size_type dst_len = pool_type::align_to(d.size() + len_len);
    m_index[i] = m_data.alloc(dst_len);
    byte *dst_ptr = m_data.at<data_object>(m_index[i]).data;
    std::memcpy(dst_ptr, len_data, len_len);
    std::memcpy(dst_ptr + len_len, d.data(), d.size());
}

bool TrbWritableStore::removeItem(size_type i)
{
    if(terark_unlikely(i >= m_index.size() || m_index[i] == 0x80000000U))
    {
        return false;
    }
    byte const *ptr = m_data.at<data_object>(m_index[i]).data, *end_ptr;
    size_type len = load_var_uint32(ptr, &end_ptr);
    m_data.sfree(m_index[i], pool_type::align_to(end_ptr - ptr + len));
    m_index[i] = 0x80000000U;
    return true;
}

std::string TrbWritableStore::fixFilePath(PathRef path)
{
    return fstring(path.string()).endsWith(".trb")
        ? path.string()
        : path.string() + ".trb";
}

TrbWritableStore::~TrbWritableStore()
{
}

void TrbWritableStore::save(PathRef) const
{
    //nothing todo ...
    assert(false);
}

void TrbWritableStore::load(PathRef)
{
    //nothing todo ...
    assert(false);
}

llong TrbWritableStore::dataStorageSize() const
{
    return m_index.used_mem_size() + m_data.size();
}

llong TrbWritableStore::dataInflateSize() const
{
    return m_data.size();
}

llong TrbWritableStore::numDataRows() const
{
    return m_index.size();
}

void TrbWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext *) const
{
    fstring item = readItem(size_t(id));
    val->append(item.begin(), item.end());
}

StoreIterator *TrbWritableStore::createStoreIterForward(DbContext *) const
{
    return new TrbStoreIterForward(this);
}

StoreIterator *TrbWritableStore::createStoreIterBackward(DbContext *) const
{
    return new TrbStoreIterBackward(this);
}

llong TrbWritableStore::append(fstring row, DbContext *)
{
    size_t id;
    storeItem(id = m_index.size(), row);
    m_out << uint32_t(id) << row;
    m_out.flush();
    return id;
}

void TrbWritableStore::update(llong id, fstring row, DbContext *)
{
    storeItem(size_t(id), row);
    m_out << uint32_t(id) << row;
    m_out.flush();
}

void TrbWritableStore::remove(llong id, DbContext *)
{
    removeItem(size_t(id));
    m_out << (uint32_t(id) | 0x80000000U);
    m_out.flush();
}

void TrbWritableStore::shrinkToFit()
{
    m_index.shrink_to_fit();
    m_data.shrink_to_fit();
}

AppendableStore *TrbWritableStore::getAppendableStore()
{
    return this;
}

UpdatableStore *TrbWritableStore::getUpdatableStore()
{
    return this;
}

WritableStore *TrbWritableStore::getWritableStore()
{
    return this;
}

}}} //namespace terark { namespace db { namespace trbdb {