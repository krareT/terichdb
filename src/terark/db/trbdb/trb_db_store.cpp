
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
        if(o->m_isFreezed)
        {
            size_t max = o->m_index.size();
            while(m_where < max)
            {
                size_t k = m_where++;
                if(o->m_index[k] != 0x80000000U)
                {
                    fstring item = o->readItem(k);
                    *id = k;
                    val->assign(item.data(), item.size());
                    return true;
                }
            }
        }
        else
        {
            TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
            size_t max = o->m_index.size();
            while(m_where < max)
            {
                size_t k = m_where++;
                if(o->m_index[k] != 0x80000000U)
                {
                    fstring item = o->readItem(k);
                    *id = k;
                    val->assign(item.data(), item.size());
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
            if(id < 0 || id >= llong(o->m_index.size()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_index.size());
            }
            if(o->m_index[id] != 0x80000000U)
            {
                fstring item = o->readItem(size_t(id));
                val->assign(item.data(), item.size());
                return true;
            }
        }
        else
        {
            TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
            if(id < 0 || id >= llong(o->m_index.size()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_index.size());
            }
            if(o->m_index[id] != 0x80000000U)
            {
                fstring item = o->readItem(size_t(id));
                val->assign(item.data(), item.size());
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

class TrbStoreIterBackward : public StoreIterator
{
    typedef TrbWritableStore owner_t;
    size_t m_where;
public:
    TrbStoreIterBackward(owner_t const *o)
    {
        m_store.reset(const_cast<owner_t *>(o));
        //TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
        m_where = o->m_index.size();
    }
    bool increment(llong* id, valvec<byte>* val) override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        if(o->m_isFreezed)
        {
            while(m_where > 0)
            {
                size_t k = --m_where;
                if(o->m_index[k] != 0x80000000U)
                {
                    fstring item = o->readItem(k);
                    *id = k;
                    val->assign(item.data(), item.size());
                    return true;
                }
            }
        }
        else
        {
            TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
            while(m_where > 0)
            {
                size_t k = --m_where;
                if(o->m_index[k] != 0x80000000U)
                {
                    fstring item = o->readItem(k);
                    *id = k;
                    val->assign(item.data(), item.size());
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
            if(id < 0 || id >= llong(o->m_index.size()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_index.size());
            }
            if(o->m_index[id] != 0x80000000U)
            {
                fstring item = o->readItem(size_t(id));
                val->assign(item.data(), item.size());
                return true;
            }
        }
        else
        {
            TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
            if(id < 0 || id >= llong(o->m_index.size()))
            {
                THROW_STD(out_of_range, "Invalid id = %lld, rows = %zd"
                          , id, o->m_index.size());
            }
            if(o->m_index[id] != 0x80000000U)
            {
                fstring item = o->readItem(size_t(id));
                val->assign(item.data(), item.size());
                return true;
            }
        }
        return false;
    }
    void reset() override
    {
        auto const *o = static_cast<owner_t const *>(m_store.get());
        //TrbStoreRWLock::scoped_lock l(o->m_rwMutex, false);
        m_where = o->m_index.size();
    }
};


TrbWritableStore::TrbWritableStore(Schema const &)
    : m_data(256)
{
}

TrbWritableStore::~TrbWritableStore()
{
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
    //TrbStoreRWLock::scoped_lock l(m_rwMutex, false);
    return m_index.used_mem_size() + m_data.size();
}

llong TrbWritableStore::dataInflateSize() const
{
    //TrbStoreRWLock::scoped_lock l(m_rwMutex, false);
    return m_data.size();
}

llong TrbWritableStore::numDataRows() const
{
    //TrbStoreRWLock::scoped_lock l(m_rwMutex, false);
    return m_index.size();
}

void TrbWritableStore::getValueAppend(llong id, valvec<byte>* val, DbContext *) const
{
    if(m_isFreezed)
    {
        fstring item = readItem(size_t(id));
        val->append(item.data(), item.size());
    }
    else
    {
        TrbStoreRWLock::scoped_lock l(m_rwMutex, false);
        fstring item = readItem(size_t(id));
        val->append(item.data(), item.size());
    }
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
    TrbStoreRWLock::scoped_lock l(m_rwMutex);
    size_t id;
    storeItem(id = m_index.size(), row);
    return id;
}

void TrbWritableStore::update(llong id, fstring row, DbContext *)
{
    TrbStoreRWLock::scoped_lock l(m_rwMutex);
    storeItem(size_t(id), row);
}

void TrbWritableStore::remove(llong id, DbContext *)
{
    TrbStoreRWLock::scoped_lock l(m_rwMutex);
    removeItem(size_t(id));
}

void TrbWritableStore::shrinkToFit()
{
    TrbStoreRWLock::scoped_lock l(m_rwMutex);
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

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

#define ScopeLock(WriteLock) \
	TrbStoreRWLock::scoped_lock lock; \
    if (!m_isFreezed) lock.acquire(m_rwMutex, WriteLock)

MemoryFixedLenStore::MemoryFixedLenStore(Schema const &schema)
    : m_fixlen(schema.getFixedRowLen())
    , m_data()
{
    m_recordsBasePtr = m_data.data();
}
MemoryFixedLenStore::~MemoryFixedLenStore()
{
}

void MemoryFixedLenStore::save(PathRef) const
{
}

void MemoryFixedLenStore::load(PathRef)
{
}

llong MemoryFixedLenStore::dataStorageSize() const
{
    //ScopeLock(false);
    return m_data.used_mem_size() + sizeof m_fixlen;
}

llong MemoryFixedLenStore::dataInflateSize() const
{
    //ScopeLock(false);
    return m_data.used_mem_size();
}

llong MemoryFixedLenStore::numDataRows() const
{
    //ScopeLock(false);
    return m_data.size() / m_fixlen;
}

void MemoryFixedLenStore::getValueAppend(llong id, valvec<byte>* val, DbContext *) const
{
    ScopeLock(false);
    size_t offset = size_t(id) * m_fixlen;
    if(terark_unlikely(offset >= m_data.size()))
    {
        return;
    }
    val->append(m_data.data() + offset, m_data.data() + offset + m_fixlen);
}

StoreIterator *MemoryFixedLenStore::createStoreIterForward(DbContext *) const
{
    return nullptr;
}

StoreIterator *MemoryFixedLenStore::createStoreIterBackward(DbContext *) const
{
    return nullptr;
}

llong MemoryFixedLenStore::append(fstring row, DbContext *)
{
    ScopeLock(true);
    assert(row.size() == m_fixlen);
    assert(m_data.size() % m_fixlen == 0);
    size_t size = m_data.size();
    m_data.append(row.begin(), row.end());
    m_recordsBasePtr = m_data.data();
    return llong(size / m_fixlen);
}

void MemoryFixedLenStore::update(llong id, fstring row, DbContext *)
{
    ScopeLock(true);
    assert(row.size() == m_fixlen);
    size_t offset = size_t(id) * m_fixlen;
    if(terark_likely(offset >= m_data.size()))
    {
        m_data.resize(offset + m_fixlen);
        m_recordsBasePtr = m_data.data();
    }
    std::memcpy(m_data.data() + offset, row.data(), m_fixlen);
}

void MemoryFixedLenStore::remove(llong id, DbContext *)
{
    ScopeLock(true);
    size_t offset = size_t(id) * m_fixlen;
    if(terark_unlikely(offset + m_fixlen == m_data.size()))
    {
        m_data.risk_set_size(offset);
    }
}

void MemoryFixedLenStore::shrinkToFit()
{
    ScopeLock(true);
    m_data.shrink_to_fit();
    m_recordsBasePtr = m_data.data();
}

AppendableStore *MemoryFixedLenStore::getAppendableStore()
{
    return this;
}

UpdatableStore *MemoryFixedLenStore::getUpdatableStore()
{
    return this;
}

WritableStore *MemoryFixedLenStore::getWritableStore()
{
    return this;
}

}}} //namespace terark { namespace db { namespace trbdb {