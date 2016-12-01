#include "db_store.hpp"
#include <terark/rank_select.hpp>
//#include "db_index.hpp"

namespace terark { namespace db {

Permanentable::Permanentable() {
}
Permanentable::~Permanentable() {
}
void Permanentable::save(PathRef) const {
	THROW_STD(invalid_argument, "This method should not be called");
}
void Permanentable::load(PathRef) {
	THROW_STD(invalid_argument, "This method should not be called");
}

StoreIterator::~StoreIterator() {
}

llong StoreIterator::seekLowerBound(llong id, valvec<byte>* val) {
	if (seekExact(id, val)) {
		return id;
	}
	llong id2 = id;
	if (increment(&id2, val)) {
		return id2;
	}
	return -1;
}

ForwardPartStoreIterator::ForwardPartStoreIterator(ReadableStore* store,
                                                   size_t baseId,
                                                   const bm_uint_t* isDel,
                                                   const rank_select_se* isPurged,
                                                   DbContext *ctx)
    : m_ctx(ctx)
    , m_where()
    , m_baseId(baseId)
    , m_isDel(isDel)
    , m_isPurged(isPurged)
{
    m_store.reset(store);
}

ForwardPartStoreIterator::~ForwardPartStoreIterator() {
}

bool ForwardPartStoreIterator::increment(llong* id, valvec<byte>* val) {
    size_t num_data_rows = size_t(m_store->numDataRows());
    while(m_where < num_data_rows)
    {
        size_t k = m_where++;
        size_t logicId = m_isPurged
            ? m_isPurged->select0(m_baseId + k)
            : m_baseId + k;
        if(!terark_bit_test(m_isDel, logicId))
        {
            m_store->getValue(k, val, m_ctx);
            *id = k;
            return true;
        }
    }
    return false;
}

bool ForwardPartStoreIterator::seekExact(llong id, valvec<byte>* val) {
    m_where = size_t(id++);
    size_t logicId = m_isPurged
        ? m_isPurged->select0(m_baseId + m_where)
        : m_baseId + m_where;
    if(!terark_bit_test(m_isDel, logicId))
    {
        m_store->getValue(m_where, val, m_ctx);
        return true;
    }
    return false;
}

void ForwardPartStoreIterator::reset() {
    m_where = 0;
};

///////////////////////////////////////////////////////////////////////////////
typedef hash_strmap< std::function<ReadableStore*(const Schema& schema)>
					, fstring_func::hash_align
					, fstring_func::equal_align
					, ValueInline, SafeCopy
					>
		StoreFactory;
static	StoreFactory& s_storeFactory() {
	static StoreFactory instance;
	return instance;
}

ReadableStore::RegisterStoreFactory::RegisterStoreFactory
(const char* fnameSuffix, const StoreFactory& f)
{
	auto ib = s_storeFactory().insert_i(fnameSuffix, f);
	assert(ib.second);
	if (!ib.second)
		THROW_STD(invalid_argument, "duplicate suffix: %s", fnameSuffix);
}

ReadableStore::ReadableStore()
    : m_recordsBasePtr()
    , m_isFreezed()
{
}

ReadableStore::~ReadableStore() {
}

llong ReadableStore::dataFileSize() const {
    return 0;
}
llong ReadableStore::dataDictSize() const {
    return 0;
}

ReadableStore* ReadableStore::openStore(const Schema& schema, PathRef segDir, fstring fname) {
	size_t sufpos = fname.size();
	while (sufpos > 0 && fname[sufpos-1] != '.') --sufpos;
	auto suffix = fname.substr(sufpos);
	size_t idx = s_storeFactory().find_i(suffix);
	if (idx < s_storeFactory().end_i()) {
		const auto& factory = s_storeFactory().val(idx);
		ReadableStore* store = factory(schema);
		assert(NULL != store);
		if (NULL == store) {
			THROW_STD(runtime_error, "store factory should not return NULL store");
		}
	//	fstring baseName = fname.substr(0, sufpos-1);
	//	auto fpath = segDir / baseName.str();
		auto fpath = segDir / fname.str();
		store->load(fpath);
		return store;
	}
	else {
		THROW_STD(invalid_argument
			, "store type '%.*s' of '%s' is not registered"
			, suffix.ilen(), suffix.data()
			, (segDir / fname.str()).string().c_str()
			);
		return NULL; // avoid compiler warning
	}
}

WritableStore* ReadableStore::getWritableStore() {
	return nullptr;
}

ReadableIndex* ReadableStore::getReadableIndex() {
	return nullptr;
}

AppendableStore* ReadableStore::getAppendableStore() {
	return nullptr;
}

UpdatableStore* ReadableStore::getUpdatableStore() {
	return nullptr;
}

void ReadableStore::setStorePath(PathRef) {
    // nothing default ...
}

void ReadableStore::deleteFiles() {
	THROW_STD(invalid_argument, "Unsupportted Method");
}

namespace {
	class DefaultStoreIterForward : public StoreIterator {
		DbContextPtr m_ctx;
		llong m_rows;
		llong m_id;
	public:
		DefaultStoreIterForward(ReadableStore* store, DbContext* ctx) {
			m_store.reset(store);
			m_ctx.reset(ctx);
			m_rows = store->numDataRows();
			m_id = 0;
		}
		bool increment(llong* id, valvec<byte>* val) override {
			if (m_id < m_rows) {
				m_store->getValue(m_id, val, m_ctx.get());
				*id = m_id++;
				return true;
			}
			return false;
		}
		bool seekExact(llong id, valvec<byte>* val) override {
			assert(id >= 0);
			m_id = id + 1;
			if (terark_likely(id < m_rows)) {
				m_store->getValue(id, val, m_ctx.get());
				return true;
			}
			fprintf(stderr, "ERROR: %s: id = %lld, rows = %lld\n"
				, BOOST_CURRENT_FUNCTION, id, m_rows);
			return false;
		}
		void reset() override {
			m_rows = m_store->numDataRows();
			m_id = 0;
		}
	};
	class DefaultStoreIterBackward : public StoreIterator {
		DbContextPtr m_ctx;
		llong m_rows;
		llong m_id;
	public:
		DefaultStoreIterBackward(ReadableStore* store, DbContext* ctx) {
			m_store.reset(store);
			m_ctx.reset(ctx);
			m_rows = store->numDataRows();
			m_id = m_rows;
		}
		bool increment(llong* id, valvec<byte>* val) override {
			if (m_id > 0) {
				m_store->getValue(m_id, val, m_ctx.get());
				*id = --m_id;
				return true;
			}
			return false;
		}
		bool seekExact(llong id, valvec<byte>* val) override {
			assert(id >= 0);
			if (terark_likely(id < m_rows)) {
				m_id = id;
				m_store->getValue(id, val, m_ctx.get());
				return true;
			}
			m_id = m_rows;
			fprintf(stderr, "ERROR: %s: id = %lld, rows = %lld\n"
				, BOOST_CURRENT_FUNCTION, id, m_rows);
			return false;
		}
		void reset() override {
			m_rows = m_store->numDataRows();
			m_id = m_rows;
		}
	};
} // namespace

StoreIterator*
ReadableStore::createDefaultStoreIterForward(DbContext* ctx) const {
	return new DefaultStoreIterForward(const_cast<ReadableStore*>(this), ctx);
}
StoreIterator*
ReadableStore::createDefaultStoreIterBackward(DbContext* ctx) const {
	return new DefaultStoreIterBackward(const_cast<ReadableStore*>(this), ctx);
}

StoreIterator*
ReadableStore::ensureStoreIterForward(DbContext* ctx) const {
	StoreIterator* iter = this->createStoreIterForward(ctx);
	if (iter)
		return iter;
	return new DefaultStoreIterForward(const_cast<ReadableStore*>(this), ctx);
}
StoreIterator*
ReadableStore::ensureStoreIterBackward(DbContext* ctx) const {
	StoreIterator* iter = this->createStoreIterBackward(ctx);
	if (iter)
		return iter;
	return new DefaultStoreIterBackward(const_cast<ReadableStore*>(this), ctx);
}

void ReadableStore::markFrozen()
{
    m_isFreezed = true;
}

///////////////////////////////////////////////////////////////////////////////

AppendableStore::~AppendableStore() {
}

///////////////////////////////////////////////////////////////////////////////

UpdatableStore::~UpdatableStore() {
}

///////////////////////////////////////////////////////////////////////////////

WritableStore::~WritableStore() {
}

///////////////////////////////////////////////////////////////////////////////

MultiPartStore::MultiPartStore() {
}

MultiPartStore::~MultiPartStore() {
}

llong MultiPartStore::dataInflateSize() const {
	size_t size = 0;
	for (auto& part : m_parts)
		size += part->dataInflateSize();
	return size;
}
llong MultiPartStore::dataStorageSize() const {
	size_t size = 0;
	for (auto& part : m_parts)
		size += part->dataStorageSize();
	return size;
}

llong MultiPartStore::numDataRows() const {
	return m_rowNumVec.back();
}

void
MultiPartStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	assert(m_parts.size() + 1 == m_rowNumVec.size());
	llong maxId = m_rowNumVec.back();
	assert(id < maxId);
	if (id >= maxId) {
		THROW_STD(out_of_range, "id %lld, maxId = %lld", id, maxId);
	}
	size_t upp = upper_bound_a(m_rowNumVec, uint32_t(id));
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	m_parts[upp-1]->getValueAppend(id - baseId, val, ctx);
}

class MultiPartStore::MyStoreIterForward : public StoreIterator {
	size_t m_partIdx = 0;
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const MultiPartStore* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<MultiPartStore*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		assert(m_partIdx < owner->m_parts.size());
		if (terark_likely(m_id < owner->m_rowNumVec[m_partIdx + 1])) {
			// do nothing
		}
		else if (m_partIdx + 1 < owner->m_parts.size()) {
			m_partIdx++;
			assert(owner->m_parts[m_partIdx]->numDataRows() > 0);
		}
		else {
			return false;
		}
		*id = m_id++;
		llong baseId = owner->m_rowNumVec[m_partIdx];
		llong subId = *id - baseId;
		owner->m_parts[m_partIdx]->getValue(subId, val, m_ctx.get());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		const auto& vec = owner->m_rowNumVec;
		assert(vec.size() >= 2);
		assert(id >= 0);
		llong rows = vec.back();
		if (terark_unlikely(id < 0)) {
			fprintf(stderr, "ERROR: %s, %s:%d: id = %lld, rows = %lld\n"
				, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, id, rows);
			return false;
		}
		size_t upp = upper_bound_0(vec.data(), vec.size()-1, id);
		llong  baseId = vec[upp-1];
		llong  subId = id - baseId;
		m_id = id+1;
		m_partIdx = upp-1;
		if (id < rows) {
			owner->m_parts[upp-1]->getValue(subId, val, m_ctx.get());
			return true;
		}
		return false;
	}
	void reset() override {
		m_partIdx = 0;
		m_id = 0;
	}
};

class MultiPartStore::MyStoreIterBackward : public StoreIterator {
	size_t m_partIdx;
	llong  m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterBackward(const MultiPartStore* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<MultiPartStore*>(owner));
		m_partIdx = owner->m_parts.size();
		m_id = owner->m_rowNumVec.back();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		if (owner->m_parts.empty()) {
			return false;
		}
		assert(m_partIdx > 0);
		if (terark_likely(m_id > owner->m_rowNumVec[m_partIdx-1])) {
			// do nothing
		}
		else if (m_partIdx > 1) {
			--m_partIdx;
		}
		else {
			return false;
		}
		*id = --m_id;
		llong baseId = owner->m_rowNumVec[m_partIdx-1];
		llong subId = *id - baseId;
		owner->m_parts[m_partIdx-1]->getValue(subId, val, m_ctx.get());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		const auto& vec = owner->m_rowNumVec;
		assert(vec.size() >= 2);
		assert(id >= 0);
		llong rows = vec.back();
		if (terark_unlikely(id < 0)) {
			fprintf(stderr, "ERROR: %s, %s:%d: id = %lld, rows = %lld\n"
				, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, id, rows);
			return false;
		}
		size_t upp = upper_bound_0(vec.data(), vec.size()-1, id);
		llong  baseId = vec[upp-1];
		llong  subId = id - baseId;
		m_partIdx = upp;
		if (id < rows) {
			m_id = id;
			owner->m_parts[upp-1]->getValue(subId, val, m_ctx.get());
			return true;
		}
		m_id = rows;
		return false;
	}
	void reset() override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		m_partIdx = owner->m_parts.size();
		m_id = owner->m_rowNumVec.back();
	}
};
StoreIterator* MultiPartStore::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, ctx);
}
StoreIterator* MultiPartStore::createStoreIterBackward(DbContext* ctx) const {
	return new MyStoreIterBackward(this, ctx);
}

void MultiPartStore::load(PathRef path) {
	fprintf(stderr
		, "FATAL: %s:%d: %s: This function should not be called\n"
		, __FILE__, __LINE__, BOOST_CURRENT_FUNCTION);
	abort();
}

void MultiPartStore::save(PathRef path) const {
	char szNum[16];
	for (size_t i = 0; i < m_parts.size(); ++i) {
		snprintf(szNum, sizeof(szNum), ".%04zd", i);
		m_parts[i]->save(path + szNum);
	}
}

void MultiPartStore::setStorePath(PathRef path) {
    char szNum[16];
	for (size_t i = 0; i < m_parts.size(); ++i) {
		snprintf(szNum, sizeof(szNum), ".%04zd", i);
		m_parts[i]->setStorePath(path + szNum);
	}
}

void MultiPartStore::addpart(ReadableStore* store) {
	assert(m_rowNumVec.empty());
	assert(store->numDataRows() > 0);
	m_parts.push_back(store);
}

void MultiPartStore::addpartIfNonEmpty(ReadableStore* store) {
	assert(m_rowNumVec.empty());
	if (store->numDataRows() > 0) {
		m_parts.push_back(store);
	}
}

ReadableStore* MultiPartStore::finishParts() {
	assert(m_parts.size() > 0);
	assert(m_rowNumVec.size() == 0);
	syncRowNumVec();
//	if (m_parts.size() == 0) {
//		return new EmptyIndexStore();
//	}
	if (m_parts.size() == 1) {
		return m_parts[0].get();
	}
	return this;
}

void MultiPartStore::syncRowNumVec() {
	// must be called only once
	assert(m_rowNumVec.empty());
	m_rowNumVec.resize_no_init(m_parts.size() + 1);
	llong rows = 0;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		assert(m_parts[i]->numDataRows() > 0);
		m_rowNumVec[i] = uint32_t(rows);
		rows += m_parts[i]->numDataRows();
	}
	m_rowNumVec.back() = uint32_t(rows);
}

/////////////////////////////////////////////////////////////////////////////

ReadRecordException::~ReadRecordException() {
}
static std::string
ReadRecordExceptionErrMessage(const char* errType, const std::string& segDir, llong baseId, llong subId) {
	char szBuf[96];
	std::string msg;
	msg.reserve(512);
	msg += errType;
	msg += " in \"";
	msg += segDir;
	sprintf(szBuf, "\", baseId = %lld, subId = %lld", baseId, subId);
	msg += szBuf;
	return msg;
}
ReadRecordException::ReadRecordException(const char* errType, const std::string& segDir, llong baseId, llong subId)
  : DbException(ReadRecordExceptionErrMessage(errType, segDir, baseId, subId))
{
	m_segDir = segDir;
	m_baseId = baseId;
	m_subId = subId;
}
ReadRecordException::ReadRecordException(const ReadRecordException&) = default;
ReadRecordException& ReadRecordException::operator=(const ReadRecordException&) = default;

ReadDeletedRecordException::ReadDeletedRecordException(const std::string& segDir, llong baseId, llong subId)
  : ReadRecordException("ReadDeletedRecordException", segDir, baseId, subId)
{}
ReadUncommitedRecordException::ReadUncommitedRecordException(const std::string& segDir, llong baseId, llong subId)
  : ReadRecordException("ReadUncommitedRecordException", segDir, baseId, subId)
{}

} } // namespace terark::db
