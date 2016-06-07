/*-
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "leveldb_terark.h"
#include <errno.h>
#include <sstream>
#include <stdint.h>
#include <terark/stdtypes.hpp>
#include <terark/num_to_str.hpp>

//using namespace terark;
using terark::string_appender;

static std::string escape(terark::fstring x) {
	std::string y;
	y.reserve(2*x.size());
	for (size_t i = 0; i < x.size(); ++i) {
		unsigned char c = x[i];
		switch (c) {
		default: y.push_back(c); break;
		case '\0': y.append("\\0"); break;
		case '\1': y.append("\\1"); break;
		case '\2': y.append("\\2"); break;
		case '\3': y.append("\\3"); break;
		case '\4': y.append("\\4"); break;
		case '\5': y.append("\\5"); break;
		case '\6': y.append("\\6"); break;
		case '\7': y.append("\\7"); break;
		case '\x08': y.append("\\x08"); break;
		case '\x0B': y.append("\\x0B"); break;
		case '\x0C': y.append("\\x0C"); break;
		case '\x0E': y.append("\\x0E"); break;
		case '\x0F': y.append("\\x0F"); break;
		case '\x10': y.append("\\x10"); break;
		case '\x11': y.append("\\x11"); break;
		case '\x12': y.append("\\x12"); break;
		case '\x13': y.append("\\x13"); break;
		case '\x14': y.append("\\x14"); break;
		case '\x15': y.append("\\x15"); break;
		case '\x16': y.append("\\x16"); break;
		case '\x17': y.append("\\x17"); break;
		case '\x18': y.append("\\x18"); break;
		case '\x19': y.append("\\x19"); break;
		case '\x1A': y.append("\\x1A"); break;
		case '\x1B': y.append("\\x1B"); break;
		case '\x1C': y.append("\\x1C"); break;
		case '\x1D': y.append("\\x1D"); break;
		case '\x1E': y.append("\\x1E"); break;
		case '\x1F': y.append("\\x1F"); break;
		case '\\': y.append("\\\\"); break;
		case '\t': y.append("\\t"); break;
		case '\r': y.append("\\r"); break;
		case '\n': y.append("\\n"); break;
		}
	}
	return y;
}

#if !defined(NDEBUG)
#define TRACE_KEY_VAL(key, val) \
  fprintf(stderr \
    , "TRACE: teark-db-leveldb-api: dbdir=%s : %s: key=[%zd: %s] val=[%zd: %s]\n" \
    , m_tab->getDir().string().c_str() \
    , BOOST_CURRENT_FUNCTION \
    , key.size(), escape(key).c_str() \
    , val.size(), escape(val).c_str() \
    )

#define TRACE_CMP_KEY_VAL() \
  fprintf(stderr \
    , "TRACE: teark-db-leveldb-api: dbdir=%s : %s: cmp=%d, posKey=[%zd: %s] key=[%zd: %s] val=[%zd: %s]\n" \
    , m_tab->getDir().string().c_str() \
    , BOOST_CURRENT_FUNCTION, cmp \
    , m_posKey.size(), escape(m_posKey).c_str() \
    , m_key.size(), escape(m_key).c_str() \
    , m_val.size(), escape(m_val).c_str() \
    )

#else
  #define TRACE_KEY_VAL(key, val)
  #define TRACE_CMP_KEY_VAL()
#endif

namespace leveldb {

#if HAVE_BASHOLEVELDB
Value::~Value() {}

class StringValue : public Value {
 public:
  explicit StringValue(std::string& val) : value_(val) {}
  ~StringValue() {}

  StringValue& assign(const char* data, size_t size) {
    value_.assign(data, size);
    return *this;
  }

 private:
  std::string& value_;
};
#endif

Status DestroyDB(const std::string& name, const Options& options) {
  fs::path dbdir = fs::path(name) / "TerarkDB";
  if (!fs::exists(dbdir))
    return Status::OK();
  fs::remove_all(dbdir);
  return Status::OK();
}

Status RepairDB(const std::string& dbname, const Options& options) {
  return Status::NotSupported("sorry!");
}

/* Destructors required for interfaces. */
DB::~DB() {}

#ifdef HAVE_ROCKSDB
bool DB::KeyMayExist(const ReadOptions&,
                         ColumnFamilyHandle*, const Slice&,
                         std::string*, bool* value_found = NULL) {
  if (value_found != NULL) {
    *value_found = false;
  }
  return true;
}
#endif

} // namespace leveldb

Snapshot::~Snapshot() {}

/* Iterators, from leveldb/table/iterator.cc */
Iterator::Iterator() {
  cleanup_.function = NULL;
  cleanup_.next = NULL;
}

Iterator::~Iterator() {
  if (cleanup_.function != NULL) {
    (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
    for (Cleanup* c = cleanup_.next; c != NULL; ) {
      (*c->function)(c->arg1, c->arg2);
      Cleanup* next = c->next;
      delete c;
      c = next;
    }
  }
}

void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != NULL);
  Cleanup* c;
  if (cleanup_.function == NULL) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

namespace {
class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) { }
  virtual bool Valid() const { return false; }
  virtual void Seek(const Slice& target) { }
  virtual void SeekToFirst() { }
  virtual void SeekToLast() { }
  virtual void Next() { assert(false); }
  virtual void Prev() { assert(false); }
  Slice key() const { assert(false); return Slice(); }
  Slice value() const { assert(false); return Slice(); }
  virtual Status status() const { return status_; }
 private:
  Status status_;
};
}  // namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

namespace {
class FilterPolicyImpl : public FilterPolicy {
public:
  FilterPolicyImpl(int bits_per_key) : bits_per_key_(bits_per_key) {}
  ~FilterPolicyImpl() {}
  virtual const char *Name() const { return "FilterPolicyImpl"; }
  virtual void CreateFilter(const Slice *keys, int n, std::string *dst) const {}
  virtual bool KeyMayMatch(const Slice &key, const Slice &filter) const { return true; }

  int bits_per_key_;
};
};

namespace leveldb {
FilterPolicy::~FilterPolicy() {}

const FilterPolicy *NewBloomFilterPolicy(int bits_per_key) {
  return new FilterPolicyImpl(bits_per_key);
}
#if HAVE_BASHOLEVELDB
const FilterPolicy *NewBloomFilterPolicy2(int bits_per_key) {
  return NewBloomFilterPolicy(bits_per_key);
}
#endif

Cache::~Cache() {}
Cache *NewLRUCache(size_t capacity) {
  return new CacheImpl(capacity);
}

static const char g_keyValueSchema[] = R"({
  "RowSchema" : {
    "columns" : {
      "key": { "type": "carbin" },
      "val": { "type": "carbin" }
    }
  },
  "TableIndex": [
    {
       "fields": "key",
       "unique": true
    }
  ],
  "MinMergeSegNum": 3
}
)";
Status
DB::Open(const Options &options, const std::string &name, leveldb::DB** dbptr) {
	fs::path dbdir = fs::path(name) / "TerarkDB";
	fs::path metaPath = dbdir / "dbmeta.json";
	if (fs::exists(dbdir)) {
		if (options.error_if_exists) {
			fprintf(stderr, "ERROR: options.error_if_exists: %s\n", dbdir.string().c_str());
			return Status::InvalidArgument("Database have existed", dbdir.string());
		}
	}
	else if (options.create_if_missing) {
		if (!fs::exists(dbdir)) {
			fs::create_directories(dbdir);
		}
		if (!fs::exists(metaPath)) {
			WriteStringToFile(Env::Default(), g_keyValueSchema, metaPath.string());
		}
	}
	if (!fs::exists(metaPath)) {
		fprintf(stderr, "ERROR: not exists: %s\n", metaPath.string().c_str());
		return Status::InvalidArgument("dbmeta.json is missing", dbdir.string());
	}
	try {
		*dbptr = new DbImpl(dbdir);
		return Status::OK();
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: caught exception: %s for dbdir: %s\n"
				, ex.what(), dbdir.string().c_str());
		return Status::InvalidArgument("DbTable::open failed", ex.what());
	}
}
} // namespace leveldb

DbImpl::DbImpl(const fs::path& dbdir) {
	m_tab = terark::db::DbTable::open(dbdir);
}

DbImpl::~DbImpl() {
  // m_tab destruct must after m_ctx destruct
  BOOST_STATIC_ASSERT(offsetof(DbImpl, m_tab) < offsetof(DbImpl, m_ctx));
}

static void
encodeKeyVal(terark::valvec<unsigned char>& buf,
			 const Slice& key, const Slice& val) {
	buf.erase_all();
	aligned_save(buf.grow_no_init(4), uint32_t(key.size()));
	buf.append((unsigned char*)key.begin(), key.size());
//	unaligned_save(buf.grow_no_init(4), uint32_t(val.size()));
	buf.append((unsigned char*)val.begin(), val.size());
};

// Set the database entry for "key" to "value".  Returns OK on success,
// and a non-OK status on error.
// Note: consider setting options.sync = true.
Status
DbImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  terark::db::DbContext* ctx = GetDbContext();
  assert(NULL != ctx);
  try {
	  TRACE_KEY_VAL(key, value);
	  encodeKeyVal(ctx->userBuf, key, value);
	  long long recId = ctx->upsertRow(ctx->userBuf);
	  TERARK_RT_assert(recId >= 0, std::logic_error);
	  return Status::OK();
  }
  catch (const std::exception& ex) {
	  return Status::Corruption("DbTable::upsertRow failed", ex.what());
  }
}

// Remove the database entry (if any) for "key".  Returns OK on
// success, and a non-OK status on error.  It is not an error if "key"
// did not exist in the database.
// Note: consider setting options.sync = true.
Status
DbImpl::Delete(const WriteOptions& options, const Slice& key)
{
  terark::db::DbContext* ctx = GetDbContext();
  assert(NULL != ctx);
  ctx->indexSearchExact(0, key, &ctx->exactMatchRecIdvec);
  if (!ctx->exactMatchRecIdvec.empty()) {
	  auto recId = ctx->exactMatchRecIdvec[0];
	  try {
		  ctx->removeRow(recId);
	  }
	  catch (const std::exception&) {

	  }
  }
  return Status::OK();
}

void
WriteBatchHandler::Put(const Slice& key, const Slice& value) {
//	THROW_STD(invalid_argument, "Not supported");
	auto opctx = context_;
	terark::db::DbContext* ctx = opctx->m_batchWriter.getCtx();
	encodeKeyVal(ctx->userBuf, key, value);
	opctx->m_batchWriter.upsertRow(ctx->userBuf);
}

void WriteBatchHandler::Delete(const Slice& key) {
//	THROW_STD(invalid_argument, "Not supported");
	auto opctx = context_;
	terark::db::DbContext* ctx = opctx->m_batchWriter.getCtx();
	ctx->indexSearchExact(0, key, &ctx->exactMatchRecIdvec);
	if (!ctx->exactMatchRecIdvec.empty()){
		long long recId = ctx->exactMatchRecIdvec[0];
		opctx->m_batchWriter.removeRow(recId);
	}
	else {
		fprintf(stderr, "ERROR: Delete(key = %s), NotFound\n", escape(key).c_str());
	}
}

// Apply the specified updates to the database.
// Returns OK on success, non-OK on failure.
// Note: consider setting options.sync = true.
Status
DbImpl::Write(const WriteOptions& options, WriteBatch* updates) {
//	return Status::NotSupported("Batch write is not supported");
  Status status = Status::OK();
  std::unique_ptr<OperationContext> context(GetContext());
  WriteBatchHandler handler(this, context.get());
#if 0
  status = updates->Iterate(&handler);
#else
  try {
    status = updates->Iterate(&handler);
  } catch(...) {
    context->m_batchWriter.rollback();
    throw;
  }
#endif
  if (context->m_batchWriter.commit()) {
	return status;
  }
  else {
	return Status::InvalidArgument("Commit BatchWriter failed", context->m_batchWriter.strError());
  }
}

// If the database contains an entry for "key" store the
// corresponding value in *value and return OK.
//
// If there is no entry for "key" leave *value unchanged and return
// a status for which Status::IsNotFound() returns true.
//
// May return some other Status on an error.
Status
DbImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  terark::db::DbContext* ctx = GetDbContext();
  assert(NULL != ctx);
  ctx->indexSearchExact(0, key, &ctx->exactMatchRecIdvec);
  if (!ctx->exactMatchRecIdvec.empty()) {
	  auto recId = ctx->exactMatchRecIdvec[0];
	  try {
		  ctx->selectOneColgroup(recId, 1, &ctx->userBuf);
	//	  fprintf(stderr
	//		, "DEBUG: recId=%lld, colgroup[1]={size=%zd, content=%.*s}\n"
	//		, recId, ctx->userBuf.size(), (int)ctx->userBuf.size(), ctx->userBuf.data());
		  value->resize(0);
		  value->append((char*)ctx->userBuf.data(), ctx->userBuf.size());
		  return Status::OK();
	  }
	  catch (const std::exception&) {
	  }
  }
  return Status::NotFound(key);
}

#if HAVE_BASHOLEVELDB
// If the database contains an entry for "key" store the
// corresponding value in *value and return OK.
//
// If there is no entry for "key" leave *value unchanged and return
// a status for which Status::IsNotFound() returns true.
//
// May return some other Status on an error.
Status
DbImpl::Get(const ReadOptions& options, const Slice& key, Value* value) {
  const char *errmsg = NULL;

  WT_CURSOR *cursor = GetContext(options)->GetCursor();
  WT_ITEM item;
  item.data = key.data();
  item.size = key.size();
  cursor->set_key(cursor, &item);
  int ret = cursor->search(cursor);
  if (ret == 0) {
    ret = cursor->get_value(cursor, &item);
    if (ret == 0) {
      // This call makes a copy, reset the cursor afterwards.
      value->assign((const char *)item.data, item.size);
      ret = cursor->reset(cursor);
    }
  } else if (ret == WT_NOTFOUND)
    errmsg = "DB::Get key not found";
err:
  return WiredTigerErrorToStatus(ret, errmsg);
}
#endif

// Return a heap-allocated iterator over the contents of the database.
// The result of NewIterator() is initially invalid (caller must
// call one of the Seek methods on the iterator before using it).
//
// Caller should delete the iterator when it is no longer needed.
// The returned iterator should be deleted before this db is deleted.
Iterator*
DbImpl::NewIterator(const ReadOptions& options) {
	return new IteratorImpl(m_tab.get());
}

SnapshotImpl::SnapshotImpl(DbImpl *db) :
    Snapshot(), db_(db), context_(db->NewContext()), status_(Status::OK())
{
}

// Return a handle to the current DB state.  Iterators created with
// this handle will all observe a stable snapshot of the current DB
// state.  The caller must call ReleaseSnapshot(result) when the
// snapshot is no longer needed.
const Snapshot* DbImpl::GetSnapshot() {
  THROW_STD(invalid_argument, "terark-db-leveldb-api doesn't support snapshot");
  return NULL;
}

// Release a previously acquired snapshot.  The caller must not
// use "snapshot" after this call.
void
DbImpl::ReleaseSnapshot(const Snapshot* snapshot)
{
  THROW_STD(invalid_argument, "terark-db-leveldb-api doesn't support snapshot");
  SnapshotImpl *si =
    static_cast<SnapshotImpl*>(const_cast<Snapshot*>(snapshot));
  if (si != NULL) {
    // We started a transaction: we could commit it here, but it will be rolled
    // back automatically by closing the session, which we have to do anyway.
    int ret = si->GetContext()->Close();
    TERARK_RT_assert(ret == 0, std::logic_error);
    delete si;
  }
}

// DB implementations can export properties about their state
// via this method.  If "property" is a valid property understood by this
// DB implementation, fills "*value" with its current value and returns
// true.  Otherwise returns false.
//
//
// Valid property names include:
//
//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
//     where <N> is an ASCII representation of a level number (e.g. "0").
//  "leveldb.stats" - returns a multi-line string that describes statistics
//     about the internal operation of the DB.
//  "leveldb.sstables" - returns a multi-line string that describes all
//     of the sstables that make up the db contents.
bool
DbImpl::GetProperty(const Slice& property, std::string* value)
{
  /* Not supported */
  return false;
}

// For each i in [0,n-1], store in "sizes[i]", the approximate
// file system space used by keys in "[range[i].start .. range[i].limit)".
//
// Note that the returned sizes measure file system space usage, so
// if the user data compresses by a factor of ten, the returned
// sizes will be one-tenth the size of the corresponding user data size.
//
// The results may not include the sizes of recently written data.
void
DbImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes)
{
  int i;

  /* XXX Not supported */
  for (i = 0; i < n; i++)
    sizes[i] = 1;
}

// Compact the underlying storage for the key range [*begin,*end].
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data.  This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// begin==NULL is treated as a key before all keys in the database.
// end==NULL is treated as a key after all keys in the database.
// Therefore the following call will compact the entire database:
//    db->CompactRange(NULL, NULL);
void
DbImpl::CompactRange(const Slice* begin, const Slice* end)
{
  // The compact doesn't need a cursor, but the context always opens a
  // cursor when opening the session - so grab that, and use the session.
  fprintf(stderr, "INFO: %s\n", BOOST_CURRENT_FUNCTION);
  m_tab->compact();
}

// Suspends the background compaction thread.  This methods
// returns once suspended.
void DbImpl::SuspendCompactions()
{
  /* Not supported */
}

// Resumes a suspended background compaction thread.
void DbImpl::ResumeCompactions()
{
  /* Not supported */
}

OperationContext* DbImpl::GetContext() {
	return new OperationContext(m_tab.get(), GetDbContext());
}

OperationContext* DbImpl::GetContext(const ReadOptions &options) {
  if (options.snapshot == NULL) {
    return GetContext();
  }
  THROW_STD(invalid_argument,
    "terark-db-leveldb-api doesn't support snapshot, "
    "ReadOptions.snapshot should alsway be NULL"
    );
  auto si = static_cast<const SnapshotImpl*>(options.snapshot);
  assert(si->GetStatus().ok());
  return si->GetContext();
}

terark::db::DbContext* DbImpl::GetDbContext() {
#if 0
  terark::db::MyRwLock lock(m_ctxMapRwMutex, false);
  std::thread::id tid = std::this_thread::get_id();
  size_t idx = m_ctxMap.find_i(tid);
  if (idx < m_ctxMap.end_i()) {
	  return m_ctxMap.val(idx).get();
  }
  lock.upgrade_to_writer();
  idx = m_ctxMap.insert_i(tid).first;
  auto& refctx = m_ctxMap.val(idx);
#else
  DbContextPtr& refctx = m_ctx.local();
#endif
  if (!refctx) {
	  refctx.reset(m_tab->createDbContext());
#if !defined(NDEBUG)
	  fprintf(stderr, "DEBUG: thread DbContext object number = %zd\n", m_ctx.size());
#endif
  }
  return refctx.get();
}

std::atomic<size_t> g_iterLiveCnt;
std::atomic<size_t> g_iterCreatedCnt;

IteratorImpl::IteratorImpl(terark::db::DbTable *db) {
	m_tab = db;
	m_ctx = db->createDbContext();
	m_recId = -1;
	m_valid = false;
	m_direction = Direction::forward;
	g_iterLiveCnt++;
	g_iterCreatedCnt++;
#if !defined(NDEBUG)
  fprintf(stderr
    , "DEBUG: teark-db-leveldb-api: dbdir=%s : %s : Iterator live count = %zd, created = %zd\n"
    , db->getDir().string().c_str(), BOOST_CURRENT_FUNCTION
    , g_iterLiveCnt.load(), g_iterCreatedCnt.load());
#endif
}

IteratorImpl::~IteratorImpl() {
	m_iter = nullptr;
	g_iterLiveCnt--;
}

void IteratorImpl::iterIncrement() {
	m_valid = m_iter->increment(&m_recId, &m_key);
	while (m_valid) {
		try {
			m_tab->selectOneColgroup(m_recId, 1, &m_val, m_ctx.get());
			break;
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: %s: what=%s\n", BOOST_CURRENT_FUNCTION, ex.what());
			m_valid = m_iter->increment(&m_recId, &m_key);
		}
	}
}


// Position at the first key in the source.  The iterator is Valid()
// after this call iff the source is not empty.
void
IteratorImpl::SeekToFirst() {
	if (Direction::forward != m_direction) {
		m_iter = nullptr;
		m_direction = Direction::forward;
	}
	if (!m_iter) {
		m_iter = m_tab->createIndexIterForward(0);
	}
	m_iter->reset();
	iterIncrement();
	TRACE_KEY_VAL(m_key, m_val);
}

// Position at the last key in the source.  The iterator is
// Valid() after this call iff the source is not empty.
void
IteratorImpl::SeekToLast() {
	if (Direction::backward != m_direction) {
		m_iter = nullptr;
		m_direction = Direction::backward;
	}
	if (!m_iter) {
		m_iter = m_tab->createIndexIterBackward(0);
	}
	m_iter->reset();
	iterIncrement();
	TRACE_KEY_VAL(m_key, m_val);
}

// Position at the first key in the source that at or past target
// The iterator is Valid() after this call iff the source contains
// an entry that comes at or past target.
void
IteratorImpl::Seek(const Slice& target) {
	if (Direction::backward == m_direction) {
		if (!m_iter) {
			m_iter = m_tab->createIndexIterBackward(0);
		}
	//	fprintf(stderr, "DEBUG: %s: direction=backward\n", BOOST_CURRENT_FUNCTION);
	}
	else {
		if (!m_iter) {
			m_iter = m_tab->createIndexIterForward(0);
		}
	//	fprintf(stderr, "DEBUG: %s: direction=forward\n", BOOST_CURRENT_FUNCTION);
	}
	int cmp = m_iter->seekLowerBound(target, &m_recId, &m_key);
	if (cmp < 0) {
		m_valid = false;
	}
	else {
		m_tab->selectOneColgroup(m_recId, 1, &m_val, m_ctx.get());
		m_valid = true;
	}
	TRACE_KEY_VAL(m_key, m_val);
}

// Moves to the next entry in the source.  After this call, Valid() is
// true iff the iterator was not positioned at the last entry in the source.
// REQUIRES: Valid()
void
IteratorImpl::Next() {
	assert(m_valid);
	assert(m_iter != nullptr);
	if (Direction::forward == m_direction) {
		iterIncrement();
		TRACE_KEY_VAL(m_key, m_val);
	}
	else {
		m_iter = m_tab->createIndexIterForward(0);
		m_direction = Direction::forward;
		m_posKey.swap(m_key);
		int cmp = m_iter->seekLowerBound(m_posKey, &m_recId, &m_key);
		TRACE_CMP_KEY_VAL();
		if (cmp < 0) {
			m_valid = false;
		}
		else if (0 == cmp) {
			iterIncrement();
			TRACE_KEY_VAL(m_key, m_val);
		}
		else try {
			m_tab->selectOneColgroup(m_recId, 1, &m_val, m_ctx.get());
			m_valid = true;
			TRACE_KEY_VAL(m_key, m_val);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: %s: what=%s\n", BOOST_CURRENT_FUNCTION, ex.what());
			iterIncrement();
		}
	}
}

// Moves to the previous entry in the source.  After this call, Valid() is
// true iff the iterator was not positioned at the first entry in source.
// REQUIRES: Valid()
void
IteratorImpl::Prev() {
	assert(m_valid);
	assert(m_iter != nullptr);
	if (Direction::backward == m_direction) {
		iterIncrement();
		TRACE_KEY_VAL(m_key, m_val);
	}
	else {
		m_iter = m_tab->createIndexIterBackward(0);
		m_direction = Direction::backward;
		m_posKey.swap(m_key);
		int cmp = m_iter->seekLowerBound(m_posKey, &m_recId, &m_key);
		TRACE_CMP_KEY_VAL();
		if (cmp < 0) {
			m_valid = false;
		}
		else if (0 == cmp) {
			iterIncrement();
			TRACE_KEY_VAL(m_key, m_val);
		}
		else try {
			m_tab->selectOneColgroup(m_recId, 1, &m_val, m_ctx.get());
			m_valid = true;
			TRACE_KEY_VAL(m_key, m_val);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: %s: what=%s\n", BOOST_CURRENT_FUNCTION, ex.what());
			iterIncrement();
		}
	}
}

