#include "db_table.hpp"
#include "db_segment.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/util/linebuf.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>
#include <nark/util/fstrvec.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/scope_exit.hpp>
#include <thread> // for std::this_thread::sleep_for
#include <tbb/task.h>
#include <tbb/tbb_thread.h>
#include <nark/util/concurrent_queue.hpp>

//#undef min
//#undef max

namespace nark { namespace db {

namespace fs = boost::filesystem;

const size_t DEFAULT_maxSegNum = 4095;

///////////////////////////////////////////////////////////////////////////////

CompositeTable::CompositeTable() {
	m_tableScanningRefCount = 0;
	m_tobeDrop = false;
	m_segments.reserve(DEFAULT_maxSegNum);
	m_rowNumVec.reserve(DEFAULT_maxSegNum+1);
	m_mergeSeqNum = 0;
	m_newWrSegNum = 0;
	m_bgTaskNum = 0;
//	m_ctxListHead = new DbContextLink();
}

CompositeTable::~CompositeTable() {
	if (m_tobeDrop) {
		// should delete m_dir?
		fs::remove_all(m_dir);
		return;
	}
	flush();
	m_segments.clear();
//	removeStaleDir(m_dir, m_mergeSeqNum);
//	if (m_wrSeg)
//		m_wrSeg->flushSegment();
/*
	// list must be empty: has only the dummy head
	FEBIRD_RT_assert(m_ctxListHead->m_next == m_ctxListHead, std::logic_error);
	FEBIRD_RT_assert(m_ctxListHead->m_prev == m_ctxListHead, std::logic_error);
	delete m_ctxListHead;
*/
}

// msvc std::function is not memmovable, use SafeCopy
static
hash_strmap < std::function<CompositeTable*()>
			, fstring_func::hash_align
			, fstring_func::equal_align
			, ValueInline, SafeCopy
			>
s_tableFactory;
CompositeTable::RegisterTableClass::RegisterTableClass
(fstring tableClass, const std::function<CompositeTable*()>& f)
{
	auto ib = s_tableFactory.insert_i(tableClass, f);
	assert(ib.second);
	if (!ib.second) {
		THROW_STD(invalid_argument, "duplicate suffix: %.*s",
			tableClass.ilen(), tableClass.data());
	}
}

CompositeTable* CompositeTable::createTable(fstring tableClass) {
	size_t idx = s_tableFactory.find_i(tableClass);
	if (idx >= s_tableFactory.end_i()) {
		THROW_STD(invalid_argument, "tableClass = '%.*s' is not registered",
			tableClass.ilen(), tableClass.data());
	}
	const auto& factory = s_tableFactory.val(idx);
	CompositeTable* table = factory();
	assert(table);
	return table;
}

void
CompositeTable::init(PathRef dir, SchemaConfigPtr schema) {
	assert(!dir.empty());
	assert(schema->columnNum() > 0);
	assert(schema->getIndexNum() > 0);
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	m_schema = schema;
	m_dir = dir;
	m_mergeSeqNum = 0;

	m_wrSeg = this->myCreateWritableSegment(getSegPath("wr", 0));
	m_segments.push_back(m_wrSeg);
	m_rowNumVec.erase_all();
	m_rowNumVec.push_back(0);
}

static void removeStaleDir(PathRef root, size_t inUseMergeSeq) {
	for (auto& x : fs::directory_iterator(root)) {
		std::string mergeDir = x.path().filename().string();
		size_t mergeSeq = -1;
		if (sscanf(mergeDir.c_str(), "g-%04zd", &mergeSeq) == 1) {
			if (mergeSeq != inUseMergeSeq) {
				fprintf(stderr, "INFO: Remove stale dir: %s\n"
					, x.path().string().c_str());
				try { fs::remove_all(x.path()); }
				catch (const std::exception& ex) {
					fprintf(stderr, "ERROR: ex.what = %s\n", ex.what());
				}
			}
		}
	}
}

void CompositeTable::load(PathRef dir) {
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	if (m_schema) {
		THROW_STD(invalid_argument, "Invalid: schema.columnNum=%ld is not empty",
			long(m_schema->columnNum()));
	}
	m_dir = dir;
	{
		fs::path jsonFile = fs::path(m_dir) / "dbmeta.json";
		m_schema.reset(new SchemaConfig());
		m_schema->loadJsonFile(jsonFile.string());
	}
	long mergeSeq = -1;
	for (auto& x : fs::directory_iterator(m_dir)) {
		std::string mergeDir = x.path().filename().string();
		long mergeSeq2 = -1;
		if (sscanf(mergeDir.c_str(), "g-%04ld", &mergeSeq2) == 1) {
			if (mergeSeq < mergeSeq2)
				mergeSeq = mergeSeq2;
		}
	}
	if (mergeSeq < 0) {
		m_mergeSeqNum = 0;
		fs::create_directories(getMergePath(m_dir, 0));
	}
	else {
		m_mergeSeqNum = mergeSeq;
	}
	fs::path mergeDir = getMergePath(m_dir, m_mergeSeqNum);
	SortableStrVec segDirList;
	for (auto& x : fs::directory_iterator(mergeDir)) {
		std::string segDir = x.path().string();
		std::string fname = x.path().filename().string();
		fstring fstr = fname;
		if (fstr.endsWith(".tmp")) {
			fprintf(stderr, "WARN: Temporary segment: %s, remove it\n", segDir.c_str());
			fs::remove_all(segDir);
			continue;
		}
		if (fstr.startsWith("wr-") || fstr.startsWith("rd-")) {
			segDirList.push_back(fname);
		} else {
			fprintf(stderr, "WARN: Skip unknown dir: %s\n", segDir.c_str());
		}
	}
	segDirList.sort();
	for (size_t i = 0; i < segDirList.size(); ++i) {
		std::string fname = segDirList[i].str();
		fs::path    segDir = mergeDir / fname;
		std::string strDir = segDir.string();
		long segIdx = -1;
		ReadableSegmentPtr seg;
		if (sscanf(fname.c_str(), "wr-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			if (fs::is_symlink(segDir)) {
				fs::path target = fs::canonical(fs::read_symlink(segDir), mergeDir);
				fprintf(stderr
					, "WARN: writable segment: %s is symbol link to: %s, reduce it\n"
					, strDir.c_str(), target.string().c_str());
				fs::remove(segDir);
				if (fs::exists(target))
					fs::rename(target, segDir);
			}
			auto rDir = getSegPath("rd", segIdx);
			if (fs::exists(rDir)) {
				fprintf(stdout, "INFO: readonly segment: %s existed for writable seg: %s, remove it\n"
					, rDir.string().c_str(), strDir.c_str());
				fs::remove_all(segDir);
				continue;
			}
			fprintf(stdout, "INFO: loading segment: %s ... ", strDir.c_str());
			fflush(stdout);
			auto wseg = openWritableSegment(segDir);
			wseg->m_segDir = segDir;
			seg = wseg;
			fprintf(stdout, "done!\n");
		}
		else if (sscanf(fname.c_str(), "rd-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			seg = myCreateReadonlySegment(segDir);
			assert(seg);
			fprintf(stdout, "INFO: loading segment: %s ... ", strDir.c_str());
			fflush(stdout);
			seg->load(seg->m_segDir);
			fprintf(stdout, "done!\n");
		}
		if (m_segments.size() <= size_t(segIdx)) {
			m_segments.resize(segIdx + 1);
		}
		assert(seg);
		m_segments[segIdx] = seg;
	}
	for (size_t i = 0; i < m_segments.size(); ++i) {
		if (m_segments[i] == nullptr) {
			THROW_STD(invalid_argument, "ERROR: missing segment: %s\n",
				getSegPath("xx", i).string().c_str());
		}
		if (i < m_segments.size()-1 && m_segments[i]->getWritableStore()) {
			this->putToCompressionQueue(i);
		}
	}
	fprintf(stderr, "INFO: CompositeTable::load(%s): loaded %zd segs\n",
		dir.string().c_str(), m_segments.size());
	removeStaleDir(m_dir, m_mergeSeqNum);
	if (m_segments.size() == 0 || !m_segments.back()->getWritableStore()) {
		// THROW_STD(invalid_argument, "no any segment found");
		// allow user create an table dir which just contains json meta file
		AutoGrownMemIO buf;
		size_t segIdx = m_segments.size();
		m_wrSeg = myCreateWritableSegment(getSegPath("wr", segIdx));
		m_segments.push_back(m_wrSeg);
	}
	else {
		auto seg = dynamic_cast<WritableSegment*>(m_segments.back().get());
		assert(NULL != seg);
		m_wrSeg.reset(seg); // old wr seg at end
	}
	m_rowNumVec.resize_no_init(m_segments.size() + 1);
	llong baseId = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		m_rowNumVec[i] = baseId;
		baseId += m_segments[i]->numDataRows();
	}
	m_rowNumVec.back() = baseId; // the end guard
}

size_t CompositeTable::getWritableSegNum() const {
	MyRwLock lock(m_rwMutex, false);
	size_t wrNum = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		if (m_segments[i]->getWritableStore())
			wrNum++;
	}
	return wrNum;
}

struct CompareBy_baseId {
	template<class T>
	typename boost::enable_if_c<(sizeof(((T*)0)->baseId) >= 4), bool>::type
	operator()(const T& x, llong y) const { return x.baseId < y; }
	template<class T>
	typename boost::enable_if_c<(sizeof(((T*)0)->baseId) >= 4), bool>::type
	operator()(llong x, const T& y) const { return x < y.baseId; }
	bool operator()(llong x, llong y) const { return x < y; }
};

class CompositeTable::MyStoreIterBase : public StoreIterator {
protected:
	size_t m_segIdx;
	DbContextPtr m_ctx;
	struct OneSeg {
		ReadableSegmentPtr seg;
		StoreIteratorPtr   iter;
		llong  baseId;
	};
	valvec<OneSeg> m_segs;

	void init(const CompositeTable* tab, DbContext* ctx) {
		this->m_store.reset(const_cast<CompositeTable*>(tab));
		this->m_ctx.reset(ctx);
	// MyStoreIterator creation is rarely used, lock it by m_rwMutex
		MyRwLock lock(tab->m_rwMutex, false);
		m_segs.resize(tab->m_segments.size() + 1);
		for (size_t i = 0; i < m_segs.size()-1; ++i) {
			ReadableSegment* seg = tab->m_segments[i].get();
			m_segs[i].seg = seg;
		//	m_segs[i].iter = createSegStoreIter(seg);
			m_segs[i].baseId = tab->m_rowNumVec[i];
		}
		m_segs.back().baseId = tab->m_rowNumVec.back();
		lock.upgrade_to_writer();
		tab->m_tableScanningRefCount++;
		assert(tab->m_segments.size() > 0);
	}

	~MyStoreIterBase() {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		{
			MyRwLock lock(tab->m_rwMutex, true);
			tab->m_tableScanningRefCount--;
		}
	}

	bool syncTabSegs() {
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		MyRwLock lock(tab->m_rwMutex, false);
		assert(m_segs.size() <= tab->m_segments.size() + 1);
		if (m_segs.size() == tab->m_segments.size() + 1) {
			// there is no new segments
			llong oldmaxId = m_segs.back().baseId;
			if (tab->m_rowNumVec.back() == oldmaxId)
				return false; // no new records
			// records may be 'pop_back'
			m_segs.back().baseId = tab->m_rowNumVec.back();
			return tab->m_rowNumVec.back() > oldmaxId;
		}
		size_t oldsize = m_segs.size() - 1;
		m_segs.resize(tab->m_segments.size() + 1);
		for (size_t i = oldsize; i < m_segs.size() - 1; ++i) {
			m_segs[i].seg = tab->m_segments[i];
			m_segs[i].baseId = tab->m_rowNumVec[i];
		}
		m_segs.back().baseId = tab->m_rowNumVec.back();
		return true;
	}

	void resetIterBase() {
		syncTabSegs();
		for (size_t i = 0; i < m_segs.size()-1; ++i) {
			if (m_segs[i].iter)
				m_segs[i].iter->reset();
		}
		m_segs.ende(1).baseId = m_segs.ende(2).baseId +
								m_segs.ende(2).seg->numDataRows();
	}

	std::pair<size_t, bool> seekExactImpl(llong id, valvec<byte>* val) {
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		do {
			size_t upp = upper_bound_a(m_segs, id, CompareBy_baseId());
			if (upp < m_segs.size()) {
				llong subId = id - m_segs[upp-1].baseId;
				auto cur = &m_segs[upp-1];
				MyRwLock lock(tab->m_rwMutex, false);
				if (!cur->seg->m_isDel[subId]) {
					resetOneSegIter(cur);
					return std::make_pair(upp, cur->iter->seekExact(subId, val));
				}
			}
		} while (syncTabSegs());
		return std::make_pair(m_segs.size()-1, false);
	}

	void resetOneSegIter(OneSeg* x) {
		if (x->iter)
			x->iter->reset();
		else
			x->iter = createSegStoreIter(x->seg.get());
	}

	virtual StoreIterator* createSegStoreIter(ReadableSegment*) = 0;
};

class CompositeTable::MyStoreIterForward : public MyStoreIterBase {
	StoreIterator* createSegStoreIter(ReadableSegment* seg) override {
		return seg->createStoreIterForward(m_ctx.get());
	}
public:
	MyStoreIterForward(const CompositeTable* tab, DbContext* ctx) {
		init(tab, ctx);
		m_segIdx = 0;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		llong subId = -1;
		while (incrementNoCheckDel(&subId, val)) {
			assert(subId >= 0);
			assert(subId < m_segs[m_segIdx].seg->numDataRows());
			llong baseId = m_segs[m_segIdx].baseId;
			if (m_segIdx < m_segs.size()-2) {
				if (!m_segs[m_segIdx].seg->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
			else {
				MyRwLock lock(tab->m_rwMutex, false);
				if (!tab->m_segments[m_segIdx]->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
		auto cur = &m_segs[m_segIdx];
		if (nark_unlikely(!cur->iter))
			 cur->iter = cur->seg->createStoreIterForward(m_ctx.get());
		if (!cur->iter->increment(subId, val)) {
			syncTabSegs();
			if (m_segIdx < m_segs.size()-2) {
				m_segIdx++;
				cur = &m_segs[m_segIdx];
				resetOneSegIter(cur);
				bool ret = cur->iter->increment(subId, val);
				if (ret) {
					assert(*subId < cur->seg->numDataRows());
				}
				return ret;
			}
			return false;
		}
		assert(*subId < cur->seg->numDataRows());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto ib = seekExactImpl(id, val);
		if (ib.second) {
			m_segIdx = ib.first - 1; // first is upp
		}
		return ib.second;
	}
	void reset() override {
		resetIterBase();
		m_segIdx = 0;
	}
};

class CompositeTable::MyStoreIterBackward : public MyStoreIterBase {
	StoreIterator* createSegStoreIter(ReadableSegment* seg) override {
		return seg->createStoreIterForward(m_ctx.get());
	}
public:
	MyStoreIterBackward(const CompositeTable* tab, DbContext* ctx) {
		init(tab, ctx);
		m_segIdx = m_segs.size() - 1;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		llong subId = -1;
		while (incrementNoCheckDel(&subId, val)) {
			assert(subId >= 0);
			assert(subId < m_segs[m_segIdx-1].seg->numDataRows());
			llong baseId = m_segs[m_segIdx-1].baseId;
			if (m_segIdx > 0) {
				if (!m_segs[m_segIdx-1].seg->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
			else {
				MyRwLock lock(tab->m_rwMutex, false);
				if (!tab->m_segments[m_segIdx-1]->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
	//	auto tab = static_cast<const CompositeTable*>(m_store.get());
		auto cur = &m_segs[m_segIdx-1];
		if (nark_unlikely(!cur->iter))
			 cur->iter = cur->seg->createStoreIterBackward(m_ctx.get());
		if (!cur->iter->increment(subId, val)) {
		//	syncTabSegs(); // don't need to sync, because new segs are appended
			if (m_segIdx > 1) {
				m_segIdx--;
				cur = &m_segs[m_segIdx-1];
				resetOneSegIter(cur);
				bool ret = cur->iter->increment(subId, val);
				if (ret) {
					assert(*subId < m_segs[m_segIdx-1].seg->numDataRows());
				}
				return ret;
			}
			return false;
		}
		assert(*subId < m_segs[m_segIdx-1].seg->numDataRows());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto ib = seekExactImpl(id, val);
		if (ib.second) {
			m_segIdx = ib.first; // first is upp
		}
		return ib.second;
	}
	void reset() override {
		resetIterBase();
		m_segIdx = m_segs.size()-1;
	}
};

StoreIterator* CompositeTable::createStoreIterForward(DbContext* ctx) const {
	assert(m_schema);
	return new MyStoreIterForward(this, ctx);
}

StoreIterator* CompositeTable::createStoreIterBackward(DbContext* ctx) const {
	assert(m_schema);
	return new MyStoreIterBackward(this, ctx);
}

llong CompositeTable::totalStorageSize() const {
	MyRwLock lock(m_rwMutex, false);
	llong size = m_wrSeg->dataStorageSize();
	for (size_t i = 0; i < m_schema->getIndexNum(); ++i) {
		for (size_t i = 0; i < m_segments.size(); ++i) {
			size += m_segments[i]->totalStorageSize();
		}
	}
	size += m_wrSeg->totalStorageSize();
	return size;
}

llong CompositeTable::numDataRows() const {
	return m_rowNumVec.back();
}

llong CompositeTable::dataStorageSize() const {
	MyRwLock lock(m_rwMutex, false);
	return m_wrSeg->dataStorageSize();
}

void
CompositeTable::getValueAppend(llong id, valvec<byte>* val, DbContext* txn)
const {
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size() + 1);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
	seg->getValueAppend(subId, val, txn);
}

bool
CompositeTable::maybeCreateNewSegment(MyRwLock& lock) {
	if (m_isMerging) {
		return false;
	}
	if (m_wrSeg->dataStorageSize() >= m_schema->m_maxWrSegSize) {
		if (lock.upgrade_to_writer() ||
			// if upgrade_to_writer fails, it means the lock has been
			// temporary released and re-acquired, so we need check
			// the condition again
			m_wrSeg->dataStorageSize() >= m_schema->m_maxWrSegSize)
		{
			doCreateNewSegmentInLock();
		}
	//	lock.downgrade_to_reader(); // TBB bug, sometimes didn't downgrade
		return true;
	}
	return false;
}

void
CompositeTable::doCreateNewSegmentInLock() {
	assert(!m_isMerging);
	if (m_segments.size() == m_segments.capacity()) {
		THROW_STD(invalid_argument,
			"Reaching maxSegNum=%d", int(m_segments.capacity()));
	}
	// createWritableSegment should be fast, other wise the lock time
	// may be too long
	putToCompressionQueue(m_segments.size() - 1);
	size_t newSegIdx = m_segments.size();
	m_wrSeg = myCreateWritableSegment(getSegPath("wr", newSegIdx));
	m_segments.push_back(m_wrSeg);
	llong newMaxRowNum = m_rowNumVec.back();
	m_rowNumVec.push_back(newMaxRowNum);
	m_newWrSegNum++;
	// freeze oldwrseg, this may be too slow
	// auto& oldwrseg = m_segments.ende(2);
	// oldwrseg->saveIsDel(oldwrseg->m_segDir);
	// oldwrseg->loadIsDel(oldwrseg->m_segDir); // mmap
}

ReadonlySegment*
CompositeTable::myCreateReadonlySegment(PathRef segDir) const {
	std::unique_ptr<ReadonlySegment> seg(createReadonlySegment(segDir));
	seg->m_segDir = segDir;
	seg->m_schema = this->m_schema;
	return seg.release();
}

WritableSegment*
CompositeTable::myCreateWritableSegment(PathRef segDir) const {
	fs::create_directories(segDir.c_str());
	std::unique_ptr<WritableSegment> seg(createWritableSegment(segDir));
	assert(seg);
	seg->m_segDir = segDir;
	seg->m_schema = this->m_schema;
	if (seg->m_indices.empty()) {
		seg->m_indices.resize(m_schema->getIndexNum());
		for (size_t i = 0; i < seg->m_indices.size(); ++i) {
			const Schema& schema = m_schema->getIndexSchema(i);
			auto indexPath = segDir + "/index-" + schema.m_name;
			seg->m_indices[i] = seg->createIndex(schema, indexPath);
		}
	}
	return seg.release();
}

llong
CompositeTable::insertRow(fstring row, DbContext* txn) {
	if (txn->syncIndex) { // parseRow doesn't need lock
		m_schema->m_rowSchema->parseRow(row, &txn->cols1);
	}
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	return insertRowImpl(row, txn, lock);
}

llong
CompositeTable::insertRowImpl(fstring row, DbContext* txn, MyRwLock& lock) {
	bool isWriteLocked = maybeCreateNewSegment(lock);
	if (txn->syncIndex) {
		const size_t old_newWrSegNum = m_newWrSegNum;
	//	lock.release(); // seg[0, oldSegNum-1) need read lock?
		if (!insertCheckSegDup(0, m_segments.size()-1, txn))
			return -1;
	//	lock.acquire(m_rwMutex, true); // write lock
		if (!isWriteLocked && !lock.upgrade_to_writer()) {
			// check for segment changes(should be very rare)
			if (old_newWrSegNum != m_newWrSegNum) {
				if (!insertCheckSegDup(m_segments.size()-2, 1, txn))
					return -1;
			}
		}
	}
	else {
		if (!isWriteLocked)
			lock.upgrade_to_writer();
	}
	llong subId;
	llong wrBaseId = m_rowNumVec.end()[-2];
	auto& ws = *m_wrSeg;
	if (ws.m_deletedWrIdSet.empty()) {
		//subId = ws.append(row, txn);
		//assert(subId == (llong)ws.m_isDel.size());
		ws.m_isDirty = true;
		subId = (llong)ws.m_isDel.size();
		ws.replace(subId, row, txn);
		ws.pushIsDel(false);
		m_rowNumVec.back() = wrBaseId + subId + 1;
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				ws.remove(subId, txn); // subId is exists, but value is set to empty
				ws.m_isDel.set1(subId);
				ws.m_deletedWrIdSet.push_back(subId);
				ws.m_delcnt++;
				return -1; // fail
			}
		}
	}
	else {
		subId = ws.m_deletedWrIdSet.back();
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				return -1; // fail
			}
		}
		ws.m_deletedWrIdSet.pop_back();
		ws.replace(subId, row, txn);
		ws.m_isDel.set0(subId);
		ws.m_delcnt--;
		ws.m_isDirty = true;
	}
	return wrBaseId + subId;
}

bool
CompositeTable::insertCheckSegDup(size_t begSeg, size_t numSeg, DbContext* txn) {
	// m_wrSeg will be check in unique index insert
	const size_t endSeg = begSeg + numSeg;
	assert(endSeg < m_segments.size()); // don't check m_wrSeg
	if (0 == numSeg)
		return true;
	const SchemaConfig& sconf = *m_schema;
	for (size_t segIdx = begSeg; segIdx < endSeg; ++segIdx) {
		auto seg = &*m_segments[segIdx];
		for(size_t i = 0; i < sconf.m_uniqIndices.size(); ++i) {
			size_t indexId = sconf.m_uniqIndices[i];
			const Schema& iSchema = sconf.getIndexSchema(indexId);
			auto rIndex = seg->m_indices[indexId];
			assert(iSchema.m_isUnique);
			iSchema.selectParent(txn->cols1, &txn->key1);
			llong exRecId = rIndex->searchExact(txn->key1, txn);
			if (exRecId >= 0 && !seg->m_isDel[exRecId]) {
				// std::move makes it no temps
				txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
							+ ", in freezen seg: " + seg->m_segDir.string();
			//	txn->errMsg += ", rowData=";
			//	txn->errMsg += sconf.m_rowSchema->toJsonStr(row);
				return false;
			}
		}
	}
	return true;
}

bool CompositeTable::insertSyncIndex(llong subId, DbContext* txn) {
	// first try insert unique index
	const SchemaConfig& sconf = *m_schema;
	size_t i = 0;
	for (; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		assert(iSchema.m_isUnique);
		iSchema.selectParent(txn->cols1, &txn->key1);
		if (!wrIndex->getWritableIndex()->insert(txn->key1, subId, txn)) {
			txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
						+ ", in writing seg: " + m_wrSeg->m_segDir.string();
			goto Fail;
		}
	}
	// insert non-unique index
	for (i = 0; i < sconf.m_multIndices.size(); ++i) {
		size_t indexId = sconf.m_multIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		assert(!iSchema.m_isUnique);
		iSchema.selectParent(txn->cols1, &txn->key1);
		wrIndex->getWritableIndex()->insert(txn->key1, subId, txn);
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = sconf.m_uniqIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(txn->cols1, &txn->key1);
		wrIndex->getWritableIndex()->remove(txn->key1, subId, txn);
	}
	return false;
}

llong
CompositeTable::replaceRow(llong id, fstring row, DbContext* txn) {
	m_schema->m_rowSchema->parseRow(row, &txn->cols1); // new row
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNumVec.back());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j > 0);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = &*m_segments[j-1];
	bool directUpgrade = true;
	if (txn->syncIndex) {
		const size_t old_newWrSegNum = m_newWrSegNum;
		if (seg->m_isDel[subId]) { // behave as insert
			if (!insertCheckSegDup(0, m_segments.size()-1, txn))
				return -1;
			if (!lock.upgrade_to_writer()) {
				// check for segment changes(should be very rare)
				if (old_newWrSegNum != m_newWrSegNum) {
					if (!insertCheckSegDup(m_segments.size()-2, 1, txn))
						return -1;
				}
				directUpgrade = false;
			}
		}
		else {
			seg->getValue(subId, &txn->row2, txn);
			m_schema->m_rowSchema->parseRow(txn->row2, &txn->cols2); // old row

			if (!replaceCheckSegDup(0, m_segments.size()-1, txn))
				return -1;
			if (!lock.upgrade_to_writer()) {
				// check for segment changes(should be very rare)
				if (old_newWrSegNum != m_newWrSegNum) {
					if (!replaceCheckSegDup(m_segments.size()-2, 1, txn))
						return -1;
				}
				directUpgrade = false;
			}
		}
	}
	else {
		directUpgrade = lock.upgrade_to_writer();
	}
	if (!directUpgrade) {
		j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
		assert(j > 0);
		assert(j < m_rowNumVec.size());
		baseId = m_rowNumVec[j-1];
		subId = id - baseId;
		seg = &*m_segments[j-1];
	}
	if (j == m_rowNumVec.size()-1) { // id is in m_wrSeg
		if (txn->syncIndex) {
			replaceSyncIndex(subId, txn, lock);
		}
		m_wrSeg->m_isDirty = true;
		m_wrSeg->replace(subId, row, txn);
		return id; // id is not changed
	}
	else {
		// mark old subId as deleted
		if (seg->m_bookDeletion) {
			assert(seg->getWritableStore() == nullptr); // must be readonly
			seg->m_deletionList.push_back(uint32_t(subId));
		}
		seg->m_isDel.set1(subId);
		seg->m_delcnt++;
		lock.downgrade_to_reader();
	//	lock.release();
	//	lock.acquire(m_rwMutex, false);
		return insertRowImpl(row, txn, lock); // id is changed
	}
}

bool
CompositeTable::replaceCheckSegDup(size_t begSeg, size_t numSeg, DbContext* txn) {
	// m_wrSeg will be check in unique index insert
	const size_t endSeg = begSeg + numSeg;
	assert(endSeg < m_segments.size()); // don't check m_wrSeg
	if (0 == numSeg)
		return true;
	const SchemaConfig& sconf = *m_schema;
	for(size_t i = 0; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		for (size_t segIdx = begSeg; segIdx < endSeg; ++segIdx) {
			auto seg = &*m_segments[segIdx];
			auto rIndex = seg->m_indices[indexId];
			assert(iSchema.m_isUnique);
			iSchema.selectParent(txn->cols1, &txn->key1);
			llong exRecId = rIndex->searchExact(txn->key1, txn);
			if (exRecId >= 0 && !seg->m_isDel[exRecId]) {
				// std::move makes it no temps
				txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
							+ ", in freezen seg: " + seg->m_segDir.string();
			//	txn->errMsg += ", rowData=";
			//	txn->errMsg += m_rowSchema->toJsonStr(row);
				return false;
			}
		}
	}
	return true;
}

bool
CompositeTable::replaceSyncIndex(llong subId, DbContext* txn, MyRwLock& lock) {
	const SchemaConfig& sconf = *m_schema;
	const WritableSegment& ws = *m_wrSeg;
	size_t i = 0;
	for (; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = ws.m_indices[indexId]->getWritableIndex();
			if (!wrIndex->insert(txn->key1, subId, txn)) {
				goto Fail;
			}
		}
	}
	for (i = 0; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = ws.m_indices[indexId]->getWritableIndex();
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	for (i = 0; i < sconf.m_multIndices.size(); ++i) {
		size_t indexId = sconf.m_multIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = ws.m_indices[indexId]->getWritableIndex();
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
			if (!wrIndex->insert(txn->key1, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = sconf.m_uniqIndices[j];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = ws.m_indices[indexId]->getWritableIndex();
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	return false;
}

bool
CompositeTable::removeRow(llong id, DbContext* txn) {
	assert(txn != nullptr);
	MyRwLock lock(m_rwMutex, true);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
	assert(seg->m_isDel.is0(subId));
	if (seg->m_isDel.is1(subId)) {
	//	THROW_STD(invalid_argument
	//		, "Row has been deleted: id=%lld seg=%zd baseId=%lld subId=%lld"
	//		, id, j, baseId, subId);
		return false;
	}
	if (j == m_rowNumVec.size()) {
		if (txn->syncIndex) {
			valvec<byte> &row = txn->row1, &key = txn->key1;
			valvec<fstring>& columns = txn->cols1;
			m_wrSeg->getValue(subId, &row, txn);
			m_schema->m_rowSchema->parseRow(row, &columns);
			for (size_t i = 0; i < m_wrSeg->m_indices.size(); ++i) {
				auto wrIndex = m_wrSeg->m_indices[i]->getWritableIndex();
				const Schema& iSchema = m_schema->getIndexSchema(i);
				iSchema.selectParent(columns, &key);
				wrIndex->remove(key, subId, txn);
			}
		}
		// TODO: if remove fail, set m_isDel[subId] = 1 ??
		m_wrSeg->remove(subId, txn);
		m_wrSeg->m_deletedWrIdSet.push_back(uint32_t(subId));
		m_wrSeg->m_isDirty = true;
	}
	else { // freezed segment, just set del mark
		if (seg->m_bookDeletion) {
			assert(seg->getWritableStore() == nullptr); // must be readonly
			seg->m_deletionList.push_back(uint32_t(subId));
		}
		seg->m_isDel.set1(subId);
		seg->m_delcnt++;
		seg->m_isDirty = true;
	}
	return true;
}

bool
CompositeTable::indexKeyExists(size_t indexId, fstring key, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	for (size_t i = m_segments.size(); i > 0; ) {
		auto& seg = m_segments[--i];
		auto index = seg->m_indices[indexId];
		llong rId = index->searchExact(key, ctx);
		if (rId >= 0 && !seg->m_isDel[rId]) {
			return true;
		}
	}
	return false;
}

llong
CompositeTable::indexSearchExact(size_t indexId, fstring key, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	for (size_t i = m_segments.size(); i > 0; ) {
		auto index = m_segments[--i]->m_indices[indexId];
		llong recId = index->searchExact(key, ctx);
		if (recId >= 0)
			return recId;
	}
	return -1;
}

bool
CompositeTable::indexInsert(size_t indexId, fstring indexKey, llong id,
							DbContext* txn)
{
	assert(txn != nullptr);
	assert(id >= 0);
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_schema->getIndexNum()));
	}
	MyRwLock lock(m_rwMutex, true);
	size_t upp = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(upp <= m_segments.size());
	auto seg = m_segments[upp-1].get();
	auto wrIndex = seg->m_indices[indexId]->getWritableIndex();
	if (!wrIndex) {
		// readonly segment must have been indexed
		fprintf(stderr, "indexInsert on readonly %s, ignored",
			getSegPath("rd", upp-1).string().c_str());
		return true;
	}
	llong wrBaseId = m_rowNumVec[upp-1];
	assert(id >= wrBaseId);
	llong subId = id - wrBaseId;
	seg->m_isDirty = true;
	return wrIndex->insert(indexKey, subId, txn);
}

bool
CompositeTable::indexRemove(size_t indexId, fstring indexKey, llong id,
							DbContext* txn)
{
	assert(txn != nullptr);
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_schema->getIndexNum()));
	}
	MyRwLock lock(m_rwMutex, true);
	size_t upp = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(upp <= m_segments.size());
	auto seg = m_segments[upp-1].get();
	auto wrIndex = seg->m_indices[indexId]->getWritableIndex();
	if (!wrIndex) {
		// readonly segment must have been indexed
		fprintf(stderr, "indexRemove on readonly %s, ignored",
			getSegPath("rd", upp-1).string().c_str());
		return true;
	}
	llong wrBaseId = m_rowNumVec[upp-1];
	assert(id >= wrBaseId);
	llong subId = id - wrBaseId;
	seg->m_isDirty = true;
	return wrIndex->remove(indexKey, subId, txn);
}

bool
CompositeTable::indexReplace(size_t indexId, fstring indexKey,
							 llong oldId, llong newId,
							 DbContext* txn)
{
	assert(txn != nullptr);
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_schema->getIndexNum()));
	}
	assert(oldId != newId);
	if (oldId == newId) {
		return true;
	}
	MyRwLock lock(m_rwMutex, false);
	size_t oldupp = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), oldId);
	size_t newupp = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), newId);
	assert(oldupp <= m_segments.size());
	assert(newupp <= m_segments.size());
	llong oldBaseId = m_rowNumVec[oldupp-1];
	llong newBaseId = m_rowNumVec[newupp-1];
	llong oldSubId = oldId - oldBaseId;
	llong newSubId = newId - newBaseId;
	if (oldupp == newupp) {
		auto seg = m_segments[oldupp-1].get();
		auto wrIndex = seg->m_indices[indexId]->getWritableIndex();
		if (!wrIndex) {
			return true;
		}
		lock.upgrade_to_writer();
		seg->m_isDirty = true;
		return wrIndex->replace(indexKey, oldSubId, newSubId, txn);
	}
	else {
		auto oldseg = m_segments[oldupp-1].get();
		auto newseg = m_segments[newupp-1].get();
		auto oldIndex = oldseg->m_indices[indexId]->getWritableIndex();
		auto newIndex = newseg->m_indices[indexId]->getWritableIndex();
		bool ret = true;
		lock.upgrade_to_writer();
		if (oldIndex) {
			ret = oldIndex->remove(indexKey, oldSubId, txn);
			oldseg->m_isDirty = true;
		}
		if (newIndex) {
			ret = oldIndex->insert(indexKey, newSubId, txn);
			newseg->m_isDirty = true;
		}
		return ret;
	}
}

llong CompositeTable::indexStorageSize(size_t indexId) const {
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_schema->getIndexNum()));
	}
	MyRwLock lock(m_rwMutex, false);
	llong sum = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		sum += m_segments[i]->m_indices[indexId]->indexStorageSize();
	}
	return sum;
}

class TableIndexIter : public IndexIterator {
	const CompositeTablePtr m_tab;
	const DbContextPtr m_ctx;
	const size_t m_indexId;
	struct OneSeg {
		ReadableSegmentPtr seg;
		IndexIteratorPtr   iter;
		valvec<byte>       data;
		llong              subId = -1;
		llong              baseId;
	};
	valvec<OneSeg> m_segs;
	bool lessThanImp(const Schema* schema, size_t x, size_t y) {
			const auto& xkey = m_segs[x].data;
			const auto& ykey = m_segs[y].data;
			if (xkey.empty()) {
				if (ykey.empty())
					return x < y;
				else
					return true; // xkey < ykey
			}
			if (ykey.empty())
				return false; // xkey > ykey
			int r = schema->compareData(xkey, ykey);
			if (r) return r < 0;
			else   return x < y;
	}
	bool lessThan(const Schema* schema, size_t x, size_t y) {
		if (m_forward)
			return lessThanImp(schema, x, y);
		else
			return lessThanImp(schema, y, x);
	}
	class HeapKeyCompare {
		TableIndexIter* owner;
		const Schema* schema;
	public:
		bool operator()(size_t x, size_t y) const {
			// min heap's compare is 'greater'
			return owner->lessThan(schema, y, x);
		}
		HeapKeyCompare(TableIndexIter* o)
			: owner(o)
			, schema(&o->m_tab->m_schema->getIndexSchema(o->m_indexId)) {}
	};
	friend class HeapKeyCompare;
	valvec<byte> m_keyBuf;
	nark::valvec<size_t> m_heap;
	const bool m_forward;
	bool m_isHeapBuilt;

	IndexIterator* createIter(const ReadableSegment& seg) {
		auto index = seg.m_indices[m_indexId];
		if (m_forward)
			return index->createIndexIterForward(m_ctx.get());
		else
			return index->createIndexIterBackward(m_ctx.get());
	}

	size_t syncSegPtr() {
		size_t numChangedSegs = 0;
		MyRwLock lock(m_tab->m_rwMutex, false);
		m_segs.resize(m_tab->m_segments.size());
		for (size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			assert(m_tab->m_segments[i]);
			if (cur.seg != m_tab->m_segments[i]) {
				if (cur.seg) { // segment converted
					cur.subId = -2; // need re-seek position??
				}
				cur.seg  = m_tab->m_segments[i];
				cur.iter = nullptr;
				cur.data.erase_all();
				cur.baseId = m_tab->m_rowNumVec[i];
				numChangedSegs++;
			}
		}
		return numChangedSegs;
	}

public:
	TableIndexIter(const CompositeTable* tab, size_t indexId, bool forward)
	  : m_tab(const_cast<CompositeTable*>(tab))
	  , m_ctx(tab->createDbContext())
	  , m_indexId(indexId)
	  , m_forward(forward)
	{
		assert(tab->m_schema->getIndexSchema(indexId).m_isOrdered);
		{
			MyRwLock lock(tab->m_rwMutex);
			tab->m_tableScanningRefCount++;
		}
		m_isHeapBuilt = false;
	}
	~TableIndexIter() {
		MyRwLock lock(m_tab->m_rwMutex);
		m_tab->m_tableScanningRefCount--;
	}
	void reset() override {
		m_heap.erase_all();
		m_segs.erase_all();
		m_keyBuf.erase_all();
		m_isHeapBuilt = false;
	}
	bool increment(llong* id, valvec<byte>* key) override {
		if (nark_unlikely(!m_isHeapBuilt)) {
			if (syncSegPtr()) {
				for (auto& cur : m_segs) {
					if (cur.iter == nullptr)
						cur.iter = createIter(*cur.seg);
					else
						cur.iter->reset();
				}
			}
			m_heap.erase_all();
			m_heap.reserve(m_segs.size());
			for (size_t i = 0; i < m_segs.size(); ++i) {
				auto& cur = m_segs[i];
				if (cur.iter->increment(&cur.subId, &cur.data)) {
					m_heap.push_back(i);
				}
			}
			std::make_heap(m_heap.begin(), m_heap.end(), HeapKeyCompare(this));
			m_isHeapBuilt = true;
		}
		while (!m_heap.empty()) {
			llong subId;
			size_t segIdx = incrementNoCheckDel(&subId);
			if (!isDeleted(segIdx, subId)) {
				assert(subId < m_segs[segIdx].seg->numDataRows());
				llong baseId = m_segs[segIdx].baseId;
				*id = baseId + subId;
				assert(*id < m_tab->numDataRows());
				if (key)
					key->swap(m_keyBuf);
				return true;
			}
		}
		return false;
	}
	size_t incrementNoCheckDel(llong* subId) {
		assert(!m_heap.empty());
		size_t segIdx = m_heap[0];
		std::pop_heap(m_heap.begin(), m_heap.end(), HeapKeyCompare(this));
		auto& cur = m_segs[segIdx];
		*subId = cur.subId;
		m_keyBuf.swap(cur.data); // should be assign, but swap is more efficient
		if (cur.iter->increment(&cur.subId, &cur.data)) {
			assert(m_heap.back() == segIdx);
			std::push_heap(m_heap.begin(), m_heap.end(), HeapKeyCompare(this));
		} else {
			m_heap.pop_back();
			cur.subId = -3; // eof
			cur.data.erase_all();
		}
		return segIdx;
	}
	bool isDeleted(size_t segIdx, llong subId) {
		if (m_tab->m_segments.size()-1 == segIdx) {
			MyRwLock lock(m_tab->m_rwMutex, false);
			return m_segs[segIdx].seg->m_isDel[subId];
		} else {
			return m_segs[segIdx].seg->m_isDel[subId];
		}
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		const Schema& schema = m_tab->m_schema->getIndexSchema(m_indexId);
#if 0
		if (key.size() == 0)
			fprintf(stderr, "TableIndexIter::seekLowerBound: segs=%zd key.len=0\n",
					m_tab->m_segments.size());
		else
			fprintf(stderr, "TableIndexIter::seekLowerBound: segs=%zd key=%s\n",
					m_tab->m_segments.size(), schema.toJsonStr(key).c_str());
#endif
		if (key.size() == 0) {
			// empty key indicate min key in both forward and backword mode
			this->reset();
			if (increment(id, retKey))
				return (retKey->size() == 0) ? 0 : 1;
			else
				return -1;
		}
		size_t fixlen = schema.getFixedRowLen();
		assert(fixlen == 0 || key.size() == fixlen);
		if (fixlen && key.size() != fixlen) {
			THROW_STD(invalid_argument,
				"bad key, len=%d is not same as fixed-len=%d",
				key.ilen(), int(fixlen));
		}
		if (syncSegPtr()) {
			for (auto& cur : m_segs)
				if (cur.iter == nullptr)
					cur.iter = createIter(*cur.seg);
		}
		m_heap.erase_all();
		m_heap.reserve(m_segs.size());
		for(size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			int ret = cur.iter->seekLowerBound(key, &cur.subId, &cur.data);
			if (ret >= 0)
				m_heap.push_back(i);
		}
		m_isHeapBuilt = true;
		if (m_heap.size()) {
			std::make_heap(m_heap.begin(), m_heap.end(), HeapKeyCompare(this));
			while (!m_heap.empty()) {
				llong subId;
				size_t segIdx = incrementNoCheckDel(&subId);
				if (!isDeleted(segIdx, subId)) {
					assert(subId < m_segs[segIdx].seg->numDataRows());
					llong baseId = m_segs[segIdx].baseId;
					*id = baseId + subId;
				#if !defined(NDEBUG)
					assert(*id < m_tab->numDataRows());
					if (m_forward) {
#if !defined(NDEBUG)
						if (schema.compareData(key, m_keyBuf) > 0) {
							fprintf(stderr, "ERROR: key=%s m_keyBuf=%s\n"
								, schema.toJsonStr(key).c_str()
								, schema.toJsonStr(m_keyBuf).c_str());
						}
#endif
						assert(schema.compareData(key, m_keyBuf) <= 0);
					} else {
						assert(schema.compareData(key, m_keyBuf) >= 0);
					}
				#endif
					int ret = (key == m_keyBuf) ? 0 : 1;
					if (retKey)
						retKey->swap(m_keyBuf);
					return ret;
				}
			}
		}
		return -1;
	}
};

IndexIteratorPtr CompositeTable::createIndexIterForward(size_t indexId) const {
	assert(indexId < m_schema->getIndexNum());
	assert(m_schema->getIndexSchema(indexId).m_isOrdered);
	return new TableIndexIter(this, indexId, true);
}

IndexIteratorPtr CompositeTable::createIndexIterForward(fstring indexCols) const {
	size_t indexId = m_schema->getIndexId(indexCols);
	if (m_schema->getIndexNum() == indexId) {
		THROW_STD(invalid_argument, "index: %s not exists", indexCols.c_str());
	}
	return createIndexIterForward(indexId);
}

IndexIteratorPtr CompositeTable::createIndexIterBackward(size_t indexId) const {
	assert(indexId < m_schema->getIndexNum());
	assert(m_schema->getIndexSchema(indexId).m_isOrdered);
	return new TableIndexIter(this, indexId, false);
}

IndexIteratorPtr CompositeTable::createIndexIterBackward(fstring indexCols) const {
	size_t indexId = m_schema->getIndexId(indexCols);
	if (m_schema->getIndexNum() == indexId) {
		THROW_STD(invalid_argument, "index: %s not exists", indexCols.c_str());
	}
	return createIndexIterBackward(indexId);
}

template<class T>
static
valvec<size_t>
doGetProjectColumns(const hash_strmap<T>& colnames, const Schema& rowSchema) {
	valvec<size_t> colIdVec(colnames.end_i(), valvec_no_init());
	for(size_t i = 0; i < colIdVec.size(); ++i) {
		fstring colname = colnames.key(i);
		size_t f = rowSchema.m_columnsMeta.find_i(colname);
		if (f >= rowSchema.m_columnsMeta.end_i()) {
			THROW_STD(invalid_argument,
				"colname=%s is not in RowSchema", colname.c_str());
		}
		colIdVec[i] = f;
	}
	return colIdVec;
}
valvec<size_t>
CompositeTable::getProjectColumns(const hash_strmap<>& colnames) const {
	assert(colnames.delcnt() == 0);
	return doGetProjectColumns(colnames, *m_schema->m_rowSchema);
}

void
CompositeTable::selectColumns(llong id, const valvec<size_t>& cols,
							  valvec<byte>* colsData, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	llong rows = m_rowNumVec.back();
	if (id < 0 || id >= rows) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(m_rowNumVec, id);
	llong baseId = m_rowNumVec[upp-1];
	m_segments[upp-1]->selectColumns(id - baseId, cols.data(), cols.size(), colsData, ctx);
}

void
CompositeTable::selectColumns(llong id, const size_t* colsId, size_t colsNum,
							  valvec<byte>* colsData, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	llong rows = m_rowNumVec.back();
	if (id < 0 || id >= rows) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(m_rowNumVec, id);
	llong baseId = m_rowNumVec[upp-1];
	m_segments[upp-1]->selectColumns(id - baseId, colsId, colsNum, colsData, ctx);
}

void
CompositeTable::selectOneColumn(llong id, size_t columnId,
								valvec<byte>* colsData, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	llong rows = m_rowNumVec.back();
	if (id < 0 || id >= rows) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(m_rowNumVec, id);
	llong baseId = m_rowNumVec[upp-1];
	m_segments[upp-1]->selectOneColumn(id - baseId, columnId, colsData, ctx);
}

#if 0
StoreIteratorPtr
CompositeTable::createProjectIterForward(const valvec<size_t>& cols, DbContext* ctx)
const {
	return createProjectIterForward(cols.data(), cols.size(), ctx);
}
StoreIteratorPtr
CompositeTable::createProjectIterBackward(const valvec<size_t>& cols, DbContext* ctx)
const {
	return createProjectIterBackward(cols.data(), cols.size(), ctx);
}

StoreIteratorPtr
CompositeTable::createProjectIterForward(const size_t* colsId, size_t colsNum, DbContext*)
const {
}

StoreIteratorPtr
CompositeTable::createProjectIterBackward(const size_t* colsId, size_t colsNum, DbContext*)
const {
}
#endif

namespace {
fstring getDotExtension(fstring fpath) {
	for (size_t extPos = fpath.size(); extPos > 0; --extPos) {
		if ('.' == fpath[extPos-1])
			return fpath.substr(extPos-1);
	}
	THROW_STD(invalid_argument, "fpath=%s has no extesion", fpath.c_str());
}

struct SegEntry {
	ReadableSegment* seg;
	size_t idx;
	SortableStrVec files;
	SegEntry(ReadableSegment* s, size_t i) : seg(s), idx(i) {}
};

} // namespace

class CompositeTable::MergeParam : public valvec<SegEntry> {
public:
	size_t m_tabSegNum = 0;
	bool canMerge(CompositeTable* tab) {
		if (tab->m_isMerging)
			return false;
		this->reserve(tab->m_segments.size() + 1);
		{
			MyRwLock lock(tab->m_rwMutex, false);
			for (size_t i = 0; i < tab->m_segments.size(); ++i) {
				auto seg = tab->m_segments[i].get();
				if (seg->getWritableStore())
					break; // writable seg must be at top side
				else
					this->push_back({seg, i});
			}
			if (this->size() <= 1)
				return false;
			if (this->size() + 1 < tab->m_segments.size())
				return false;
			if (tab->m_isMerging)
				return false;
			if (!lock.upgrade_to_writer()) {
				if (tab->m_isMerging) // check again
					return false;
			}
			tab->m_isMerging = true;
			// if tab->m_isMerging is false, tab can create new segments
			// then this->m_tabSegNum would be staled, this->m_tabSegNum is
			// used for violation check
			this->m_tabSegNum = tab->m_segments.size();
		}
		size_t sumSegRows = 0;
		for (size_t i = 0; i < this->size(); ++i) {
			sumSegRows += this->p[i].seg->m_isDel.size();
		}
		size_t avgSegRows = sumSegRows / this->size();

		// find max range in which every seg rows < avg*1.5
		size_t rngBeg = 0, rngLen = 0;
		for(size_t j = 0; j < this->size(); ) {
			size_t k = j;
			for (; k < this->size(); ++k) {
				if (this->p[k].seg->m_isDel.size() > avgSegRows*3/2)
					break;
			}
			if (k - j > rngLen) {
				rngBeg = j;
				rngLen = k - j;
			}
			j = k + 1;
		}
		for (size_t j = 0; j < rngLen; ++j) {
			this->p[j] = this->p[rngBeg + j];
		}
		this->trim(rngLen);
		if (rngLen < 2) {
			tab->m_isMerging = false; // can omit write lock
			return false;
		}
		return true;
	}

	std::string joinPathList() const {
		std::string str;
		for (auto& x : *this) {
			str += "\t";
			str += x.seg->m_segDir.string();
			str += "\n";
		}
		return str;
	}
};

void CompositeTable::merge(MergeParam& toMerge) {
	fs::path destMergeDir = getMergePath(m_dir, m_mergeSeqNum+1);
	if (fs::exists(destMergeDir)) {
		THROW_STD(logic_error, "dir: '%s' should not existed"
			, destMergeDir.string().c_str());
	}
	fs::path destSegDir = getSegPath2(m_dir, m_mergeSeqNum+1, "rd", toMerge[0].idx);
	std::string segPathList = toMerge.joinPathList();
	fprintf(stderr, "INFO: merge segments:\n%sTo\t%s ...\n"
		, segPathList.c_str(), destSegDir.string().c_str());
try{
	fs::create_directories(destSegDir);
	ReadonlySegmentPtr dseg = this->myCreateReadonlySegment(destSegDir);
	const size_t indexNum = m_schema->getIndexNum();
	const size_t colgroupNum = m_schema->getColgroupNum();
	dseg->m_indices.resize(indexNum);
	DbContextPtr ctx(this->createDbContext());
	for (size_t i = 0; i < indexNum; ++i) {
		valvec<byte> buf;
		SortableStrVec strVec;
		const Schema& schema = m_schema->getIndexSchema(i);
		size_t fixedIndexRowLen = schema.getFixedRowLen();
		for (size_t j = 0; j < toMerge.size(); ++j) {
			auto seg = toMerge[j].seg;
			auto indexStore = seg->m_indices[i]->getReadableStore();
			assert(nullptr != indexStore);
			llong rows = seg->numDataRows();
			for (llong id = 0; id < rows; ++id) {
				if (!seg->m_isDel[size_t(id)]) {
					indexStore->getValue(id, &buf, ctx.get());
					if (fixedIndexRowLen) {
						assert(buf.size() == fixedIndexRowLen);
						strVec.m_strpool.append(buf);
					} else
						strVec.push_back(buf);
				}
			}
		}
		ReadableIndex* index = dseg->buildIndex(schema, strVec);
		dseg->m_indices[i] = index;
	}
	for (auto& e : toMerge) {
		for(auto fpath : fs::directory_iterator(e.seg->m_segDir)) {
			e.files.push_back(fpath.path().filename().string());
		}
		e.files.sort();
	}

	for (size_t i = indexNum; i < colgroupNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		std::string prefix = "colgroup-" + schema.m_name;
		size_t newPartIdx = 0;
		for (auto& e : toMerge) {
			auto& segDir = e.seg->m_segDir;
			size_t lo = e.files.lower_bound(prefix);
			if (lo >= e.files.size() || !e.files[lo].startsWith(prefix)) {
				THROW_STD(invalid_argument, "missing: %s",
					(segDir / prefix).string().c_str());
			}
			size_t prevOldpartIdx = 0;
			size_t j = lo;
			while (j < e.files.size() && e.files[j].startsWith(prefix)) {
				fstring fname = e.files[j];
				std::string dotExt = getDotExtension(fname).str();
				size_t oldpartIdx = 0;
				if (prefix.size() + dotExt.size() < fname.size()) {
					// oldpartIdx is between prefix and dotExt
					// one part can have multiple different dotExt file
					oldpartIdx = lcast(fname.substr(prefix.size()+1));
					assert(oldpartIdx - prevOldpartIdx <= 1);
					if (oldpartIdx - prevOldpartIdx > 1) {
						THROW_STD(invalid_argument, "missing part: %s.%zd%s"
							, (segDir / prefix).string().c_str()
							, prevOldpartIdx+1, dotExt.c_str());
					}
					if (prevOldpartIdx != oldpartIdx) {
						assert(prevOldpartIdx + 1 == oldpartIdx);
						newPartIdx++;
						prevOldpartIdx = oldpartIdx;
					}
				}
				char szNumBuf[16];
				snprintf(szNumBuf, sizeof(szNumBuf), ".%04zd", newPartIdx);
				std::string destFname = prefix + szNumBuf + dotExt;
				fs::path    destFpath = destSegDir / destFname;
				try {
					fprintf(stderr, "INFO: create_hard_link(%s, %s)\n"
						, (segDir / fname.str()).string().c_str()
						, destFpath.string().c_str());
					fs::create_hard_link(segDir / fname.str(), destFpath);
				}
				catch (const std::exception& ex) {
					fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
					throw;
				}
				j++;
			}
			newPartIdx++;
		}
	}

	size_t rows = 0;
	for (auto& e : toMerge) {
		rows += e.seg->m_isDel.size();
		e.seg->m_bookDeletion = true;
	}
	dseg->m_isDel.erase_all();
	dseg->m_isDel.reserve(rows);
	for (auto& e : toMerge) {
		dseg->m_isDel.append(e.seg->m_isDel);
	}
	dseg->m_delcnt = dseg->m_isDel.popcnt();
	dseg->saveIndices(destSegDir);
	dseg->saveIsDel(destSegDir);

	// load as mmap
	dseg->m_isDel.clear();
	dseg->m_indices.erase_all();
	dseg->m_colgroups.erase_all();
	dseg->load(destSegDir);

	// m_isMerging is true, m_segments will never be changed
	// so lock is not needed
	assert(m_isMerging);
	assert(m_segments.size() == toMerge.m_tabSegNum);
	if (m_segments.size() != toMerge.m_tabSegNum) {
		THROW_STD(logic_error
			, "Unexpected: m_segments.size = %zd , toMerge.m_tabSegNum = %zd"
			, m_segments.size(), toMerge.m_tabSegNum);
	}
	// newSegPathes don't include m_wrSeg
	valvec<fs::path> newSegPathes(m_segments.size()-1, valvec_reserve());
	valvec<ReadableSegmentPtr> newSegs(m_segments.capacity(), valvec_reserve());
	valvec<llong> newRowNumVec(m_rowNumVec.capacity(), valvec_reserve());
	newRowNumVec.push_back(0);
	rows = 0;
	auto addseg = [&](const ReadableSegmentPtr& seg) {
		rows += seg->m_isDel.size();
		newSegs.push_back(seg);
		newRowNumVec.push_back(llong(rows));
	};
	auto shareReadonlySeg = [&](size_t Old) {
		size_t New = newSegs.size();
		auto&  seg = m_segments[Old];
		assert(nullptr == seg->getWritableStore());
		auto newSegDir = getSegPath2(m_dir, m_mergeSeqNum+1, "rd", New);
#if 0
		fs::create_directory(newSegDir);
		for (auto& fpath : fs::directory_iterator(seg->m_segDir)) {
			fs::path linkPath = newSegDir / fpath.path().filename();
			try { fs::create_hard_link(fpath, linkPath); }
			catch (const std::exception& ex) {
				fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
				throw;
			}
		}
#else
		fprintf(stderr, "INFO: rename(%s, %s)\n"
			, seg->m_segDir.string().c_str()
			, newSegDir.string().c_str());
		fs::rename(seg->m_segDir, newSegDir);
#endif
		addseg(seg);
		newSegPathes.emplace_back(std::move(newSegDir));
	};
	for (size_t i = 0; i < toMerge[0].idx; ++i) {
		shareReadonlySeg(i);
	}
	addseg(dseg);
	newSegPathes.emplace_back();
	for (size_t i = toMerge.back().idx + 1; i < m_segments.size()-1; ++i) {
		shareReadonlySeg(i);
	}
	if (m_segments.back()->getWritableStore()) {
		auto& seg = m_segments.back();
		fs::path Old = seg->m_segDir;
		fs::path New = getSegPath2(m_dir, m_mergeSeqNum+1, "wr", newSegs.size());
		fs::path Rela = ".." / Old.parent_path().filename() / Old.filename();
		try { fs::create_directory_symlink(Rela, New); }
		catch (const std::exception& ex) {
			fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
			throw;
		}
		addseg(seg);
	}
	else if (toMerge.back().idx + 1 < m_segments.size()) {
		assert(nullptr == m_wrSeg);
		shareReadonlySeg(m_segments.size()-1);
	}
	else {
		// called by syncFinishWriting(), and
		// last ReadonlySegment is in 'toMerge'
		assert(nullptr == m_wrSeg);
		assert(toMerge.back().idx + 1 == m_segments.size());
	}
	{
		MyRwLock lock(m_rwMutex, true);
		rows = 0;
		for (auto& e : toMerge) {
			for (size_t subId : e.seg->m_deletionList) {
				dseg->m_isDel.set1(rows + subId);
			}
			rows += e.seg->m_isDel.size();
			e.seg->m_bookDeletion = false;
			e.seg->m_deletionList.erase_all();
		}
		for (size_t i = 0; i < newSegs.size()-1; ++i) {
			auto&  seg = newSegs[i];
			assert(nullptr == seg->getWritableStore());
			if (!newSegPathes[i].empty())
				seg->m_segDir.swap(newSegPathes[i]);
		}
		if (newSegs.back()->getWritableStore() == nullptr) {
			assert(nullptr == m_wrSeg);
			if (toMerge.back().idx + 1 == m_segments.size()) {
				// called by syncFinishWriting(), and
				// last ReadonlySegment is merged
				assert(newSegPathes.back().empty());
			}
			else {
				newSegs.back()->m_segDir.swap(newSegPathes.back());
			}
		}
		m_segments.swap(newSegs);
		m_rowNumVec.swap(newRowNumVec);
		m_mergeSeqNum++;
		m_isMerging = false;
		// m_wrSeg == NULL indicate writing is stopped
		if (m_wrSeg && m_wrSeg->dataStorageSize() >= m_schema->m_maxWrSegSize) {
			doCreateNewSegmentInLock();
		}
	}
	for (auto& tobeDel : toMerge) {
		tobeDel.seg->deleteSegment();
	}
	fprintf(stderr, "INFO: merge segments:\n%sTo\t%s done!\n"
		, segPathList.c_str(), destSegDir.string().c_str());
}
catch (const std::exception& ex) {
	fprintf(stderr
		, "ERROR: merge segments: ex.what = %s\n%sTo\t%s failed, rollback!\n"
		, ex.what(), segPathList.c_str(), destSegDir.string().c_str());
	FEBIRD_IF_DEBUG(throw,;);
	fs::remove_all(destMergeDir);
}
}

void CompositeTable::clear() {
	MyRwLock lock(m_rwMutex, true);
	for (size_t i = 0; i < m_segments.size(); ++i) {
		m_segments[i]->deleteSegment();
		m_segments[i] = nullptr;
	}
	m_segments.clear();
	m_rowNumVec.clear();
}

void CompositeTable::flush() {
	valvec<ReadableSegmentPtr> segsCopy;
	{
		MyRwLock lock(m_rwMutex, false);
		segsCopy.assign(m_segments);
	}
	for (size_t i = 0; i < segsCopy.size(); ++i) {
		auto seg = segsCopy[i].get();
		auto wStore = seg->getWritableStore();
		if (wStore) {
			auto wSeg = dynamic_cast<WritableSegment*>(seg);
			wSeg->flushSegment();
		}
	}
}

static void waitForBackgroundTasks(MyRwMutex& m_rwMutex, size_t& m_bgTaskNum) {
	size_t retryNum = 0;
	for (;;retryNum++) {
		size_t bgTaskNum;
		{
			MyRwLock lock(m_rwMutex, false);
			bgTaskNum = m_bgTaskNum;
		}
		if (0 == bgTaskNum)
			break;
		if (retryNum % 100 == 0) {
			fprintf(stderr
				, "INFO: waitForBackgroundTasks: tasks = %zd, retry = %zd\n"
				, bgTaskNum, retryNum);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

void CompositeTable::syncFinishWriting() {
	m_wrSeg = nullptr; // can't write anymore
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
	{
		MyRwLock lock(m_rwMutex, true);
		putToCompressionQueue(m_segments.size()-1);
	}
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
}

void CompositeTable::dropTable() {
	assert(!m_dir.empty());
	for (auto& seg : m_segments) {
		seg->deleteSegment();
	}
	m_segments.erase_all();
	m_tobeDrop = true;
}

std::string CompositeTable::toJsonStr(fstring row) const {
	return m_schema->m_rowSchema->toJsonStr(row);
}

boost::filesystem::path
CompositeTable::getMergePath(PathRef dir, size_t mergeSeq)
const {
	auto res = dir;
	char szBuf[32];
	snprintf(szBuf, sizeof(szBuf), "g-%04ld", long(mergeSeq));
	res /= szBuf;
	return res;
}

boost::filesystem::path
CompositeTable::getSegPath(const char* type, size_t segIdx)
const {
	return getSegPath2(m_dir, m_mergeSeqNum, type, segIdx);
}

boost::filesystem::path
CompositeTable::getSegPath2(PathRef dir, size_t mergeSeq, const char* type, size_t segIdx)
const {
	auto res = dir;
	char szBuf[32];
	int len = snprintf(szBuf, sizeof(szBuf), "g-%04ld", long(mergeSeq));
	res /= szBuf;
	len = snprintf(szBuf, sizeof(szBuf), "-%04ld", long(segIdx));
	res /= type;
	res.concat(szBuf, szBuf + len);
	return res;
}

void CompositeTable::save(PathRef dir) const {
	if (dir == m_dir) {
		fprintf(stderr, "WARN: save self(%s), skipped\n", dir.string().c_str());
		return;
	}
	MyRwLock lock(m_rwMutex, true);

	m_tableScanningRefCount++;
	BOOST_SCOPE_EXIT(&m_tableScanningRefCount){
		m_tableScanningRefCount--;
	}BOOST_SCOPE_EXIT_END;

	size_t segNum = m_segments.size();

	// save segments except m_wrSeg
	lock.release(); // doesn't need any lock
	AutoGrownMemIO buf(1024);
	for (size_t segIdx = 0; segIdx < segNum-1; ++segIdx) {
		auto seg = m_segments[segIdx];
		if (seg->getWritableStore())
			seg->save(getSegPath2(dir, 0, "wr", segIdx));
		else
			seg->save(getSegPath2(dir, 0, "rd", segIdx));
	}

	// save the remained segments, new segment may created during
	// time pieriod of saving previous segments
	lock.acquire(m_rwMutex, false); // need read lock
	size_t segNum2 = m_segments.size();
	for (size_t segIdx = segNum-1; segIdx < segNum2; ++segIdx) {
		auto seg = m_segments[segIdx];
		assert(seg->getWritableStore());
		seg->save(getSegPath2(dir, 0, "wr", segIdx));
	}
	lock.upgrade_to_writer();
	fs::path jsonFile = dir / "dbmeta.json";
	m_schema->saveJsonFile(jsonFile.string());
}

void CompositeTable::convWritableSegmentToReadonly(size_t segIdx) {
	BOOST_SCOPE_EXIT(&m_rwMutex, &m_bgTaskNum){
		MyRwLock lock(m_rwMutex, true);
		m_bgTaskNum--;
	}BOOST_SCOPE_EXIT_END;
  {
	DbContextPtr ctx(this->createDbContext());
	auto segDir = getSegPath("rd", segIdx);
	fprintf(stderr, "INFO: convWritableSegmentToReadonly: %s\n", segDir.string().c_str());
	ReadonlySegmentPtr newSeg = myCreateReadonlySegment(segDir);
	newSeg->convFrom(this, segIdx);
	fprintf(stderr, "INFO: convWritableSegmentToReadonly: %s done!\n", segDir.string().c_str());
	fs::path wrSegPath = getSegPath("wr", segIdx);
	try {
	  if (fs::is_symlink(wrSegPath)) {
		fs::path base = wrSegPath.parent_path();
		fs::path target = fs::read_symlink(wrSegPath);
		fs::path targetMergeDir = fs::canonical(target.parent_path(), base);
		if (fs::exists(wrSegPath)) {
			// do nothing
		}
		else if (fs::exists(targetMergeDir)) {
			try { fs::remove_all(targetMergeDir); }
			catch (const std::exception& ex) {
				// windows can not delete a hardlink when another hardlink
				// to the same file is in use
				fprintf(stderr
					, "ERROR: convWritableSegmentToReadonly: ex.what = %s\n"
					, ex.what());
			}
		}
		fs::remove(wrSegPath);
	  }
	} catch (const std::exception& ex) {
		fprintf(stderr
			, "WARN: convWritableSegmentToReadonly: ex.what = %s\n"
			, ex.what());
	}
	if (this->m_isMerging) {
		return;
	}
  }
  MergeParam toMerge;
  if (toMerge.canMerge(this)) {
	  assert(this->m_isMerging);
	  this->merge(toMerge);
  }
}

void CompositeTable::freezeFlushWritableSegment(size_t segIdx) {
	ReadableSegmentPtr seg;
	{
		MyRwLock lock(m_rwMutex, false);
		seg = m_segments[segIdx];
	}
	if (seg->m_isDelMmap) {
		return;
	}
	fprintf(stderr, "freezeFlushWritableSegment: %s\n", seg->m_segDir.string().c_str());
	seg->saveIndices(seg->m_segDir);
	seg->saveRecordStore(seg->m_segDir);
	seg->saveIsDel(seg->m_segDir);
	febitvec isDel;
	byte*  isDelMmap = seg->loadIsDel_aux(seg->m_segDir, isDel);
	assert(isDel.size() == seg->m_isDel.size());
	size_t old_delcnt = isDel.popcnt();
	{
		MyRwLock lock(m_rwMutex, false);
		assert(old_delcnt <= seg->m_delcnt);
		if (seg->m_delcnt > old_delcnt) { // should be very rare
			memcpy(isDel.data(), seg->m_isDel.data(), isDel.mem_size());
		}
		seg->m_isDel.swap(isDel);
		seg->m_isDelMmap = isDelMmap;
	}
	fprintf(stderr, "freezeFlushWritableSegment: %s done!\n", seg->m_segDir.string().c_str());
}

/////////////////////////////////////////////////////////////////////

namespace {

class MyTask : public RefCounter {
public:
	virtual void execute() = 0;
};
typedef boost::intrusive_ptr<MyTask> MyTaskPtr;
nark::util::concurrent_queue<std::deque<MyTaskPtr> > g_flushQueue;
nark::util::concurrent_queue<std::deque<MyTaskPtr> > g_compressQueue;

volatile bool g_stopCompress = false;
volatile bool g_flushStopped = false;

void FlushThreadFunc() {
	while (true) {
		MyTaskPtr t;
		while (g_flushQueue.pop_front(t, 100)) {
			if (t)
				t->execute();
			else // must only one flush thread
				goto Done; // nullptr is stop notifier
		}
	}
Done:
	assert(g_flushQueue.empty());
	g_flushStopped = true;
}

void CompressThreadFunc() {
	while (!g_flushStopped && !g_stopCompress) {
		MyTaskPtr t;
		while (!g_stopCompress && g_compressQueue.pop_front(t, 100)) {
			if (g_stopCompress)
				break;
			t->execute();
		}
	}
	g_compressQueue.clearQueue();
}

class CompressionThreadsList : private std::vector<tbb::tbb_thread*> {
public:
	CompressionThreadsList() {
		size_t n = tbb::tbb_thread::hardware_concurrency();
		this->resize(n);
		for (size_t i = 0; i < n; ++i) {
			(*this)[i] = new tbb::tbb_thread(&CompressThreadFunc);
		}
	}
	~CompressionThreadsList() {
		if (!this->empty())
			CompositeTable::safeStopAndWaitForFlush();
	}
	void resize(size_t newSize) {
		if (size() < newSize) {
			reserve(newSize);
			for (size_t i = size(); i < newSize; ++i) {
				push_back(new tbb::tbb_thread(&CompressThreadFunc));
			}
		} else {
			fprintf(stderr
				, "WARN: CompressionThreadsList::resize: ignored newSize=%zd oldSize=%zd\n"
				, newSize, size()
				);
		}
	}
	void join() {
		for (auto th : *this) {
			th->join();
			delete th;
		}
		this->clear();
	}
};
tbb::tbb_thread g_flushThread(&FlushThreadFunc);
CompressionThreadsList g_compressThreads;

class SegWrToRdConvTask : public MyTask {
	CompositeTablePtr m_tab;
	size_t m_segIdx;

public:
	SegWrToRdConvTask(CompositeTablePtr tab, size_t segIdx)
		: m_tab(tab), m_segIdx(segIdx) {}

	void execute() override {
		m_tab->convWritableSegmentToReadonly(m_segIdx);
	}
};

class WrSegFreezeFlushTask : public MyTask {
	CompositeTablePtr m_tab;
	size_t m_segIdx;
public:
	WrSegFreezeFlushTask(CompositeTablePtr tab, size_t segIdx)
		: m_tab(tab), m_segIdx(segIdx) {}

	void execute() override {
		m_tab->freezeFlushWritableSegment(m_segIdx);
		g_compressQueue.push_back(new SegWrToRdConvTask(m_tab, m_segIdx));
	}
};

} // namespace

void CompositeTable::putToCompressionQueue(size_t segIdx) {
	assert(segIdx < m_segments.size());
	assert(m_segments[segIdx]->m_isDel.size() > 0);
	g_flushQueue.push_back(new WrSegFreezeFlushTask(this, segIdx));
	m_bgTaskNum++;
}

void CompositeTable::setCompressionThreadsNum(size_t threadsNum) {
	g_compressThreads.resize(threadsNum);
}

// flush is the most urgent
void CompositeTable::safeStopAndWaitForFlush() {
	g_stopCompress = true;
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	g_compressThreads.join();
}

void CompositeTable::safeStopAndWaitForCompress() {
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	g_compressThreads.join();
}

/*
void CompositeTable::registerDbContext(DbContext* ctx) const {
	assert(m_ctxListHead != ctx);
	MyRwLock lock(m_rwMutex);
	ctx->m_prev = m_ctxListHead;
	ctx->m_next = m_ctxListHead->m_next;
	m_ctxListHead->m_next->m_prev = ctx;
	m_ctxListHead->m_next = ctx;
}

void CompositeTable::unregisterDbContext(DbContext* ctx) const {
	assert(m_ctxListHead != ctx);
	MyRwLock lock(m_rwMutex);
	ctx->m_prev->m_next = ctx->m_next;
	ctx->m_next->m_prev = ctx->m_prev;
}
*/

} } // namespace nark::db
