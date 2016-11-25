#include "db_table.hpp"
#include "db_segment.hpp"
#include "appendonly.hpp"
#include <terark/db/fixed_len_store.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <boost/scope_exit.hpp>
#include <thread> // for std::this_thread::sleep_for
#include <tbb/tbb_thread.h>
#include <terark/util/concurrent_queue.hpp>
#include <float.h>
#include <terark/util/profiling.hpp>

#undef min
#undef max

using std::min;
using std::max;

//#define SegDir_c_str(seg) seg->m_segDir.string().c_str()

//#define SLOW_DEBUG_CHECK

namespace terark { namespace db {

namespace fs = boost::filesystem;

const size_t DEFAULT_maxSegNum = 4095;

///////////////////////////////////////////////////////////////////////////////

#if 1 || defined(NDEBUG)
	#define DebugCheckRowNumVecNoLock(This)
#else
	#define DebugCheckRowNumVecNoLock(This) \
	This->checkRowNumVecNoLock(); \
	auto BOOST_PP_CAT(Self, __LINE__) = This; \
	BOOST_SCOPE_EXIT(BOOST_PP_CAT(Self, __LINE__)) { \
		BOOST_PP_CAT(Self, __LINE__)->checkRowNumVecNoLock(); \
	} BOOST_SCOPE_EXIT_END
#endif

#if 1 || defined(NDEBUG)
#   define DebugCheckUnique(ctx,row,uniqueIndexId) do{}while(0)
#else
#   define DebugCheckUnique(ctx,row,uniqueIndexId)\
    TERARK_IF_DEBUG((ctx)->debugCheckUnique((row), (uniqueIndexId)),;);
#endif

DbTable* DbTable::open(PathRef dbPath) {
	fs::path jsonFile = dbPath / "dbmeta.json";
	SchemaConfigPtr sconf = new SchemaConfig();
	sconf->loadJsonFile(jsonFile.string());
	std::unique_ptr<DbTable> tab(new DbTable());
	tab->m_schema = sconf;
	tab->doLoad(dbPath);
	return tab.release();
}

DbTable::DbTable()
    : m_inprogressWritingCount{0}
{
	m_tableScanningRefCount = 0;
	m_tobeDrop = false;
	m_merging = MergeStatus::None;
    m_autoTask = true;
	m_segments.reserve(DEFAULT_maxSegNum);
	m_rowNumVec.reserve(DEFAULT_maxSegNum+1);
	m_mergeSeqNum = 0;
	m_newWrSegNum = 0;
	m_bgTaskNum = 0;
	m_rowNum = 0;
	m_oldestSnapshotVersion = 0;
	m_segArrayUpdateSeq = 1;
	m_throwOnThrottle = false; // if true, auto delay/sleep on throttle
//	m_ctxListHead = new DbContextLink();
}

DbTable::~DbTable() {
	m_wrSeg = nullptr;
//	fprintf(stderr, "INFO: DbTable::~DbTable(): m_dir = %s\n", m_dir.string().c_str());
//	fprintf(stderr, "INFO: DbTable::~DbTable(): m_segments.size = %zd\n", m_segments.size());
	if (m_dir.empty()) {
		return;
	}
	fprintf(stderr, "INFO: DbTable::~DbTable(): m_tobeDrop = %d\n", m_tobeDrop);
	if (m_tobeDrop) {
		// should delete m_dir?
		m_segments.clear();
		fprintf(stderr, "INFO: DbTable::~DbTable(): remove(%s)\n", m_dir.string().c_str());
		try {
			fs::remove_all(m_dir);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: remove_all(%s) failed: %s\n"
				, m_dir.string().c_str(), ex.what());
		}
		return;
	}
	flush();
	m_segments.clear();
	try {
		fs::remove(m_dir / "run.lock");
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: remove(%s/run.lock) failed: %s\n"
			, m_dir.string().c_str(), ex.what());
	}
//	removeStaleDir(m_dir, m_mergeSeqNum);
//	if (m_wrSeg)
//		m_wrSeg->flushSegment();
/*
	// list must be empty: has only the dummy head
	TERARK_RT_assert(m_ctxListHead->m_next == m_ctxListHead, std::logic_error);
	TERARK_RT_assert(m_ctxListHead->m_prev == m_ctxListHead, std::logic_error);
	delete m_ctxListHead;
*/
}

static void tryReduceSymlink(PathRef segDir, PathRef mergeDir) {
	if (fs::is_symlink(segDir)) {
		std::string strDir = segDir.string();
		fs::path target = fs::read_symlink(segDir);
		std::string dirName = segDir.filename().string();
		if (fstring(dirName).startsWith("wr-")) {
			long segIdx = strtol(dirName.c_str()+3, NULL, 10);
			char rddir[32];
			sprintf(rddir, "rd-%04ld", segIdx);
			fs::path rddirPath = mergeDir / rddir;
			if (fs::exists(rddirPath)) {
				fprintf(stderr, "INFO: remove symlink: %s .. ", strDir.c_str());
				fflush(stderr);
				fs::remove(segDir);
				fprintf(stderr, "done\n");
				return;
			}
		}
		bool segDirRemoved = false;
		try {
			target = fs::canonical(target, mergeDir);
			fprintf(stderr
				, "WARN: writable segment: %s is symbol link to: %s, reduce it\n"
				, strDir.c_str(), target.string().c_str());
			fs::remove(segDir);
			segDirRemoved = true;
			if (fs::exists(target))
				fs::rename(target, segDir);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: tryReduceSymlink: exception = %s\n", ex.what());
			if (segDirRemoved) {
				return;
			}
			try {
				fs::remove(segDir);
			}
			catch (const std::exception& ex2) {
				fprintf(stderr
					, "ERROR: remove symbol link segment: %s --> %s failed: %s\n"
					, strDir.c_str(), target.string().c_str(), ex2.what());
			}
		}
	}
}

void DbTable::removeStaleDir(PathRef root, size_t inUseMergeSeq) const {
	fs::path inUseMergeDir = getMergePath(root, inUseMergeSeq);
	for (auto& x : fs::directory_iterator(inUseMergeDir)) {
		PathRef segDir = x.path();
		tryReduceSymlink(segDir, inUseMergeDir);
	}
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

void DbTable::discoverMergeDir(PathRef dir) {
	long mergeSeq = -1;
	for (auto& x : fs::directory_iterator(dir)) {
		fs::path    mergeDirPath = x.path();
		std::string mergeDirName = mergeDirPath.filename().string();
		long mergeSeq2 = -1;
		if (sscanf(mergeDirName.c_str(), "g-%04ld", &mergeSeq2) == 1) {
			fs::path mergingLockFile = mergeDirPath / "merging.lock";
			if (fs::exists(mergingLockFile)) {
#if 1
				THROW_STD(logic_error
					, "ERROR: merging is not completed: '%s'\n"
					  "\tit should caused by a process crash!\n"
					  "\tto continue, remove dir: %s\n"
					, mergingLockFile.string().c_str()
					, mergeDirPath.string().c_str()
					);
#else
				fs::remove_all(mergeDirPath);
				fprintf(stderr
					, "ERROR: merging is not completed: '%s'\n"
					"\tit should caused by a process crash, skipped and removed '%s'\n"
					, mergingLockFile.string().c_str()
					, mergeDirPath.string().c_str()
					);
#endif
			}
			else {
				mergeSeq = max(mergeSeq, mergeSeq2);
			}
		}
	}
	if (mergeSeq < 0) {
		m_mergeSeqNum = 0;
		fs::create_directories(getMergePath(dir, 0));
	}
	else {
		removeStaleDir(dir, mergeSeq);
		m_mergeSeqNum = mergeSeq;
	}
}

static bool isBackupSegDir(fstring segDirName) {
	const char* end = segDirName.end();
	const char* dot = std::find(segDirName.begin(), end, '.');
	return fstring(dot, end).startsWith(".backup-");
}

static SortableStrVec getWorkingSegDirList(PathRef mergeDir) {
	SortableStrVec segDirList;
	for (auto& x : fs::directory_iterator(mergeDir)) {
		std::string segDir = x.path().string();
		std::string fname = x.path().filename().string();
		fs::path    stem = x.path().stem();
		fs::path    rightDir = mergeDir / stem;
		fstring fstr = fname;
		if (isBackupSegDir(fstr)) {
			fprintf(stderr, "WARN: Found backup segment: %s\n", segDir.c_str());
			if (fs::exists(rightDir)) {
				if (fs::last_write_time(segDir) <= fs::last_write_time(rightDir)) {
					fprintf(stderr, "WARN: Remove backup segment: %s\n", segDir.c_str());
					fs::remove_all(segDir);
				}
				else {
					fprintf(stderr, "WARN: Remove outdated segment: %s\n", rightDir.string().c_str());
					fs::remove_all(rightDir);
					goto RenameBackupToRightDir;
				}
			}
			else {
			  RenameBackupToRightDir:
				fprintf(stderr, "WARN: rename(%s, %s)\n", segDir.c_str(), rightDir.string().c_str());
				fs::rename(segDir, rightDir);
				segDirList.push_back(stem.string());
			}
			continue;
		}
		if (fstr.endsWith(".tmp")) {
			fprintf(stderr, "WARN: Remove temp dir: %s\n", segDir.c_str());
			fs::remove_all(segDir);
			continue;
		}
		if (fstr.startsWith("wr-") || fstr.startsWith("rd-")) {
			segDirList.push_back(fname);
		}
		else {
			fprintf(stderr, "WARN: Skip unknown dir: %s\n", segDir.c_str());
		}
	}
	segDirList.sort();
	return segDirList;
}

void DbTable::load(PathRef dir) {
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	if (m_schema) {
		THROW_STD(invalid_argument, "Invalid: schema.columnNum=%ld is not empty",
			long(m_schema->columnNum()));
	}
	{
		fs::path jsonFile = fs::path(dir) / "dbmeta.json";
		m_schema.reset(new SchemaConfig());
		m_schema->loadJsonFile(jsonFile.string());
	}
	doLoad(dir);
}

void DbTable::doLoad(PathRef dir) {
	assert(m_schema.get() != nullptr);
	fs::path runLockFpath = dir / "run.lock";
	if (fs::exists(runLockFpath)) {
		THROW_STD(invalid_argument
			, "Table is in using or closed unclean/crashed: %s"
			, dir.string().c_str());
	}
	FileStream runLockFile(runLockFpath.string().c_str(), "w");
	BOOST_SCOPE_EXIT(&runLockFile, &runLockFpath) {
		if (runLockFile.fp()) { // failed
			runLockFile.close();
			fs::remove(runLockFpath);
		}
	} BOOST_SCOPE_EXIT_END;
	m_dir = dir;
	discoverMergeDir(m_dir);
	fs::path mergeDir = getMergePath(m_dir, m_mergeSeqNum);
	SortableStrVec segDirList = getWorkingSegDirList(mergeDir);
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
			tryReduceSymlink(segDir, mergeDir);
			auto rDir = getSegPath("rd", segIdx);
			if (fs::exists(rDir)) {
				fprintf(stdout
					, "INFO: readonly segment: %s existed for writable seg: %s, remove it\n"
					, rDir.string().c_str(), strDir.c_str());
				fs::remove_all(segDir);
				continue;
			}
			fprintf(stdout, "INFO: loading segment: %s ... ", strDir.c_str());
			fflush(stdout);
			auto wseg = openWritableSegment(segDir);
			wseg->m_segDir = segDir;
			seg = wseg;
		}
		else if (sscanf(fname.c_str(), "rd-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			seg = myCreateReadonlySegment(segDir);
			assert(seg);
			fprintf(stdout, "INFO: loading segment: %s ... ", strDir.c_str());
			fflush(stdout);
			// If m_withPurgeBits is false, ReadonlySegment::load will
			// delete purge bits and squeeze record id space tighter,
			// so record id will be changed in this case
			seg->m_withPurgeBits = m_schema->m_usePermanentRecordId;
			seg->load(seg->m_segDir);
		}
		assert(seg);
		fprintf(stdout, "done, records: total = %zd, deleted = %zd, purged = %zd\n"
			, seg->m_isDel.size(), seg->m_delcnt, seg->m_isPurged.max_rank1());
		m_segments.ensure_set(segIdx, seg);
	}
	for (size_t i = 0; i < m_segments.size(); ++i) {
		if (m_segments[i] == nullptr) {
			THROW_STD(invalid_argument, "ERROR: missing segment: %s\n",
				getSegPath("xx", i).string().c_str());
		}
		if (i < m_segments.size()-1 && m_segments[i]->getWritableSegment()) {
			m_segments[i]->getWritableSegment()->markFrozen();
			this->putToCompressionQueue(i);
		}
	}
	fprintf(stderr, "INFO: DbTable::load(%s): loaded %zd segs\n",
		dir.string().c_str(), m_segments.size());
	if (m_segments.size() == 0 || !m_segments.back()->getWritableStore()) {
		// THROW_STD(invalid_argument, "no any segment found");
		// allow user create an table dir which just contains json meta file
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
	m_rowNum = baseId;
	runLockFile.close(); // notify DO NOT delete in BOOST_SCOPE_EXIT
}

size_t DbTable::findSegIdx(size_t segIdxBeg, ReadableSegment* seg) const {
	const ReadableSegmentPtr* segBase = m_segments.data();
	const size_t segNum = m_segments.size();
	for (size_t segIdx = segIdxBeg; segIdx < segNum; ++segIdx) {
		if (segBase[segIdx].get() == seg)
			return  segIdx;
	}
	return segNum;
}

size_t DbTable::getWritableSegNum() const {
	MyRwLock lock(m_rwMutex, false);
	size_t wrNum = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		if (m_segments[i]->getWritableStore())
			wrNum++;
	}
	return wrNum;
}

size_t DbTable::getSegmentIndexOfRecordIdNoLock(llong recId) const {
	assert(recId >= 0);
	size_t segIdx = upper_bound_a(m_rowNumVec, recId);
	return segIdx-1;
}

namespace {
	struct By_seg_get {
		template<class OneSeg>
		const ReadableSegment*
		operator()(const OneSeg& p) const { return p.seg.get(); }

		template<class OneSeg>
		bool operator()(const OneSeg& x, const OneSeg& y) const {
			return x.seg.get() < y.seg.get();
		}
	};
}

class DbTable::MyStoreIterBase : public StoreIterator {
protected:
	size_t m_segIdx;
	size_t m_segArrayUpdateSeq;
	DbContextPtr m_ctx;
	struct OneSeg {
		ReadableSegmentPtr seg;
		StoreIteratorPtr   iter;
	};
	valvec<OneSeg> m_segs;
	valvec<llong>  m_rowNumVec;

	void init(const DbTable* tab, DbContext* ctx) {
		this->m_store.reset(const_cast<DbTable*>(tab));
		this->m_ctx.reset(ctx);
	// MyStoreIterator creation is rarely used, lock it by m_rwMutex
		MyRwLock lock(tab->m_rwMutex, false);
		m_segArrayUpdateSeq = tab->m_segArrayUpdateSeq;
		m_segs.resize_fill(tab->m_segments.size());
		for (size_t i = 0; i < m_segs.size(); ++i) {
			ReadableSegment* seg = tab->m_segments[i].get();
			m_segs[i].seg = seg;
		}
		m_rowNumVec = tab->m_rowNumVec;
		assert(m_rowNumVec.size() == m_segs.size()+1);
		lock.upgrade_to_writer();
		tab->m_tableScanningRefCount++;
		assert(tab->m_segments.size() > 0);
	}

	~MyStoreIterBase() {
		assert(dynamic_cast<const DbTable*>(m_store.get()));
		auto tab = static_cast<const DbTable*>(m_store.get());
		MyRwLock lock(tab->m_rwMutex, true);
		tab->m_tableScanningRefCount--;
	}

	bool syncTabSegs() {
		auto tab = static_cast<const DbTable*>(m_store.get());
		if (m_segArrayUpdateSeq == tab->m_segArrayUpdateSeq) {
			// there is no new segments
			llong oldmaxId = m_rowNumVec.back();
			if (tab->m_rowNum == oldmaxId)
				return false; // no new records
			// records may be 'pop_back'
			m_rowNumVec.back() = tab->m_rowNum;
			return tab->m_rowNum > oldmaxId;
		}
		valvec<OneSeg> tmp(tab->m_segments.size()+2); // with 2 extra
		OneSeg* segA = m_segs.data();
		size_t  segN = m_segs.size();
		sort_0(segA, segN, By_seg_get());
		MyRwLock lock(tab->m_rwMutex, false);
		tmp.resize(tab->m_segments.size()); // size may change during lock
		for (size_t i = 0; i < tmp.size(); ++i) {
			auto seg = tab->m_segments[i].get();
			tmp[i].seg = seg;
			size_t lo = lower_bound_ex_0(segA, segN, seg, By_seg_get());
			if (lo < segN && segA[lo].seg.get() == seg) {
				tmp[i].iter = std::move(segA[lo].iter);
			}
		}
		m_segs.swap(tmp);
		m_rowNumVec = tab->m_rowNumVec;
		m_segArrayUpdateSeq = tab->m_segArrayUpdateSeq;
		assert(m_rowNumVec.size() == m_segs.size()+1);
		return true;
	}

	void resetIterBase() {
		syncTabSegs();
		for (size_t i = 0; i < m_segs.size()-1; ++i) {
			if (m_segs[i].iter)
				m_segs[i].iter->reset();
		}
		syncTabSegs();
	}

	bool increment(llong* id, valvec<byte>* val) override {
		assert(dynamic_cast<const DbTable*>(m_store.get()));
		assert(nullptr != id);
		assert(nullptr != val);
		llong subId = -1;
		while (incrementNoCheckDel(&subId, val)) {
			const auto& cur = m_segs[m_segIdx-1];
			assert(subId >= 0);
			assert(subId < cur.seg->numDataRows());
			llong baseId = m_rowNumVec[m_segIdx-1];
			if (!cur.seg->m_isDel[subId]) {
				*id = baseId + subId;
				assert(*id < m_rowNumVec[m_segIdx]);
				return true;
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
		assert(m_segIdx >= 1);
		assert(m_segIdx <= m_segs.size());
		auto cur = &m_segs[m_segIdx-1];
		if (terark_unlikely(!cur->iter)) {
			 cur->iter = cur->seg->createStoreIterForward(m_ctx.get());
		}
		if (!cur->iter->increment(subId, val)) {
			syncTabSegs();
			if (incrementSegIndex()) {
				cur = &m_segs[m_segIdx-1];
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
	virtual bool incrementSegIndex() = 0;

	///! on success, position must point to next record
	///! on fail, position is unspecified
	bool seekExact(llong id, valvec<byte>* val) override {
		auto tab = static_cast<const DbTable*>(m_store.get());
		assert(m_segIdx >= 1);
		assert(m_segIdx <= m_segs.size());
		do {
			syncTabSegs();
			size_t upp = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size()-1, id);
			llong subId = id - m_rowNumVec[upp-1];
			auto cur = &m_segs[upp-1];
			auto seg = cur->seg.get();
			m_segIdx = upp;
			if (id >= m_rowNumVec.back()) {
				resetOneSegIter(cur);
				return cur->iter->seekExact(subId, val);
			}
			assert(size_t(subId) < seg->m_isDel.size());
			const size_t ProtectNum = 100;
			if (seg->m_isFreezed || seg->m_isDel.unused() >= ProtectNum) {
				if (!seg->m_isDel[subId]) {
					resetOneSegIter(cur);
					return cur->iter->seekExact(subId, val);
				}
			}
			else {
				if (!seg->locked_testIsDel(subId)) {
					resetOneSegIter(cur);
					return cur->iter->seekExact(subId, val);
				}
			}
		} while (m_rowNumVec.back() < tab->inlineGetRowNum());
		return false;
	}

	void resetOneSegIter(OneSeg* x) {
		if (x->iter)
			x->iter->reset();
		else
			x->iter = createSegStoreIter(x->seg.get());
	}

	virtual StoreIterator* createSegStoreIter(ReadableSegment*) = 0;
};

class DbTable::MyStoreIterForward : public MyStoreIterBase {
	StoreIterator* createSegStoreIter(ReadableSegment* seg) override {
		return seg->createStoreIterForward(m_ctx.get());
	}
public:
	MyStoreIterForward(const DbTable* tab, DbContext* ctx) {
		init(tab, ctx);
		m_segIdx = 1;
	}
	bool incrementSegIndex() override {
		if (m_segIdx < m_segs.size()) {
			m_segIdx++;
			return true;
		}
		return false;
	}
	void reset() override {
		resetIterBase();
		m_segIdx = 1;
	}
};

class DbTable::MyStoreIterBackward : public MyStoreIterBase {
	StoreIterator* createSegStoreIter(ReadableSegment* seg) override {
		return seg->createStoreIterForward(m_ctx.get());
	}
public:
	MyStoreIterBackward(const DbTable* tab, DbContext* ctx) {
		init(tab, ctx);
		m_segIdx = m_segs.size();
	}
	bool incrementSegIndex() override {
		if (m_segIdx > 1) {
			m_segIdx--;
			return true;
		}
		return false;
	}
	void reset() override {
		resetIterBase();
		m_segIdx = m_segs.size();
	}
};

const std::string& BatchWriter::strError() const {
	return m_ctx->m_transaction->strError();
}
const char* BatchWriter::szError() const {
	return m_ctx->m_transaction->strError().c_str();
}

// if ctx is NULL, will create a new DbContext for m_ctx
BatchWriter::BatchWriter(DbTable* tab, DbContext* ctx)
{
    TERARK_THROW(DbException
                 , "FATAL: "
                 "BatchWriter temporarily unsupported ."
    );
//	assert(nullptr != tab);
//	assert(nullptr == ctx || ctx->m_tab == tab);
//	if (!tab->m_wrSeg) {
//		THROW_STD(invalid_argument, "the writing segment is NULL: %s"
//			, tab->m_dir.string().c_str());
//	}
//	const SchemaConfig& sconf = *tab->m_schema;
//	if (sconf.m_uniqIndices.size() > 1) {
//		THROW_STD(invalid_argument
//			, "this table has %zd unique indices, "
//				"must have at most one unique index for calling this method"
//			, sconf.m_uniqIndices.size());
//	}
//	bool inprogressWritingCountInced = false;
//	try {
//		MyRwLock lock(tab->m_rwMutex, false);
//		DebugCheckRowNumVecNoLock(tab);
//		tab->maybeCreateNewSegment(lock);
//		if (ctx == nullptr) {
//			ctx = tab->createDbContextNoLock();
//		}
//		tab->m_inprogressWritingCount += 2;
//		inprogressWritingCountInced = true;
//		m_wrSeg = tab->m_wrSeg.get();
//		m_ctx = ctx;
//		ctx->trySyncSegCtxNoLock(tab);
//		ctx->ensureTransactionNoLock();
//		auto txn = ctx->m_transaction.get();
//		m_txn = txn;
//		txn->startTransaction();
//		assert(txn->m_removeOnCommit.size() == 0); // strict on debug
//		assert(txn->m_appearOnCommit.size() == 0);
//		txn->m_removeOnCommit.erase_all(); // tolerate on release
//		txn->m_appearOnCommit.erase_all();
//	}
//	catch (const std::exception&) {
//		if (inprogressWritingCountInced) {
//			tab->m_inprogressWritingCount -= 2;
//		}
//		throw;
//	}
}

BatchWriter::~BatchWriter() {
//	auto tab = m_ctx->m_tab;
//	auto txn = m_ctx->m_transaction.get();
//	assert(DbTransaction::started != txn->m_status);
//	if (DbTransaction::started == txn->m_status) {
//		// abort();
//		fprintf(stderr
//			, "ERROR: commit or rollback was not called for BatchWriter, rollback by default\n");
//		this->rollback();
//	}
//	tab->m_inprogressWritingCount -= 2;
//	txn->m_removeOnCommit.erase_all();
//	txn->m_appearOnCommit.erase_all();
}

//llong BatchWriter::overwriteExisting(fstring row) {
//	auto ctx = m_ctx.get();
//	auto tab = ctx->m_tab;
//	const SchemaConfig& sconf = *tab->m_schema;
//	llong subId = ctx->exactMatchRecIdvec[0];
//	llong baseId = tab->m_rowNumVec.ende(2);
//	assert(ctx->exactMatchRecIdvec.size() == 1);
//	DbTransaction* txn(ctx->m_transaction.get());
//	assert(tab->m_wrSeg.get() == m_wrSeg);
//	assert(txn == m_txn);
//	if (!sconf.m_multIndices.empty()) {
//		try {
//			txn->storeGetRow(subId, &ctx->row2);
//		}
//		catch (const ReadRecordException&) {
//			fprintf(stderr
//				, "ERROR: upsertRow(baseId=%lld, subId=%lld): read old row data failed: %s\n"
//				, baseId, subId, tab->m_wrSeg->m_segDir.string().c_str());
//			throw ReadRecordException("pre updateSyncMultIndex",
//						tab->m_wrSeg->m_segDir.string(), baseId, subId);
//		}
//		sconf.m_rowSchema->parseRow(ctx->row2, &ctx->cols2); // old
//		tab->updateSyncMultIndex(subId, txn, ctx);
//	}
//	txn->storeUpsert(subId, row);
//	return baseId + subId;
//}

llong BatchWriter::upsertRow(fstring row) {
    return -1;
//	for (size_t retry = 0; retry < 3; ++retry) {
//		llong recId = upsertRowImpl(row);
//		if (recId >= 0)
//			return recId;
//		std::this_thread::yield();
//	}
//	TERARK_THROW(NeedRetryException, "Concurrent transaction conflict, retry again");
}

//llong BatchWriter::upsertRowImpl(fstring row) {
//	auto ctx = m_ctx.get();
//	auto tab = ctx->m_tab;
//	auto txn = ctx->m_transaction.get();
//	const SchemaConfig& sconf = *tab->m_schema;
//	assert(tab->m_wrSeg.get() == m_wrSeg);
//	assert(txn == m_txn);
//	if (!tab->m_wrSeg) {
//		THROW_STD(invalid_argument
//			, "syncFinishWriting('%s') was called, now writing is not allowed"
//			, tab->m_dir.string().c_str());
//	}
//	assert(sconf.m_uniqIndices.size() <= 1);
//	size_t uniqueIndexId = sconf.m_uniqIndices[0];
//	llong  newRecId;
//	// parseRow doesn't need lock
//	sconf.m_rowSchema->parseRow(row, &ctx->cols1);
//	const Schema& indexSchema = sconf.getIndexSchema(uniqueIndexId);
//	indexSchema.selectParent(ctx->cols1, &ctx->key1);
//{
//	MyRwLock lock(tab->m_rwMutex, false);
//	ctx->trySyncSegCtxNoLock(tab);
//	ctx->ensureTransactionNoLock();
//	txn->indexSearch(uniqueIndexId, ctx->key1, &ctx->exactMatchRecIdvec);
//	if (!ctx->exactMatchRecIdvec.empty()) {
//		return overwriteExisting(row);
//	}
//	llong wrBaseId = tab->m_rowNumVec.ende(2);
//	llong wrSubId = tab->allocInvisibleWrSubId_NoTabLock();
//	if (ctx->syncIndex) {
//		if (tab->insertSyncIndex(wrSubId, txn, ctx)) {
//			txn->storeUpsert(wrSubId, row);
//			tab->m_accumulateWrittenBytes += row.size();
//		} else {
//			return -1; // fail
//		}
//	} else {
//		txn->storeUpsert(wrSubId, row);
//		tab->m_accumulateWrittenBytes += row.size();
//	}
//	newRecId = wrBaseId + wrSubId;
//	assert(tab->m_wrSeg->m_isDel[wrSubId]); // unvisible
//	txn->m_appearOnCommit.push_back(uint32_t(wrSubId));
//}
//// Find and put existing row with same unique key to txn->m_removeOnCommit
//	for (size_t segIdx = 0; segIdx < ctx->m_segCtx.size()-1; ++segIdx) {
//		auto seg = ctx->m_segCtx[segIdx]->seg;
//		assert(seg->m_isFreezed);
//		seg->indexSearchExact(segIdx, uniqueIndexId, ctx->key1, &ctx->exactMatchRecIdvec, ctx);
//		if (!ctx->exactMatchRecIdvec.empty()) {
//			llong subId = ctx->exactMatchRecIdvec[0];
//			llong baseId = ctx->m_rowNumVec[segIdx];
//			llong recId = baseId + subId;
//			assert(ctx->exactMatchRecIdvec.size() == 1);
//			MyRwLock lock(tab->m_rwMutex, false);
//			if (ctx->segArrayUpdateSeq != tab->m_segArrayUpdateSeq) {
//				ctx->doSyncSegCtxNoLock(tab);
//				size_t upp = upper_bound_a(ctx->m_rowNumVec, recId);
//#if !defined(NDEBUG)
//				if (seg != ctx->m_segCtx[upp-1]->seg) {
//					seg = ctx->m_segCtx[upp-1]->seg; // for set break point
//				}
//#endif
//				segIdx = upp - 1;
//				seg = ctx->m_segCtx[segIdx]->seg;
//				baseId = ctx->m_rowNumVec[segIdx];
//				subId = recId - baseId;
//			}
//			else {
//				ctx->m_rowNumVec.back() = tab->m_rowNum;
//			}
//			if (seg->m_isDel[subId]) { // should be very rare
//				//break;
//			} else {
//				txn->m_removeOnCommit.push_back(recId);
//			}
//		}
//	}
//	return newRecId;
//}

void BatchWriter::removeRow(llong recId) {
//	auto ctx = m_ctx.get();
//	auto tab = ctx->m_tab;
//	auto txn = ctx->m_transaction.get();
//	auto& sconf = *tab->m_schema;
//	assert(recId >= 0);
//	assert(recId < tab->m_rowNum);
//	assert(tab->m_wrSeg.get() == m_wrSeg);
//	assert(txn == m_txn);
//	ctx->trySyncSegCtxSpeculativeLock(tab);
//	size_t upp = upper_bound_a(ctx->m_rowNumVec, recId);
//	llong baseId = ctx->m_rowNumVec[upp-1];
//	llong subId = recId - baseId;
//	assert(recId >= baseId);
////	fprintf(stderr
////		, "TRACE: BatchWriter::removeRow: recId = %lld, subId = %lld, segIdx = %zd, segNum = %zd\n"
////		, recId, subId, upp-1, tab->m_segments.size());
//	auto seg = ctx->m_segCtx[upp-1]->seg;
//	if (upp == ctx->m_rowNumVec.size()-1) {
//		auto wrseg = tab->m_wrSeg.get();
//		assert(wrseg == seg);
//		assert(!wrseg->m_isFreezed);
//		assert(!wrseg->m_bookUpdates);
//		{
//			if (wrseg->locked_testIsDel(subId))
//				return;
//			else
//				txn->m_removeOnCommit.push_back(recId);
//		}
//		valvec<byte> &row = ctx->row1, &key = ctx->key1;
//		ColumnVec& columns = ctx->cols1;
//		try {
//			txn->storeGetRow(subId, &row);
//		}
//		catch (const ReadRecordException& ex) {
//			fprintf(stderr
//				, "ERROR: removeRow(id=%lld): read row data failed: %s\n"
//				, recId, ex.what());
//		//	throw ReadRecordException("removeRow: pre remove index",
//		//		wrseg->m_segDir.string(), baseId, subId);
//			return;
//		}
//		sconf.m_rowSchema->parseRow(row, &columns);
//		for (size_t i = 0; i < wrseg->m_indices.size(); ++i) {
//			const Schema& iSchema = sconf.getIndexSchema(i);
//			iSchema.selectParent(columns, &key);
//			txn->indexRemove(i, key, subId);
//		}
//		txn->storeRemove(subId);
//	}
//	else {
//		if (!seg->m_isDel[subId])
//			txn->m_removeOnCommit.push_back(recId);
//	}
}

bool BatchWriter::commit() {
    return false;
//	auto tab = m_ctx->m_tab;
//	DbTransaction* txn(m_ctx->m_transaction.get());
//	auto& ws = *tab->m_wrSeg;
//	assert(&ws == m_wrSeg);
//	assert(txn == m_txn);
//	assert(DbTransaction::started == txn->m_status);
//	if (!txn->commit()) {
//		return false;
//	}
//	const size_t batchCnt = 100; // don't lock too long time
//	sort_a(txn->m_removeOnCommit);
//	sort_a(txn->m_appearOnCommit);
////	fprintf(stderr, "TRACE: BatchWriter::commit: txn->m_removeOnCommit.size = %zd\n", txn->m_removeOnCommit.size());
//	ws.m_deletedWrIdSet.grow_capacity(txn->m_appearOnCommit.size());
//	{
//		MyRwLock lock(tab->m_rwMutex, false);
//		SpinRwLock segLock(ws.m_segMutex, true);
//		auto bits = ws.m_isDel.bldata();
//		for (llong wrSubId : txn->m_appearOnCommit) {
//			terark_bit_set0(bits, wrSubId);
//		}
//		ws.m_delcnt -= txn->m_appearOnCommit.size();
//	}
//	for(size_t i = 0; i < txn->m_removeOnCommit.size(); ) {
//		size_t upper = std::min(i + batchCnt, txn->m_removeOnCommit.size());
//		MyRwLock lock(tab->m_rwMutex, false);
//		for(; i < upper; ++i) {
//			llong recId = txn->m_removeOnCommit[i];
//			size_t upp = upper_bound_a(tab->m_rowNumVec, recId);
//			llong baseId = tab->m_rowNumVec[upp-1];
//			size_t subId = size_t(recId - baseId);
//			auto seg = tab->m_segments[upp-1].get();
//			SpinRwLock segLock(seg->m_segMutex, true);
//		//	fprintf(stderr
//		//		, "TRACE: BatchWriter::commit: remove: recId = %lld, subId = %zd, segIdx = %zd, segNum = %zd, seg[del = %zd, all = %zd], delratio = %f\n"
//		//		, recId, subId, upp-1, tab->m_segments.size(), seg->m_delcnt, seg->m_isDel.size(), double(seg->m_delcnt) / seg->m_isDel.size());
//			if (seg->m_isDel[subId]) {
//				continue;
//			}
//			seg->m_isDel.set1(subId);
//			seg->m_delcnt++;
//			if (&ws == seg) {
//				ws.m_deletedWrIdSet.push_back(uint32_t(subId));
//			} else {
//				seg->addtoUpdateList(subId);
//			}
//		}
//	}
//	if (txn->m_removeOnCommit.size() > 0) {
//		MyRwLock lock(tab->m_rwMutex, true);
//		const size_t segNum = tab->m_segments.size();
//		for(size_t i = 0; i < segNum-1; ++i) {
//			auto seg = tab->m_segments[i].get();
//			if (seg->getReadonlySegment()) {
//				if (tab->checkPurgeDeleteNoLock(seg)) {
//					tab->asyncPurgeDeleteInLock();
//					break;
//				}
//			}
//		}
//	}
//	return true;
}

void BatchWriter::rollback() {
//	auto tab = m_ctx->m_tab;
//	DbTransaction* txn(m_ctx->m_transaction.get());
//	assert(tab->m_wrSeg.get() == m_wrSeg);
//	assert(txn == m_txn);
//	assert(DbTransaction::started == txn->m_status);
//	txn->rollback();
//	auto& ws = *tab->m_wrSeg;
//	ws.m_deletedWrIdSet.append(txn->m_appearOnCommit);
}

StoreIterator* DbTable::createStoreIterForward(DbContext* ctx) const {
	assert(m_schema);
	return new MyStoreIterForward(this, ctx);
}

StoreIterator* DbTable::createStoreIterBackward(DbContext* ctx) const {
	assert(m_schema);
	return new MyStoreIterBackward(this, ctx);
}

DbContext* DbTable::createDbContext() const {
	MyRwLock lock(m_rwMutex, false);
	return this->createDbContextNoLock();
}

DbContext* DbTable::createDbContextNoLock() const {
	return new DbContext(this);
}

llong DbTable::existingRows(DbContext* ctx) const {
	MyRwLock lock(m_rwMutex, false);
	llong delcnt = 0;
	auto segA = m_segments.data();
	auto segN = m_segments.size();
	for (size_t i = 0; i < segN; ++i) {
		auto seg = segA[i].get();
		delcnt += seg->m_delcnt;
	}
//	fprintf(stderr, "INFO: m_rowNum = %lld, delcnt = %lld\n", m_rowNum, delcnt);
	llong r = m_rowNum - delcnt;
	return r;
}

llong DbTable::totalStorageSize() const {
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

llong DbTable::numDataRows() const {
//	return m_rowNumVec.back();
	return m_rowNum;
}

llong DbTable::dataStorageSize() const {
	MyRwLock lock(m_rwMutex, false);
	llong size = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		size += m_segments[i]->dataStorageSize();
	}
	return size;
}

llong DbTable::dataInflateSize() const {
	MyRwLock lock(m_rwMutex, false);
	llong size = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		size += m_segments[i]->dataInflateSize();
	}
	return size;
}

void
DbTable::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
// this assert is very unlikely but still possibly failed
//	assert(ctx->m_rowNumVec.size() == ctx->m_segCtx.size() + 1);
	if (terark_unlikely(id < 0 || id >= m_rowNum)) {
		THROW_STD(out_of_range,
			"invalid id = %lld, m_rowNum = %lld", id, m_rowNum);
	}
	auto rowNumPtr = ctx->m_rowNumVec.data();
	size_t upp = upper_bound_0(rowNumPtr, ctx->m_rowNumVec.size(), id);
	assert(upp < ctx->m_rowNumVec.size());
	llong baseId = rowNumPtr[upp-1];
	llong subId = id - baseId;
	auto seg = ctx->m_segCtx[upp-1]->seg;
    if(seg->testIsDel(subId))
    {
        throw ReadDeletedRecordException(seg->m_segDir.string(), baseId, subId);
    }
	seg->getValueAppend(subId, val, ctx);
}

bool DbTable::maybeCreateNewSegment(MyRwLock& lock) {
	DebugCheckRowNumVecNoLock(this);
	if (m_inprogressWritingCount > 1) {
		return false;
	}
	if (m_wrSeg->dataStorageSize() < m_schema->m_maxWritingSegmentSize) {
		return false;
	}
	if (!lock.upgrade_to_writer()) {
		// if upgrade_to_writer fails, it means the lock has been
		// temporary released and re-acquired, so we need check
		// the condition again
		if (m_inprogressWritingCount > 1) {
			return false;
		}
		if (m_wrSeg->dataStorageSize() < m_schema->m_maxWritingSegmentSize) {
			return false;
		}
	}
	doCreateNewSegmentInLock();
//	lock.downgrade_to_reader(); // TBB bug, sometimes didn't downgrade
	return true;
}

void DbTable::maybeCreateNewSegmentInWriteLock() {
	DebugCheckRowNumVecNoLock(this);
	if (m_inprogressWritingCount > 1) {
		return;
	}
	if (m_wrSeg->dataStorageSize() >= m_schema->m_maxWritingSegmentSize) {
		doCreateNewSegmentInLock();
	}
}

void DbTable::doCreateNewSegmentInLock() {
	if (m_segments.size() == m_segments.capacity()) {
		THROW_STD(invalid_argument,
			"Reaching maxSegNum=%d", int(m_segments.capacity()));
	}
	auto oldwrseg = m_wrSeg.get();
	{
		SpinRwLock wrsegLock(oldwrseg->m_segMutex, true);
		while (oldwrseg->m_isDel.size() && oldwrseg->m_isDel.back()) {
			assert(oldwrseg->m_delcnt > 0);
			oldwrseg->popIsDel();
			oldwrseg->m_delcnt--;
		}
		m_rowNum = m_rowNumVec.back()
				 = m_rowNumVec.ende(2) + oldwrseg->m_isDel.size();
        if(oldwrseg->m_isDel.empty())
        {
            oldwrseg->m_deletedWrIdSet.clear();
            return;
        }
	}
	// createWritableSegment should be fast, other wise the lock time
	// may be too long
	putToFlushQueue(m_segments.size() - 1);
	size_t newSegIdx = m_segments.size();
	m_wrSeg = myCreateWritableSegment(getSegPath("wr", newSegIdx));
    oldwrseg->shrinkToSize(oldwrseg->m_isDel.size());
	oldwrseg->markFrozen();
	assert(oldwrseg->m_isFreezed);
	m_segments.push_back(m_wrSeg);
	llong newMaxRowNum = m_rowNumVec.back();
	m_rowNumVec.push_back(newMaxRowNum);
	m_newWrSegNum++;
	m_segArrayUpdateSeq++;
	oldwrseg->m_deletedWrIdSet.clear(); // free memory
	// freeze oldwrseg, this may be too slow
	// auto& oldwrseg = m_segments.ende(2);
	// oldwrseg->saveIsDel(oldwrseg->m_segDir);
	// oldwrseg->loadIsDel(oldwrseg->m_segDir); // mmap
}

ReadonlySegment*
DbTable::myCreateReadonlySegment(PathRef segDir) const {
	fstring clazz = m_schema->m_readonlySegmentClass;
	std::unique_ptr<ReadableSegment>
	seg(ReadableSegment::createSegment(clazz, segDir, m_schema.get()));
	if (auto rdseg = seg->getReadonlySegment()) {
		seg.release();
		return rdseg;
	}
	THROW_STD(invalid_argument, "bad ReadonlySegmentClass: %s", clazz.c_str());
}

WritableSegment*
DbTable::myCreateWritableSegment(PathRef segDir) const {
	fs::create_directories(segDir.c_str());
	fstring clazz = m_schema->m_writableSegmentClass;
	std::unique_ptr<ReadableSegment>
	seg(ReadableSegment::createSegment(clazz, segDir, m_schema.get()));
	if (auto wrseg = seg->getWritableSegment()) {
		assert(seg->getColgroupSegment() || seg->getPlainWritableSegment());
		wrseg->initEmptySegment();
		seg.release();
		return wrseg;
	}
	THROW_STD(invalid_argument, "bad WritableSegmentClass: %s", clazz.c_str());
}

WritableSegment*
DbTable::openWritableSegment(PathRef segDir) const {
	fstring clazz = m_schema->m_writableSegmentClass;
	std::unique_ptr<ReadableSegment>
	seg(ReadableSegment::createSegment(clazz, segDir, m_schema.get()));
	if (auto wrseg = seg->getWritableSegment()) {
		auto isDelPath = segDir / "IsDel";
		if (boost::filesystem::exists(isDelPath)) {
			wrseg->load(segDir);
		} else {
			wrseg->initEmptySegment();
		}
		seg.release();
		return wrseg;
	}
	THROW_STD(invalid_argument, "bad WritableSegmentClass: %s", clazz.c_str());
}

bool DbTable::exists(llong id) const {
	assert(id >= 0);
	if (terark_unlikely(id >= llong(m_rowNum))) {
		return false;
	}
	MyRwLock lock(m_rwMutex, false);
	size_t upp = upper_bound_a(m_rowNumVec, id);
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	size_t subId = size_t(id - baseId);
	auto seg = m_segments[upp-1].get();
#if !defined(NDEBUG)
	size_t upperId = m_rowNumVec[upp];
//	assert(subId < seg->m_isDel.size());
	if (terark_unlikely(seg->m_isDel.size() != upperId - baseId)) {
		fprintf(stderr, "INFO: DbTable::exists(id=%lld): "
			"temporay error: seg->m_isDel.size() = %zd, (upperId - baseId) = %zd\n"
			, id, seg->m_isDel.size() , size_t(upperId - baseId));
	}
#endif
	const size_t ProtectNum = 100;
	if (seg->m_isFreezed || seg->m_isDel.unused() >= ProtectNum) {
		if (subId >= seg->m_isDel.size()) {
			return false;
		}
		return !seg->m_isDel[subId];
	}
	else {
		SpinRwLock lock(seg->m_segMutex, false);
		if (subId >= seg->m_isDel.size()) {
			return false;
		}
		return !seg->m_isDel[subId];
	}
}

llong DbTable::insertRow(fstring row, DbContext* txn) {
	this->throttleWrite();
    auto cols = txn->cols.get();
	if (txn->syncIndex) { // parseRow doesn't need lock
		m_schema->m_rowSchema->parseRow(row, cols.get());
	}
	IncrementGuard_size_t guard(m_inprogressWritingCount);
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	return insertRowImpl(row, cols.get(), txn, lock);
}

llong DbTable::insertRowImpl(fstring row, ColumnVec *cols, DbContext* ctx, MyRwLock& lock) {
	DebugCheckRowNumVecNoLock(this);
	maybeCreateNewSegment(lock);
	ctx->trySyncSegCtxNoLock(this);
	ctx->ensureTransactionNoLock();
	if (!ctx->syncIndex) {
		return insertRowDoInsert(row, cols, ctx);
	}
	const SchemaConfig& sconf = *m_schema;
    auto key = ctx->bufs.get();
	for (size_t segIdx = 0; segIdx < m_segments.size()-1; ++segIdx) {
		auto seg = m_segments[segIdx].get();
		for(size_t indexId : sconf.m_uniqIndices) {
			const Schema& iSchema = sconf.getIndexSchema(indexId);
			assert(iSchema.m_isUnique);
			iSchema.selectParent(*cols, key.get());
			seg->indexSearchExact(segIdx, indexId, *key, &ctx->exactMatchRecIdvec, ctx);
			for(llong logicId : ctx->exactMatchRecIdvec) {
				if (!seg->m_isDel[logicId]) {
					char szIdstr[96];
					snprintf(szIdstr, sizeof(szIdstr), "logicId = %lld", logicId);
					ctx->errMsg = "DupKey=" + iSchema.toJsonStr(*key)
								+ ", " + szIdstr
								+ ", in frozen seg: " + seg->m_segDir.string();
				//	txn->errMsg += ", rowData=";
				//	txn->errMsg += sconf.m_rowSchema->toJsonStr(row);
					return -1;
				}
			}
		}
	}
	return insertRowDoInsert(row, cols, ctx);
}

llong DbTable::insertRowDoInsert(fstring row, ColumnVec *cols, DbContext* ctx) {
    llong subId = allocInvisibleWrSubId_NoTabLock();
	TransactionGuard txn(ctx->m_transaction.get(), subId);
	llong recId = insertRowDoInsertNoCommit(subId, row, cols, ctx);
	if (recId >= 0) {
		if (!txn.commit()) {
			llong wrBaseId = m_rowNumVec.end()[-2];
			llong subId = recId - wrBaseId;
			auto& ws = *m_wrSeg;
			TERARK_THROW(CommitException
				, "commit failed: %s, baseId=%lld, subId=%lld, seg = %s"
				, txn.szError(), wrBaseId, subId, ws.m_segDir.string().c_str());
		}
	}
	else {
		txn.rollback();
	}
	return recId;
}

llong DbTable::allocInvisibleWrSubId_NoTabLock() {
	auto& ws = *m_wrSeg;
	SpinRwLock wsLock(ws.m_segMutex, true);
	if (ws.m_deletedWrIdSet.empty()) {
		llong subId = (llong)ws.m_isDel.size();
		ws.pushIsDel(true); // invisible to others
		ws.m_delcnt++;
		m_rowNumVec.back() = ++m_rowNum;
		assert(ws.m_isDel.popcnt() == ws.m_delcnt);
		return subId;
	}
	else {
		llong subId = ws.m_deletedWrIdSet.pop_val();
		assert(ws.m_isDel[subId]);
		assert(ws.m_isDel.popcnt() == ws.m_delcnt);
		return subId;
	}
}

void DbTable::freeInvisibleWrSubId_NoTabLock(llong wrSubId) {
	auto& ws = *m_wrSeg;
	SpinRwLock wsLock(ws.m_segMutex, true);
	assert(ws.m_isDel[wrSubId]);
#if 0
	llong wrBaseId = m_rowNumVec.ende(2);
	if (wrBaseId + wrSubId + 1 == m_rowNum) {
		m_rowNumVec.back()--;
		m_rowNum--;
		ws.popIsDel();
		ws.m_delcnt--;
		assert(ws.m_isDel.popcnt() == ws.m_delcnt);
	}
	else {
		ws.m_deletedWrIdSet.push_back(wrSubId);
	}
#else
	// the free'ed subId will be reused soon
	ws.m_deletedWrIdSet.push_back(uint32_t(wrSubId));
#endif
}

llong DbTable::insertRowDoInsertNoCommit(llong subId, fstring row, ColumnVec *cols, DbContext* ctx) {
	if (ctx->syncIndex) {
		DbTransaction* txn = ctx->m_transaction.get();
		if (insertSyncIndex(subId, cols, txn, ctx)) {
			txn->storeUpdate(row);
			m_wrSeg->delmarkSet0(subId);
		}
		else {
			freeInvisibleWrSubId_NoTabLock(subId);
			return -1; // fail
		}
	}
	else {
		m_wrSeg->update(subId, row, ctx);
		m_wrSeg->delmarkSet0(subId);
	}
	m_accumulateWrittenBytes += row.size();
	llong  wrBaseId = m_rowNumVec.ende(2);
	return wrBaseId + subId;
}

bool
DbTable::insertSyncIndex(llong subId, ColumnVec *cols, DbTransaction* txn, DbContext* ctx) {
	// first try insert unique index
	const SchemaConfig& sconf = *m_schema;
    auto key = ctx->bufs.get();
	size_t i = 0;
	for (; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		assert(iSchema.m_isUnique);
		iSchema.selectParent(*cols, key.get());
		if (!txn->indexInsert(indexId, *key)) {
			ctx->errMsg = "DupKey=" + iSchema.toJsonStr(*key)
						+ ", in writing seg: " + m_wrSeg->m_segDir.string();
			goto Fail;
		}
	}
	// insert non-unique index
	for (i = 0; i < sconf.m_multIndices.size(); ++i) {
		size_t indexId = sconf.m_multIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		assert(!iSchema.m_isUnique);
		iSchema.selectParent(*cols, key.get());
		txn->indexInsert(indexId, *key);
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = sconf.m_uniqIndices[j];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(*cols, key.get());
		txn->indexRemove(indexId, *key);
	}
	return false;
}

// dup keys in unique index errors will be ignored
llong DbTable::upsertRow(fstring row, DbContext* ctx) {
	for (int retry = 0; ; ++retry) {
		llong recId = doUpsertRow(row, ctx);
		if (recId >= 0) {
			return recId;
		}
		if (retry >= ctx->upsertMaxRetry) {
			break;
		}
		// sleep a while to recover race conditions caused errors
		int millisec = 100 * std::pow(2, retry);
		fprintf(stderr
			, "ERROR: DbTable::upsertRow(%s) failed, sleep for %d'ms and auto retry\n"
			, rowSchema().toJsonStr(row).c_str(), millisec
			);
		tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(millisec*0.001));
	//	std::this_thread::sleep_for(std::chrono::milliseconds(millisec));
	}
	//TERARK_THROW(NeedRetryException, "Insertion temporary failed, retry later");
    throw NeedRetryException("Insertion temporary failed, retry later");
}

llong DbTable::doUpsertRow(fstring row, DbContext* ctx) {
	const SchemaConfig& sconf = *m_schema;
	if (sconf.m_uniqIndices.size() > 1) {
		THROW_STD(invalid_argument
			, "this table has %zd unique indices, "
			  "must have at most one unique index for calling this method"
			, sconf.m_uniqIndices.size());
	}
	ctx->isUpsertOverwritten = 0;
	if (sconf.m_uniqIndices.empty()) {
		return insertRow(row, ctx); // should always success
	}
	this->throttleWrite();
	IncrementGuard_size_t guard(m_inprogressWritingCount);
	assert(sconf.m_uniqIndices.size() == 1);
	if (!ctx->syncIndex) {
		THROW_STD(invalid_argument,
			"ctx->syncIndex must be true for calling this method");
	}
	if (!m_wrSeg) {
		THROW_STD(invalid_argument
			, "syncFinishWriting('%s') was called, now writing is not allowed"
			, m_dir.string().c_str());
	}
	size_t uniqueIndexId = sconf.m_uniqIndices[0];
	// parseRow doesn't need lock
    auto cols1 = ctx->cols.get();
    auto key1 = ctx->bufs.get();
	sconf.m_rowSchema->parseRow(row, cols1.get());
	const Schema& indexSchema = sconf.getIndexSchema(uniqueIndexId);
	indexSchema.selectParent(*cols1, key1.get());
	{
		MyRwLock lock(m_rwMutex, false);
		ctx->trySyncSegCtxNoLock(this);
		ctx->ensureTransactionNoLock();
	}
	for (size_t segIdx = 0; segIdx < ctx->m_segCtx.size()-1; ++segIdx) {
		auto seg = ctx->m_segCtx[segIdx]->seg;
		assert(seg->m_isFreezed);
		seg->indexSearchExact(segIdx, uniqueIndexId, *key1, &ctx->exactMatchRecIdvec, ctx);
		if (!ctx->exactMatchRecIdvec.empty()) {
			llong subId = ctx->exactMatchRecIdvec[0];
			llong baseId = ctx->m_rowNumVec[segIdx];
			assert(ctx->exactMatchRecIdvec.size() == 1);
			MyRwLock lock(m_rwMutex, false);
			if (ctx->segArrayUpdateSeq != m_segArrayUpdateSeq) {
				ctx->doSyncSegCtxNoLock(this);
				llong recId = baseId + subId;
				size_t upp = upper_bound_a(ctx->m_rowNumVec, recId);
#if !defined(NDEBUG)
				if (seg != ctx->m_segCtx[upp-1]->seg) {
					seg = ctx->m_segCtx[upp-1]->seg; // for set break point
				}
#endif
				segIdx = upp - 1;
				seg = ctx->m_segCtx[segIdx]->seg;
				baseId = ctx->m_rowNumVec[segIdx];
				subId = recId - baseId;
			}
			else {
				ctx->m_rowNumVec.back() = m_rowNum;
			}
			if (seg->m_isDel[subId]) { // should be very rare
				break;
			}
			llong newRecId = insertRowDoInsert(row, cols1.get(), ctx);
			if (newRecId >= 0) {
				{
					SpinRwLock segLock(seg->m_segMutex, true);
					seg->m_delcnt++;
					seg->m_isDel.set1(subId);
					seg->addtoUpdateList(subId);
				}
				DebugCheckUnique(ctx, row, uniqueIndexId);
				ctx->isUpsertOverwritten = 2;
				if (checkPurgeDeleteNoLock(seg)) {
					lock.upgrade_to_writer();
					inLockPutPurgeDeleteTaskToQueue();
					maybeCreateNewSegmentInWriteLock();
				}
				else {
					maybeCreateNewSegment(lock);
				}
			}
			return newRecId;
		}
	}
	MyRwLock lock(m_rwMutex, false);
	ctx->trySyncSegCtxNoLock(this);
	ctx->ensureTransactionNoLock();
	m_wrSeg->indexSearchExact(m_segments.size()-1, uniqueIndexId,
		*key1, &ctx->exactMatchRecIdvec, ctx);
	if (ctx->exactMatchRecIdvec.empty()) {
		llong recId = insertRowDoInsert(row, cols1.get(), ctx);
        DebugCheckUnique(ctx, row, uniqueIndexId);
		maybeCreateNewSegment(lock);
		return recId;
	}
	llong subId = ctx->exactMatchRecIdvec[0];
	llong baseId = m_rowNumVec.ende(2);
	assert(ctx->exactMatchRecIdvec.size() == 1);
	TransactionGuard txn(ctx->m_transaction.get(), subId);
	if (!sconf.m_multIndices.empty()) {
        auto cols2 = ctx->cols.get();
        auto row2 = ctx->bufs.get();
		try {
			txn.storeGetRow(row2.get());
		}
		catch (const ReadRecordException&) {
			fprintf(stderr
				, "ERROR: upsertRow(baseId=%lld, subId=%lld): read old row data failed: %s\n"
				, baseId, subId, m_wrSeg->m_segDir.string().c_str());
			txn.rollback();
			throw ReadRecordException("pre updateSyncMultIndex",
						m_wrSeg->m_segDir.string(), baseId, subId);
		}
		sconf.m_rowSchema->parseRow(*row2, cols2.get()); // old
		updateSyncMultIndex(subId, cols1.get(), cols2.get(), txn.getTxn(), ctx);
	}
	txn.storeUpdate(row);
	if (!txn.commit()) {
		TERARK_THROW(CommitException
			, "commit failed: %s, baseId=%lld, subId=%lld, seg = %s, caller should retry"
			, txn.szError(), baseId, subId, m_wrSeg->m_segDir.string().c_str());
	}
	ctx->isUpsertOverwritten = 1;
	maybeCreateNewSegment(lock);
	return baseId + subId;
}

void
DbTable::upsertRowMultiUniqueIndices(fstring row, valvec<llong>* resRecIdvec, DbContext* ctx) {
	THROW_STD(domain_error, "This method is not supported for now");
	if (!ctx->syncIndex) {
		THROW_STD(invalid_argument,
			"txn->syncIndex must be true for calling this method");
	}
}

llong
DbTable::updateRow(llong id, fstring row, DbContext* ctx) {
	this->throttleWrite();
    auto cols1 = ctx->cols.get();
	m_schema->m_rowSchema->parseRow(row, cols1.get()); // new row
	IncrementGuard_size_t guard(m_inprogressWritingCount);
	MyRwLock lock(m_rwMutex, false);
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNumVec.back());
	if (id >= m_rowNumVec.back()) {
		THROW_STD(invalid_argument
			, "id=%lld is large/equal than rows=%lld"
			, id, m_rowNumVec.back());
	}
	ctx->trySyncSegCtxNoLock(this);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j > 0);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = &*m_segments[j-1];
	bool directUpgrade = true;
	if (ctx->syncIndex) {
		const size_t old_newWrSegNum = m_newWrSegNum;
		if (seg->m_isDel[subId]) {
			THROW_STD(invalid_argument
				, "id=%lld has been deleted, segIdx=%zd, baseId=%lld, subId=%lld"
				, id, j, baseId, subId);
		}
		else {
            auto cols2 = ctx->cols.get();
            auto row2 = ctx->bufs.get();
			seg->getValue(subId, row2.get(), ctx);
			m_schema->m_rowSchema->parseRow(*row2, cols2.get()); // old row

			if (!updateCheckSegDup(0, m_segments.size()-1, cols2.get(), ctx))
				return -1;
			if (!lock.upgrade_to_writer()) {
				// check for segment changes(should be very rare)
				if (old_newWrSegNum != m_newWrSegNum) {
					if (!updateCheckSegDup(m_segments.size()-2, 1, cols2.get(), ctx))
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
		if (ctx->syncIndex) {
			updateWithSyncIndex(subId, row, cols1.get(), ctx);
		}
		else {
			m_wrSeg->m_isDirty = true;
			m_wrSeg->update(subId, row, ctx);
		}
		return id; // id is not changed
	}
	else {
		tryAsyncPurgeDeleteInLock(seg);
		lock.downgrade_to_reader();
	//	lock.release();
	//	lock.acquire(m_rwMutex, false);
		llong recId = insertRowImpl(row, cols1.get(), ctx, lock); // id is changed
		if (recId >= 0) {
			// mark old subId as deleted
			SpinRwLock segLock(seg->m_segMutex);
			seg->addtoUpdateList(size_t(subId));
			seg->m_isDel.set1(subId);
			seg->m_delcnt++;
			assert(seg->m_isDel.popcnt() == seg->m_delcnt);
		}
		return recId;
	}
}

bool
DbTable::updateCheckSegDup(size_t begSeg, size_t numSeg, ColumnVec *cols, DbContext* ctx) {
	// m_wrSeg will be check in unique index insert
	const size_t endSeg = begSeg + numSeg;
	assert(endSeg < m_segments.size()); // don't check m_wrSeg
	if (0 == numSeg)
		return true;
	const SchemaConfig& sconf = *m_schema;
    auto key = ctx->bufs.get();
	for(size_t i = 0; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		for (size_t segIdx = begSeg; segIdx < endSeg; ++segIdx) {
			auto seg = &*m_segments[segIdx];
			assert(iSchema.m_isUnique);
			assert(seg->m_isFreezed);
			iSchema.selectParent(*cols, key.get());
			seg->indexSearchExact(segIdx, indexId, *key, &ctx->exactMatchRecIdvec, ctx);
			for(llong physicId : ctx->exactMatchRecIdvec) {
				llong logicId = seg->getLogicId(physicId);
				if (!seg->m_isDel[logicId]) {
					// std::move makes it no temps
					char szIdstr[96];
					snprintf(szIdstr, sizeof(szIdstr)
						, "logicId = %lld , physicId = %lld"
						, logicId, physicId);
					ctx->errMsg = "DupKey=" + iSchema.toJsonStr(*key)
								+ ", " + szIdstr
								+ ", in frozen seg: " + seg->m_segDir.string();
				//	txn->errMsg += ", rowData=";
				//	txn->errMsg += m_rowSchema->toJsonStr(row);
					return false;
				}
			}
		}
	}
	return true;
}

bool
DbTable::updateWithSyncIndex(llong subId, fstring row, ColumnVec *cols1, DbContext* ctx) {
	const SchemaConfig& sconf = *m_schema;
    auto cols2 = ctx->cols.get();
    auto row2 = ctx->bufs.get();
    auto key1 = ctx->bufs.get();
    auto key2 = ctx->bufs.get();
	TransactionGuard txn(ctx->m_transaction.get(), subId);
	try {
		txn.storeGetRow(row2.get());
	}
	catch (const ReadRecordException&) {
		txn.rollback();
		llong baseId = m_rowNumVec.ende(2);
		throw ReadRecordException("updateWithSyncIndex"
			, m_wrSeg->m_segDir.string(), baseId, subId);
	}
	sconf.m_rowSchema->parseRow(*row2, cols2.get()); // old
	size_t i = 0;
	for (; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(*cols2, key2.get()); // old
		iSchema.selectParent(*cols1, key1.get()); // new
		if (!valvec_equalTo(*key1, *key2)) {
			if (!txn.indexInsert(indexId, *key1)) {
				goto Fail;
			}
		}
	}
	for (i = 0; i < sconf.m_uniqIndices.size(); ++i) {
		size_t indexId = sconf.m_uniqIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(*cols2, key2.get()); // old
		iSchema.selectParent(*cols1, key1.get()); // new
		if (!valvec_equalTo(*key1, *key2)) {
			txn.indexRemove(indexId, *key1);
		}
	}
	updateSyncMultIndex(subId, cols1, cols2.get(), txn.getTxn(), ctx);
	txn.storeUpdate(row);
	if (!txn.commit()) {
		llong baseId = m_rowNumVec.ende(2);
		TERARK_THROW(CommitException
			, "commit failed: %s, baseId=%lld, subId=%lld, seg = %s"
			, txn.szError(), baseId, subId
			, m_wrSeg->m_segDir.string().c_str());
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = sconf.m_uniqIndices[j];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(*cols2, key2.get()); // old
		iSchema.selectParent(*cols1, key1.get()); // new
		if (!valvec_equalTo(*key1, *key2)) {
			txn.indexRemove(indexId, *key1);
		}
	}
	txn.rollback();
	return false;
}

void
DbTable::updateSyncMultIndex(llong subId, ColumnVec *cols1, ColumnVec *cols2, DbTransaction* txn, DbContext* ctx) {
	const SchemaConfig& sconf = *m_schema;
    auto key1 = ctx->bufs.get();
    auto key2 = ctx->bufs.get();
	for (size_t i = 0; i < sconf.m_multIndices.size(); ++i) {
		size_t indexId = sconf.m_multIndices[i];
		const Schema& iSchema = sconf.getIndexSchema(indexId);
		iSchema.selectParent(*cols2, key2.get()); // old
		iSchema.selectParent(*cols1, key1.get()); // new
		if (!valvec_equalTo(*key1, *key2)) {
			txn->indexRemove(indexId, *key2);
			txn->indexInsert(indexId, *key1);
		}
	}
}

static profiling g_pf;

/// @returns number of sleep and retries for throttle
size_t DbTable::throttleWrite() {
	const SchemaConfig& sconf = *m_schema;
	size_t retry = 0, sleepMicrosec = 500;
	for (; ; retry++) {
		size_t throttleRate = sconf.m_writeThrottleBytesPerSecond;
		if (0 == throttleRate)
			return retry;
		ullong newBytes =
			m_accumulateWrittenBytes.load(std::memory_order_relaxed) -
			m_lastWriteThrottleBytes.load(std::memory_order_relaxed);
		if (terark_likely(newBytes < 256*1024)) {
			return retry;
		}
		ullong prev = m_lastWriteThrottleTimePoint.load(std::memory_order_relaxed);
		if (terark_unlikely(0 == prev)) {
			m_lastWriteThrottleTimePoint.store(g_pf.now());
		}
		ullong curr = g_pf.now();
		ullong dura = g_pf.ns(prev, curr); // nanoseconds
		if (newBytes < 1e-9*dura*throttleRate) {
			if (newBytes > 2*1024*1024) {
				m_lastWriteThrottleBytes.store(
					m_accumulateWrittenBytes.load(std::memory_order_relaxed));
				m_lastWriteThrottleTimePoint.store(curr);
			}
			return retry;
		}
		if (m_throwOnThrottle) {
			std::string msg =
				 "WriteThrottleException: dbdir = " + m_dir.string();
			throw WriteThrottleException(msg);
		}
		tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(sleepMicrosec*1e-6));
	//	std::this_thread::sleep_for(std::chrono::microseconds(sleepMicrosec));
		sleepMicrosec = sleepMicrosec*21/13; // fibonacci ratio
	}
	abort();
//	return 0; // never goes here
}

bool DbTable::removeRow(llong id, DbContext* ctx) {
	assert(ctx != nullptr);
	assert(id >= 0);
	assert(id < m_rowNum);
	if (id < 0 || id >= m_rowNum) {
		THROW_STD(invalid_argument,
			"Invalid id = %lld, m_rowNum = %lld\n", id, m_rowNum);
	}
	IncrementGuard_size_t guard(m_inprogressWritingCount);
	const llong snapshotVersion = this->m_rowNum - 1;
	assert(snapshotVersion >= id);
	MyRwLock lock(m_rwMutex, false);
	ctx->ensureTransactionNoLock();
	ctx->trySyncSegCtxNoLock(this);
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNum);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
	if (!seg->m_isFreezed) {
		auto wrseg = m_wrSeg.get();
		assert(wrseg == seg);
		assert(!wrseg->m_bookUpdates);
		{
			SpinRwLock wsLock(wrseg->m_segMutex);
		//	assert(!seg->m_isDel[subId]);
			if (!wrseg->m_isDel[subId]) {
				wrseg->m_delcnt++;
				wrseg->m_isDel.set1(subId); // always set delmark
				wrseg->m_isDirty = true;
		#if !defined(NDEBUG)
				size_t delcnt = wrseg->m_isDel.popcnt();
				assert(delcnt == wrseg->m_delcnt);
		#endif
			}
			else {
				return false;
			}
		}
        BOOST_SCOPE_EXIT(&wrseg, &subId){
            SpinRwLock wsLock(wrseg->m_segMutex);
            wrseg->m_deletedWrIdSet.push_back(uint32_t(subId));
        }BOOST_SCOPE_EXIT_END;
		if (ctx->syncIndex) {
			TransactionGuard txn(ctx->m_transaction.get(), subId);
            auto cols = ctx->cols.get();
			auto row = ctx->bufs.get();
            auto key = ctx->bufs.get();
			try {
				txn.storeGetRow(row.get());
			}
			catch (const ReadRecordException& ex) {
#if 0//!defined(NDEBUG)
                fprintf(stderr
                        , "ERROR: removeRow(id=%lld): read row data failed: %s\n"
                        , id, ex.what());
#else
                (void)ex;
#endif
				txn.rollback();
				throw ReadRecordException("removeRow: pre remove index",
					wrseg->m_segDir.string(), baseId, subId);
			}
			m_schema->m_rowSchema->parseRow(*row, cols.get());
			for (size_t i = 0; i < wrseg->m_indices.size(); ++i) {
				const Schema& iSchema = m_schema->getIndexSchema(i);
				iSchema.selectParent(*cols, key.get());
				txn.indexRemove(i, *key);
			}
			txn.storeRemove();
			if (!txn.commit()) {
				// this fail should be ignored, because the deletion bit
				// have always be set, remove index is just an optimization
				// for future search
				fprintf(stderr
					, "WARN: removeRow: commit failed: recId=%lld, baseId=%lld, subId=%lld, seg = %s"
					, id, baseId, subId, wrseg->m_segDir.string().c_str());
			}
		}
		return true;
	}
	else { // freezed segment, just set del mark
		bool success = false;
		if (seg->m_deletionTime) {
			assert(nullptr != m_schema->m_snapshotSchema);
			llong* deltime = (llong*)seg->getRecordsBasePtr();
			SpinRwLock wsLock(seg->m_segMutex);
			if (deltime[subId] != LLONG_MAX) {
				deltime[subId] = snapshotVersion;
				seg->addtoUpdateList(size_t(subId));
				success = true;
			}
		}
		else {
			SpinRwLock wsLock(seg->m_segMutex);
		//	assert(!seg->m_isDel[subId]);
			if (!seg->m_isDel[subId]) {
				seg->addtoUpdateList(size_t(subId));
				seg->m_isDel.set1(subId);
				seg->m_delcnt++;
				seg->m_isDirty = true;
		#if !defined(NDEBUG)
				size_t delcnt = seg->m_isDel.popcnt();
				assert(delcnt == seg->m_delcnt);
		#endif
				success = true;
			}
		}
		if (checkPurgeDeleteNoLock(seg)) {
			lock.upgrade_to_writer();
			inLockPutPurgeDeleteTaskToQueue();
		}
		return success;
	}
}

void DbTable::delmarkSet0(llong id) {
	assert(id >= 0);
	assert(id < m_rowNum);
	if (id < 0 || id >= m_rowNum) {
		THROW_STD(invalid_argument,
			"Invalid id = %lld, m_rowNum = %lld\n", id, m_rowNum);
	}
	MyRwLock lock(m_rwMutex, false);
	size_t upp = upper_bound_a(m_rowNumVec, id);
	auto seg = m_segments[upp-1].get();
	llong baseId = m_rowNumVec[upp-1];
	size_t subId = size_t(id - baseId);
	SpinRwLock segLock(seg->m_segMutex, true);
	assert(seg->m_isDel[subId]);
	seg->m_isDel.set0(subId);
	seg->m_delcnt--;
}

void DbTable::delmarkSet1(llong id) {
	assert(id >= 0);
	assert(id < m_rowNum);
	if (id < 0 || id >= m_rowNum) {
		THROW_STD(invalid_argument,
			"Invalid id = %lld, m_rowNum = %lld\n", id, m_rowNum);
	}
	MyRwLock lock(m_rwMutex, false);
	size_t upp = upper_bound_a(m_rowNumVec, id);
	auto seg = m_segments[upp-1].get();
	llong baseId = m_rowNumVec[upp-1];
	size_t subId = size_t(id - baseId);
	bool success = false;
	{
		SpinRwLock segLock(seg->m_segMutex, true);
		assert(subId < seg->m_isDel.size());
	//	assert(!seg->m_isDel[subId]);
		if (!seg->m_isDel[subId]) {
			seg->m_isDel.set1(subId);
			seg->m_delcnt++;
			success = true;
		}
	}
	if (success && seg->getReadonlySegment()) {
		if (checkPurgeDeleteNoLock(seg)) {
			lock.upgrade_to_writer();
			inLockPutPurgeDeleteTaskToQueue();
		}
	}
}

void DbTable::putToFreeList(llong id) {
	assert(id >= 0);
	assert(id < m_rowNum);
	if (id < 0 || id >= m_rowNum) {
		THROW_STD(invalid_argument,
			"Invalid id = %lld, m_rowNum = %lld\n", id, m_rowNum);
	}
	MyRwLock lock(m_rwMutex, false);
	size_t upp = upper_bound_a(m_rowNumVec, id);
	llong baseId = m_rowNumVec[upp-1];
	size_t subId = size_t(id - baseId);
	auto seg = m_segments[upp-1].get();
	SpinRwLock segLock(seg->m_segMutex, true);
	assert(seg->m_isDel[subId]);
	if (terark_likely(!seg->m_isFreezed)) {
		auto wrseg = static_cast<WritableSegment*>(seg);
		wrseg->m_deletedWrIdSet.push_back(subId);
	} else {
		seg->addtoUpdateList(subId);
	}
}

///! Can inplace update column in ReadonlySegment
void
DbTable::updateColumn(llong recordId, size_t columnId,
							 fstring newColumnData, DbContext* ctx) {
#include "update_column_impl.hpp"
	if (newColumnData.size() != rowSchema.getColumnMeta(columnId).fixedLen) {
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s fixedLen=%zd newLen=%zd"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			, size_t(rowSchema.getColumnMeta(columnId).fixedLen)
			, newColumnData.size()
			);
	}
	SpinRwLock segLock(seg->m_segMutex);
	memcpy(coldata, newColumnData.data(), newColumnData.size());
	if (seg->m_isFreezed)
		seg->addtoUpdateList(subId);
}

void
DbTable::updateColumn(llong recordId, fstring colname,
							 fstring newColumnData, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumn(recordId, columnId, newColumnData, ctx);
}

template<class WireType, class LlongOrFloat, class OP>
static inline
bool updateValueByOp(ReadableSegment* seg, llong subId, byte& byteRef, const OP& op) {
	LlongOrFloat val = reinterpret_cast<WireType&>(byteRef);
	if (op(val)) {
		SpinRwLock segLock(seg->m_segMutex);
		reinterpret_cast<WireType&>(byteRef) = val;
		if (seg->m_isFreezed)
			seg->addtoUpdateList(size_t(subId));
		return true;
	}
	return false;
}

void
DbTable::updateColumnInteger(llong recordId, size_t columnId,
									const std::function<bool(llong&val)>& op,
									DbContext* ctx) {
#include "update_column_impl.hpp"
	switch (rowSchema.getColumnType(columnId)) {
	default:
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			);
	case ColumnType::Uint08:  updateValueByOp<uint8_t , llong>(seg, subId, *coldata, op); break;
	case ColumnType::Sint08:  updateValueByOp< int8_t , llong>(seg, subId, *coldata, op); break;
	case ColumnType::Uint16:  updateValueByOp<uint16_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Sint16:  updateValueByOp< int16_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Uint32:  updateValueByOp<uint32_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Sint32:  updateValueByOp< int32_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Uint64:  updateValueByOp<uint64_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Sint64:  updateValueByOp< int64_t, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Float32: updateValueByOp<   float, llong>(seg, subId, *coldata, op); break;
	case ColumnType::Float64: updateValueByOp<  double, llong>(seg, subId, *coldata, op); break;
	}
}

void
DbTable::updateColumnInteger(llong recordId, fstring colname,
									const std::function<bool(llong&val)>& op,
									DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumnInteger(recordId, colname, op, ctx);
}

void
DbTable::updateColumnDouble(llong recordId, size_t columnId,
								   const std::function<bool(double&val)>& op,
								   DbContext* ctx) {
#include "update_column_impl.hpp"
	switch (rowSchema.getColumnType(columnId)) {
	default:
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			);
	case ColumnType::Uint08:  updateValueByOp<uint08_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Sint08:  updateValueByOp< int08_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Uint16:  updateValueByOp<uint16_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Sint16:  updateValueByOp< int16_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Uint32:  updateValueByOp<uint32_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Sint32:  updateValueByOp< int32_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Uint64:  updateValueByOp<uint64_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Sint64:  updateValueByOp< int64_t, double>(seg, subId, *coldata, op); break;
	case ColumnType::Float32: updateValueByOp<   float, double>(seg, subId, *coldata, op); break;
	case ColumnType::Float64: updateValueByOp<  double, double>(seg, subId, *coldata, op); break;
	}
}

void
DbTable::updateColumnDouble(llong recordId, fstring colname,
								   const std::function<bool(double&val)>& op,
								   DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumnDouble(recordId, colname, op, ctx);
}

void
DbTable::incrementColumnValue(llong recordId, size_t columnId,
									 llong incVal, DbContext* ctx) {
#include "update_column_impl.hpp"
	SpinRwLock segLock(seg->m_segMutex);
	switch (rowSchema.getColumnType(columnId)) {
	default:
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			);
	case ColumnType::Uint08:
	case ColumnType::Sint08: *(int8_t*)coldata += incVal; break;
	case ColumnType::Uint16:
	case ColumnType::Sint16: *(int16_t*)coldata += incVal; break;
	case ColumnType::Uint32:
	case ColumnType::Sint32: *(int32_t*)coldata += incVal; break;
	case ColumnType::Uint64:
	case ColumnType::Sint64: *(int64_t*)coldata += incVal; break;
	case ColumnType::Float32: *(float *)coldata += incVal; break;
	case ColumnType::Float64: *(double*)coldata += incVal; break;
	}
	if (seg->m_isFreezed)
		seg->addtoUpdateList(subId);
}

void
DbTable::incrementColumnValue(llong recordId, fstring colname,
									 llong incVal, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	incrementColumnValue(recordId, colname, incVal, ctx);
}

void
DbTable::incrementColumnValue(llong recordId, size_t columnId,
									 double incVal, DbContext* ctx) {
#include "update_column_impl.hpp"
	SpinRwLock segLock(seg->m_segMutex);
	switch (rowSchema.getColumnType(columnId)) {
	default:
		THROW_STD(invalid_argument
			, "Invalid column(id=%zd, name=%s) which columnType=%s"
			, columnId, rowSchema.getColumnName(columnId).c_str()
			, Schema::columnTypeStr(rowSchema.getColumnType(columnId))
			);
	case ColumnType::Uint08:
	case ColumnType::Sint08: *(int8_t*)coldata += incVal; break;
	case ColumnType::Uint16:
	case ColumnType::Sint16: *(int16_t*)coldata += incVal; break;
	case ColumnType::Uint32:
	case ColumnType::Sint32: *(int32_t*)coldata += incVal; break;
	case ColumnType::Uint64:
	case ColumnType::Sint64: *(int64_t*)coldata += incVal; break;
	case ColumnType::Float32: *(float *)coldata += incVal; break;
	case ColumnType::Float64: *(double*)coldata += incVal; break;
	}
	if (seg->m_isFreezed)
		seg->addtoUpdateList(subId);
}

void
DbTable::incrementColumnValue(llong recordId, fstring colname,
									 double incVal, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	incrementColumnValue(recordId, colname, incVal, ctx);
}

bool
DbTable::indexKeyExists(size_t indexId, fstring key, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	return indexKeyExistsNoLock(indexId, key, ctx);
}

bool
DbTable::indexKeyExistsNoLock(size_t indexId, fstring key, DbContext* ctx)
const {
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument, "invalid indexId = %zd, indexNum = %zd"
			, indexId, m_schema->getIndexNum());
	}
	ctx->exactMatchRecIdvec.erase_all();
	size_t segNum = ctx->m_segCtx.size();
	for (size_t i = 0; i < segNum; ++i) {
		auto seg = ctx->m_segCtx[i]->seg;
		seg->indexSearchExactAppend(i, indexId, key, &ctx->exactMatchRecIdvec, ctx);
		if (ctx->exactMatchRecIdvec.size()) {
			return true;
		}
	}
	return false;
}

void
DbTable::indexSearchExact(size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	indexSearchExactNoLock(indexId, key, recIdvec, ctx);
}

/// returned recIdvec is sorted by recId ascending
void
DbTable::indexSearchExactNoLock(size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext* ctx)
const {
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument, "invalid indexId = %zd, indexNum = %zd"
			, indexId, m_schema->getIndexNum());
	}
	recIdvec->erase_all();
	const bool isUnique = m_schema->getIndexSchema(indexId).m_isUnique;
	size_t segNum = ctx->m_segCtx.size();
#if 0
	// search older segments first
	for (size_t i = 0; i < segNum; ++i) {
		auto seg = ctx->m_segCtx[i]->seg;
		if (seg->m_isDel.size() == seg->m_delcnt)
			continue;
		size_t oldsize = recIdvec->size();
		seg->indexSearchExactAppend(i, indexId, key, recIdvec, ctx);
		size_t newsize = recIdvec->size();
		size_t len = newsize - oldsize;
		if (len) {
			llong* p = recIdvec->data() + oldsize;
			llong baseId = ctx->m_rowNumVec[i];
			for (size_t j = 0; j < len; ++j) {
				p[j] += baseId;
			}
			if (isUnique) {
			//	assert(1 == newsize);
				TERARK_IF_DEBUG(;,return);
			}
			if (len >= 2)
				std::sort(p, p + len);
		}
	}
//	std::reverse(recIdvec->begin(), recIdvec->end()); // make descending
#else
	// search newer segments first
	for (size_t i = segNum; i > 0; ) {
		auto seg = ctx->m_segCtx[--i]->seg;
		if (seg->m_isDel.size() == seg->m_delcnt)
			continue;
		size_t oldsize = recIdvec->size();
		seg->indexSearchExactAppend(i, indexId, key, recIdvec, ctx);
		size_t newsize = recIdvec->size();
		size_t len = newsize - oldsize;
		if (len) {
			llong* p = recIdvec->data() + oldsize;
			llong baseId = ctx->m_rowNumVec[i];
			for (size_t j = 0; j < len; ++j) {
				p[j] += baseId;
			}
			if (isUnique) {
			//	assert(1 == newsize);
				TERARK_IF_DEBUG(;,return);
			}
			if (len >= 2) {
				std::sort(p, p + len); // don't use std::greater
				std::reverse(p, p + len); // in descending order
			}
		}
	}
#endif
}

// implemented in DfaDbTable
///@params recIdvec result of matched record id list
bool
DbTable::indexMatchRegex(size_t indexId, RegexForIndex* regex,
						 valvec<llong>* recIdvec, DbContext* ctx)
const {
	if (indexId >= m_schema->getIndexNum()) {
		THROW_STD(invalid_argument
			, "invalid indexId=%zd is not less than indexNum=%zd"
			, indexId, m_schema->getIndexNum());
	}
	const Schema& schema = m_schema->getIndexSchema(indexId);
	if (schema.columnNum() > 1) {
		THROW_STD(invalid_argument
			, "can not MatchRegex on composite indexId=%zd indexName=%s"
			, indexId, schema.m_name.c_str());
	}
	auto& colmeta = schema.getColumnMeta(0);
	if (!colmeta.isString()) {
		THROW_STD(invalid_argument
			, "can not MatchRegex on non-string indexId=%zd indexName=%s"
			, indexId, schema.m_name.c_str());
	}
	ctx->trySyncSegCtxSpeculativeLock(this);
	recIdvec->erase_all();
	for (size_t i = 0; i < ctx->m_segCtx.size(); ++i) {
		auto seg = ctx->m_segCtx[i]->seg;
		if (seg->getWritableStore()) {
			if (seg->m_isDel.size() > 0) {
			  fprintf(stderr
				, "WARN: segment: %s is a writable segment, can not MatchRegex\n"
				, getSegPath("wr", i).string().c_str());
			}
			continue;
		}
		auto index = seg->m_indices[indexId].get();
		size_t oldsize = recIdvec->size();
		const llong* deltime = nullptr;
		const llong  baseId = ctx->m_rowNumVec[i];
		llong snapshotVersion = ctx->m_mySnapshotVersion;
		if (seg->m_deletionTime) {
			assert(nullptr != m_schema->m_snapshotSchema);
			deltime = (const llong*)(seg->m_deletionTime->getRecordsBasePtr());
		}
		if (index->matchRegexAppend(regex, recIdvec, ctx)) {
			size_t i = oldsize;
			for(size_t j = oldsize; j < recIdvec->size(); ++j) {
				size_t subPhysicId = (*recIdvec)[j];
				size_t subLogicId = seg->getLogicId(subPhysicId);
				if (deltime) {
					if (deltime[subPhysicId] > snapshotVersion)
						(*recIdvec)[i++] = baseId + subLogicId;
				}
				else {
					if (!seg->m_isDel[subLogicId])
						(*recIdvec)[i++] = baseId + subLogicId;
				}
			}
			recIdvec->risk_set_size(i);
		}
		else if (schema.m_enableLinearScan) {
			fprintf(stderr
				, "WARN: RegexForIndex match exceeded memory limit(%zd bytes) on index '%s' of segment: '%s', try linear scan...\n"
				, ctx->regexMatchMemLimit
				, schema.m_name.c_str(), seg->m_segDir.string().c_str());
			valvec<byte> key;
			size_t subPhysicId = 0;
			size_t subLogicId = 0;
			size_t subRowsNum = seg->m_isDel.size();
			boost::intrusive_ptr<SeqReadAppendonlyStore>
				seqStore(new SeqReadAppendonlyStore(seg->m_segDir, schema));
			StoreIteratorPtr iter = seqStore->createStoreIterForward(ctx);
			const bm_uint_t* isDel = seg->m_isDel.bldata();
			const bm_uint_t* isPurged = seg->m_isPurged.bldata();
			for (; subLogicId < subRowsNum; subLogicId++) {
				if (!isPurged || !terark_bit_test(isPurged, subLogicId)) {
					llong subCheckPhysicId = INT_MAX; // for fail fast
					bool hasData = iter->increment(&subCheckPhysicId, &key);
					TERARK_RT_assert(hasData, std::logic_error);
					TERARK_RT_assert(size_t(subCheckPhysicId) == subPhysicId, std::logic_error);
					if (deltime) {
						if (deltime[subPhysicId] > snapshotVersion) {
							if (regex->matchText(key)) {
								recIdvec->push_back(baseId + subLogicId);
							}
						}
					}
					else {
						if (!terark_bit_test(isDel, subLogicId)) {
							if (regex->matchText(key)) {
								recIdvec->push_back(baseId + subLogicId);
							}
						}
					}
					subPhysicId++;
				}
			}
		}
		else { // failed because exceeded memory limit
			// should fallback to use linear scan?
			fprintf(stderr
				, "ERROR: RegexMatch exceeded memory limit(%zd bytes) on index '%s' of segment: '%s', and linear scan is not enabled, failed!\n"
				, ctx->regexMatchMemLimit
				, schema.m_name.c_str(), seg->m_segDir.string().c_str());
		}
	}
	return true;
}

bool
DbTable::indexInsert(size_t indexId, fstring indexKey, llong id,
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
DbTable::indexRemove(size_t indexId, fstring indexKey, llong id,
							DbContext* ctx)
{
	assert(ctx != nullptr);
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
	return wrIndex->remove(indexKey, subId, ctx);
}

bool
DbTable::indexUpdate(size_t indexId, fstring indexKey,
							 llong oldId, llong newId,
							 DbContext* ctx)
{
	assert(ctx != nullptr);
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
		return wrIndex->replace(indexKey, oldSubId, newSubId, ctx);
	}
	else {
		auto oldseg = m_segments[oldupp-1].get();
		auto newseg = m_segments[newupp-1].get();
		auto oldIndex = oldseg->m_indices[indexId]->getWritableIndex();
		auto newIndex = newseg->m_indices[indexId]->getWritableIndex();
		bool ret = true;
		lock.upgrade_to_writer();
		if (oldIndex) {
			ret = oldIndex->remove(indexKey, oldSubId, ctx);
			oldseg->m_isDirty = true;
		}
		if (newIndex) {
			ret = oldIndex->insert(indexKey, newSubId, ctx);
			newseg->m_isDirty = true;
		}
		return ret;
	}
}

llong DbTable::indexStorageSize(size_t indexId) const {
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
	const DbTablePtr m_tab;
	const DbContextPtr m_ctx;
	const size_t m_indexId;
	const Schema& m_ischema;
	struct OneSeg {
		ReadableSegmentPtr seg;
		IndexIteratorPtr   iter;
		valvec<byte>       data;
		llong              subId = -1;
		llong              baseId = -1;
		llong              padding = 0;
	};
	valvec<OneSeg> m_segs;
	static bool
	lessThanImp(const Schema* schema, const OneSeg* segs, size_t x, size_t y) {
		const auto& xkey = segs[x].data;
		const auto& ykey = segs[y].data;
		if (xkey.empty()) {
			if (ykey.empty())
				return false; // equal
			else
				return true; // xkey < ykey
		}
		if (ykey.empty())
			return false; // xkey > ykey
		int r = schema->compareData(xkey, ykey);
		if (r) return r < 0;
		else   return x < y;
	}
	template<bool Forward> class HeapKeyCompareMultiColumns {
		const Schema* schema;
		const OneSeg* segs;
	public:
		bool operator()(size_t x, size_t y) const {
			// min heap's compare is 'greater'
			if (Forward)
				return lessThanImp(schema, segs, y, x);
			else
				return lessThanImp(schema, segs, x, y);
		}
		HeapKeyCompareMultiColumns(const Schema& schema1, const OneSeg* segs1)
		: schema(&schema1), segs(segs1) {}
	};
	template<bool Forward> class HeapKeyCompareOneColumn {
		const Schema::OneColumnComparator comp;
		const OneSeg* segs;
	public:
		bool operator()(size_t x, size_t y) const {
			// min heap's compare is 'greater'
			const auto& xkey = segs[x].data;
			const auto& ykey = segs[y].data;
			if (Forward)
				return comp(ykey, xkey) < 0;
			else
				return comp(xkey, ykey) < 0;
		}
		HeapKeyCompareOneColumn(const Schema& schema1, const OneSeg* segs1)
		: comp(schema1.getOneColumnComparator()), segs(segs1) {}
	};
	valvec<byte> m_keyBuf;
	ColumnVec    m_keyColvec;
	terark::valvec<size_t> m_heap;
	size_t m_oldsegArrayUpdateSeq;
	const bool m_forward;
	bool m_isHeapBuilt;

	void makeHeap() {
		size_t* beg = m_heap.begin();
		size_t* end = m_heap.end();
		const OneSeg* segs = m_segs.data();
		if (m_ischema.columnNum() == 1) {
			if (m_forward)
				std::make_heap(beg, end, HeapKeyCompareOneColumn<1>(m_ischema, segs));
			else
				std::make_heap(beg, end, HeapKeyCompareOneColumn<0>(m_ischema, segs));
		}
		else {
			if (m_forward)
				std::make_heap(beg, end, HeapKeyCompareMultiColumns<1>(m_ischema, segs));
			else
				std::make_heap(beg, end, HeapKeyCompareMultiColumns<0>(m_ischema, segs));
		}
	}

	void pushHeap() {
		size_t* beg = m_heap.begin();
		size_t* end = m_heap.end();
		const OneSeg* segs = m_segs.data();
		if (m_ischema.columnNum() == 1) {
			if (m_forward)
				std::push_heap(beg, end, HeapKeyCompareOneColumn<1>(m_ischema, segs));
			else
				std::push_heap(beg, end, HeapKeyCompareOneColumn<0>(m_ischema, segs));
		}
		else {
			if (m_forward)
				std::push_heap(beg, end, HeapKeyCompareMultiColumns<1>(m_ischema, segs));
			else
				std::push_heap(beg, end, HeapKeyCompareMultiColumns<0>(m_ischema, segs));
		}
	}

	void popHeap() {
		size_t* beg = m_heap.begin();
		size_t* end = m_heap.end();
		const OneSeg* segs = m_segs.data();
		if (m_ischema.columnNum() == 1) {
			if (m_forward)
				std::pop_heap(beg, end, HeapKeyCompareOneColumn<1>(m_ischema, segs));
			else
				std::pop_heap(beg, end, HeapKeyCompareOneColumn<0>(m_ischema, segs));
		}
		else {
			if (m_forward)
				std::pop_heap(beg, end, HeapKeyCompareMultiColumns<1>(m_ischema, segs));
			else
				std::pop_heap(beg, end, HeapKeyCompareMultiColumns<0>(m_ischema, segs));
		}
	}

	IndexIterator* createIter(const ReadableSegment& seg) {
		auto index = seg.m_indices[m_indexId];
		if (m_forward)
			return index->createIndexIterForward(m_ctx.get());
		else
			return index->createIndexIterBackward(m_ctx.get());
	}

	size_t syncSegPtr() {
		m_ctx->trySyncSegCtxSpeculativeLock(m_tab.get());
		if (m_oldsegArrayUpdateSeq == m_tab->m_segArrayUpdateSeq) {
			return 0;
		}
		size_t numChangedSegs = 0;
		sort_a(m_segs, By_seg_get());
		valvec<OneSeg> tmp(m_tab->m_segments.size()+2); // with 2 extra
		MyRwLock lock(m_tab->m_rwMutex, false);
		m_oldsegArrayUpdateSeq = m_tab->m_segArrayUpdateSeq;
		tmp.resize(m_tab->m_segments.size());
		const llong* rowNumVec = m_tab->m_rowNumVec.data();
		OneSeg* segA = m_segs.data();
		size_t  segN = m_segs.size();
		for (size_t i = 0; i < tmp.size(); ++i) {
			auto& cur = tmp[i];
			auto  seg = m_tab->m_segments[i].get();
			assert(NULL != seg);
			size_t lo = lower_bound_ex_0(segA, segN, seg, By_seg_get());
			if (lo < segN && segA[lo].seg.get() == seg) {
				assert(segA[lo].baseId == rowNumVec[i]);
				cur.seg .swap(segA[lo].seg);
				cur.iter.swap(segA[lo].iter);
				cur.data.swap(segA[lo].data);
				cur.subId = segA[lo].subId;
			}
			else {
				cur.seg = seg;
				cur.subId = -2; // need re-seek position??
				numChangedSegs++;
			}
			cur.baseId = rowNumVec[i];
		}
		assert(numChangedSegs > 0);
		m_segs.swap(tmp);
		return numChangedSegs;
	}

public:
	TableIndexIter(const DbTable* tab, size_t indexId, bool forward, DbContext* ctx)
	  : m_tab(const_cast<DbTable*>(tab))
	  , m_ctx(ctx ? ctx : tab->createDbContext())
	  , m_indexId(indexId)
	  , m_ischema(tab->getIndexSchema(m_indexId))
	  , m_forward(forward)
	{
		assert(tab->m_schema->getIndexSchema(indexId).m_isOrdered);
		m_isUniqueInSchema = tab->m_schema->getIndexSchema(indexId).m_isUnique;
		{
			MyRwLock lock(tab->m_rwMutex);
			tab->m_tableScanningRefCount++;
		}
		m_oldsegArrayUpdateSeq = 0;
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
		m_oldsegArrayUpdateSeq = 0;
		m_isHeapBuilt = false;
		m_ctx->trySyncSegCtxSpeculativeLock(m_tab.get());
	}
	bool increment(llong* id, valvec<byte>* key) override {
		if (terark_unlikely(!m_isHeapBuilt)) {
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
					cur.subId = cur.seg->getLogicId(cur.subId);
				}
			}
			makeHeap();
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
					*key = m_keyBuf;
				return true;
			}
		}
		return false;
	}
	size_t incrementNoCheckDel(llong* subId) {
		assert(!m_heap.empty());
		size_t segIdx = m_heap[0];
		popHeap();
		auto& cur = m_segs[segIdx];
		*subId = cur.subId;
		m_keyBuf.swap(cur.data); // should be assign, but swap is more efficient
		if (cur.iter->increment(&cur.subId, &cur.data)) {
			assert(m_heap.back() == segIdx);
			pushHeap();
			cur.subId = cur.seg->getLogicId(cur.subId);
		}
		else {
			m_heap.pop_back();
			cur.subId = -3; // eof
			cur.data.erase_all();
		}
		return segIdx;
	}
	bool isDeleted(size_t segIdx, llong subId) {
		auto seg = m_segs[segIdx].seg.get();
		if (seg->m_isFreezed) {
			return seg->m_isDel[subId];
		} else {
			return seg->locked_testIsDel(subId);
		}
	}
	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		return seekBound(key, id, retKey, true);
	}
	int seekUpperBound(fstring key, llong* id, valvec<byte>* retKey) override {
		return seekBound(key, id, retKey, false);
	}
	int seekBound(fstring key, llong* id, valvec<byte>* retKey, bool inclusive) {
		const Schema& schema = m_ischema;
#if 0//!defined(NDEBUG)
		fprintf(stderr, "DEBUG: TableIndexIter::%s: segs=%zd key=%s, keylen=%zd\n",
				inclusive?"seekLowerBound":"seekUpperBound",
				m_tab->m_segments.size(), schema.toJsonStr(key).c_str(), key.size());
#endif
		if (schema.getColumnType(schema.columnNum()-1) == ColumnType::StrZero) {
			if (schema.columnNum() == 1) {
				assert(key.size() == 0 || key.ende(1) != 0);
				if (key.size() > 0 && key.ende(1) == 0)
					key.n--;
			}
			else {
				schema.parseRow(key, &m_keyColvec);
				assert(m_keyColvec.size() == schema.columnNum());
				fstring lastCol = m_keyColvec[schema.columnNum()-1];
				assert(lastCol.size() == 0 || lastCol.ende(1) != 0);
				if (key.end() > lastCol.begin() && key.ende(1) == 0)
					key.n--;
			}
		}
		if (key.size() == 0 && inclusive) {
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
			int ret = inclusive
					? cur.iter->seekLowerBound(key, &cur.subId, &cur.data)
					: cur.iter->seekUpperBound(key, &cur.subId, &cur.data)
					;
			if (ret >= 0) {
				m_heap.push_back(i);
				cur.subId = cur.seg->getLogicId(cur.subId);
			}
		#if 0//!defined(NDEBUG)
			fprintf(stderr
				, "DEBUG: %s, seg[%zd].iter->%s(%s) = %d, retKey=%s\n"
				, cur.seg->m_segDir.string().c_str()
				, i, inclusive?"seekLowerBound":"seekUpperBound"
				, schema.toJsonStr(key).c_str(), ret
				, schema.toJsonStr(cur.data).c_str()
				);
		#endif
		}
		m_isHeapBuilt = true;
		if (m_heap.size()) {
			makeHeap();
			while (!m_heap.empty()) {
				llong subId;
				size_t segIdx = incrementNoCheckDel(&subId);
				if (!isDeleted(segIdx, subId)) {
					assert(subId < m_segs[segIdx].seg->numDataRows());
					llong baseId = m_segs[segIdx].baseId;
					*id = baseId + subId;
				#if !defined(NDEBUG)
					assert(*id < m_tab->m_rowNum);
					if (m_forward) {
						if (schema.compareData(key, m_keyBuf) > 0) {
							fprintf(stderr, "ERROR: key=%s m_keyBuf=%s\n"
								, schema.toJsonStr(key).c_str()
								, schema.toJsonStr(m_keyBuf).c_str());
						}
						assert(schema.compareData(key, m_keyBuf) <= 0);
					} else {
						assert(schema.compareData(key, m_keyBuf) >= 0);
					}
				#endif
					int ret = (key == m_keyBuf) ? 0 : 1;
					if (retKey)
						*retKey = m_keyBuf;
					return ret;
				}
			}
		}
		else {
		#if !defined(NDEBUG) && 0
			fprintf(stderr, "DEBUG: heap is empty: key=%s\n"
				, schema.toJsonStr(key).c_str());
		#endif
		}
		return -1;
	}
};

IndexIteratorPtr DbTable::createIndexIterForward(size_t indexId, DbContext* ctx) const {
	assert(indexId < m_schema->getIndexNum());
	assert(m_schema->getIndexSchema(indexId).m_isOrdered);
	return new TableIndexIter(this, indexId, true, ctx);
}

IndexIteratorPtr DbTable::createIndexIterForward(fstring indexCols, DbContext* ctx) const {
	size_t indexId = m_schema->getIndexId(indexCols);
	if (m_schema->getIndexNum() == indexId) {
		THROW_STD(invalid_argument, "index: %s not exists", indexCols.c_str());
	}
	return createIndexIterForward(indexId, ctx);
}

IndexIteratorPtr DbTable::createIndexIterBackward(size_t indexId, DbContext* ctx) const {
	assert(indexId < m_schema->getIndexNum());
	assert(m_schema->getIndexSchema(indexId).m_isOrdered);
	return new TableIndexIter(this, indexId, false, ctx);
}

IndexIteratorPtr DbTable::createIndexIterBackward(fstring indexCols, DbContext* ctx) const {
	size_t indexId = m_schema->getIndexId(indexCols);
	if (m_schema->getIndexNum() == indexId) {
		THROW_STD(invalid_argument, "index: %s not exists", indexCols.c_str());
	}
	return createIndexIterBackward(indexId, ctx);
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
DbTable::getProjectColumns(const hash_strmap<>& colnames) const {
	assert(colnames.delcnt() == 0);
	return doGetProjectColumns(colnames, *m_schema->m_rowSchema);
}

void
DbTable::selectColumns(llong id, const valvec<size_t>& cols,
							  valvec<byte>* colsData, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectColumnsNoLock(id, cols, colsData, ctx);
}

void
DbTable::selectColumnsNoLock(llong id, const valvec<size_t>& cols,
									valvec<byte>* colsData, DbContext* ctx)
const {
//	DebugCheckRowNumVecNoLock(this);
	llong rows = m_rowNum;
	if (terark_unlikely(id < 0 || id >= rows)) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(ctx->m_rowNumVec, id);
	llong baseId = ctx->m_rowNumVec[upp-1];
	auto seg = ctx->m_segCtx[upp-1]->seg;
	llong subId = id - baseId;
    if(seg->testIsDel(subId))
    {
        throw ReadDeletedRecordException(seg->m_segDir.string(), baseId, subId);
    }
	seg->selectColumns(subId, cols.data(), cols.size(), colsData, ctx);
}

void
DbTable::selectColumns(llong id, const size_t* colsId, size_t colsNum,
							  valvec<byte>* colsData, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectColumnsNoLock(id, colsId, colsNum, colsData, ctx);
}

void
DbTable::selectColumnsNoLock(llong id, const size_t* colsId, size_t colsNum,
									valvec<byte>* colsData, DbContext* ctx)
const {
//	DebugCheckRowNumVecNoLock(this);
	llong rows = m_rowNum;
	if (terark_unlikely(id < 0 || id >= rows)) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(ctx->m_rowNumVec, id);
	llong baseId = ctx->m_rowNumVec[upp-1];
	auto seg = ctx->m_segCtx[upp-1]->seg;
	llong subId = id - baseId;
    if(seg->testIsDel(subId))
    {
        throw ReadDeletedRecordException(seg->m_segDir.string(), baseId, subId);
    }
	seg->selectColumns(subId, colsId, colsNum, colsData, ctx);
}

void
DbTable::selectOneColumn(llong id, size_t columnId,
								valvec<byte>* colsData, DbContext* ctx)
const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectOneColumnNoLock(id, columnId, colsData, ctx);
}

void
DbTable::selectOneColumnNoLock(llong id, size_t columnId,
									  valvec<byte>* colsData, DbContext* ctx)
const {
//	DebugCheckRowNumVecNoLock(this);
	llong rows = m_rowNum;
	if (terark_unlikely(id < 0 || id >= rows)) {
		THROW_STD(out_of_range, "id = %lld, rows=%lld", id, rows);
	}
	size_t upp = upper_bound_a(ctx->m_rowNumVec, id);
	llong baseId = ctx->m_rowNumVec[upp-1];
	auto seg = ctx->m_segCtx[upp-1]->seg;
	llong subId = id - baseId;
    if(seg->testIsDel(subId))
    {
        throw ReadDeletedRecordException(seg->m_segDir.string(), baseId, subId);
    }
	seg->selectOneColumn(subId, columnId, colsData, ctx);
}

void DbTable::selectColgroups(llong recId, const valvec<size_t>& cgIdvec,
						valvec<valvec<byte> >* cgDataVec, DbContext* ctx) const {
	cgDataVec->resize(cgIdvec.size());
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectColgroupsNoLock(recId, cgIdvec.data(), cgIdvec.size(), cgDataVec->data(), ctx);
}
void DbTable::selectColgroupsNoLock(llong recId, const valvec<size_t>& cgIdvec,
						valvec<valvec<byte> >* cgDataVec, DbContext* ctx) const {
	cgDataVec->resize(cgIdvec.size());
	selectColgroupsNoLock(recId, cgIdvec.data(), cgIdvec.size(), cgDataVec->data(), ctx);
}

void DbTable::selectColgroups(llong recId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectColgroupsNoLock(recId, cgIdvec, cgIdvecSize, cgDataVec, ctx);
}
void DbTable::selectColgroupsNoLock(llong recId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
//	DebugCheckRowNumVecNoLock(this);
	llong rows = m_rowNum;
	if (terark_unlikely(recId < 0 || recId >= rows)) {
		THROW_STD(out_of_range, "recId = %lld, rows=%lld", recId, rows);
	}
	size_t upp = upper_bound_a(ctx->m_rowNumVec, recId);
	llong baseId = ctx->m_rowNumVec[upp-1];
	llong subId = recId - baseId;
	assert(recId >= baseId);
	auto seg = ctx->m_segCtx[upp-1]->seg;
	seg->selectColgroups(subId, cgIdvec, cgIdvecSize, cgDataVec, ctx);
}

void DbTable::selectOneColgroup(llong recId, size_t cgId,
						valvec<byte>* cgData, DbContext* ctx) const {
	ctx->trySyncSegCtxSpeculativeLock(this);
	selectColgroupsNoLock(recId, &cgId, 1, cgData, ctx);
}

void DbTable::selectOneColgroupNoLock(llong recId, size_t cgId,
						valvec<byte>* cgData, DbContext* ctx) const {
	selectColgroupsNoLock(recId, &cgId, 1, cgData, ctx);
}

#if 0
StoreIteratorPtr
DbTable::createProjectIterForward(const valvec<size_t>& cols, DbContext* ctx)
const {
	return createProjectIterForward(cols.data(), cols.size(), ctx);
}
StoreIteratorPtr
DbTable::createProjectIterBackward(const valvec<size_t>& cols, DbContext* ctx)
const {
	return createProjectIterBackward(cols.data(), cols.size(), ctx);
}

StoreIteratorPtr
DbTable::createProjectIterForward(const size_t* colsId, size_t colsNum, DbContext*)
const {
}

StoreIteratorPtr
DbTable::createProjectIterBackward(const size_t* colsId, size_t colsNum, DbContext*)
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
	ColgroupSegmentPtr seg;
	size_t idx;
    double purgePriority;
	SortableStrVec files;
	febitvec newIsPurged;
	size_t oldNumPurged;
	size_t newNumPurged;
	febitvec  updateBits;
	valvec<uint32_t> updateList;

	// constructor must be fast enough
	SegEntry(ColgroupSegment* s, size_t i) : seg(s), idx(i) {
        purgePriority = 0;
		oldNumPurged = 0;
		newNumPurged = 0;
	}
	bool needsRePurge() const { return newNumPurged != oldNumPurged; }
	void reuseOldStoreFiles(PathRef destSegDir, const std::string& prefix, size_t& newPartIdx);
};

void
SegEntry::reuseOldStoreFiles(PathRef destSegDir, const std::string& prefix, size_t& newPartIdx) {
	PathRef srcSegDir = seg->m_segDir;
	size_t lo = files.lower_bound(prefix);
	if (lo >= files.size() || !files[lo].startsWith(prefix)) {
		THROW_STD(invalid_argument, "missing: %s",
			(srcSegDir / prefix).string().c_str());
	}
	size_t prevOldpartIdx = 0;
	size_t j = lo;
	while (j < files.size() && files[j].startsWith(prefix)) {
		fstring fname = files[j];
		assert(!fname.endsWith(".empty"));
		std::string dotExt = getDotExtension(fname).str();
		if (prefix.size() + dotExt.size() < fname.size()) {
			// oldpartIdx is between prefix and dotExt
			// one part can have multiple different dotExt file
			size_t oldpartIdx = lcast(fname.substr(prefix.size()+1));
			assert(oldpartIdx - prevOldpartIdx <= 1);
			if (oldpartIdx - prevOldpartIdx > 1) {
				THROW_STD(invalid_argument, "missing part: %s.%zd%s"
					, (srcSegDir / prefix).string().c_str()
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
				, (srcSegDir / fname.str()).string().c_str()
				, destFpath.string().c_str());
			fs::create_hard_link(srcSegDir / fname.str(), destFpath);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: ex.what = %s\n", ex.what());
			FileStream dictFp(destFpath.string(), "wb");
		    dictFp.disbuf();
		    dictFp.cat((srcSegDir / fname.str()).string());
		}
		j++;
	}
    ++newPartIdx;
}

} // namespace

class DbTable::MergeParam {
public:
	valvec<SegEntry> m_segs;
    bool m_forcePurgeAndMerge = false;
	size_t m_newSegRows = 0;
	DbContextPtr   m_ctx;
	rank_select_se m_oldpurgeBits; // join from all input segs
	rank_select_se m_newpurgeBits;

	std::string joinPathList() const;

	void syncPurgeBits(double purgeThreshold);

	ReadableIndex*
	mergeIndex(ReadonlySegment* dseg, size_t indexId, DbContext* ctx);

	bool needsPurgeBits() const;

	void mergeFixedLenColgroup(ReadonlySegment* dseg, size_t colgroupId);
	void mergeGdictZipColgroup(ReadonlySegment* dseg, size_t colgroupId);
	void mergeAndPurgeColgroup(ReadonlySegment* dseg, size_t colgroupId);
};

std::string DbTable::MergeParam::joinPathList() const {
	std::string str;
	for (auto& x : m_segs) {
		str += "\t";
		str += x.seg->m_segDir.string();
		str += "\n";
	}
	return str;
}

void DbTable::MergeParam::syncPurgeBits(double purgeThresholdRatio) {
	size_t newSumDelcnt = 0;
	size_t oldSumPurged = 0;
	for (const auto& e : m_segs) {
		const ColgroupSegment* seg = e.seg.get();
		newSumDelcnt += seg->m_delcnt;
		oldSumPurged += seg->m_isPurged.max_rank1();
	}
	assert(newSumDelcnt >= oldSumPurged);
	size_t newIncDelcnt  = newSumDelcnt - oldSumPurged;
	size_t oldPhysicRows = m_newSegRows - oldSumPurged;
	double purgeThresholdRows = oldPhysicRows * purgeThresholdRatio;
	fprintf(stderr
		, "INFO: m_forcePurgeAndMerge = %d, "
		  "newSumDelcnt = %zd, newIncDelcnt = %zd, "
		  "oldPhysicRows = %zd, m_newSegRows = %zd, "
		  "purgeThresholdRatio = %f, purgeThresholdRows = %f\n"
		, m_forcePurgeAndMerge
		, newSumDelcnt, newIncDelcnt
		, oldPhysicRows, m_newSegRows
		, purgeThresholdRatio, purgeThresholdRows);
	if (m_forcePurgeAndMerge || newIncDelcnt >= purgeThresholdRows) {
		// all colgroups need purge
		assert(m_oldpurgeBits.empty());
		assert(m_newpurgeBits.empty());
		for (auto& e : m_segs) {
			ColgroupSegment* seg = e.seg.get();
			size_t segRows = seg->m_isDel.size();
			if (seg->m_isPurged.empty()) {
				m_oldpurgeBits.grow(segRows, false);
			}
			else {
				assert(seg->m_isPurged.size() == segRows);
				m_oldpurgeBits.append(seg->m_isPurged);
			}
			seg->m_bookUpdates = true;
			e.newIsPurged = seg->m_isDel;
			e.newNumPurged = e.newIsPurged.popcnt();
			e.oldNumPurged = seg->m_isPurged.max_rank1();
			m_newpurgeBits.append(e.newIsPurged);
		}
		m_oldpurgeBits.build_cache(true, false);
		m_newpurgeBits.build_cache(true, false);
	}
	else for (auto& e : m_segs) {
		ColgroupSegment* seg   = e.seg.get();
		size_t oldNumPurged    = seg->m_isPurged.max_rank1();
		size_t newMarkDelcnt   = seg->m_delcnt - oldNumPurged;
		size_t oldRealRecords  = seg->m_isDel.size() - oldNumPurged;
		double newMarkDelRatio = 1.0*newMarkDelcnt / (oldRealRecords + 0.1);
		// may cause book more records during 'e.newIsPurged = seg->m_isDel'
		// but this would not cause big problems
		seg->m_updateList.reserve(1024); // reduce enlarge times
		seg->m_bookUpdates = true;
		if (seg->getWritableSegment() || newMarkDelRatio > purgeThresholdRatio) {
			// do purge: physic delete
			e.newIsPurged = seg->m_isDel; // don't lock
			e.newNumPurged = e.newIsPurged.popcnt(); // recompute purge count
		} else {
			e.newIsPurged = seg->m_isPurged;
			e.newNumPurged = oldNumPurged;
		}
		e.oldNumPurged = oldNumPurged;
	}
}

ReadableIndex*
DbTable::MergeParam::
mergeIndex(ReadonlySegment* dseg, size_t indexId, DbContext* ctx) {
	valvec<byte> rec;
	SortableStrVec strVec;
	const Schema& schema = m_segs[0].seg->m_schema->getIndexSchema(indexId);
	const size_t fixedIndexRowLen = schema.getFixedRowLen();
	std::unique_ptr<SeqReadAppendonlyStore> seqStore;
	if (schema.m_enableLinearScan) {
		seqStore.reset(new SeqReadAppendonlyStore(dseg->m_segDir, schema));
	}
#if defined(SLOW_DEBUG_CHECK)
	hash_strmap<valvec<size_t> > key2id;
	size_t baseLogicId = 0;
#endif
	for (auto& e : m_segs) {
		auto seg = e.seg;
		auto indexStore = seg->m_indices[indexId]->getReadableStore();
		assert(nullptr != indexStore);
		size_t logicRows = seg->m_isDel.size();
		size_t physicId = 0;
		const bm_uint_t* oldpurgeBits = seg->m_isPurged.bldata();
		const bm_uint_t* newpurgeBits = e.newIsPurged.bldata();
		for (size_t logicId = 0; logicId < logicRows; ++logicId) {
			if (!oldpurgeBits || !terark_bit_test(oldpurgeBits, logicId)) {
				if (!newpurgeBits || !terark_bit_test(newpurgeBits, logicId)) {
					indexStore->getValue(physicId, &rec, ctx);
					if (fixedIndexRowLen) {
						assert(rec.size() == fixedIndexRowLen);
						strVec.m_strpool.append(rec);
					} else {
						strVec.push_back(rec);
					}
					if (seqStore)
						seqStore->append(rec, ctx);
#if defined(SLOW_DEBUG_CHECK)
					key2id[rec].push_back(baseLogicId + logicId);
#endif
				}
				physicId++;
			}
		}
#if defined(SLOW_DEBUG_CHECK)
		assert(!oldpurgeBits || seg->m_isPurged.max_rank0() == physicId);
		baseLogicId += logicRows;
#endif
	}
	if (strVec.str_size() == 0 && strVec.size() == 0) {
		return new EmptyIndexStore();
	}
	ReadableIndex* index = dseg->buildIndex(schema, strVec);
#if defined(SLOW_DEBUG_CHECK)
	valvec<byte> rec2;
	valvec<llong> recIdvec;
	valvec<size_t> baseIdvec;
	baseIdvec.push_back(0);
	for (auto& e : *this) {
		baseIdvec.push_back(baseIdvec.back() + e.seg->m_isDel.size());
	}
	size_t newBasePhysicId = 0;
	baseLogicId = 0;
	for (size_t segIdx = 0; segIdx < m_segs.size(); ++segIdx) {
		auto& e = (*this)[segIdx];
		auto seg = e.seg;
		auto subStore = seg->m_indices[indexId]->getReadableStore();
		assert(nullptr != subStore);
		size_t logicRows = seg->m_isDel.size();
		size_t oldPhysicId = 0;
		size_t newPhysicId = 0;
		const bm_uint_t* oldpurgeBits = seg->m_isPurged.bldata();
		const bm_uint_t* newpurgeBits = e.newIsPurged.bldata();
		for (size_t logicId = 0; logicId < logicRows; ++logicId) {
			if (!oldpurgeBits || !terark_bit_test(oldpurgeBits, logicId)) {
				if (!newpurgeBits || !terark_bit_test(newpurgeBits, logicId)) {
					subStore->getValue(oldPhysicId, &rec, ctx);
					index->getReadableStore()->getValue(newBasePhysicId + newPhysicId, &rec2, ctx);
					assert(rec.size() == rec2.size());
					if (memcmp(rec.data(), rec2.data(), rec.size()) != 0) {
						std::string js1 = schema.toJsonStr(rec);
						std::string js2 = schema.toJsonStr(rec2);
						fprintf(stderr, "%s  %s\n", js1.c_str(), js2.c_str());
					}
					assert(memcmp(rec.data(), rec2.data(), rec.size()) == 0);
					index->searchExact(rec, &recIdvec, ctx);
					assert(recIdvec.size() >= 1);
					if (schema.m_isUnique) {
						size_t realcnt = 0;
						auto&  idv2 = key2id[rec];
						assert(recIdvec.size() == idv2.size());
						size_t low = lower_bound_a(idv2, baseLogicId + logicId);
						assert(low < idv2.size()); // must found
						if (dseg->m_isPurged.size()) {
							for (size_t i = 0; i < idv2.size(); ++i) {
								size_t phyId1 = (size_t)recIdvec[i];
								size_t logId1 = dseg->m_isPurged.select0(phyId1);
								size_t logId2 = idv2[i];
								assert(logId1 == logId2);
								size_t upp = upper_bound_a(baseIdvec, logId1);
								size_t baseId = baseIdvec[upp-1];
								size_t subLogId = logId1 - baseId;
								auto yseg = (*this)[upp-1].seg;
								if (dseg->m_isDel[logId1]) {
									assert(yseg->m_isDel[subLogId]);
								} else {
									realcnt++;
								}
								if (!yseg->m_isDel[subLogId]) {
									assert(!dseg->m_isDel[logId1]);
								}
							}
							assert(realcnt <= 1);
						}
						else {
							for(size_t i = 0; i < idv2.size(); ++i) {
								size_t logId1 = (size_t)recIdvec[i];
								size_t logId2 = idv2[i];
								assert(logId1 == logId2);
								size_t upp = upper_bound_a(baseIdvec, logId1);
								size_t baseId = baseIdvec[upp-1];
								size_t subLogId = logId1 - baseId;
								auto yseg = (*this)[upp-1].seg;
								if (dseg->m_isDel[logId1]) {
									assert(yseg->m_isDel[subLogId]);
								} else {
									realcnt++;
								}
								if (!yseg->m_isDel[subLogId]) {
									assert(!dseg->m_isDel[logId1]);
								}
							}
							assert(realcnt <= 1);
						}
					}
					newPhysicId++;
				}
				oldPhysicId++;
			}
		}
		baseLogicId += logicRows;
		newBasePhysicId += newPhysicId;
		assert(!oldpurgeBits || seg->m_isPurged.max_rank0() == oldPhysicId);
	}
#endif
	return index;
}

bool DbTable::MergeParam::needsPurgeBits() const {
	for (auto& e : m_segs) {
		if (!e.newIsPurged.empty())
			return true;
	}
	return false;
}

void
DbTable::MergeParam::
mergeFixedLenColgroup(ReadonlySegment* dseg, size_t colgroupId) {
	auto& schema = dseg->m_schema->getColgroupSchema(colgroupId);
	FixedLenStorePtr dstStore = new FixedLenStore(dseg->m_segDir, schema);
	dstStore->unneedsLock();
	dstStore->reserveRows(m_newSegRows);
	byte_t* newBasePtr = dstStore->getRecordsBasePtr();
	size_t  newPhysicId = 0;
	size_t  const fixlen = schema.getFixedRowLen();
	for (auto& e : m_segs) {
		auto srcStore = e.seg->m_colgroups[colgroupId];
		assert(nullptr != srcStore);
		size_t physicSubRows = e.seg->getPhysicRows();
        if (physicSubRows == 0)
            continue;
		const byte_t* subBasePtr = srcStore->getRecordsBasePtr();
		assert(nullptr != subBasePtr);
		if (e.needsRePurge()) {
			const bm_uint_t* oldIsPurged = e.seg->m_isPurged.bldata();
			const bm_uint_t* newIsPurged = e.newIsPurged.bldata();
			size_t subRows = e.seg->m_isDel.size();
			size_t subPhysicId = 0;
			for (size_t subLogicId = 0; subLogicId < subRows; ++subLogicId) {
				if (!oldIsPurged || !terark_bit_test(oldIsPurged, subLogicId)) {
					if (!terark_bit_test(newIsPurged, subLogicId)) {
						memcpy(newBasePtr + fixlen*newPhysicId,
							   subBasePtr + fixlen*subPhysicId, fixlen);
						newPhysicId++;
					}
					subPhysicId++;
				}
			}
		}
		else {
			assert(physicSubRows <= (size_t)srcStore->numDataRows());
			memcpy(newBasePtr + fixlen * newPhysicId,
				   subBasePtr , fixlen * physicSubRows);
			newPhysicId += physicSubRows;
		}
	}
	dstStore->setNumRows(newPhysicId);
	dstStore->shrinkToFit();
	dseg->m_colgroups[colgroupId] = dstStore;
}

#if 0
class PurgeMappingStore : public ReadableStore {
	ReadableStorePtr m_realstore;
	const rank_select_se& m_isPurged;
public:
	PurgeMappingStore(ReadableStore* realstore, const rank_select_se& isPurged)
	 : m_realstore(realstore), m_isPurged(isPurged) {}
	llong dataInflateSize() const override { return m_realstore->dataInflateSize(); }
	llong dataStorageSize() const override { return m_realstore->dataStorageSize(); }
	llong numDataRows() const override { return m_realstore->numDataRows(); }
	void getValueAppend(llong logicId, valvec<byte>* val, DbContext* ctx) const override {
		llong physicId = m_isPurged.rank0(logicId);
		m_realstore->getValueAppend(physicId, val, ctx);
	}
	StoreIterator* createStoreIterForward(DbContext*) const override { return nullptr; }
	StoreIterator* createStoreIterBackward(DbContext*) const override { return nullptr; }

	void load(PathRef segDir) override {}
	void save(PathRef segDir) const override {}
};
#endif

void
DbTable::MergeParam::
mergeGdictZipColgroup(ReadonlySegment* dseg, size_t colgroupId) {
	auto& schema = dseg->m_schema->getColgroupSchema(colgroupId);
	MultiPartStorePtr parts = new MultiPartStore();
	for (const auto& e : m_segs) {
		auto sseg = e.seg;
		auto store = sseg->m_colgroups[colgroupId].get();
		if (auto mstore = dynamic_cast<MultiPartStore*>(store)) {
			for (size_t i = 0; i < mstore->numParts(); ++i) {
				parts->addpartIfNonEmpty(mstore->getPart(i));
			}
		}
		else {
			parts->addpartIfNonEmpty(store);
		}
	}
	TERARK_RT_assert(parts->numParts() > 0, std::logic_error);
	ReadableStorePtr mpstore = parts->finishParts();
	StoreIteratorPtr iter = mpstore->ensureStoreIterForward(m_ctx.get());
	dseg->m_colgroups[colgroupId] = dseg->buildDictZipStore(schema,
		dseg->m_segDir, *iter, m_newpurgeBits.bldata(), &m_oldpurgeBits);
}

void
DbTable::MergeParam::
mergeAndPurgeColgroup(ReadonlySegment* dseg, size_t colgroupId) {
	assert(dseg->m_isDel.size() == m_newSegRows);
	auto& schema = dseg->m_schema->getColgroupSchema(colgroupId);
	fs::path storeFilePath = dseg->m_segDir / ("colgroup-" + schema.m_name);
	if (m_newpurgeBits.size() &&
		m_newpurgeBits.size() == m_newpurgeBits.max_rank1()) {
		fprintf(stderr
			, "mergeAndPurgeColgroup(%zd): result is empty, newSegRows = %zd\n"
			, colgroupId, m_newSegRows);
		assert(m_newpurgeBits.size() == m_newSegRows);
		dseg->m_colgroups[colgroupId] = new EmptyIndexStore();
		dseg->m_colgroups[colgroupId]->save(storeFilePath);
		return;
	}
	if (schema.m_dictZipSampleRatio >= 0.0) {
		llong sumLen = 0;
		for (const auto& e : m_segs) {
			sumLen += e.seg->m_colgroups[colgroupId]->dataInflateSize();
		}
		size_t oldphysicRowNum = m_oldpurgeBits.size() ?
								 m_oldpurgeBits.max_rank0() : m_newSegRows;
		assert(oldphysicRowNum > 0);
		double sRatio = schema.m_dictZipSampleRatio;
		double avgLen = 1.0 * sumLen / oldphysicRowNum;
		if (sRatio > 0 || (sRatio < FLT_EPSILON && avgLen > 100)) {
			mergeGdictZipColgroup(dseg, colgroupId);
			return;
		}
	}
	valvec<byte> rec;
	SortableStrVec strVec;
	const size_t fixedIndexRowLen = schema.getFixedRowLen();
	for (auto& e : m_segs) {
		auto seg = e.seg;
		auto store = seg->m_colgroups[colgroupId].get();
		assert(nullptr != store);
		size_t logicRows = seg->m_isDel.size();
		size_t physicId = 0;
		const bm_uint_t* segOldpurgeBits = seg->m_isPurged.bldata();
		const bm_uint_t* segNewpurgeBits = e.newIsPurged.bldata();
		for (size_t logicId = 0; logicId < logicRows; ++logicId) {
			if (!segOldpurgeBits || !terark_bit_test(segOldpurgeBits, logicId)) {
				if (!segNewpurgeBits || !terark_bit_test(segNewpurgeBits, logicId)) {
					store->getValue(physicId, &rec, m_ctx.get());
					if (fixedIndexRowLen) {
						assert(rec.size() == fixedIndexRowLen);
						strVec.m_strpool.append(rec);
					} else {
						strVec.push_back(rec);
					}
				}
				physicId++;
			}
		}
	}
    size_t count = fixedIndexRowLen ?  strVec.size() / fixedIndexRowLen : strVec.size();
	ReadableStorePtr mergedstore = dseg->buildStore(schema, strVec);
    assert(mergedstore->numDataRows() == llong(count));
    (void)count;
	mergedstore->save(storeFilePath);
	dseg->m_colgroups[colgroupId] = mergedstore;
}

void DbTable::moveStoreFiles(PathRef srcDir, PathRef destDir,
			   const std::string& prefix, size_t& newPartIdx) {
	size_t prevOldpartIdx = 0;
	std::vector<std::string> fnames;
	// iterate on dir may not at lexical order
	for (auto& entry : fs::directory_iterator(srcDir)) {
		std::string fname = entry.path().filename().string();
		if ("." == fname || ".." == fname) {
			continue;
		}
		fnames.push_back(fname);
	}
	std::sort(fnames.begin(), fnames.end());
	for (const std::string& fname : fnames) {
		assert(!fstring(fname).endsWith(".empty"));
		assert(fstring(fname).startsWith(prefix));
		std::string dotExt = getDotExtension(fname).str();
		if (prefix.size() + dotExt.size() < fname.size()) {
			// oldpartIdx is between prefix and dotExt
			// one part can have multiple different dotExt file
			size_t oldpartIdx = lcast(fname.substr(prefix.size()+1));
			assert(oldpartIdx - prevOldpartIdx <= 1);
			if (oldpartIdx - prevOldpartIdx > 1) {
				THROW_STD(invalid_argument, "missing part: %s.%zd%s"
					, (srcDir / prefix).string().c_str()
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
		fs::path    destFpath = destDir / destFname;
		try {
			fprintf(stderr, "INFO: create_hard_link(%s, %s)\n"
				, (srcDir / fname).string().c_str()
				, destFpath.string().c_str());
			fs::rename(srcDir / fname, destFpath);
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
			throw;
		}
	}
    ++newPartIdx;
}

// If segments to be merged have purged records, these physical records id
// must be mapped to logical records id, thus purge bitmap is required for
// the merged result segment
void DbTable::merge(MergeParam& toMerge) {
	fs::path destMergeDir = getMergePath(m_dir, m_mergeSeqNum+1);
	if (fs::exists(destMergeDir)) {
		THROW_STD(logic_error, "dir: '%s' should not existed"
			, destMergeDir.string().c_str());
	}
	fs::path destSegDir = getSegPath2(m_dir, m_mergeSeqNum+1, "rd", toMerge.m_segs[0].idx);
	std::string segPathList = toMerge.joinPathList();
	fprintf(stderr, "INFO: merge segments:\n%sTo\t%s ...\n"
		, segPathList.c_str(), destSegDir.string().c_str());
#if defined(NDEBUG)
try{
#endif
	fs::create_directories(destSegDir);
	fs::path   mergingLockFile = destMergeDir / "merging.lock";
	FileStream mergingLockFp(mergingLockFile.string().c_str(), "wb");
	ReadonlySegmentPtr dseg = this->myCreateReadonlySegment(destSegDir);
	const size_t indexNum = m_schema->getIndexNum();
	const size_t colgroupNum = m_schema->getColgroupNum();
	dseg->m_indices.resize(indexNum);
	dseg->m_colgroups.resize(colgroupNum);
	toMerge.syncPurgeBits(m_schema->m_purgeDeleteThreshold);
	DbContextPtr ctx(this->createDbContext());
	toMerge.m_ctx = ctx;
	dseg->m_isDel.erase_all();
	dseg->m_isDel.reserve(toMerge.m_newSegRows);
	for (auto& e : toMerge.m_segs) {
		dseg->m_isDel.append(e.seg->m_isDel);
		assert(e.seg->m_bookUpdates);
#if 1 && !defined(NDEBUG)
        for(size_t cgId = 0; cgId < m_schema->getColgroupNum(); ++cgId) {
		    assert(llong(e.seg->getPhysicRows()) == e.seg->m_colgroups[cgId]->numDataRows());
        }
#endif
	}
	assert(dseg->m_isDel.size() == toMerge.m_newSegRows);
	dseg->m_delcnt = dseg->m_isDel.popcnt();
	if (toMerge.needsPurgeBits()) {
		assert(dseg->m_isPurged.size() == 0);
		dseg->m_isPurged.reserve(toMerge.m_newSegRows);
		for (auto& e : toMerge.m_segs) {
			if (e.newIsPurged.empty()) {
				dseg->m_isPurged.grow(e.seg->m_isDel.size(), false);
			} else {
				assert(e.seg->m_isDel.size() == e.newIsPurged.size());
				dseg->m_isPurged.append(e.newIsPurged);
			}
		}
		dseg->m_isPurged.build_cache(true, false);
		assert(dseg->m_isPurged.size() == toMerge.m_newSegRows);
	}
	for (size_t i = 0; i < indexNum; ++i) {
		ReadableIndex* index = toMerge.mergeIndex(dseg.get(), i, ctx.get());
		dseg->m_indices[i] = index;
		dseg->m_colgroups[i] = index->getReadableStore();
	}
	for (auto& e : toMerge.m_segs) {
		for(auto fpath : fs::directory_iterator(e.seg->m_segDir)) {
			e.files.push_back(fpath.path().filename().string());
		}
		e.files.sort();
		assert(e.seg->m_bookUpdates);
	}
	for (size_t cgId = indexNum; cgId < colgroupNum; ++cgId) {
		const Schema& schema = m_schema->getColgroupSchema(cgId);
		if (schema.should_use_FixedLenStore()) {
			toMerge.mergeFixedLenColgroup(dseg.get(), cgId);
			continue;
		}
		if (toMerge.m_newpurgeBits.size() > 0) {
			assert(toMerge.m_newpurgeBits.size() == toMerge.m_newSegRows);
			toMerge.mergeAndPurgeColgroup(dseg.get(), cgId);
			continue;
		}
		if (toMerge.m_forcePurgeAndMerge) {
			toMerge.mergeAndPurgeColgroup(dseg.get(), cgId);
			continue;
		}
		const std::string prefix = "colgroup-" + schema.m_name;
		size_t newPartIdx = 0;
		for (auto& e : toMerge.m_segs) {
			if (e.seg->getWritableSegment() || e.needsRePurge()) {
				assert(e.newIsPurged.size() >= 1);
				assert(e.newIsPurged.size() == e.seg->m_isDel.size());
				if (e.newIsPurged.size() == e.newNumPurged) {
					// new store is empty, all records are purged
					continue;
				}
				dseg->purgeColgroupMultiPart(cgId, e.newIsPurged, e.newNumPurged,
                                             e.seg.get(), ctx.get(), destSegDir, newPartIdx);
			} else {
				if (e.seg->m_isPurged.max_rank1() == e.seg->m_isDel.size()) {
					// old store is empty, all records are purged
					continue;
				}
				e.reuseOldStoreFiles(destSegDir, prefix, newPartIdx);
			}
		}
	}

	if (toMerge.needsPurgeBits() || dseg->m_isDel.empty()) {
		if (dseg->m_isPurged.max_rank1() == dseg->m_isPurged.size()) {
			ReadableStorePtr store = new EmptyIndexStore();
			dseg->m_colgroups.fill(indexNum, colgroupNum-indexNum, store);
			dseg->saveRecordStore(destSegDir);
		}
	}

	dseg->savePurgeBits(destSegDir);
	dseg->saveIndices(destSegDir);
	dseg->saveIsDel(destSegDir);

	// load as mmap
	dseg->m_withPurgeBits = true;
	dseg->m_isDel.clear();
	dseg->m_isPurged.clear();
	dseg->m_indices.erase_all();
	dseg->m_colgroups.erase_all();
	dseg->load(destSegDir);
//	assert(dseg->m_isDel.size() == dseg->m_isPurged.size());
	assert(dseg->m_isDel.size() == toMerge.m_newSegRows);

	// stop creating new segment by m_inprogressWritingCount
	// m_segments will never be changed
	assert(m_merging == MergeStatus::Merging);
    m_merging = MergeStatus::Waiting;
    while(m_bgTaskNum > 1)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
	IncrementGuard_size_t guard(m_inprogressWritingCount);
    while(m_inprogressWritingCount > 1)
        std::this_thread::yield();
    
	// newSegPathes don't include m_wrSeg
	valvec<ReadableSegmentPtr> newSegs(m_segments.capacity(), valvec_reserve());
	valvec<llong> newRowNumVec(m_rowNumVec.capacity(), valvec_reserve());
	newRowNumVec.push_back(0);
	size_t rows = 0;
	auto addseg = [&](const ReadableSegmentPtr& seg) {
		rows += seg->m_isDel.size();
		newSegs.push_back(seg);
		newRowNumVec.push_back(llong(rows));
	};
    auto shareSeg = [&](ReadableSegment *seg)
    {
        fs::path Old = seg->m_segDir;
        if(fs::is_symlink(Old))
        {
            fs::path RawOld = fs::read_symlink(Old);
            assert(RawOld.parent_path().filename() == Old.parent_path().filename());
            Old = RawOld;
        }
        fs::path New = getSegPath2(m_dir,
                                   m_mergeSeqNum + 1,
                                   seg->getWritableStore() ? "wr" : "rd",
                                   newSegs.size()
        );
        fs::path Rela = ".." / Old.parent_path().filename() / Old.filename();
        try
        {
            fs::create_directory_symlink(Rela, New);
        }
        catch(const std::exception& ex)
        {
            fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
            throw;
        }
        addseg(seg);
    };
	for (size_t i = 0; i < toMerge.m_segs[0].idx; ++i) {
        shareSeg(m_segments[i].get());
	}
	addseg(dseg);
	for (size_t i = toMerge.m_segs.back().idx + 1; i < m_segments.size(); ++i) {
        shareSeg(m_segments[i].get());
    };
	auto syncOneRecord = [](ReadonlySegment* dseg, ReadableSegment* sseg,
							size_t baseLogicId, size_t subId) {
		if (sseg->m_isDel[subId]) {
			dseg->m_isDel.set1(baseLogicId + subId);
		}
		else {
			assert(!dseg->m_isDel[baseLogicId + subId]);
			dseg->syncUpdateRecordNoLock(baseLogicId, subId, sseg);
		}
	};
	auto syncUpdates = [&](ReadonlySegment* dseg) {
		DebugCheckRowNumVecNoLock(this);
		for (auto& e : toMerge.m_segs) {
			ReadableSegment* sseg = e.seg.get();
			assert(sseg->m_bookUpdates);
			assert(e.updateBits.empty());
			assert(e.updateList.empty());
			SpinRwLock segLock(sseg->m_segMutex, true);
			e.updateBits.swap(sseg->m_updateBits);
			e.updateList.swap(sseg->m_updateList);
		}
		size_t baseLogicId = 0;
		for (auto& e : toMerge.m_segs) {
			ReadableSegment* sseg = e.seg.get();
			if (e.updateBits.empty()) {
				for (size_t subId : e.updateList) {
					syncOneRecord(dseg, sseg, baseLogicId, subId);
				}
			}
			else {
				assert(e.updateBits.size() == sseg->m_isDel.size() + 1);
				assert(e.updateList.empty());
				size_t subId = e.updateBits.zero_seq_len(0);
				size_t subRows = sseg->m_isDel.size();
				while (subId < subRows) {
					syncOneRecord(dseg, sseg, baseLogicId, subId);
					size_t zeroSeqLen = e.updateBits.zero_seq_len(subId + 1);
					subId += 1 + zeroSeqLen;
				}
				assert(subId == subRows);
			}
			baseLogicId += sseg->m_isDel.size();
			e.updateList.erase_all();
			e.updateBits.erase_all();
		}
		assert(baseLogicId == toMerge.m_newSegRows);
		dseg->m_delcnt = dseg->m_isDel.popcnt();
	};
	{
		syncUpdates(dseg.get()); // no lock
		MyRwLock lock(m_rwMutex, true);
		syncUpdates(dseg.get()); // write locked
		for (auto& e : toMerge.m_segs) {
			e.seg->m_bookUpdates = false;
		}
		m_segments.swap(newSegs);
		m_rowNumVec.swap(newRowNumVec);
		m_rowNumVec.back() = newRowNumVec.back();
		m_mergeSeqNum++;
		m_segArrayUpdateSeq++;
#if defined(SLOW_DEBUG_CHECK)
		valvec<byte> r1, r2;
		size_t baseLogicId = 0;
		for(size_t i = 0; i < toMerge.size(); ++i) {
			auto sseg = toMerge[i].seg;
			for(size_t subLogicId = 0; subLogicId < sseg->m_isDel.size(); ++subLogicId) {
				size_t logicId = baseLogicId + subLogicId;
				if (!sseg->m_isDel[subLogicId]) {
					assert(!dseg->m_isDel[logicId]);
					dseg->getValue(logicId, &r1, ctx.get());
					sseg->getValue(subLogicId, &r2, ctx.get());
					assert(r1.size() == r2.size());
					if (memcmp(r1.data(), r2.data(), r1.size()) != 0) {
						std::string js1 = this->toJsonStr(r1);
						std::string js2 = this->toJsonStr(r2);
					}
					assert(memcmp(r1.data(), r2.data(), r1.size()) == 0);
					assert(dseg->m_isPurged.empty() || !dseg->m_isPurged[logicId]);
				} else {
					assert(dseg->m_isDel[logicId]);
				}
			}
			baseLogicId += sseg->m_isDel.size();
		}
#endif
#if 0 // don't do this
		// m_wrSeg == NULL indicate writing is stopped
		if (m_wrSeg && m_wrSeg->dataStorageSize() >= m_schema->m_maxWritingSegmentSize) {
			doCreateNewSegmentInLock();
		}
		if (PurgeStatus::pending == m_purgeStatus) {
			inLockPutPurgeDeleteTaskToQueue();
		}
#endif
	}
	mergingLockFp.close();
	fs::remove(mergingLockFile);
	for (auto& tobeDel : toMerge.m_segs) {
		tobeDel.seg->deleteSegment();
	}
	fprintf(stderr, "INFO: merge segments:\n%sTo\t%s done!\n"
		, segPathList.c_str(), destSegDir.string().c_str());
#if defined(NDEBUG)
}
catch (const std::exception& ex) {
	fprintf(stderr
		, "ERROR: merge segments: ex.what = %s\n%sTo\t%s failed, rollback!\n"
		, ex.what(), segPathList.c_str(), destSegDir.string().c_str());
	TERARK_IF_DEBUG(throw,;);
	fs::remove_all(destMergeDir);
}
#endif
}

void DbTable::checkRowNumVecNoLock() const {
#if !defined(NDEBUG)
	assert(m_segments.size() >= 1);
	for(size_t i = 0; i < m_segments.size()-1; ++i) {
		size_t r1 = m_segments[i]->m_isDel.size();
		size_t r2 = m_rowNumVec[i+1] - m_rowNumVec[i];
		assert(r1 == r2);
	}
	if (m_wrSeg) {
		assert(!m_wrSeg->m_isFreezed);
		SpinRwLock seglock(m_wrSeg->m_segMutex, false);
		size_t r1 = m_wrSeg->m_isDel.size();
		size_t r2 = m_rowNumVec.ende(1) - m_rowNumVec.ende(2);
		assert(r1 == r2);
	}
	else { // does not need lock
		size_t r1 = m_segments.back()->m_isDel.size();
		size_t r2 = m_rowNumVec.ende(1) - m_rowNumVec.ende(2);
		assert(r1 == r2);
	}
#endif
}

void DbTable::clear() {
	MyRwLock lock(m_rwMutex, true);
	for (size_t i = 0; i < m_segments.size(); ++i) {
		m_segments[i]->deleteSegment();
		m_segments[i] = nullptr;
	}
	m_segments.clear();
	m_rowNumVec.clear();
	m_wrSeg = nullptr;
	m_mergeSeqNum++;
	m_segArrayUpdateSeq++;

	const size_t segIdx = 0;
	m_wrSeg = myCreateWritableSegment(getSegPath("wr", segIdx));
	m_segments.push_back(m_wrSeg);
	m_rowNumVec.push_back(0);
	m_rowNumVec.push_back(0);
	m_rowNum = 0;
}

void DbTable::flush() {
	if (this->m_tobeDrop) {
		return;
	}
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
		tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(0.1));
	//	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

void DbTable::compact() {
	profiling pf;
	llong t0 = pf.now();
	llong t1 = t0;
	for (;;) {
		MyRwLock lock(m_rwMutex, true);
		DebugCheckRowNumVecNoLock(this);
		if (m_merging != MergeStatus::None) {
			lock.release();
			tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(1.0));
		//	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			llong t2 = pf.now();
			if (pf.ms(t1, t2) > 10000) { // 10 seconds
				fprintf(stderr, "INFO: wait for merging: %s, %f seconds\n"
					, m_dir.string().c_str(), pf.sf(t0, t2));
				t1 = t2;
			}
			continue;
		}
		if (m_inprogressWritingCount > 0) {
			lock.release();
			tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(0.05));
		//	std::this_thread::sleep_for(std::chrono::milliseconds(50));
			llong t2 = pf.now();
			if (pf.ms(t1, t2) > 1000) { // 1 seconds
				fprintf(stderr, "INFO: wait for inprogress writing: %s, %f seconds\n"
					, m_dir.string().c_str(), pf.sf(t0, t2));
				t1 = t2;
			}
			continue;
		}
		if (m_wrSeg->m_isDel.size() > 0 && 0 == m_inprogressWritingCount) {
			doCreateNewSegmentInLock();
		}
		break;
	}
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
	{
		MyRwLock lock(m_rwMutex, true);
		this->m_bgTaskNum++;
	}
	BOOST_SCOPE_EXIT(&m_rwMutex, &m_bgTaskNum){
		MyRwLock lock(m_rwMutex, true);
		m_bgTaskNum--;
	}BOOST_SCOPE_EXIT_END;
    while(!autoConvMergePurge(true))
        ;
}

void DbTable::syncFinishWriting() {
    m_autoTask = false;
	m_wrSeg = nullptr; // can't write anymore
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
	{
		MyRwLock lock(m_rwMutex, true);
		auto wrseg = m_segments.back().get();
		if (wrseg->m_isDel.empty()) {
			wrseg->deleteSegment();
			m_segments.pop_back();
		}
		else if (wrseg->getWritableSegment() != nullptr) {
			wrseg->getWritableSegment()->markFrozen();
			putToFlushQueue(m_segments.size()-1);
		}
	}
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
}

void DbTable::asyncPurgeDelete() {
	MyRwLock lock(m_rwMutex, true);
	inLockPutPurgeDeleteTaskToQueue();
}

void DbTable::dropTable() {
	assert(!m_dir.empty());
	for (auto& seg : m_segments) {
		seg->deleteSegment();
	}
	m_segments.erase_all();
	m_wrSeg = nullptr;
	m_tobeDrop = true;
}

std::string DbTable::toJsonStr(fstring row) const {
	return m_schema->m_rowSchema->toJsonStr(row);
}

boost::filesystem::path
DbTable::getMergePath(PathRef dir, size_t mergeSeq)
const {
	auto res = dir;
	char szBuf[32];
	snprintf(szBuf, sizeof(szBuf), "g-%04ld", long(mergeSeq));
	res /= szBuf;
	return res;
}

boost::filesystem::path
DbTable::getSegPath(const char* type, size_t segIdx)
const {
	return getSegPath2(m_dir, m_mergeSeqNum, type, segIdx);
}

// static
boost::filesystem::path
DbTable::getSegPath2(PathRef dir, size_t mergeSeq, const char* type, size_t segIdx)
{
	auto res = dir;
	char szBuf[32];
	int len = snprintf(szBuf, sizeof(szBuf), "g-%04ld", long(mergeSeq));
	res /= szBuf;
	len = snprintf(szBuf, sizeof(szBuf), "-%04ld", long(segIdx));
	res /= type;
	res.concat(szBuf, szBuf + len);
	return res;
}

void DbTable::save(PathRef dir) const {
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

bool DbTable::autoConvMergePurge(bool forcePurgeAndMerge) {
    BOOST_SCOPE_EXIT(&m_rwMutex, &m_bgTaskNum){
		MyRwLock lock(m_rwMutex, true);
		--m_bgTaskNum;
	}BOOST_SCOPE_EXIT_END;
	if (m_bgTaskNum > 2)    //TODO use config
		return false;

    MergeParam param;

    bool convPlainWritableSegment = false;      //  1
    bool mergeColgroupSegment = false;          //  2
    bool convWritableSegment = false;           //  3
    bool mergeReadonlySegment = false;          //  4
    bool purgeReadonlySegment = false;          //  5
    
	double threshold = std::max(m_schema->m_purgeDeleteThreshold, 0.001);
    auto &m_segs = param.m_segs;
    param.m_forcePurgeAndMerge = forcePurgeAndMerge;
	m_segs.reserve(m_segments.size() + 1);

    auto getRows = [&](size_t i) {
        ColgroupSegment* seg = m_segs[i].seg.get();
        if (seg->getReadonlySegment()) {
            return seg->getReadonlySegment()->getPhysicRows()
                - (seg->m_delcnt - seg->m_isPurged.max_rank1());
        }
        else {
            return seg->m_isDel.size() - seg->m_delcnt;
        }
    };
    auto getPurge = [&](size_t i)->double {
        ReadonlySegment* seg = m_segs[i].seg->getReadonlySegment();
        if (seg) {
			size_t newDelcnt = seg->m_delcnt - seg->m_isPurged.max_rank1();
			size_t physicNum = seg->getPhysicRows();
            if (newDelcnt > physicNum * threshold)
                return newDelcnt * newDelcnt / physicNum;
        }
        return 0;
    };
    
	do {
		MyRwLock lock(m_rwMutex, false);
        assert(!m_segments.empty());
        if (m_segments.ende(1)->getPlainWritableSegment()) {
            convPlainWritableSegment = true;
        }
		for (size_t i = 0; i < m_segments.size(); ++i) {
			ColgroupSegment* seg = m_segments[i]->getMergableSegment();
			if (seg)
				m_segs.emplace_back(seg, i);
			else
                break;
        }
		//lock.upgrade_to_writer();
		//DebugCheckRowNumVecNoLock(tab);
    } while(false);

    size_t findSegmentId = size_t(-1);
	size_t rngBeg = 0, rngLen = 0;
    if (convPlainWritableSegment) {
		MyRwLock lock(m_rwMutex, false);
		for (size_t i = 0; i < m_segments.size(); ++i) {
            if (m_segments[i]->getPlainWritableSegment()
                && m_segments[i]->m_isFreezed
                && !m_segments[i]->m_onProcess
                ) {
                findSegmentId = i;
                break;
            }
        }
    }
    if (findSegmentId == size_t(-1)) {
	    size_t sumSegRows = 0;
	    for (size_t i = 0; i < m_segs.size(); ++i) {
            if ((m_segs[i].purgePriority = getPurge(i)) > 0)
                purgeReadonlySegment = true;
            if (m_segs[i].seg->getWritableSegment())
                convWritableSegment = true;
            sumSegRows += getRows(i);
        }
	    size_t largeSegRows = 2 * sumSegRows / m_segs.size();

	    // eleminate large segments and compute average of others
	    size_t smallsegNum = 0;
	    size_t smallsegRows = 0;
	
	    for (size_t i = 0; i < m_segs.size(); ++i) {
		    size_t rows = getRows(i);
		    if (rows <= largeSegRows)
			    // use '<=' for very rare case: largeSegRows==0
			    smallsegNum++, smallsegRows += rows;
	    }
	    size_t avgSegRows = smallsegRows / smallsegNum;
	    size_t maxSegRows = avgSegRows * 7 / 4;
	    size_t minMergeSegNum = m_schema->m_minMergeSegNum;
	    size_t suggestWritableSegNum = m_schema->m_suggestWritableSegNum;
	    if (forcePurgeAndMerge) {
		    //maxSegRows = avgSegRows * 3;
		    maxSegRows = size_t(-1);
		    minMergeSegNum = 2;
            suggestWritableSegNum = 0;
	    }
	    else {
		    if (minMergeSegNum < 2)	minMergeSegNum = 2;
		    if (minMergeSegNum > 9)	minMergeSegNum = 9;
		    if (suggestWritableSegNum < 3)
                minMergeSegNum = 2;
            else
                --suggestWritableSegNum;
	    }

	    // find max range in which every seg rows < maxSegRows
	    for (size_t j = 0; j < m_segs.size(); ) {
		    size_t k = j;
		    for (; k < m_segs.size(); ++k) {
			    if (getRows(k) > maxSegRows) {
                    if (m_segs[k].seg->getReadonlySegment()) {
				        break;
                    }
                    else {
                        assert(m_segs[k].seg->getColgroupSegment());
                    }
                }
		    }
		    if (k - j > rngLen) {
			    rngBeg = j;
			    rngLen = k - j;
		    }
		    j = k + 1;
	    }
        size_t rdLen = 0, wrLen = 0;
	    for (size_t j = 0; j < rngLen; ++j) {
            if (m_segs[rngBeg + j].seg->getWritableSegment())
                ++wrLen;
            else
                ++rdLen;
	    }
        mergeColgroupSegment = rngLen > 1;
	    if (rngLen < minMergeSegNum) {
            mergeColgroupSegment = wrLen >= suggestWritableSegNum;
            if (wrLen == 0) {
                size_t maxSegIndex = 0;
                if (rdLen > suggestWritableSegNum) {
                    for (size_t j = 1; j < m_segs.size(); ++j) {
                        if (getRows(j) > getRows(maxSegIndex))
                            maxSegIndex = j;
	                }
                }
                if (maxSegIndex >= 2) {
                    rngBeg = 0;
                    rngLen = maxSegIndex;
                    mergeReadonlySegment = true;
                }
            }
	    }
        // all are colgroup segments
        // needn't reuse old stores
        if (mergeColgroupSegment && rdLen == 0)
            param.m_forcePurgeAndMerge = true;
    }

    auto processSegment = [&](ReadableSegment *seg, size_t i, bool force) {
        m_segs.clear();
        {
		    MyRwLock lock(m_rwMutex, true);
            if (!force && m_merging == MergeStatus::Waiting)
                return false;
            if (seg != m_segments[i].get())
                return true;
            if (seg->m_onProcess)
                return false;
            seg->m_onProcess = true;
        }
        auto segDir = getSegPath("rd", i);
		ReadonlySegmentPtr newSeg = myCreateReadonlySegment(segDir);
        char const *processName =
            seg->getReadonlySegment()
                ? "purgeReadonlySegment"
                : seg->getColgroupSegment()
                    ? "convColgroupSegment"
                    : "convPlainWritableSegment"
            ;
        std::string strDir = seg->m_segDir.string();
        size_t rows = seg->m_isDel.size();
        size_t delcnt = seg->m_delcnt;
        size_t purged = seg->m_isPurged.max_rank1();
	    fprintf(stderr
		        , "INFO: %s: %s, rows = %zd, delcnt = %zd, purged = %zd\n"
		        , processName
                , strDir.c_str()
		        , rows
                , delcnt
		        , purged
		);
        if (seg->getColgroupSegment())
		    newSeg->purgeDeletedRecords(this, i);
        else
            newSeg->convFrom(this, i);
        fprintf(stderr
		        , "INFO: %s: %s, rows = %zd, delcnt = %zd, purged = %zd done!\n"
		        , processName
                , strDir.c_str()
		        , rows
                , delcnt
		        , purged
		);
        return true;
    };
    auto mergeSegments = [&] {
        {
		    MyRwLock lock(m_rwMutex, true);
            if (m_merging != MergeStatus::None)
                return false;
            m_merging = MergeStatus::Merging;
	        for (size_t j = 0; j < rngLen; ++j) {
                size_t i = rngBeg + j;
                if (m_segs[i].seg != m_segments[i] || m_segs[i].seg->m_onProcess) {
                    m_merging = MergeStatus::None;
                    return true;
                }
		        m_segs[j] = m_segs[i];
            }
	        for (size_t j = 0; j < rngLen; ++j) {
                m_segs[j].seg->m_onProcess = true;
            }
        }
	    m_segs.trim(rngLen);
	    param.m_newSegRows = 0;
        for (size_t j = 0; j < rngLen; ++j)
		    param.m_newSegRows += m_segs[j].seg->m_isDel.size();
        merge(param);
        assert(m_merging == MergeStatus::Waiting);
		m_merging = MergeStatus::None;
        return true;
    };

    if (convPlainWritableSegment) {
        ReadableSegmentPtr seg;
        {
		    MyRwLock lock(m_rwMutex, false);
            assert(findSegmentId < m_segments.size());
            seg = m_segments[findSegmentId];
        }
        return processSegment(seg.get(), findSegmentId, true);
    }
    if (mergeColgroupSegment && m_merging == MergeStatus::None) {
        if (mergeSegments())
            return true;
    }
    if (convWritableSegment) {
        ReadableSegmentPtr seg;
        {
		    MyRwLock lock(m_rwMutex, false);
		    for (size_t i = 0; i < m_segs.size(); ++i) {
                if (m_segs[i].seg->getWritableSegment() && !m_segs[i].seg->m_onProcess) {
                    findSegmentId = i;
                    break;
                }
            }
        }
        if (findSegmentId == size_t(-1)) {
            return false;
        }
        seg = m_segs[findSegmentId].seg;
        return processSegment(seg.get(), findSegmentId, mergeColgroupSegment);
    }
    if (mergeReadonlySegment && m_merging == MergeStatus::None) {
        return mergeSegments();
    }
    if (mergeColgroupSegment || mergeReadonlySegment)
        return false;
    if (purgeReadonlySegment) {
	    ReadableSegmentPtr seg;
        {
		    MyRwLock lock(m_rwMutex, false);
		    for (size_t i = 0; i < m_segs.size(); ++i) {
                if (m_segs[i].purgePriority > 0 && !m_segs[i].seg->m_onProcess) {
                    if (findSegmentId == size_t(-1) || m_segs[i].purgePriority > m_segs[findSegmentId].purgePriority) {
                        findSegmentId = i;
                        break;
                    }
                }
            }
            if (findSegmentId == size_t(-1)) {
                return false;
            }
        }
        seg = m_segs[findSegmentId].seg;
        return processSegment(seg.get(), findSegmentId, false);
    }
    return false;
}

void DbTable::freezeFlushWritableSegment(size_t segIdx) {
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
	fprintf(stderr, "freezeFlushWritableSegment: %s done!\n", seg->m_segDir.string().c_str());
}

/////////////////////////////////////////////////////////////////////

namespace anonymousForDebugMSVC {

class MyTask : public RefCounter {
public:
	virtual void execute() = 0;
};
typedef boost::intrusive_ptr<MyTask> MyTaskPtr;
std::mutex g_mutexForStop;
terark::util::concurrent_queue<std::deque<MyTaskPtr> > g_flushQueue;
terark::util::concurrent_queue<std::deque<MyTaskPtr> > g_compressQueue;

volatile bool g_stopPutToFlushQueue = false;
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
	fprintf(stderr, "INFO: flushing thread completed!\n");
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
}

class CompressionThreadsList : private std::vector<tbb::tbb_thread*> {
public:
	CompressionThreadsList() {
		size_t cpu = tbb::tbb_thread::hardware_concurrency();
		size_t cfg = getEnvLong("TerarkDB_CompressionThreadsNum", 0);
		size_t num = cfg ? min(cpu, cfg) : min<size_t>(cpu, 4);
		this->resize(num);
		for (size_t i = 0; i < num; ++i) {
			(*this)[i] = new tbb::tbb_thread(&CompressThreadFunc);
		}
	}
	~CompressionThreadsList() {
		if (!this->empty())
			DbTable::safeStopAndWaitForFlush();
	}
	void join() {
		for (auto& th : *this) {
			th->join();
			delete th;
			th = NULL;
		}
		fprintf(stderr, "INFO: compression threads(%zd) completed!\n", this->size());
		this->clear();
		g_compressQueue.clearQueue();
	}
};
tbb::tbb_thread g_flushThread(&FlushThreadFunc);
CompressionThreadsList g_compressThreads;

class AutoTask : public MyTask {
	DbTablePtr m_tab;
public:
	void execute() override {
		if (m_tab->autoConvMergePurge(false) && m_tab->isAutoTask()) {
            m_tab->putAutoTask();
        }
	}
	AutoTask(DbTablePtr tab) : m_tab(tab) {}
};

class WrSegFreezeFlushTask : public MyTask {
	DbTablePtr m_tab;
	size_t m_segIdx;
public:
	WrSegFreezeFlushTask(DbTablePtr tab, size_t segIdx)
		: m_tab(tab), m_segIdx(segIdx) {}

	void execute() override {
		m_tab->freezeFlushWritableSegment(m_segIdx);
		g_compressQueue.push_back(new AutoTask(m_tab));
	}
};

} // namespace
using namespace anonymousForDebugMSVC;

void DbTable::putToFlushQueue(size_t segIdx) {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return;
	}
	assert(segIdx < m_segments.size());
	assert(m_segments[segIdx]->m_isDel.size() > 0);
	assert(m_segments[segIdx]->getWritableStore() != nullptr);
	g_flushQueue.push_back(new WrSegFreezeFlushTask(this, segIdx));
	m_bgTaskNum++;
}

void DbTable::putToCompressionQueue(size_t segIdx) {
	assert(segIdx < m_segments.size());
	assert(m_segments[segIdx]->m_isDel.size() > 0);
	assert(m_segments[segIdx]->getWritableStore() != nullptr);
	if (g_stopCompress) {
		return;
	}
	m_bgTaskNum++;
	g_compressQueue.push_back(new AutoTask(this));
}

void DbTable::putAutoTask() {
	if (g_stopCompress || !m_autoTask) {
		return;
	}
    {
	    MyRwLock lock(m_rwMutex, true);
	    m_bgTaskNum++;
    }
	g_compressQueue.push_back(new AutoTask(this));
}

bool DbTable::isAutoTask() const {
    return m_autoTask;
}

inline
bool DbTable::checkPurgeDeleteNoLock(const ReadableSegment* seg) {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return false;
	}
	if (m_bgTaskNum > 0) {
		return false;
	}
	auto maxDelcnt = seg->m_isDel.size() * m_schema->m_purgeDeleteThreshold;
	if (seg->m_delcnt >= maxDelcnt) {
		return true;
	}
	return false;
}

inline
bool DbTable::tryAsyncPurgeDeleteInLock(const ReadableSegment* seg) {
	if (checkPurgeDeleteNoLock(seg)) {
		inLockPutPurgeDeleteTaskToQueue();
		return true;
	}
	return false;
}

void DbTable::inLockPutPurgeDeleteTaskToQueue() {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return;
	}
	m_bgTaskNum++;
	g_compressQueue.push_back(new AutoTask(this));
}

// flush is the most urgent
void DbTable::safeStopAndWaitForFlush() {
	std::unique_lock<std::mutex> lock(g_mutexForStop);
	if (g_stopPutToFlushQueue) {
		return;
	}
	g_stopPutToFlushQueue = true;
	g_stopCompress = true;
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	assert(g_flushStopped);
	g_compressThreads.join();
	assert(g_flushQueue.empty());
	assert(g_compressQueue.empty());
}

void DbTable::safeStopAndWaitForCompress() {
	std::unique_lock<std::mutex> lock(g_mutexForStop);
	if (g_stopPutToFlushQueue) {
		return;
	}
	g_stopPutToFlushQueue = true;
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	g_compressThreads.join();
	assert(g_flushQueue.empty());
	assert(g_compressQueue.empty());
}

/*
void DbTable::registerDbContext(DbContext* ctx) const {
	assert(m_ctxListHead != ctx);
	MyRwLock lock(m_rwMutex);
	ctx->m_prev = m_ctxListHead;
	ctx->m_next = m_ctxListHead->m_next;
	m_ctxListHead->m_next->m_prev = ctx;
	m_ctxListHead->m_next = ctx;
}

void DbTable::unregisterDbContext(DbContext* ctx) const {
	assert(m_ctxListHead != ctx);
	MyRwLock lock(m_rwMutex);
	ctx->m_prev->m_next = ctx->m_next;
	ctx->m_next->m_prev = ctx->m_prev;
}
*/

} } // namespace terark::db
