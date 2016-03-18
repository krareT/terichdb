#include "db_table.hpp"
#include "db_segment.hpp"
#include <terark/db/fixed_len_store.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <boost/scope_exit.hpp>
#include <thread> // for std::this_thread::sleep_for
#include <tbb/tbb_thread.h>
#include <terark/util/concurrent_queue.hpp>

#undef min
#undef max

namespace terark { namespace db {

namespace fs = boost::filesystem;

const size_t DEFAULT_maxSegNum = 4095;

///////////////////////////////////////////////////////////////////////////////

#if defined(NDEBUG)
	#define DebugCheckRowNumVecNoLock(This)
#else
	#define DebugCheckRowNumVecNoLock(This) \
	This->checkRowNumVecNoLock(); \
	auto BOOST_PP_CAT(Self, __LINE__) = This; \
	BOOST_SCOPE_EXIT(BOOST_PP_CAT(Self, __LINE__)) { \
		BOOST_PP_CAT(Self, __LINE__)->checkRowNumVecNoLock(); \
	} BOOST_SCOPE_EXIT_END
#endif

CompositeTable::CompositeTable() {
	m_tableScanningRefCount = 0;
	m_tobeDrop = false;
	m_isMerging = false;
	m_purgeStatus = PurgeStatus::none;
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
	TERARK_RT_assert(m_ctxListHead->m_next == m_ctxListHead, std::logic_error);
	TERARK_RT_assert(m_ctxListHead->m_prev == m_ctxListHead, std::logic_error);
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

static void tryReduceSymlink(PathRef segDir, PathRef mergeDir) {
	if (fs::is_symlink(segDir)) {
		std::string strDir = segDir.string();
		fs::path target = fs::canonical(fs::read_symlink(segDir), mergeDir);
		fprintf(stderr
			, "WARN: writable segment: %s is symbol link to: %s, reduce it\n"
			, strDir.c_str(), target.string().c_str());
		fs::remove(segDir);
		if (fs::exists(target))
			fs::rename(target, segDir);
	}
}

void CompositeTable::removeStaleDir(PathRef root, size_t inUseMergeSeq) const {
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
				if (mergeSeq < mergeSeq2)
					mergeSeq = mergeSeq2;
			}
		}
	}
	if (mergeSeq < 0) {
		m_mergeSeqNum = 0;
		fs::create_directories(getMergePath(m_dir, 0));
	}
	else {
		removeStaleDir(m_dir, mergeSeq);
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
			tryReduceSymlink(segDir, mergeDir);
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
	size_t m_mergeSeqNum;
	size_t m_newWrSegNum;
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
		m_mergeSeqNum = tab->m_mergeSeqNum;
		m_newWrSegNum = tab->m_newWrSegNum;
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
	//	MyRwLock lock(tab->m_rwMutex, false);
		if (m_mergeSeqNum == tab->m_mergeSeqNum &&
			m_newWrSegNum == tab->m_newWrSegNum) {
			// there is no new segments
			llong oldmaxId = m_segs.back().baseId;
			if (tab->m_rowNumVec.back() == oldmaxId)
				return false; // no new records
			// records may be 'pop_back'
			m_segs.back().baseId = tab->m_rowNumVec.back();
			return tab->m_rowNumVec.back() > oldmaxId;
		}
		m_segs.resize(tab->m_segments.size() + 1);
		for (size_t i = 0; i < m_segs.size() - 1; ++i) {
			m_segs[i].seg = tab->m_segments[i];
			m_segs[i].baseId = tab->m_rowNumVec[i];
			m_segs[i].iter = nullptr;
		}
		m_segs.back().baseId = tab->m_rowNumVec.back();
		m_mergeSeqNum = tab->m_mergeSeqNum;
		m_newWrSegNum = tab->m_newWrSegNum;
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
		MyRwLock lock(tab->m_rwMutex, false);
		while (incrementNoCheckDel(&subId, val)) {
			assert(subId >= 0);
			assert(subId < m_segs[m_segIdx].seg->numDataRows());
			llong baseId = m_segs[m_segIdx].baseId;
			if (!tab->m_segments[m_segIdx]->m_isDel[subId]) {
				*id = baseId + subId;
				assert(*id < tab->numDataRows());
				return true;
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
		auto cur = &m_segs[m_segIdx];
		if (terark_unlikely(!cur->iter))
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
		MyRwLock lock(tab->m_rwMutex, false);
		while (incrementNoCheckDel(&subId, val)) {
			assert(subId >= 0);
			assert(subId < m_segs[m_segIdx-1].seg->numDataRows());
			llong baseId = m_segs[m_segIdx-1].baseId;
			if (!tab->m_segments[m_segIdx-1]->m_isDel[subId]) {
				*id = baseId + subId;
				assert(*id < tab->numDataRows());
				return true;
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
	//	auto tab = static_cast<const CompositeTable*>(m_store.get());
		auto cur = &m_segs[m_segIdx-1];
		if (terark_unlikely(!cur->iter))
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

llong CompositeTable::dataInflateSize() const {
	MyRwLock lock(m_rwMutex, false);
	return m_wrSeg->dataInflateSize();
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
	DebugCheckRowNumVecNoLock(this);
	if (m_isMerging) {
		return false;
	}
	if (m_wrSeg->dataStorageSize() >= m_schema->m_maxWritingSegmentSize) {
		if (lock.upgrade_to_writer() ||
			// if upgrade_to_writer fails, it means the lock has been
			// temporary released and re-acquired, so we need check
			// the condition again
			m_wrSeg->dataStorageSize() >= m_schema->m_maxWritingSegmentSize)
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
	auto oldwrseg = m_wrSeg.get();
	while (oldwrseg->m_isDel.size() && oldwrseg->m_isDel.back()) {
		assert(oldwrseg->m_delcnt > 0);
		oldwrseg->popIsDel();
		oldwrseg->m_delcnt--;
	}
	if (oldwrseg->m_isDelMmap) {
		((uint64_t*)oldwrseg->m_isDelMmap)[0] = oldwrseg->m_isDel.size();
	}
	m_rowNumVec.back() = m_rowNumVec.ende(2) + oldwrseg->m_isDel.size();
	// createWritableSegment should be fast, other wise the lock time
	// may be too long
	putToFlushQueue(m_segments.size() - 1);
	size_t newSegIdx = m_segments.size();
	m_wrSeg = myCreateWritableSegment(getSegPath("wr", newSegIdx));
	m_segments.push_back(m_wrSeg);
	llong newMaxRowNum = m_rowNumVec.back();
	m_rowNumVec.push_back(newMaxRowNum);
	m_newWrSegNum++;
	oldwrseg->m_deletedWrIdSet.clear(); // free memory
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
	if (!m_schema->m_updatableColgroups.empty()) {
		seg->m_colgroups.resize(m_schema->getColgroupNum());
		for (size_t colgroupId : m_schema->m_updatableColgroups) {
			const Schema& schema = m_schema->getColgroupSchema(colgroupId);
			seg->m_colgroups[colgroupId] = new FixedLenStore(segDir, schema);
		}
	}
	return seg.release();
}

bool CompositeTable::exists(llong id) const {
	assert(id >= 0);
	MyRwLock lock(m_rwMutex, false);
	if (terark_unlikely(id >= llong(m_rowNumVec.back()))) {
		return false;
	}
	size_t upp = upper_bound_a(m_rowNumVec, id);
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	size_t subId = size_t(id - baseId);
	auto seg = m_segments[upp-1].get();
#if !defined(NDEBUG)
	size_t upperId = m_rowNumVec[upp];
	assert(subId < seg->m_isDel.size());
	assert(seg->m_isDel.size() == upperId - baseId);
#endif
	return seg->m_isDel.is0(subId);
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
	DebugCheckRowNumVecNoLock(this);
	bool isWriteLocked = maybeCreateNewSegment(lock);
	if (txn->syncIndex) {
		if (!insertCheckSegDup(0, m_segments.size()-1, txn))
			return -1;
		if (!isWriteLocked && !lock.upgrade_to_writer()) {
			// check again(should be very rare)
			if (!insertCheckSegDup(0, m_segments.size()-1, txn))
				return -1;
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
		ws.pushIsDel(false);
		m_rowNumVec.back() = wrBaseId + subId + 1;
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				m_rowNumVec.back()--;
				ws.popIsDel();
				return -1; // fail
			}
		}
		ws.update(subId, row, txn);
	}
	else {
		subId = ws.m_deletedWrIdSet.back();
		assert(ws.m_isDel[subId]);
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				return -1; // fail
			}
		}
		ws.m_deletedWrIdSet.pop_back();
		ws.update(subId, row, txn);
		ws.m_isDel.set0(subId);
		ws.m_delcnt--;
		ws.m_isDirty = true;
		assert(ws.m_isDel.popcnt() == ws.m_delcnt);
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
			rIndex->searchExact(txn->key1, &txn->exactMatchRecIdvec, txn);
			for(llong physicId : txn->exactMatchRecIdvec) {
				llong logicId = seg->getLogicId(physicId);
				if (!seg->m_isDel[logicId]) {
					// std::move makes it no temps
					char szIdstr[96];
					snprintf(szIdstr, sizeof(szIdstr)
						, "logicId = %lld , physicId = %lld"
						, logicId, physicId);
					txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
								+ ", " + szIdstr
								+ ", in freezen seg: " + seg->m_segDir.string();
				//	txn->errMsg += ", rowData=";
				//	txn->errMsg += sconf.m_rowSchema->toJsonStr(row);
					return false;
				}
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
CompositeTable::updateRow(llong id, fstring row, DbContext* txn) {
	m_schema->m_rowSchema->parseRow(row, &txn->cols1); // new row
	MyRwLock lock(m_rwMutex, false);
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNumVec.back());
	if (id >= m_rowNumVec.back()) {
		THROW_STD(invalid_argument
			, "id=%lld is large/equal than rows=%lld"
			, id, m_rowNumVec.back());
	}
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j > 0);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = &*m_segments[j-1];
	bool directUpgrade = true;
	if (txn->syncIndex) {
		const size_t old_newWrSegNum = m_newWrSegNum;
		if (seg->m_isDel[subId]) {
			THROW_STD(invalid_argument
				, "id=%lld has been deleted, segIdx=%zd, baseId=%lld, subId=%lld"
				, id, j, baseId, subId);
			/*
			// behave as insert
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
			*/
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
		m_wrSeg->update(subId, row, txn);
		return id; // id is not changed
	}
	else {
		// mark old subId as deleted
		seg->addtoUpdateList(size_t(subId));
		seg->m_isDel.set1(subId);
		seg->m_delcnt++;
		assert(seg->m_isDel.popcnt() == seg->m_delcnt);
		tryAsyncPurgeDeleteInLock(seg);
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
			rIndex->searchExact(txn->key1, &txn->exactMatchRecIdvec, txn);
			for(llong physicId : txn->exactMatchRecIdvec) {
				llong logicId = seg->getLogicId(physicId);
				if (!seg->m_isDel[logicId]) {
					// std::move makes it no temps
					char szIdstr[96];
					snprintf(szIdstr, sizeof(szIdstr)
						, "logicId = %lld , physicId = %lld"
						, logicId, physicId);
					txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
								+ ", " + szIdstr
								+ ", in freezen seg: " + seg->m_segDir.string();
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
	DebugCheckRowNumVecNoLock(this);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNumVec.back());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
//	assert(seg->m_isDel.is0(subId));
	if (seg->m_isDel.is1(size_t(subId))) {
	//	THROW_STD(invalid_argument
	//		, "Row has been deleted: id=%lld seg=%zd baseId=%lld subId=%lld"
	//		, id, j, baseId, subId);
		return false;
	}
	if (j == m_rowNumVec.size()-1) {
		assert(!m_wrSeg->m_bookUpdates);
		if (txn->syncIndex) {
			valvec<byte> &row = txn->row1, &key = txn->key1;
			ColumnVec& columns = txn->cols1;
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
		m_wrSeg->m_delcnt++;
		m_wrSeg->m_isDel.set1(subId); // always set delmark
		m_wrSeg->m_isDirty = true;
		assert(m_wrSeg->m_isDel.popcnt() == m_wrSeg->m_delcnt);
	}
	else { // freezed segment, just set del mark
		seg->addtoUpdateList(size_t(subId));
		seg->m_isDel.set1(subId);
		seg->m_delcnt++;
		seg->m_isDirty = true;
#if !defined(NDEBUG)
		size_t delcnt = seg->m_isDel.popcnt();
		assert(delcnt == seg->m_delcnt);
#endif
		tryAsyncPurgeDeleteInLock(seg);
	}
	return true;
}

///! Can inplace update column in ReadonlySegment
void
CompositeTable::updateColumn(llong recordId, size_t columnId,
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
	memcpy(coldata, newColumnData.data(), newColumnData.size());
	seg->addtoUpdateList(subId);
}

void
CompositeTable::updateColumn(llong recordId, fstring colname,
							 fstring newColumnData, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumn(recordId, columnId, newColumnData);
}

namespace {
	template<class WireType, class LlongOrFloat, class OP>
	bool updateValueByOp(byte& byteRef, const OP& op) {
		LlongOrFloat val = reinterpret_cast<WireType&>(byteRef);
		if (op(val)) {
			reinterpret_cast<WireType&>(byteRef) = val;
			return true;
		}
		return false;
	}
}

void
CompositeTable::updateColumnInteger(llong recordId, size_t columnId,
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
	case ColumnType::Uint08:  updateValueByOp<uint8_t , llong>(*coldata, op); break;
	case ColumnType::Sint08:  updateValueByOp< int8_t , llong>(*coldata, op); break;
	case ColumnType::Uint16:  updateValueByOp<uint16_t, llong>(*coldata, op); break;
	case ColumnType::Sint16:  updateValueByOp< int16_t, llong>(*coldata, op); break;
	case ColumnType::Uint32:  updateValueByOp<uint32_t, llong>(*coldata, op); break;
	case ColumnType::Sint32:  updateValueByOp< int32_t, llong>(*coldata, op); break;
	case ColumnType::Uint64:  updateValueByOp<uint64_t, llong>(*coldata, op); break;
	case ColumnType::Sint64:  updateValueByOp< int64_t, llong>(*coldata, op); break;
	case ColumnType::Float32: updateValueByOp<   float, llong>(*coldata, op); break;
	case ColumnType::Float64: updateValueByOp<  double, llong>(*coldata, op); break;
	}
	seg->addtoUpdateList(subId);
}

void
CompositeTable::updateColumnInteger(llong recordId, fstring colname,
									const std::function<bool(llong&val)>& op,
									DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumnInteger(recordId, colname, op);
}

void
CompositeTable::updateColumnDouble(llong recordId, size_t columnId,
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
	case ColumnType::Uint08:  updateValueByOp<uint08_t, double>(*coldata, op); break;
	case ColumnType::Sint08:  updateValueByOp< int08_t, double>(*coldata, op); break;
	case ColumnType::Uint16:  updateValueByOp<uint16_t, double>(*coldata, op); break;
	case ColumnType::Sint16:  updateValueByOp< int16_t, double>(*coldata, op); break;
	case ColumnType::Uint32:  updateValueByOp<uint32_t, double>(*coldata, op); break;
	case ColumnType::Sint32:  updateValueByOp< int32_t, double>(*coldata, op); break;
	case ColumnType::Uint64:  updateValueByOp<uint64_t, double>(*coldata, op); break;
	case ColumnType::Sint64:  updateValueByOp< int64_t, double>(*coldata, op); break;
	case ColumnType::Float32: updateValueByOp<   float, double>(*coldata, op); break;
	case ColumnType::Float64: updateValueByOp<  double, double>(*coldata, op); break;
	}
	seg->addtoUpdateList(subId);
}

void
CompositeTable::updateColumnDouble(llong recordId, fstring colname,
								   const std::function<bool(double&val)>& op,
								   DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	updateColumnDouble(recordId, colname, op);
}

void
CompositeTable::incrementColumnValue(llong recordId, size_t columnId,
									 llong incVal, DbContext* ctx) {
#include "update_column_impl.hpp"
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
	seg->addtoUpdateList(subId);
}

void
CompositeTable::incrementColumnValue(llong recordId, fstring colname,
									 llong incVal, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	incrementColumnValue(recordId, colname, incVal);
}

void
CompositeTable::incrementColumnValue(llong recordId, size_t columnId,
									 double incVal, DbContext* ctx) {
#include "update_column_impl.hpp"
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
	seg->addtoUpdateList(subId);
}

void
CompositeTable::incrementColumnValue(llong recordId, fstring colname,
									 double incVal, DbContext* ctx) {
	size_t columnId = m_schema->m_rowSchema->getColumnId(colname);
	if (columnId >= m_schema->columnNum()) {
		THROW_STD(invalid_argument, "colname = %.*s is not existed"
			, colname.ilen(), colname.data());
	}
	incrementColumnValue(recordId, colname, incVal);
}

bool
CompositeTable::indexKeyExists(size_t indexId, fstring key, DbContext* ctx)
const {
	MyRwLock lock(m_rwMutex, false);
	for (size_t i = m_segments.size(); i > 0; ) {
		auto seg = m_segments[--i].get();
		auto index = seg->m_indices[indexId];
		index->searchExact(key, &ctx->exactMatchRecIdvec, ctx);
		for(llong physicId : ctx->exactMatchRecIdvec) {
			llong logicId = seg->getLogicId(physicId);
			if (!seg->m_isDel[logicId])
				return true;
		}
	}
	return false;
}

void
CompositeTable::indexSearchExact(size_t indexId, fstring key, valvec<llong>* recIdvec, DbContext* ctx)
const {
	recIdvec->erase_all();
	MyRwLock lock(m_rwMutex, false);
	for (size_t i = m_segments.size(); i > 0; ) {
		auto seg = m_segments[--i].get();
		auto index = seg->m_indices[indexId];
		index->searchExact(key, &ctx->exactMatchRecIdvec, ctx);
		for(llong physicId : ctx->exactMatchRecIdvec) {
			llong logicId = seg->getLogicId(physicId);
			if (!seg->m_isDel[logicId])
				recIdvec->push_back(m_rowNumVec[i] + logicId);
		}
	}
}

// implemented in DfaDbTable
///@params recIdvec result of matched record id list
bool
CompositeTable::indexMatchRegex(size_t indexId, BaseDFA* regexDFA,
								valvec<llong>* recIdvec, DbContext*)
const {
	THROW_STD(invalid_argument, "Methed is not implemented");
}

bool
CompositeTable::indexMatchRegex(size_t indexId,
								fstring regexStr, fstring regexOpt,
								valvec<llong>* recIdvec, DbContext*)
const {
	THROW_STD(invalid_argument, "Methed is not implemented");
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
	terark::valvec<size_t> m_heap;
	size_t m_oldmergeSeqNum;
	size_t m_oldnewWrSegNum;
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
		if (m_oldmergeSeqNum == m_tab->m_mergeSeqNum &&
			m_oldnewWrSegNum == m_tab->m_newWrSegNum)
		{
			return 0;
		}
		m_oldmergeSeqNum = m_tab->m_mergeSeqNum;
		m_oldnewWrSegNum = m_tab->m_newWrSegNum;
		m_segs.resize(m_tab->m_segments.size());
		for (size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			assert(m_tab->m_segments[i]);
			if (cur.seg != m_tab->m_segments[i]) {
				if (cur.seg) { // segment converted
					cur.subId = -2; // need re-seek position??
				}
				cur.iter = nullptr;
				cur.seg  = m_tab->m_segments[i];
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
		m_oldmergeSeqNum = size_t(-1);
		m_oldnewWrSegNum = size_t(-1);
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
			if (ret >= 0) {
				m_heap.push_back(i);
				cur.subId = cur.seg->getLogicId(cur.subId);
			}
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
	DebugCheckRowNumVecNoLock(this);
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
	DebugCheckRowNumVecNoLock(this);
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
	DebugCheckRowNumVecNoLock(this);
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
	ReadonlySegment* seg;
	size_t idx;
	SortableStrVec files;
	febitvec newIsPurged;
	size_t oldNumPurged;
	size_t newNumPurged;

	// constructor must be fast enough
	SegEntry(ReadonlySegment* s, size_t i) : seg(s), idx(i) {
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
			fprintf(stderr, "FATAL: ex.what = %s\n", ex.what());
			throw;
		}
		j++;
	}
}

} // namespace

class CompositeTable::MergeParam : public valvec<SegEntry> {
public:
	size_t m_tabSegNum = 0;
	size_t m_newSegRows = 0;

	bool canMerge(CompositeTable* tab) {
		if (tab->m_isMerging)
			return false;
		if (PurgeStatus::none != tab->m_purgeStatus)
			return false;

		// memory alloc should be out of lock scope
		this->reserve(tab->m_segments.size() + 1);
		{
			MyRwLock lock(tab->m_rwMutex, false);
			for (size_t i = 0; i < tab->m_segments.size(); ++i) {
				auto seg = tab->m_segments[i].get();
				if (seg->getWritableStore())
					break; // writable seg must be at top side
				else
					this->push_back({seg->getReadonlySegment(), i});
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
			DebugCheckRowNumVecNoLock(tab);
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
		if (rngLen < tab->m_schema->m_minMergeSegNum) {
			tab->m_isMerging = false;
			return false;
		}
		m_newSegRows = 0;
		for (size_t j = 0; j < rngLen; ++j) {
			m_newSegRows += this->p[j].seg->m_isDel.size();
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

	void syncPurgeBits(double purgeThreshold);

	ReadableIndex*
	mergeIndex(ReadonlySegment* dseg, size_t indexId, DbContext* ctx);

	bool needsPurgeBits() const;

	void mergeInplaceUpdatable(ReadonlySegment* dseg, size_t colgroupId);
};


void CompositeTable::MergeParam::syncPurgeBits(double purgeThreshold) {
	for (auto& e : *this) {
		ReadonlySegment* seg   = e.seg;
		size_t oldNumPurged    = seg->m_isPurged.max_rank1();
		size_t newMarkDelcnt   = seg->m_delcnt - oldNumPurged;
		size_t oldRealRecords  = seg->m_isDel.size() - oldNumPurged;
		double newMarkDelRatio = 1.0*newMarkDelcnt / (oldRealRecords + 0.1);
		// may cause book more records during 'e.newIsPurged = seg->m_isDel'
		// but this would not cause big problems
		seg->m_bookUpdates = true;
		if (newMarkDelRatio > purgeThreshold) {
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
CompositeTable::MergeParam::
mergeIndex(ReadonlySegment* dseg, size_t indexId, DbContext* ctx) {
	valvec<byte> rec;
	SortableStrVec strVec;
	const Schema& schema = this->p[0].seg->m_schema->getIndexSchema(indexId);
	const size_t fixedIndexRowLen = schema.getFixedRowLen();
#if !defined(NDEBUG)
	hash_strmap<size_t> key2id;
#endif
	for (auto& e : *this) {
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
#if !defined(NDEBUG)
					auto& cnt = key2id[rec];
					if (++cnt > 1) {
						physicId = physicId;
					}
#endif
				}
				physicId++;
			}
		}
		assert(!oldpurgeBits || seg->m_isPurged.max_rank0() == physicId);
	}
	ReadableIndex* index = dseg->buildIndex(schema, strVec);
#if !defined(NDEBUG)
	valvec<byte> rec2;
	size_t newBasePhysicId = 0;
	for (auto& e : *this) {
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
				//	assert(memcmp(rec.data(), rec2.data(), rec.size()) == 0);
					newPhysicId++;
				}
				oldPhysicId++;
			}
		}
		newBasePhysicId += newPhysicId;
		assert(!oldpurgeBits || seg->m_isPurged.max_rank0() == oldPhysicId);
	}
#endif
	return index;
}

bool CompositeTable::MergeParam::needsPurgeBits() const {
	for (auto& e : *this) {
		if (!e.newIsPurged.empty())
			return true;
	}
	return false;
}

void
CompositeTable::MergeParam::
mergeInplaceUpdatable(ReadonlySegment* dseg, size_t colgroupId) {
	auto& schema = dseg->m_schema->getColgroupSchema(colgroupId);
	FixedLenStorePtr dstStore = new FixedLenStore(dseg->m_segDir, schema);
	dstStore->reserveRows(m_newSegRows);
	byte_t* newBasePtr = dstStore->getRecordsBasePtr();
	size_t  newPhysicId = 0;
	size_t  const fixlen = schema.getFixedRowLen();
	for (auto& e : *this) {
		auto srcStore = e.seg->m_colgroups[colgroupId];
		assert(nullptr != srcStore);
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
			size_t physicSubRows = e.seg->getPhysicRows();
			assert(physicSubRows == (size_t)srcStore->numDataRows());
			memcpy(newBasePtr + fixlen * newPhysicId,
				   subBasePtr , fixlen * physicSubRows);
			newPhysicId += physicSubRows;
		}
	}
	dstStore->setNumRows(newPhysicId);
	dstStore->shrinkToFit();
	dseg->m_colgroups[colgroupId] = dstStore;
}

static void
moveStoreFiles(PathRef srcDir, PathRef destDir,
			   const std::string& prefix, size_t& newPartIdx) {
	size_t prevOldpartIdx = 0;
	for (auto& entry : fs::directory_iterator(srcDir)) {
		std::string fname = entry.path().filename().string();
		if ("." == fname || ".." == fname) {
			continue;
		}
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
}

// If segments to be merged have purged records, these physical records id
// must be mapped to logical records id, thus purge bitmap is required for
// the merged result segment
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
	fs::path   mergingLockFile = destMergeDir / "merging.lock";
	FileStream mergingLockFp(mergingLockFile.string().c_str(), "wb");
	ReadonlySegmentPtr dseg = this->myCreateReadonlySegment(destSegDir);
	const size_t indexNum = m_schema->getIndexNum();
	const size_t colgroupNum = m_schema->getColgroupNum();
	dseg->m_indices.resize(indexNum);
	dseg->m_colgroups.resize(colgroupNum);
	toMerge.syncPurgeBits(m_schema->m_purgeDeleteThreshold);
	DbContextPtr ctx(this->createDbContext());
	for (size_t i = 0; i < indexNum; ++i) {
		ReadableIndex* index = toMerge.mergeIndex(dseg.get(), i, ctx.get());
		dseg->m_indices[i] = index;
		dseg->m_colgroups[i] = index->getReadableStore();
	}
	for (auto& e : toMerge) {
		for(auto fpath : fs::directory_iterator(e.seg->m_segDir)) {
			e.files.push_back(fpath.path().filename().string());
		}
		e.files.sort();
	}
	for (size_t i = indexNum; i < colgroupNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		if (schema.m_isInplaceUpdatable) {
			toMerge.mergeInplaceUpdatable(dseg.get(), i);
			continue;
		}
		const std::string prefix = "colgroup-" + schema.m_name;
		size_t newPartIdx = 0;
		for (auto& e : toMerge) {
			if (e.needsRePurge()) {
				auto tmpDir1 = destSegDir / "temp-store";
				fs::create_directory(tmpDir1);
				dseg->m_isDel.swap(e.newIsPurged);
				auto store = dseg->purgeColgroup(i, e.seg, ctx.get(), tmpDir1);
				dseg->m_isDel.swap(e.newIsPurged);
				store->save(tmpDir1 / prefix);
				moveStoreFiles(tmpDir1, destSegDir, prefix, newPartIdx);
				fs::remove_all(tmpDir1);
			} else {
				e.reuseOldStoreFiles(destSegDir, prefix, newPartIdx);
			}
			newPartIdx++;
		}
	}

	size_t rows = 0;
	for (auto& e : toMerge) {
		rows += e.seg->m_isDel.size();
	}
	if (toMerge.needsPurgeBits()) {
		assert(dseg->m_isPurged.size() == 0);
		dseg->m_isPurged.reserve(rows);
		for (auto& e : toMerge) {
			if (e.newIsPurged.empty()) {
				dseg->m_isPurged.grow(e.seg->m_isDel.size(), false);
			} else {
				assert(e.seg->m_isDel.size() == e.seg->m_isPurged.size());
				dseg->m_isPurged.append(e.newIsPurged);
			}
		}
		dseg->m_isPurged.build_cache(true, false);
		assert(dseg->m_isPurged.size() == rows);
		assert(dseg->m_isPurged.size() == toMerge.m_newSegRows);
	}
	assert(rows == toMerge.m_newSegRows);
	dseg->m_isDel.erase_all();
	dseg->m_isDel.reserve(rows);
	for (auto& e : toMerge) {
		dseg->m_isDel.append(e.seg->m_isDel);
	}
//	assert(dseg->m_isDel.size() == dseg->m_isPurged.size());
	assert(dseg->m_isDel.size() == toMerge.m_newSegRows);
	dseg->m_delcnt = dseg->m_isDel.popcnt();

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
		size_t baseLogicId = 0;
		for (auto& e : toMerge) {
			ReadableSegment* sseg = e.seg;
			if (sseg->m_updateBits.empty()) {
				for (size_t subId : sseg->m_updateList) {
					syncOneRecord(dseg, sseg, baseLogicId, subId);
				}
			}
			else {
				assert(sseg->m_updateBits.size() == sseg->m_isDel.size() + 1);
				assert(sseg->m_updateList.empty());
				size_t subId = sseg->m_updateBits.zero_seq_len(0);
				size_t subRows = sseg->m_isDel.size();
				while (subId < subRows) {
					syncOneRecord(dseg, sseg, baseLogicId, subId);
					size_t zeroSeqLen = sseg->m_updateBits.zero_seq_len(subId + 1);
					subId += 1 + zeroSeqLen;
				}
				assert(subId == subRows);
			}
			baseLogicId += sseg->m_isDel.size();
			// it is safe to change these 2 member in reader lock
			sseg->m_updateList.erase_all();
			sseg->m_updateBits.erase_all();
		}
		assert(baseLogicId == toMerge.m_newSegRows);
		dseg->m_delcnt = dseg->m_isDel.popcnt();
	};
	{
		MyRwLock lock(m_rwMutex, false);
		syncUpdates(dseg.get());
		if (!lock.upgrade_to_writer()) {
			syncUpdates(dseg.get());
		}
		for (auto& e : toMerge) {
			e.seg->m_bookUpdates = false;
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
		m_rowNumVec.back() = newRowNumVec.back();
		m_mergeSeqNum++;
		m_isMerging = false;
#if !defined(NDEBUG)
		valvec<byte> r1, r2;
		rows = dseg->m_isDel.size();
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
			m_purgeStatus = PurgeStatus::inqueue;
		}
#endif
	}
	mergingLockFp.close();
	fs::remove(mergingLockFile);
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
	TERARK_IF_DEBUG(throw,;);
	fs::remove_all(destMergeDir);
}
}

void CompositeTable::checkRowNumVecNoLock() const {
#if !defined(NDEBUG)
	for(size_t i = 0; i < m_segments.size(); ++i) {
		size_t r1 = m_segments[i]->m_isDel.size();
		size_t r2 = m_rowNumVec[i+1] - m_rowNumVec[i];
		assert(r1 == r2);
	}
#endif
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
		auto wrseg = m_segments.back().get();
		if (wrseg->m_isDel.empty()) {
			wrseg->deleteSegment();
			m_segments.pop_back();
		}
		else {
			putToFlushQueue(m_segments.size()-1);
		}
	}
	waitForBackgroundTasks(m_rwMutex, m_bgTaskNum);
}

void CompositeTable::asyncPurgeDelete() {
	MyRwLock lock(m_rwMutex, true);
	asyncPurgeDeleteInLock();
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
	fprintf(stderr, "freezeFlushWritableSegment: %s done!\n", seg->m_segDir.string().c_str());
}

void CompositeTable::runPurgeDelete() {
	BOOST_SCOPE_EXIT(&m_rwMutex, &m_purgeStatus, &m_bgTaskNum) {
		MyRwLock lock(m_rwMutex, true);
		m_purgeStatus = PurgeStatus::none;
		m_bgTaskNum--;
	} BOOST_SCOPE_EXIT_END;
	for (;;) {
		double threshold = std::max(m_schema->m_purgeDeleteThreshold, 0.001);
		size_t segIdx = size_t(-1);
		ReadonlySegmentPtr srcSeg;
		{
			MyRwLock lock(m_rwMutex, false);
			auto segs = m_segments.data();
			for (size_t i = 0, n = m_segments.size(); i < n; ++i) {
				auto r = segs[i]->getReadonlySegment();
				if (r) {
					size_t newDelcnt = r->m_delcnt - r->m_isPurged.max_rank1();
					size_t physicNum = r->m_isPurged.max_rank0();
					if (newDelcnt > physicNum * threshold) {
						assert(newDelcnt > 0);
						segIdx = i;
						srcSeg = r;
						break;
					}
				}
			}
		}
		if (size_t(-1) == segIdx) {
			break;
		}
		ReadonlySegmentPtr dest = myCreateReadonlySegment(srcSeg->m_segDir);
		dest->purgeDeletedRecords(this, segIdx);
	}
}

/////////////////////////////////////////////////////////////////////

namespace anonymousForDebugMSVC {

class MyTask : public RefCounter {
public:
	virtual void execute() = 0;
};
typedef boost::intrusive_ptr<MyTask> MyTaskPtr;
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
		size_t n = tbb::tbb_thread::hardware_concurrency();
		if (const char* env = getenv("TerarkDbCompressionThreadsNum")) {
			size_t n2 = atoi(env);
			n = std::min(n, n2);
		}
		this->resize(n);
		for (size_t i = 0; i < n; ++i) {
			(*this)[i] = new tbb::tbb_thread(&CompressThreadFunc);
		}
	}
	~CompressionThreadsList() {
		if (!this->empty())
			CompositeTable::safeStopAndWaitForFlush();
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

class PurgeDeleteTask : public MyTask {
	CompositeTablePtr m_tab;
public:
	void execute() override {
		m_tab->runPurgeDelete();
	}
	PurgeDeleteTask(CompositeTablePtr tab) : m_tab(tab) {}
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
using namespace anonymousForDebugMSVC;

void CompositeTable::putToFlushQueue(size_t segIdx) {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return;
	}
	assert(segIdx < m_segments.size());
	assert(m_segments[segIdx]->m_isDel.size() > 0);
	g_flushQueue.push_back(new WrSegFreezeFlushTask(this, segIdx));
	m_bgTaskNum++;
}

void CompositeTable::putToCompressionQueue(size_t segIdx) {
	assert(segIdx < m_segments.size());
	assert(m_segments[segIdx]->m_isDel.size() > 0);
	if (g_stopCompress) {
		return;
	}
	g_compressQueue.push_back(new SegWrToRdConvTask(this, segIdx));
	m_bgTaskNum++;
}

inline
bool CompositeTable::tryAsyncPurgeDeleteInLock(const ReadableSegment* seg) {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return false;
	}
	auto maxDelcnt = seg->m_isDel.size() * m_schema->m_purgeDeleteThreshold;
	if (seg->m_delcnt >= maxDelcnt) {
		asyncPurgeDeleteInLock();
		return true;
	}
	return false;
}

void CompositeTable::asyncPurgeDeleteInLock() {
	if (PurgeStatus::purging == m_purgeStatus) {
		// do nothing
		assert(!m_isMerging);
	}
	else if (m_isMerging) {
		m_purgeStatus = PurgeStatus::pending;
	}
	else if (PurgeStatus::pending == m_purgeStatus ||
			 PurgeStatus::none    == m_purgeStatus) {
		inLockPutPurgeDeleteTaskToQueue();
		m_purgeStatus = PurgeStatus::inqueue;
	}
	else {
		// do nothing
	}
}

void CompositeTable::inLockPutPurgeDeleteTaskToQueue() {
	assert(!g_stopPutToFlushQueue);
	if (g_stopPutToFlushQueue) {
		return;
	}
	g_compressQueue.push_back(new PurgeDeleteTask(this));
	m_purgeStatus = PurgeStatus::purging;
	m_bgTaskNum++;
}

// flush is the most urgent
void CompositeTable::safeStopAndWaitForFlush() {
	g_stopPutToFlushQueue = true;
	g_stopCompress = true;
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	g_compressThreads.join();
	assert(g_flushQueue.empty());
	assert(g_compressQueue.empty());
}

void CompositeTable::safeStopAndWaitForCompress() {
	g_stopPutToFlushQueue = true;
	g_flushQueue.push_back(nullptr); // notify and stop flag
	g_flushThread.join();
	g_compressThreads.join();
	assert(g_flushQueue.empty());
	assert(g_compressQueue.empty());
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

} } // namespace terark::db
