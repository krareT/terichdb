/*
 * db_context.cpp
 *
 *  Created on: 2015Äê11ÔÂ26ÈÕ
 *      Author: leipeng
 */
#include "db_context.hpp"
#include "db_segment.hpp"
#include "db_table.hpp"
#include <terark/num_to_str.hpp>

namespace terark { namespace db {

template<class T>
static inline void RefcntPtr_release(T*& p) {
	if (p) {
		p->release();
		p = NULL;
	}
}

DbContext::SegCtx*
DbContext::SegCtx::create(ReadableSegment* seg, size_t indexNum) {
	size_t memsize = sizeof(SegCtx) + sizeof(IndexIteratorPtr) * (indexNum-1);
	SegCtx* p = (SegCtx*)malloc(memsize);
	seg->add_ref();
	p->seg = seg;
	p->wrtStoreIter = NULL;
	for (size_t i = 0; i < indexNum; ++i) {
		p->indexIter[i] = NULL;
	}
	return p;
}
void DbContext::SegCtx::destory(SegCtx*& rp, size_t indexNum) {
	SegCtx* p = rp;
	for (size_t i = 0; i < indexNum; ++i) {
		RefcntPtr_release(p->indexIter[i]);
	}
	RefcntPtr_release(p->wrtStoreIter);
	assert(NULL != p->seg);
#if 0
	if (p->seg->get_refcount() == 1) {
		fprintf(stderr, "INFO: last refcnt, DbContext::SegCtx::destory(%s)\n"
			, p->seg->m_segDir.string().c_str());
	}
#endif
	p->seg->release();
	::free(p);
	rp = NULL;
}
void DbContext::SegCtx::reset(SegCtx* p, size_t indexNum, ReadableSegment* seg) {
	for (size_t i = 0; i < indexNum; ++i) {
		RefcntPtr_release(p->indexIter[i]);
	}
	RefcntPtr_release(p->wrtStoreIter);
	assert(NULL != p->seg);
	p->seg->release();
	p->seg = seg;
	seg->add_ref();
}

DbContextLink::DbContextLink() {
//	m_prev = m_next = this;
}

DbContextLink::~DbContextLink() {
}

static std::atomic<size_t> g_dbCtxLiveCnt;
static std::atomic<size_t> g_dbCtxCreatedCnt;

DbContext::DbContext(const DbTable* tab)
  : m_tab(const_cast<DbTable*>(tab))
{
// must calling the constructor in lock tab->m_rwMutex
	size_t oldtab_segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
//	tab->registerDbContext(this);
	regexMatchMemLimit = 16*1024*1024; // 16MB
	size_t indexNum = tab->getIndexNum();
	size_t segNum = tab->getSegNum();
	m_segCtx.resize(segNum, NULL);
	SegCtx** sctx = m_segCtx.data();
	for (size_t i = 0; i < segNum; ++i) {
		sctx[i] = SegCtx::create(tab->getSegmentPtr(i), indexNum);
	}
	m_rowNumVec.assign(tab->m_rowNumVec);

	// record id is also used as a snapshot version
	m_mySnapshotVersion = tab->m_rowNum - 1;
	m_isUserDefineSnapshot = false;

	segArrayUpdateSeq = tab->m_segArrayUpdateSeq;
	syncIndex = true;
	isUpsertOverwritten = 0;
	TERARK_RT_assert(tab->getSegArrayUpdateSeq() == oldtab_segArrayUpdateSeq,
					 std::logic_error);
	g_dbCtxLiveCnt++;
	g_dbCtxCreatedCnt++;
	if (getEnvBool("TerarkDB_TrackBuggyObjectLife")) {
		fprintf(stderr, "DEBUG: DbContext live count = %zd, created = %zd\n"
			, g_dbCtxLiveCnt.load(), g_dbCtxCreatedCnt.load());
	}
	upsertMaxRetry = 0;
}

DbContext::~DbContext() {
//	m_tab->unregisterDbContext(this);
	this->m_transaction.reset(); // destory before m_segCtx
	size_t indexNum = m_tab->getIndexNum();
	for (auto& x : m_segCtx) {
		assert(NULL != x);
		SegCtx::destory(x, indexNum);
	}
	g_dbCtxLiveCnt--;
}

void DbContext::doSyncSegCtxNoLock(const DbTable* tab) {
	assert(tab == m_tab);
	assert(this->segArrayUpdateSeq < tab->getSegArrayUpdateSeq());
	if (!m_isUserDefineSnapshot) {
		m_mySnapshotVersion = tab->m_rowNum - 1;
	}
	size_t indexNum = tab->getIndexNum();
	size_t oldtab_segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
	size_t oldSegNum = m_segCtx.size();
	size_t segNum = tab->getSegNum();
	if (m_segCtx.size() < segNum) {
		m_segCtx.resize(segNum, NULL);
		for (size_t i = oldSegNum; i < segNum; ++i)
			m_segCtx[i] = SegCtx::create(tab->getSegmentPtr(i), indexNum);
	}
	if (m_transaction && tab->m_wrSeg.get() != m_wrSegPtr) {
		// m_transaction is useless, reset it!
		m_transaction.reset();
		m_wrSegPtr = NULL;
	}
	SegCtx** sctx = m_segCtx.data();
	for (size_t i = 0; i < segNum; ++i) {
		ReadableSegment* seg = tab->getSegmentPtr(i);
		if (NULL == sctx[i]) {
			sctx[i] = SegCtx::create(seg, indexNum);
			continue;
		}
		if (sctx[i]->seg == seg)
			continue;
		for (size_t j = i; j < oldSegNum; ++j) {
			assert(NULL != sctx[j]);
			if (sctx[j]->seg == seg) {
				for (size_t k = i; k < j; ++k) {
					// this should be a merged segments range
					assert(NULL != sctx[k]);
					SegCtx::destory(sctx[k], indexNum);
				}
				for (size_t k = 0; k < oldSegNum - j; ++k) {
					sctx[i + k] = sctx[j + k];
					sctx[j + k] = NULL;
				}
				oldSegNum -= j - i;
				goto Done;
			}
		}
		// a WritableSegment was compressed into a ReadonlySegment, or
		// a ReadonlySegment was purged into a new ReadonlySegment
		SegCtx::reset(sctx[i], indexNum, seg);
	Done:;
	}
	for (size_t i = segNum; i < m_segCtx.size(); ++i) {
		if (sctx[i])
			SegCtx::destory(sctx[i], indexNum);
	}
	for (size_t i = 0; i < segNum; ++i) {
		TERARK_RT_assert(NULL != sctx[i], std::logic_error);
		TERARK_RT_assert(NULL != sctx[i]->seg, std::logic_error);
		TERARK_RT_assert(tab->getSegmentPtr(i) == sctx[i]->seg, std::logic_error);
	}
	m_segCtx.risk_set_size(segNum);
	m_rowNumVec.assign(tab->m_rowNumVec);
	TERARK_RT_assert(m_rowNumVec.size() == segNum + 1, std::logic_error);
	TERARK_RT_assert(tab->getSegArrayUpdateSeq() == oldtab_segArrayUpdateSeq,
					 std::logic_error);
	segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
}

StoreIterator* DbContext::getWrtStoreIterNoLock(size_t segIdx) {
	assert(segIdx < m_segCtx.size());
	SegCtx* p = m_segCtx[segIdx];
	assert(p->seg);
	assert(p->seg->getWritableSegment() != nullptr);
	assert(!p->seg->m_hasLockFreePointSearch);
	if (p->wrtStoreIter == nullptr) {
		auto wrseg = p->seg->getWritableSegment();
		p->wrtStoreIter = wrseg->m_wrtStore->createStoreIterForward(this);
		p->wrtStoreIter->add_ref();
	}
	return p->wrtStoreIter;
}

IndexIterator* DbContext::getIndexIterNoLock(size_t segIdx, size_t indexId) {
// can be slightly not sync with tab
	assert(segIdx < m_segCtx.size());
	assert(indexId < m_tab->getIndexNum());
	SegCtx* sc = m_segCtx[segIdx];
	auto& indexIter = sc->indexIter[indexId];
	if (indexIter == nullptr) {
		indexIter = m_segCtx[segIdx]->seg->m_indices[indexId]->createIndexIterForward(this);
		indexIter->add_ref();
	}
	return indexIter;
}

void DbContext::debugCheckUnique(fstring row, size_t uniqueIndexId) {
	assert(this->segArrayUpdateSeq == m_tab->m_segArrayUpdateSeq);
	const Schema& indexSchema = m_tab->getIndexSchema(uniqueIndexId);
	m_tab->m_schema->m_rowSchema->parseRow(row, &cols1);
	indexSchema.selectParent(cols1, &key1);
	indexSearchExactNoLock(uniqueIndexId, key1, &exactMatchRecIdvec);
	assert(exactMatchRecIdvec.size() <= 1);
}

void
DbContext::getWrSegWrtStoreData(const ReadableSegment* seg, llong subId, valvec<byte>* buf) {
	assert(seg->getWritableSegment() != NULL);
	assert(!seg->m_hasLockFreePointSearch);
	auto sctx = m_segCtx.data();
	size_t segIdx = m_segCtx.size();
	while (segIdx > 0) {
		auto seg2 = sctx[--segIdx]->seg;
		if (seg2 == seg) {
			auto wrtStoreIter = getWrtStoreIterNoLock(segIdx);
			buf->erase_all();
			wrtStoreIter->reset();
			if (!wrtStoreIter->seekExact(subId, buf)) {
				llong baseId = m_rowNumVec[segIdx];
				throw ReadRecordException("wrtStoreIter->seekExact",
					seg->m_segDir.string().c_str(), baseId, subId);
			}
			wrtStoreIter->reset();
			return;
		}
	}
	fprintf(stderr
		, "WARN: DbContext::getWrSegWrtStoreData: not found segment: %s\n"
		, seg->m_segDir.string().c_str());
	// fallback to slow locked getValue:
	WritableSegment* wrseg = seg->getWritableSegment();
	assert(wrseg != nullptr);
	wrseg->m_wrtStore->getValue(subId, buf, this);
}

void DbContext::ensureTransactionNoLock() {
	DbTable* tab = m_tab;
	auto new_wrseg = tab->m_wrSeg.get();
	if (new_wrseg != m_wrSegPtr) {
		if (m_transaction) {
			assert(DbTransaction::started != m_transaction->m_status);
			m_transaction.reset();
		}
		if (new_wrseg) {
			m_transaction.reset(new_wrseg->createTransaction());
		}
		m_wrSegPtr = new_wrseg;
	}
	else {
		assert(m_transaction.get() != nullptr);
	}
}

void DbContext::freeWritableSegmentResources() {
	DbTable* tab = m_tab;
	size_t indexNum = tab->getIndexNum();
	for (size_t i = 0; i < m_segCtx.size(); ++i) {
		auto cur = m_segCtx[i];
		assert(nullptr != cur->seg);
		if (cur->seg->getWritableSegment()) {
			RefcntPtr_release(cur->wrtStoreIter);
			for (size_t j = 0; j < indexNum; ++j)
				RefcntPtr_release(cur->indexIter[j]);
		}
	}
	m_wrSegPtr = nullptr;
	m_transaction.reset();
}

} } // namespace terark::db
