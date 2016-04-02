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

struct DbContext::SegCtx {
	ReadableSegmentPtr seg;
	StoreIteratorPtr storeIter;
	IndexIteratorPtr indexIter[1];

	~SegCtx() = delete;
	SegCtx() = delete;
	SegCtx(const SegCtx&) = delete;
	SegCtx& operator=(const SegCtx&) = delete;

	static SegCtx* create(ReadableSegment* seg, size_t indexNum) {
		size_t memsize = sizeof(SegCtx) + sizeof(IndexIteratorPtr) * (indexNum-1);
		SegCtx* p = (SegCtx*)malloc(memsize);
		new(&p->seg)ReadableSegmentPtr(seg);
		new(&p->storeIter)StoreIteratorPtr();
		for (size_t i = 0; i < indexNum; ++i) {
			new(&p->indexIter[i])IndexIteratorPtr();
		}
		return p;
	}
	static void destory(SegCtx* p, size_t indexNum) {
		for (size_t i = 0; i < indexNum; ++i) {
			p->indexIter[i].reset();
		}
		p->storeIter.reset();
		p->seg.reset();
		::free(p);
	}
	static void reset(SegCtx* p, size_t indexNum, ReadableSegment* seg) {
		for (size_t i = 0; i < indexNum; ++i) {
			p->indexIter[i].reset();
		}
		p->storeIter.reset();
		p->seg = seg;
	}
};

DbContextLink::DbContextLink() {
//	m_prev = m_next = this;
}

DbContextLink::~DbContextLink() {
}

DbContext::DbContext(const CompositeTable* tab)
  : m_tab(const_cast<CompositeTable*>(tab))
{
//	tab->registerDbContext(this);
//	m_segCtx.resize(tab->getIndexNum(), nullptr);
	regexMatchMemLimit = 16*1024*1024; // 16MB
	segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
	syncIndex = true;
	isUpsertOverwritten = 0;
}

DbContext::~DbContext() {
//	m_tab->unregisterDbContext(this);
	size_t indexNum = m_tab->getIndexNum();
	for (auto x : m_segCtx) {
		if (x) {
			SegCtx::destory(x, indexNum);
		}
	}
}

void DbContext::syncSegCtxNoLock() {
	CompositeTable* tab = m_tab;
	size_t oldtab_segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
	size_t segNum = tab->getSegNum();
	if (m_segCtx.size() < segNum) {
		m_segCtx.resize(segNum, NULL);
	}
	SegCtx** sctx = m_segCtx.data();
	size_t oldSegNum = m_segCtx.size();
	size_t indexNum = tab->getIndexNum();
	for (size_t i = 0; i < segNum; ++i) {
		if (NULL == sctx[i])
			continue;
		ReadableSegment* seg = tab->getSegmentPtr(i);
		if (sctx[i]->seg.get() == seg)
			continue;
		for (size_t j = i; j < oldSegNum; ++j) {
			if (sctx[j] && sctx[j]->seg.get() == seg) {
				for (size_t k = i; k < j; ++k) {
					// this should be a merged segments range
					if (sctx[k]) {
						SegCtx::destory(sctx[k], indexNum);
						sctx[k] = NULL;
					}
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
		if (sctx[i]) {
			SegCtx::destory(sctx[i], indexNum);
			sctx[i] = NULL;
		}
	}
	m_segCtx.risk_set_size(segNum);
	TERARK_RT_assert(tab->getSegArrayUpdateSeq() == oldtab_segArrayUpdateSeq,
					 std::logic_error);
	segArrayUpdateSeq = tab->getSegArrayUpdateSeq();
}

StoreIterator* DbContext::getStoreIterNoLock(size_t segIdx) {
	CompositeTable* tab = m_tab;
	assert(segIdx < tab->getSegNum());
	if (tab->getSegArrayUpdateSeq() != segArrayUpdateSeq) {
		assert(segArrayUpdateSeq < tab->getSegArrayUpdateSeq());
		this->syncSegCtxNoLock();
	}
	else if (m_segCtx.size() <= segIdx) {
		m_segCtx.resize(tab->getSegNum(), NULL);
	}
	assert(tab->getSegArrayUpdateSeq() == segArrayUpdateSeq);
	assert(m_segCtx.size() == tab->getSegNum());
	if (NULL == m_segCtx[segIdx]) {
		size_t indexNum = tab->getIndexNum();
		m_segCtx[segIdx] = SegCtx::create(tab->getSegmentPtr(segIdx), indexNum);
	}
	SegCtx* p = m_segCtx[segIdx];
	if (p->storeIter.get() == nullptr) {
		p->storeIter = tab->getSegmentPtr(segIdx)->createStoreIterForward(this);
	}
	return p->storeIter.get();
}

IndexIterator* DbContext::getIndexIterNoLock(size_t segIdx, size_t indexId) {
	CompositeTable* tab = m_tab;
	assert(segIdx < tab->getSegNum());
	if (tab->getSegArrayUpdateSeq() != segArrayUpdateSeq) {
		assert(segArrayUpdateSeq < tab->getSegArrayUpdateSeq());
		this->syncSegCtxNoLock();
	}
	else if (m_segCtx.size() <= segIdx) {
		m_segCtx.resize(tab->getSegNum(), NULL);
	}
	size_t indexNum = tab->getIndexNum();
	assert(indexId < indexNum);
	assert(m_segCtx.size() == tab->getSegNum());
	if (NULL == m_segCtx[segIdx]) {
		m_segCtx[segIdx] = SegCtx::create(tab->getSegmentPtr(segIdx), indexNum);
	}
	SegCtx* sc = m_segCtx[segIdx];
	if (sc->indexIter[indexId].get() == nullptr) {
		sc->indexIter[indexId] = tab->getSegmentPtr(segIdx)->
			m_indices[indexId]->createIndexIterForward(this);
	}
	return sc->indexIter[indexId].get();
}

} } // namespace terark::db
