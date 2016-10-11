#include "dfadb_table.hpp"
#include "dfadb_segment.hpp"
#undef min
#undef max
#include <terark/fsa/create_regex_dfa.hpp>
#include <terark/fsa/dense_dfa.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/mmap.hpp>
#include <terark/db/appendonly.hpp>
#include <terark/db/mock_db_engine.hpp>
#include <terark/db/dfadb/nlt_index.hpp>
#include <terark/fsa/fsa.hpp>
#include <terark/int_vector.hpp>
#include <terark/rank_select.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>

namespace terark { namespace db { namespace dfadb {

DfaDbContext::DfaDbContext(const DbTable* tab) : DbContext(tab) {
}
DfaDbContext::~DfaDbContext() {
}

DbContext* DfaDbTable::createDbContextNoLock() const {
	return new DfaDbContext(this);
}

class AdapterRegexDFA : public DenseDFA_uint32_320 {
public:
	typedef AdapterRegexDFA MyType;
#include <terark/fsa/ppi/match_path.hpp>
};

bool
DfaDbTable::indexMatchRegex(size_t indexId, BaseDFA* regexDFA,
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
		auto index = dynamic_cast<const NestLoudsTrieIndex*>(&*seg->m_indices[indexId]);
		if (!index) {
			THROW_STD(logic_error, "MatchRegex must be run on NestLoudsTrieIndex\n");
		}
		size_t oldsize = recIdvec->size();
		const llong* deltime = nullptr;
		const llong  baseId = ctx->m_rowNumVec[i];
		llong snapshotVersion = ctx->m_mySnapshotVersion;
		if (seg->m_deletionTime) {
			assert(nullptr != m_schema->m_snapshotSchema);
			deltime = (const llong*)(seg->m_deletionTime->getRecordsBasePtr());
		}
		if (index->matchRegexAppend(regexDFA, recIdvec, ctx)) {
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
				, "WARN: RegexMatch exceeded memory limit(%zd bytes) on index '%s' of segment: '%s', try linear scan...\n"
				, ctx->regexMatchMemLimit
				, schema.m_name.c_str(), seg->m_segDir.string().c_str());
			auto matchDFA = static_cast<const AdapterRegexDFA*>(
					dynamic_cast<const DenseDFA_uint32_320*>(regexDFA)
				);
			assert(NULL != matchDFA);
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
							if (matchDFA->first_mismatch_pos(key) == key.size()) {
								recIdvec->push_back(baseId + subLogicId);
							}
						}
					}
					else {
						if (!terark_bit_test(isDel, subLogicId)) {
							if (matchDFA->first_mismatch_pos(key) == key.size()) {
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
DfaDbTable::indexMatchRegex(size_t indexId,
							fstring regexStr, fstring regexOptions,
							valvec<llong>* recIdvec, DbContext* ctx)
const {
	std::unique_ptr<BaseDFA> regexDFA(create_regex_dfa(regexStr, regexOptions));
	return indexMatchRegex(indexId, regexDFA.get(), recIdvec, ctx);
}

TERARK_DB_REGISTER_TABLE_CLASS(DfaDbTable);

}}} // namespace terark::db::dfadb
