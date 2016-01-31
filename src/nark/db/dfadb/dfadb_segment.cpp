#include "dfadb_segment.hpp"
#include "nlt_index.hpp"
#include "nlt_store.hpp"

namespace nark { namespace db { namespace dfadb {

DfaDbReadonlySegment::DfaDbReadonlySegment() {
}
DfaDbReadonlySegment::~DfaDbReadonlySegment() {
}

ReadableIndex*
DfaDbReadonlySegment::openIndex(const Schema& schema, PathRef path) const {
	if (boost::filesystem::exists(path + ".nlt")) {
		std::unique_ptr<NestLoudsTrieIndex> store(new NestLoudsTrieIndex());
		store->load(path);
		return store.release();
	}
	return ReadonlySegment::openIndex(schema, path);
}

static void patchStrVec(SortableStrVec& strVec, size_t fixlen) {
	const size_t rows = strVec.str_size() / fixlen;
	assert(strVec.str_size() % rows == 0);
	strVec.m_index.resize_no_init(rows);
	for (size_t i = 0; i < rows; ++i) {
		strVec.m_index[i].seq_id = i;
		strVec.m_index[i].length = fixlen;
		strVec.m_index[i].offset = fixlen * i;
	}
}

ReadableIndex*
DfaDbReadonlySegment::buildIndex(const Schema& schema, SortableStrVec& indexData)
const {
	ReadableIndex* index0 = ReadonlySegment::buildIndex(schema, indexData);
	if (index0) {
		return index0;
	}
	if (indexData.m_index.size() == 0) {
		const size_t fixlen = schema.getFixedRowLen();
		assert(fixlen > 0);
		patchStrVec(indexData, fixlen);
	}
	std::unique_ptr<NestLoudsTrieIndex> index(new NestLoudsTrieIndex());
	index->build(schema, indexData);
	return index.release();
}

ReadableStore*
DfaDbReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	ReadableStore* store = ReadonlySegment::buildStore(schema, storeData);
	if (store) {
		return store;
	}
	std::unique_ptr<NestLoudsTrieStore> nlt(new NestLoudsTrieStore());
	if (storeData.m_index.size() == 0) {
		const size_t fixlen = schema.getFixedRowLen();
		assert(fixlen > 0);
		patchStrVec(storeData, fixlen);
	}
	nlt->build(schema, storeData);
	return nlt.release();
}

ReadableStore*
DfaDbReadonlySegment::buildDictZipStore(const Schema& schema, StoreIterator& inputIter)
const {
	std::unique_ptr<NestLoudsTrieStore> nlt(new NestLoudsTrieStore());
	nlt->build_by_iter(schema, inputIter);
	return nlt.release();
}


}}} // namespace nark::db::dfadb
