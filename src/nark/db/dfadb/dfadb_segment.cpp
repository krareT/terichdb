#include "dfadb_segment.hpp"
#include <nark/db/intkey_index.hpp>
#include "nlt_index.hpp"
#include "nlt_store.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db { namespace dfadb {

DfaDbReadonlySegment::DfaDbReadonlySegment() {
}
DfaDbReadonlySegment::~DfaDbReadonlySegment() {
}

ReadableStore*
DfaDbReadonlySegment::openPart(fstring path) const {
	std::unique_ptr<NestLoudsTrieStore> store(new NestLoudsTrieStore());
	store->load(path);
	return store.release();
}

ReadableIndex*
DfaDbReadonlySegment::openIndex(fstring path, const Schema& schema) const {
	if (boost::filesystem::exists(path + ".nlt")) {
		std::unique_ptr<NestLoudsTrieIndex> store(new NestLoudsTrieIndex());
		store->load(path);
		return store.release();
	}
	else {
		std::unique_ptr<ZipIntKeyIndex> store(new ZipIntKeyIndex());
		store->load(path);
		return store.release();
	}
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
DfaDbReadonlySegment::buildIndex(const Schema& indexSchema,
								 SortableStrVec& indexData)
const {
	if (indexSchema.columnNum() == 1 && indexSchema.getColumnMeta(0).isInteger()) {
		std::unique_ptr<ZipIntKeyIndex> index(new ZipIntKeyIndex());
		index->build(indexSchema.getColumnMeta(0).type, indexData);
		return index.release();
	}
	else {
		if (indexData.m_index.size() == 0) {
			const size_t fixlen = indexSchema.getFixedRowLen();
			assert(fixlen > 0);
			patchStrVec(indexData, fixlen);
		}
		std::unique_ptr<NestLoudsTrieIndex> index(new NestLoudsTrieIndex());
		index->build(indexData);
		return index.release();
	}
}

ReadableStore*
DfaDbReadonlySegment::buildStore(SortableStrVec& storeData) const {
	std::unique_ptr<NestLoudsTrieStore> store(new NestLoudsTrieStore());
	if (storeData.m_index.size() == 0) {
		const size_t fixlen = m_rowSchema->getFixedRowLen();
		assert(fixlen > 0);
		patchStrVec(storeData, fixlen);
	}
	store->build(storeData);
	return store.release();
}

}}} // namespace nark::db::dfadb
