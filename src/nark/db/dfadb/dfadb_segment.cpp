#include "dfadb_segment.hpp"
#include <nark/db/intkey_index.hpp>
#include <nark/db/zip_int_store.hpp>
#include <nark/db/fixed_len_key_index.hpp>
//#include <nark/db/fixed_len_key_index.hpp>
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
DfaDbReadonlySegment::openStore(const Schema& schema, fstring path) const {
	if (boost::filesystem::exists(path + ".nlt")) {
		std::unique_ptr<NestLoudsTrieStore> store(new NestLoudsTrieStore());
		store->load(path);
		return store.release();
	}
	return ReadonlySegment::openStore(schema, path);
}

ReadableIndex*
DfaDbReadonlySegment::openIndex(const Schema& schema, fstring path) const {
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
	std::unique_ptr<ReadableIndex>
		index0(ReadonlySegment::buildIndex(schema, indexData));
	if (!index0) {
		if (indexData.m_index.size() == 0) {
			const size_t fixlen = schema.getFixedRowLen();
			assert(fixlen > 0);
			patchStrVec(indexData, fixlen);
		}
		std::unique_ptr<NestLoudsTrieIndex> index(new NestLoudsTrieIndex());
		index->build(indexData);
		return index.release();
	}
	return index0.release();
}

ReadableStore*
DfaDbReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	std::unique_ptr<ReadableStore>
		store(ReadonlySegment::buildStore(schema, storeData));
	if (!store) {
		std::unique_ptr<NestLoudsTrieStore> nltStore(new NestLoudsTrieStore());
		if (storeData.m_index.size() == 0) {
			const size_t fixlen = m_nonIndexRowSchema->getFixedRowLen();
			assert(fixlen > 0);
			patchStrVec(storeData, fixlen);
		}
		nltStore->build(storeData);
		return nltStore.release();
	}
	return store.release();
}

}}} // namespace nark::db::dfadb
