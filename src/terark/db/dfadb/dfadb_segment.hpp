#pragma once

#include <terark/db/db_segment.hpp>

namespace terark {
//	class Nest
} // namespace terark

namespace terark { namespace db { namespace dfadb {

class DfaDbReadonlySegment : public ReadonlySegment {
public:
	DfaDbReadonlySegment();
	~DfaDbReadonlySegment();
protected:
	ReadableIndex* openIndex(const Schema&, PathRef path) const override;

	ReadableIndex* buildIndex(const Schema&, SortableStrVec& indexData) const override;
	ReadableStore* buildStore(const Schema&, SortableStrVec& storeData) const override;
	ReadableStore*
	buildDictZipStore(const Schema&, PathRef dir, StoreIterator& iter,
		const bm_uint_t* isDel, const febitvec* isPurged) const override;
	ReadableStore*
	purgeDictZipStore(const Schema&, PathRef pathWithPrefix, const ReadableStore* inputStore,
                      size_t throttleBytesPerSecond, const bm_uint_t* isDel,
                      const rank_select_se* isPurged, size_t baseId) const override;
	void compressSingleColgroup(ReadableSegment* input, DbContext* ctx) override;
	void compressSingleKeyValue(ReadableSegment* input, DbContext* ctx) override;
};

}}} // namespace terark::db::dfadb
