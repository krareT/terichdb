#pragma once

#include <nark/db/db_segment.hpp>

namespace nark {
//	class Nest
} // namespace nark

namespace nark { namespace db { namespace dfadb {

class NARK_DB_DLL DfaDbReadonlySegment : public ReadonlySegment {
public:
	DfaDbReadonlySegment();
	~DfaDbReadonlySegment();
protected:
	ReadableIndex* openIndex(const Schema&, PathRef path) const override;

	ReadableIndex* buildIndex(const Schema&, SortableStrVec& indexData) const override;
	ReadableStore* buildStore(const Schema&, SortableStrVec& storeData) const override;
	ReadableStore* buildDictZipStore(const Schema&, StoreIterator&iter, const bm_uint_t* isDel) const override;
};

}}} // namespace nark::db::dfadb
