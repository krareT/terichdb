#pragma once

#include <nark/db/db_segment.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/int_vector.hpp>
#include <nark/rank_select.hpp>
#include <nark/fsa/nest_trie_dawg.hpp>

namespace nark {
//	class Nest
} // namespace nark

namespace nark { namespace db { namespace dfadb {

class NARK_DB_DLL DfaDbReadonlySegment : public ReadonlySegment {
public:
	DfaDbReadonlySegment();
	~DfaDbReadonlySegment();
protected:
	ReadableStore* openPart(fstring path) const override;
	ReadableIndex* openIndex(fstring path, const Schema&) const override;

	ReadableIndex* buildIndex(const Schema& indexSchema,
								   SortableStrVec& indexData) const override;
	ReadableStore* buildStore(SortableStrVec& storeData) const override;
};

}}} // namespace nark::db::dfadb
