#pragma once

#include <terark/db/db_table.hpp>
#include <terark/fsa/fsa.hpp>
#include <terark/int_vector.hpp>
#include <terark/rank_select.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>

namespace terark {
//	class Nest
} // namespace terark

namespace terark { namespace db { namespace dfadb {

class TERARK_DB_DLL DfaDbContext : public DbContext {
public:
	explicit DfaDbContext(const DbTable* tab);
	~DfaDbContext();
	std::string m_nltRecBuf;
};
class TERARK_DB_DLL DfaDbTable : public DbTable {
public:
	DbContext* createDbContextNoLock() const override;
	ReadonlySegment* createReadonlySegment(PathRef dir) const override;
	WritableSegment* createWritableSegment(PathRef dir) const override;
	WritableSegment* openWritableSegment(PathRef dir) const override;
	bool indexMatchRegex(size_t indexId, BaseDFA* regexDFA, valvec<llong>* recIdvec, DbContext*) const override;
	bool indexMatchRegex(size_t indexId, fstring  regexStr, fstring  regexOptions, valvec<llong>* recIdvec, DbContext*) const override;
};

}}} // namespace terark::db::dfadb
