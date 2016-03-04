#include "dfadb_table.hpp"
#include "dfadb_segment.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>
#include <terark/db/mock_db_engine.hpp>
#include <terark/db/wiredtiger/wt_db_segment.hpp>
#include <boost/filesystem.hpp>

namespace terark { namespace db { namespace dfadb {


DfaDbContext::DfaDbContext(const CompositeTable* tab) : DbContext(tab) {
}
DfaDbContext::~DfaDbContext() {
}

DbContext* DfaDbTable::createDbContext() const {
	return new DfaDbContext(this);
}

ReadonlySegment*
DfaDbTable::createReadonlySegment(PathRef dir) const {
	std::unique_ptr<DfaDbReadonlySegment> seg(new DfaDbReadonlySegment());
	return seg.release();
}

WritableSegment*
DfaDbTable::createWritableSegment(PathRef dir) const {
	const char* dfaWritableSeg = getenv("NarkDB_DfaWritableSegment");
	if (dfaWritableSeg && strcasecmp(dfaWritableSeg, "mock") == 0) {
		std::unique_ptr<WritableSegment> seg(new MockWritableSegment(dir));
		seg->m_schema = this->m_schema;
		return seg.release();
	}
	else {
		using terark::db::wt::WtWritableSegment;
		std::unique_ptr<WtWritableSegment> seg(new WtWritableSegment());
		seg->m_schema = this->m_schema;
		seg->load(dir);
		return seg.release();
	}
}

WritableSegment*
DfaDbTable::openWritableSegment(PathRef dir) const {
	auto isDelPath = dir / "isDel";
	if (boost::filesystem::exists(isDelPath)) {
		const char* dfaWritableSeg = getenv("NarkDB_DfaWritableSegment");
		if (dfaWritableSeg && strcasecmp(dfaWritableSeg, "mock") == 0) {
			std::unique_ptr<WritableSegment> seg(new MockWritableSegment(dir));
			seg->m_schema = this->m_schema;
			seg->load(dir);
			return seg.release();
		}
		else {
			using terark::db::wt::WtWritableSegment;
			std::unique_ptr<WtWritableSegment> seg(new WtWritableSegment());
			seg->m_schema = this->m_schema;
			seg->load(dir);
			return seg.release();
		}
	}
	else {
		return myCreateWritableSegment(dir);
	}
}

NARK_DB_REGISTER_TABLE_CLASS(DfaDbTable);

}}} // namespace terark::db::dfadb
