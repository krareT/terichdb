#include "dfadb_table.hpp"
#include "dfadb_segment.hpp"
#include <nark/fsa/nest_trie_dawg.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>

#include <nark/db/mock_db_engine.hpp>

namespace nark { namespace db { namespace dfadb {


DfaDbContext::DfaDbContext(const CompositeTable* tab) : DbContext(tab) {
}
DfaDbContext::~DfaDbContext() {
}

DbContext* DfaDbTable::createDbContext() const {
	return new DfaDbContext(this);
}

ReadonlySegment*
DfaDbTable::createReadonlySegment(fstring dir) const {
	std::unique_ptr<DfaDbReadonlySegment> seg(new DfaDbReadonlySegment());
	return seg.release();
}

WritableSegment*
DfaDbTable::createWritableSegment(fstring dir) const {
	std::unique_ptr<MockWritableSegment> seg(new MockWritableSegment(dir));
	return seg.release();
}

WritableSegment*
DfaDbTable::openWritableSegment(fstring dir) const {
	std::unique_ptr<WritableSegment> seg(new MockWritableSegment(dir));
	seg->m_rowSchema = m_rowSchema;
	seg->m_indexSchemaSet = m_indexSchemaSet;
	seg->m_nonIndexRowSchema = m_nonIndexRowSchema;
	seg->load(dir);
	return seg.release();
}


}}} // namespace nark::db::dfadb
