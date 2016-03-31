/*
 * db_context.cpp
 *
 *  Created on: 2015Äê11ÔÂ26ÈÕ
 *      Author: leipeng
 */
#include "db_context.hpp"
#include "db_table.hpp"

namespace terark { namespace db {

DbContextLink::DbContextLink() {
//	m_prev = m_next = this;
}

DbContextLink::~DbContextLink() {
}

DbContext::DbContext(const CompositeTable* tab)
  : m_tab(const_cast<CompositeTable*>(tab))
{
//	tab->registerDbContext(this);
	regexMatchMemLimit = 16*1024*1024; // 16MB
	syncIndex = true;
	isUpsertOverwritten = false;
}

DbContext::~DbContext() {
//	m_tab->unregisterDbContext(this);
}

/*
void
DbContext::onSegCompressed(size_t segIdx, WritableSegment*, ReadonlySegment*) {
}
*/

} } // namespace terark::db
