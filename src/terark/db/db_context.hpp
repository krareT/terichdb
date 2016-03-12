#ifndef __terark_db_db_context_hpp__
#define __terark_db_db_context_hpp__

#include "db_conf.hpp"

namespace terark { namespace db {

typedef boost::intrusive_ptr<class CompositeTable> CompositeTablePtr;
typedef boost::intrusive_ptr<class StoreIterator> StoreIteratorPtr;

class TERARK_DB_DLL DbContextLink : public RefCounter {
	friend class CompositeTable;
protected:
	DbContextLink();
	~DbContextLink();
//	DbContextLink *m_prev, *m_next;
};
class TERARK_DB_DLL DbContext : public DbContextLink {
	friend class CompositeTable;
public:
	explicit DbContext(const CompositeTable* tab);
	~DbContext();

//	virtual void onSegCompressed(size_t segIdx, class WritableSegment*, class ReadonlySegment*);

	StoreIteratorPtr createTableIter();

	void getValueAppend(llong id, valvec<byte>* val);
	void getValue(llong id, valvec<byte>* val);

	llong insertRow(fstring row);
	llong updateRow(llong id, fstring row);
	void  removeRow(llong id);

	void indexInsert(size_t indexId, fstring indexKey, llong id);
	void indexRemove(size_t indexId, fstring indexKey, llong id);
	void indexReplace(size_t indexId, fstring indexKey, llong oldId, llong newId);

public:
	CompositeTable* m_tab;
	std::string  errMsg;
	valvec<byte> buf1;
	valvec<byte> buf2;
	valvec<byte> row1;
	valvec<byte> row2;
	valvec<byte> key1;
	valvec<byte> key2;
	valvec<uint32_t> offsets;
	ColumnVec    cols1;
	ColumnVec    cols2;
	valvec<llong> exactMatchRecIdvec;
	bool syncIndex;
};
typedef boost::intrusive_ptr<DbContext> DbContextPtr;

} } // namespace terark::db

#endif // __terark_db_db_context_hpp__
