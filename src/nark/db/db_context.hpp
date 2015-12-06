#ifndef __nark_db_db_context_hpp__
#define __nark_db_db_context_hpp__

#include "db_conf.hpp"

namespace nark { namespace db {

typedef boost::intrusive_ptr<class CompositeTable> CompositeTablePtr;
typedef boost::intrusive_ptr<class StoreIterator> StoreIteratorPtr;

class NARK_DB_DLL DbContext : public RefCounter {
public:
	explicit DbContext(const CompositeTable* tab);
	~DbContext();

	StoreIteratorPtr createTableIter();

	void getValueAppend(llong id, valvec<byte>* val);
	void getValue(llong id, valvec<byte>* val);

	llong insertRow(fstring row);
	llong replaceRow(llong id, fstring row);
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
	valvec<size_t> offsets;
	valvec<fstring> cols1;
	valvec<fstring> cols2;
	bool syncIndex;
};
typedef boost::intrusive_ptr<DbContext> DbContextPtr;



} } // namespace nark::db

#endif // __nark_db_db_context_hpp__
