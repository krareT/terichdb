#ifndef __nark_db_seq_num_index_hpp__
#define __nark_db_seq_num_index_hpp__

#include "data_index.hpp"

namespace nark { namespace db {

// SeqNumIndex can be used as a primary key of Id
template<class Int>
class NARK_DB_DLL SeqNumIndex : public WritableIndexStore {
	Int m_min;
	Int m_cnt;
	class MyIndexIter; friend class MyIndexIter;
	class MyStoreIter; friend class MyStoreIter;
public:
	SeqNumIndex(Int min, Int cnt);
	~SeqNumIndex();

	IndexIterator* createIndexIter(DbContext*) const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;

	size_t remove(fstring key, llong id, DbContext*) override;
	size_t insert(fstring key, llong id, DbContext*) override;
	size_t replace(fstring key, llong id, llong newId, DbContext*) override;
	void clear() override;
	void flush() const override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIter(DbContext*) const override;
};

} } // namespace nark::db

#endif // __nark_db_seq_num_index_hpp__
