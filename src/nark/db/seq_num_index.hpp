#ifndef __nark_db_seq_num_index_hpp__
#define __nark_db_seq_num_index_hpp__

#include "data_index.hpp"

namespace nark {

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

	IndexIteratorPtr createIndexIter() const override;
	BaseContextPtr createIndexContext() const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;

	size_t remove(fstring key, llong id, BaseContextPtr&) override;
	size_t insert(fstring key, llong id, BaseContextPtr&) override;
	size_t replace(fstring key, llong id, llong newId, BaseContextPtr&) override;
	void flush() override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValue(llong id, valvec<byte>* val, BaseContextPtr&) const override;
	StoreIteratorPtr createStoreIter() const override;
	BaseContextPtr createStoreContext() const override;
};

} // namespace nark

#endif // __nark_db_seq_num_index_hpp__
