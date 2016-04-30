#ifndef __terark_util_blob_store_hpp__
#define __terark_util_blob_store_hpp__

#include <terark/valvec.hpp>
#include <terark/fstring.hpp>

namespace terark {

class TERARK_DLL_EXPORT BlobStore {
protected:
	size_t m_numRecords;
	void risk_swap(BlobStore& y);
public:
	static BlobStore* load_from(fstring fpath, bool mmapPopulate);
	BlobStore();
	virtual ~BlobStore();
	size_t num_records() const { return m_numRecords; }
	virtual size_t mem_size() const = 0;
	virtual long long total_data_size() const = 0;
	virtual void get_record_append(size_t recID, valvec<byte_t>* recData) const = 0;
	void get_record(size_t recID, valvec<byte_t>* recData) const;
	valvec<byte_t> get_record(size_t recID) const;
};

} // namespace terark

#endif // __terark_util_blob_store_hpp__
