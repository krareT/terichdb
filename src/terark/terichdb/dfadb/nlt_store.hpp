#pragma once

#include <terark/terichdb/db_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>

namespace terark {
//	class Nest
} // namespace terark

namespace terark { namespace terichdb { namespace dfadb {

class NestLoudsTrieStore : public ReadableStore {
public:
	explicit NestLoudsTrieStore(const Schema& schema);
	explicit NestLoudsTrieStore(const Schema& schema, BlobStore* blobStore);
	~NestLoudsTrieStore();

	llong dataStorageSize() const override;
	llong dataInflateSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	void build(const Schema&, SortableStrVec& strVec);
	void build_by_iter(const Schema&, PathRef fpath, StoreIterator& iter,
					   const bm_uint_t* isDel, const febitvec* isPurged);
	void load(PathRef path) override;
	void save(PathRef path) const override;

protected:
	const Schema& m_schema;
	std::unique_ptr<BlobStore> m_store;
};

std::unique_ptr<DictZipBlobStore::ZipBuilder>
createDictZipBlobStoreBuilder(const Schema& schema);


}}} // namespace terark::terichdb::dfadb
