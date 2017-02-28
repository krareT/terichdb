#ifndef __terark_db_appendonly_hpp__
#define __terark_db_appendonly_hpp__

#include "db_store.hpp"

namespace terark { namespace terichdb {

class TERICHDB_DLL RandomReadAppendonlyStore final : public ReadableStore, public AppendableStore {
public:
	explicit RandomReadAppendonlyStore(const Schema&);
	~RandomReadAppendonlyStore();

	AppendableStore* getAppendableStore() override;
	llong dataInflateSize() const override;
	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  shrinkToFit() override;
    void  shrinkToSize(size_t size) override;
	void  deleteFiles() override;

	void load(PathRef fpath) override;
	void save(PathRef fpath) const override;

private:
	struct Header;
	Header* allocIndexRows(ullong fileSize);
	Header* m_index; // just index by Record ID
	byte_t* m_store;
	size_t  m_indexBytes;
	size_t  m_storeBytes;
	std::string m_indexFile;
	std::string m_storeFile;
};

class TERICHDB_DLL SeqReadAppendonlyStore final : public ReadableStore, public AppendableStore {
	class MyStoreIterForward; friend class MyStoreIterForward;
public:
	explicit SeqReadAppendonlyStore(const Schema&);
	SeqReadAppendonlyStore(PathRef segDir, const Schema&);
	~SeqReadAppendonlyStore();

	AppendableStore* getAppendableStore() override;
	llong dataInflateSize() const override;
	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  shrinkToFit() override;
    void  shrinkToSize(size_t size) override;
	void  deleteFiles() override;

	void load(PathRef fpath) override;
	void save(PathRef fpath) const override;

private:
	void doLoad();
	struct IoImpl;
	std::unique_ptr<IoImpl> m_io;
	llong       m_fsize;
	llong       m_inflateSize;
	llong       m_rows;
	std::string m_fpath;
};


} } // namespace terark::terichdb

#endif // __terark_db_appendonly_hpp__
