#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <set>

namespace nark { namespace db {

class NARK_DB_DLL MockReadonlyStore : public ReadableStore {
	size_t    m_fixedLen;
	SchemaPtr m_schema;
public:
	fstrvec m_rows;

	void build(SchemaPtr schema, SortableStrVec& storeData);

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const;
	StoreIteratorPtr createStoreIter(DbContext*) const override;
};
typedef boost::intrusive_ptr<MockReadonlyStore> MockReadonlyStorePtr;

class NARK_DB_DLL MockReadonlyIndex : public ReadableIndexStore {
	friend class MockReadonlyIndexIterator;
	fstrvec          m_keys; // keys[recId] is the key
	valvec<uint32_t> m_ids;  // keys[ids[i]] <= keys[ids[i+1]]
	size_t m_fixedLen;
	SchemaPtr m_schema;
public:
	MockReadonlyIndex(SchemaPtr schema);
	~MockReadonlyIndex();

	void build(SortableStrVec& indexData);

	void save(fstring) const override;
	void load(fstring) override;

	StoreIteratorPtr createStoreIter(DbContext*) const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* key, DbContext*) const override;

	IndexIteratorPtr createIndexIter(DbContext*) const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
};
typedef boost::intrusive_ptr<MockReadonlyIndex> MockReadonlyIndexPtr;

class NARK_DB_DLL MockWritableStore : public ReadableStore, public WritableStore {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	void save(fstring) const override;
	void load(fstring) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIteratorPtr createStoreIter(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  replace(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;
};
typedef boost::intrusive_ptr<MockWritableStore> MockWritableStorePtr;

template<class Key>
class NARK_DB_DLL MockWritableIndex : public WritableIndex {
	class MyIndexIter; friend class MyIndexIter;
	typedef std::pair<Key, llong> kv_t;
	std::set<kv_t> m_kv;
	size_t m_keysLen = 0;
	ullong m_removeVer = 0;
public:
	void save(fstring) const override;
	void load(fstring) override;

	IndexIteratorPtr createIndexIter(DbContext*) const override;
	llong numIndexRows() const override;
	llong indexStorageSize() const override;
	size_t remove(fstring key, llong id, DbContext*) override;
	size_t insert(fstring key, llong id, DbContext*) override;
	size_t replace(fstring key, llong oldId, llong newId, DbContext*) override;
	void flush() override;
};

class NARK_DB_DLL MockReadonlySegment : public ReadonlySegment {
public:
	MockReadonlySegment();
	~MockReadonlySegment();
protected:
	ReadableStorePtr openPart(fstring path) const override;
	ReadableIndexStorePtr openIndex(fstring path, SchemaPtr) const override;

	ReadableIndexStorePtr buildIndex(SchemaPtr indexSchema,
									 SortableStrVec& indexData) const override;
	ReadableStorePtr buildStore(SortableStrVec& storeData) const override;
};

class NARK_DB_DLL MockWritableSegment : public PlainWritableSegment {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	MockWritableSegment(fstring dir);
	~MockWritableSegment();

	WritableIndexPtr createIndex(fstring path, SchemaPtr) const override;

protected:
	void save(fstring) const override;
	void load(fstring) override;
	WritableIndexPtr openIndex(fstring, SchemaPtr) const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIteratorPtr createStoreIter(DbContext*) const override;
	llong totalStorageSize() const override;
	llong append(fstring row, DbContext*) override;
	void replace(llong id, fstring row, DbContext*) override;
	void remove(llong id, DbContext*) override;
	void flush() override;
};

class NARK_DB_DLL MockDbContext : public DbContext {
public:
	explicit MockDbContext(const CompositeTable* tab);
	~MockDbContext();
};
class NARK_DB_DLL MockCompositeTable : public CompositeTable {
public:
	DbContextPtr createDbContext() const override;
	ReadonlySegmentPtr createReadonlySegment(fstring dir) const override;
	WritableSegmentPtr createWritableSegment(fstring dir) const override;
	WritableSegmentPtr openWritableSegment(fstring dir) const override;
};

} } // namespace nark::db

#endif // __nark_db_mock_data_index_hpp__
