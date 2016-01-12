#ifndef __nark_db_mock_data_index_hpp__
#define __nark_db_mock_data_index_hpp__

#include <nark/db/db_table.hpp>
#include <nark/db/db_segment.hpp>
#include <nark/util/fstrvec.hpp>
#include <set>

namespace nark { namespace db {

class NARK_DB_DLL MockReadonlyStore : public ReadableStore {
	size_t    m_fixedLen;
	SchemaPtr m_schema;
public:
	fstrvec m_rows;

	void build(const Schema&, SortableStrVec& storeData);

	void save(PathRef) const override;
	void load(PathRef) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;
};
typedef boost::intrusive_ptr<MockReadonlyStore> MockReadonlyStorePtr;

class NARK_DB_DLL MockReadonlyIndex : public ReadableIndex, public ReadableStore {
	friend class MockReadonlyIndexIterator;
	friend class MockReadonlyIndexIterBackward;
	fstrvec          m_keys; // keys[recId] is the key
	valvec<uint32_t> m_ids;  // keys[ids[i]] <= keys[ids[i+1]]
	size_t m_fixedLen;
	const Schema* m_schema;
	void getIndexKey(llong* id, valvec<byte>* key, size_t pos) const;
	int forwardLowerBound(fstring key, size_t* pLower) const;
public:
	MockReadonlyIndex(const Schema& schema);
	~MockReadonlyIndex();

	void build(SortableStrVec& indexData);

	void save(PathRef) const override;
	void load(PathRef) override;

	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;
	llong numDataRows() const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* key, DbContext*) const override;

	llong searchExact(fstring key, DbContext*) const override;

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;
	llong indexStorageSize() const override;

	ReadableIndex* getReadableIndex() override;
	ReadableStore* getReadableStore() override;
};
typedef boost::intrusive_ptr<MockReadonlyIndex> MockReadonlyIndexPtr;

class NARK_DB_DLL MockWritableStore : public ReadableStore, public WritableStore {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	void save(PathRef) const override;
	void load(PathRef) override;

	llong dataStorageSize() const override;
	llong numDataRows() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;

	llong append(fstring row, DbContext*) override;
	void  replace(llong id, fstring row, DbContext*) override;
	void  remove(llong id, DbContext*) override;

	void clear() override;
};
typedef boost::intrusive_ptr<MockWritableStore> MockWritableStorePtr;

template<class Key>
class NARK_DB_DLL MockWritableIndex : public ReadableIndex, public WritableIndex {
	class MyIndexIterForward;  friend class MyIndexIterForward;
	class MyIndexIterBackward; friend class MyIndexIterBackward;
	typedef std::pair<Key, llong> kv_t;
	std::set<kv_t> m_kv;
	size_t m_keysLen;
public:
	explicit MockWritableIndex(bool isUnique);
	void save(PathRef) const override;
	void load(PathRef) override;

	IndexIterator* createIndexIterForward(DbContext*) const override;
	IndexIterator* createIndexIterBackward(DbContext*) const override;
	llong indexStorageSize() const override;
	bool remove(fstring key, llong id, DbContext*) override;
	bool insert(fstring key, llong id, DbContext*) override;
	bool replace(fstring key, llong oldId, llong newId, DbContext*) override;
	void clear() override;

	llong searchExact(fstring key, DbContext*) const override;
	WritableIndex* getWritableIndex() override { return this; }
};

class NARK_DB_DLL MockReadonlySegment : public ReadonlySegment {
public:
	MockReadonlySegment();
	~MockReadonlySegment();
protected:
	ReadableIndex* openIndex(const Schema&, PathRef path) const override;

	ReadableIndex* buildIndex(const Schema&, SortableStrVec& indexData) const override;
	ReadableStore* buildStore(const Schema&, SortableStrVec& storeData) const override;
};

class NARK_DB_DLL MockWritableSegment : public PlainWritableSegment {
public:
	valvec<valvec<byte> > m_rows;
	llong m_dataSize;

	MockWritableSegment(PathRef dir);
	~MockWritableSegment();

	ReadableIndex* createIndex(const Schema&, PathRef path) const override;

protected:
	ReadableIndex* openIndex(const Schema&, PathRef) const override;
	llong dataStorageSize() const override;
	void getValueAppend(llong id, valvec<byte>* val, DbContext*) const override;
	StoreIterator* createStoreIterForward(DbContext*) const override;
	StoreIterator* createStoreIterBackward(DbContext*) const override;
	llong totalStorageSize() const override;
	llong append(fstring row, DbContext*) override;
	void replace(llong id, fstring row, DbContext*) override;
	void remove(llong id, DbContext*) override;
	void clear() override;
	void loadRecordStore(PathRef segDir) override;
	void saveRecordStore(PathRef segDir) const override;
};

class NARK_DB_DLL MockDbContext : public DbContext {
public:
	explicit MockDbContext(const CompositeTable* tab);
	~MockDbContext();
};
class NARK_DB_DLL MockCompositeTable : public CompositeTable {
public:
	DbContext* createDbContext() const override;
	ReadonlySegment* createReadonlySegment(PathRef dir) const override;
	WritableSegment* createWritableSegment(PathRef dir) const override;
	WritableSegment* openWritableSegment(PathRef dir) const override;
};

} } // namespace nark::db

#endif // __nark_db_mock_data_index_hpp__
