#include "nlt_store.hpp"
#include <typeinfo>

namespace nark { namespace db { namespace dfadb {

NARK_DB_REGISTER_STORE("nlt", NestLoudsTrieStore);

NestLoudsTrieStore::NestLoudsTrieStore() {
}
NestLoudsTrieStore::~NestLoudsTrieStore() {
}

llong NestLoudsTrieStore::dataStorageSize() const {
	return m_store->mem_size();
}

llong NestLoudsTrieStore::numDataRows() const {
	return m_store->num_records();
}

void NestLoudsTrieStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx) const {
	m_store->get_record_append(size_t(id), val);
}

StoreIterator* NestLoudsTrieStore::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* NestLoudsTrieStore::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

void NestLoudsTrieStore::build(const Schema& schema, SortableStrVec& strVec) {
	NestLoudsTrieConfig conf;
//	conf.maxFragLen = 512;
	conf.initFromEnv();
	if (schema.m_sufarrCompressMinFreq) {
		conf.saFragMinFreq = (byte_t)schema.m_sufarrCompressMinFreq;
	}
	m_store.reset(new NestLoudsTrieDataStore_SE_512());
	m_store->build_from(strVec, conf);
}

void NestLoudsTrieStore::load(PathRef path) {
	std::string fpath = fstring(path.string()).endsWith(".nlt")
					  ? path.string()
					  : path.string() + ".nlt";
	std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath.c_str()));
	m_store.reset(dynamic_cast<NestLoudsTrieDataStore_SE_512*>(dfa.get()));
	if (m_store) {
		dfa.release();
	}
	else {
		THROW_STD(invalid_argument
			, "FATAL: file: %s is %s, not a NestLoudsTrieDataStore_SE_512"
			, typeid(*dfa).name(), fpath.c_str());
	}
}

void NestLoudsTrieStore::save(PathRef path) const {
	std::string fpath = fstring(path.string()).endsWith(".nlt")
					  ? path.string()
					  : path.string() + ".nlt";
	m_store->save_mmap(fpath.c_str());
}

}}} // namespace nark::db::dfadb
