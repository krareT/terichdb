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

template<class Class>
static
Class* doBuild(const NestLoudsTrieConfig& conf,
			   const Schema& schema, SortableStrVec& strVec) {
	std::unique_ptr<Class> trie(new Class());
	trie->build_from(strVec, conf);
	return trie.release();
}

void NestLoudsTrieStore::build(const Schema& schema, SortableStrVec& strVec) {
	NestLoudsTrieConfig conf;
	conf.initFromEnv();
	if (schema.m_sufarrMinFreq) {
		conf.saFragMinFreq = (byte_t)schema.m_sufarrMinFreq;
	}
	if (schema.m_minFragLen) {
		conf.minFragLen = schema.m_minFragLen;
	}
	if (schema.m_maxFragLen) {
		conf.maxFragLen = schema.m_maxFragLen;
	}
	switch (schema.m_rankSelectClass) {
	case -256:
		m_store.reset(doBuild<NestLoudsTrieDataStore_IL>(conf, schema, strVec));
		break;
	case +256:
		m_store.reset(doBuild<NestLoudsTrieDataStore_SE>(conf, schema, strVec));
		break;
	case +512:
		m_store.reset(doBuild<NestLoudsTrieDataStore_SE_512>(conf, schema, strVec));
		break;
	default:
		fprintf(stderr, "WARN: invalid schema(%s).rs = %d, use default: se_512\n"
					  , schema.m_name.c_str(), schema.m_rankSelectClass);
		m_store.reset(doBuild<NestLoudsTrieDataStore_SE_512>(conf, schema, strVec));
		break;
	}
}

void NestLoudsTrieStore::load(PathRef path) {
	std::string fpath = fstring(path.string()).endsWith(".nlt")
					  ? path.string()
					  : path.string() + ".nlt";
	std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath.c_str()));
	m_store.reset(dynamic_cast<DataStore*>(dfa.get()));
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
