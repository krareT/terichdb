#include "nlt_index.hpp"
#include "dfadb_table.hpp"
#include <nark/io/FileStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/util/mmap.hpp>

namespace nark { namespace db { namespace dfadb {

NestLoudsTrieIndex::NestLoudsTrieIndex() {
	m_idmapBase = nullptr;
	m_idmapSize = 0;
	m_dataInflateSize = 0;
}
NestLoudsTrieIndex::~NestLoudsTrieIndex() {
	if (m_idmapBase) {
		m_idToKey.risk_release_ownership();
		m_keyToId.risk_release_ownership();
		m_recBits.risk_release_ownership();
		mmap_close(m_idmapBase, m_idmapSize);
	}
}

ReadableIndex* NestLoudsTrieIndex::getReadableIndex() {
	return this;
}

ReadableStore* NestLoudsTrieIndex::getReadableStore() {
	return this;
}

///@{ ordered and unordered index
llong NestLoudsTrieIndex::indexStorageSize() const {
	return m_dfa->mem_size() + m_keyToId.mem_size();
}

size_t NestLoudsTrieIndex::searchExact(fstring key, valvec<llong>* recIdvec, DbContext*) const {
	auto dawg = m_dfa->get_dawg();
	assert(dawg);
	size_t dawgNum = dawg->num_words();
	size_t dawgIdx = dawg->index(key);
	assert(dawgIdx < dawgNum || size_t(-1) == dawgIdx);
	recIdvec->erase_all();
	if (m_isUnique) {
		assert(m_recBits.size() == 0);
		if (dawgIdx < dawgNum) {
			recIdvec->push_back(m_keyToId.get(dawgIdx));
			return 1;
		}
	}
	else {
		assert(m_recBits.size() >= dawg->num_words()+2);
		if (dawgIdx < dawgNum) {
			size_t bitpos = m_recBits.select1(dawgIdx);
			assert(bitpos < m_recBits.size());
			size_t dupcnt = m_recBits.zero_seq_len(bitpos+1) + 1;
			for (size_t i = 0; i < dupcnt; ++i) {
				recIdvec->push_back(m_keyToId.get(bitpos+i));
			}
			return dupcnt;
		}
	}
	return 0;
}
///@}

llong NestLoudsTrieIndex::dataStorageSize() const {
	return m_idToKey.mem_size();
}

llong NestLoudsTrieIndex::dataInflateSize() const {
	return m_dataInflateSize;
}

llong NestLoudsTrieIndex::numDataRows() const {
	auto dawg = m_dfa->get_dawg();
	assert(dawg);
	if (m_isUnique) {
		assert(m_recBits.size() == 0);
		return dawg->num_words();
	}
	else {
		assert(m_recBits.size() >= dawg->num_words()+2);
		return m_recBits.size() - 1;
	}
}

void
NestLoudsTrieIndex::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
//	assert(dynamic_cast<DfaDbContext*>(ctx) != nullptr);
//	DfaDbContext* ctx1 = static_cast<DfaDbContext*>(ctx);
	auto dawg = m_dfa->get_dawg();
	assert(dawg);
	std::string buf;
//	std::string& buf = ctx1->m_nltRecBuf;
	size_t dawgIdx = m_idToKey.get(id);
	assert(dawgIdx < dawg->num_words());
	dawg->nth_word(dawgIdx, &buf);
	val->append(buf);
}

StoreIterator* NestLoudsTrieIndex::createStoreIterForward(DbContext*) const {
	return nullptr; // not needed
}

StoreIterator* NestLoudsTrieIndex::createStoreIterBackward(DbContext*) const {
	return nullptr; // not needed
}

void NestLoudsTrieIndex::build(const Schema& schema, SortableStrVec& strVec) {
	m_dataInflateSize = strVec.str_size();
	NARK_IF_DEBUG(SortableStrVec backup = strVec, ;);
	const size_t rows = strVec.size();
	NestLoudsTrieConfig conf;
	conf.initFromEnv();
	conf.nestLevel = schema.m_nltNestLevel;
	valvec<uint32_t> idToKey;
	m_dfa.reset(new NestLoudsTrieDAWG_SE_512());
	m_dfa->build_with_id(strVec, idToKey, conf);
	const size_t keys = m_dfa->num_words();
	valvec<uint32_t> keyToId(rows, valvec_no_init());
#if !defined(NDEBUG)
	assert(keys <= rows);
	assert(rows == idToKey.size());
	keyToId.fill(UINT32_MAX);
	for(size_t i = 0; i < rows; ++i) { assert(idToKey[i] < keys); }
#endif
	if (keys == rows) {
		m_isUnique = true;
		for (size_t i = 0; i < keys; ++i)
			keyToId[idToKey[i]] = uint32_t(i);
	#if !defined(NDEBUG)
		for(size_t i = 0; i < rows; ++i) { assert(keyToId[i] < rows); }
	#endif
	}
	else {
		m_isUnique = false;
		assert(keys < rows);
		valvec<uint32_t> psum(keys+1, 0);
		for (size_t i = 0; i < rows; ++i) psum[1+idToKey[i]]++;
		for (size_t i = 0; i < keys; ++i) psum[1+i] += psum[i];
		assert(psum[keys] == rows);
		m_recBits.resize_fill(rows+1, false);
		for (size_t i = 0; i < psum.size(); ++i) {
			m_recBits.set1(psum[i]); // also set the last 1
		}
		assert(m_recBits.is1(rows)); // last guard bit is 1
		for(size_t i = 0; i < rows; ++i) {
			size_t keyIdx = idToKey[i];
			keyToId[psum[keyIdx]++] = i;
		}
		m_recBits.build_cache(false, true);
	#if !defined(NDEBUG)
		for(size_t i = 0; i < rows; ++i) { assert(keyToId[i] < rows); }
		for(size_t i = 0; i < rows; ++i) {
			size_t keyIdx = idToKey[i];
			size_t dupBeg = m_recBits.select1(keyIdx);
			size_t dupCnt = m_recBits.zero_seq_len(dupBeg + 1) + 1;
			for(size_t j = 0; j < dupCnt; ++j) {
				size_t recIdx = keyToId[dupBeg + j];
				assert(idToKey[recIdx] == keyIdx);
			}
		}
	#endif
	}
	m_idToKey.build_from(idToKey);
	m_keyToId.build_from(keyToId);
#if !defined(NDEBUG)
	for(size_t i = 0; i < idToKey.size(); ++i) {
		assert(m_idToKey[i] == idToKey[i]);
		assert(m_keyToId[i] == keyToId[i]);
	}
	valvec<byte> rec;
	valvec<llong> recIdvec;
//	DfaDbContext ctx;
	for(intptr_t id = 0; id < intptr_t(backup.size()); ++id) {
	//	size_t keyIdx = m_dfa->index(backup[id]);
		fstring key = backup[id];
		this->getValue(id, &rec, NULL);
		assert(rec == key);
		searchExact(backup[id], &recIdvec, NULL);
		size_t nth = std::find(recIdvec.begin(), recIdvec.end(), id)
				   - recIdvec.begin();
		assert(nth < recIdvec.size());
	}
#endif
}

struct NestLoudsTrieIndex::FileHeader {
	uint32_t rows;
	uint32_t keys;
	uint32_t idToKeyBytesDiv16; // real size may overflow uint32
	uint32_t recBitsMemSize;
	uint64_t dataInflateSize;
	uint64_t pad1;
	uint64_t pad2[4];
};

void NestLoudsTrieIndex::load(PathRef path) {
	BOOST_STATIC_ASSERT(sizeof(FileHeader) == 64);
	auto pathNLT = path + ".nlt";
	std::unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(pathNLT.string().c_str()));
	m_dfa.reset(dynamic_cast<NestLoudsTrieDAWG_SE_512*>(dfa.get()));
	if (m_dfa) {
		dfa.release();
	}

	auto pathIdMap = path + ".idmap";
	m_idmapBase = (FileHeader*)mmap_load(pathIdMap.string(), &m_idmapSize);

	size_t rows  = m_idmapBase->rows;
	size_t keys  = m_idmapBase->keys;
	size_t bytes = m_idmapBase->idToKeyBytesDiv16;
	size_t rslen = m_idmapBase->recBitsMemSize;
	size_t rbits = nark_bsr_u64(rows-1) + 1;
	size_t kbits = nark_bsr_u64(keys-1) + 1;
	bytes *= 16; // large rows will overflow m_idToKeys.mem_size() as uint32
	if (m_dfa->num_words() != keys) {
		THROW_STD(invalid_argument,
			"path=%s, broken data: keys[dfa=%zd map=%zd]",
			path.string().c_str(), m_dfa->num_words(), keys);
	}
	m_idToKey.risk_set_data((byte*)(m_idmapBase+1)        , rows, kbits);
	m_keyToId.risk_set_data((byte*)(m_idmapBase+1) + bytes, rows, rbits);
	assert(m_idToKey.mem_size() == bytes);
//	assert(m_keyToId.mem_size() == bytes);
	m_isUnique = keys == rows;
	if (!m_isUnique) {
		m_recBits.risk_mmap_from((byte*)(m_idmapBase+1)+bytes+m_keyToId.mem_size(), rslen);
		m_recBits.risk_set_size(rows + 1);
	}
	m_dataInflateSize = m_idmapBase->dataInflateSize;
}

void NestLoudsTrieIndex::save(PathRef path) const {
#ifndef NDEBUG
	if (m_isUnique) {
		assert(m_dfa->num_words() == m_idToKey.size());
		assert(m_idToKey.mem_size() == m_keyToId.mem_size());
	}
	else {
		assert(m_dfa->num_words() < m_idToKey.size());
		assert(m_idToKey.uintbits() <= m_keyToId.uintbits());
	}
	assert(m_idToKey.size() == m_keyToId.size());
	assert(m_idToKey.mem_size() % 16 == 0);
#endif
	if (m_idToKey.mem_size() % 16 != 0) {
		THROW_STD(logic_error,
			"(m_idToKey.mem_size()=%zd) %% 16 = %zd, must be 0",
			  m_idToKey.mem_size(), m_idToKey.mem_size() % 16);
	}

	auto pathNLT = path + ".nlt";
	m_dfa->save_mmap(pathNLT.string().c_str());

	auto pathIdMap = path + ".idmap";
	FileStream dio(pathIdMap.string().c_str(), "wb");
	FileHeader header;
	memset(&header, 0, sizeof(FileHeader));
	header.rows = uint32_t(numDataRows());
	header.keys = uint32_t(m_dfa->num_words());
	header.idToKeyBytesDiv16 = uint32_t(m_idToKey.mem_size()/16);
	header.recBitsMemSize = uint32_t(m_recBits.mem_size());
	header.dataInflateSize = m_dataInflateSize;
	dio.ensureWrite(&header, sizeof(FileHeader));
	dio.ensureWrite(m_idToKey.data(), m_idToKey.mem_size());
	dio.ensureWrite(m_keyToId.data(), m_keyToId.mem_size());
	if (!m_isUnique) {
		assert(m_recBits.size() >= m_dfa->num_words()+2);
		dio.ensureWrite(m_recBits.data(), m_recBits.mem_size());
	}
}

class NestLoudsTrieIndex::UniqueIndexIterForward : public IndexIterator {
public:
	std::unique_ptr<ADFA_LexIterator> m_iter;
	const NestLoudsTrieIndex* m_owner;
	bool m_hasNext;

	UniqueIndexIterForward(const NestLoudsTrieIndex* owner) {
		m_iter.reset(owner->m_dfa->adfa_make_iter());
		m_hasNext = m_iter->seek_begin();
		m_owner = owner;
	}

	void reset() override {
		m_hasNext = m_iter->seek_begin();
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_hasNext) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			*id = m_owner->m_keyToId.get(dawgIdx);
			if (key)
				key->assign(m_iter->word());
			m_hasNext = m_iter->incr();
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		if (m_iter->seek_lower_bound(key)) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			*id = m_owner->m_keyToId.get(dawgIdx);
			if (retKey)
				retKey->assign(m_iter->word());
			int ret = (m_iter->word() == key) ? 0 : 1;
			m_hasNext = m_iter->incr();
			return ret;
		}
		m_hasNext = false;
		return -1;
	}
};

class NestLoudsTrieIndex::UniqueIndexIterBackward : public IndexIterator {
public:
	std::unique_ptr<ADFA_LexIterator> m_iter;
	const NestLoudsTrieIndex* m_owner;
	bool m_hasNext;

	UniqueIndexIterBackward(const NestLoudsTrieIndex* owner) {
		m_iter.reset(owner->m_dfa->adfa_make_iter());
		m_hasNext = m_iter->seek_end();
		m_owner = owner;
	}

	void reset() override {
		m_hasNext = m_iter->seek_end();
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_hasNext) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			*id = m_owner->m_keyToId.get(dawgIdx);
			if (key)
				key->assign(m_iter->word());
			m_hasNext = m_iter->decr();
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		if (m_iter->seek_lower_bound(key) && m_iter->word() == key) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			*id = m_owner->m_keyToId.get(dawgIdx);
			if (retKey)
				retKey->assign(key);
			m_hasNext = m_iter->decr();
			return 0;
		}
		m_hasNext = m_iter->decr();
		return increment(id, retKey) ? 1 : -1;
	}
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

class NestLoudsTrieIndex::DupableIndexIterForward : public IndexIterator {
	std::unique_ptr<ADFA_LexIterator> m_iter;
	const NestLoudsTrieIndex* m_owner;
	size_t m_bitPosCur;
	size_t m_bitPosUpp;
	bool m_hasNext;

	void syncBitPos(bool hasNext) {
		if (hasNext) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			m_bitPosCur = m_owner->m_recBits.select1(dawgIdx);
			m_bitPosUpp = m_owner->m_recBits.zero_seq_len(m_bitPosCur + 1)
						+ m_bitPosCur + 1;
		}
		m_hasNext = hasNext;
	}

public:
	DupableIndexIterForward(const NestLoudsTrieIndex* owner) {
		m_iter.reset(owner->m_dfa->adfa_make_iter());
		m_owner = owner;
		this->reset();
	}

	void reset() override {
		m_bitPosCur = size_t(-1);
		m_bitPosUpp = size_t(-1);
		syncBitPos(m_iter->seek_begin());
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_hasNext) {
			assert(m_bitPosCur < m_bitPosUpp);
			*id = m_owner->m_keyToId.get(m_bitPosCur++);
			if (key)
				key->assign(m_iter->word());
			if (m_bitPosCur == m_bitPosUpp)
				syncBitPos(m_iter->incr());
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		if (m_iter->seek_lower_bound(key)) {
			syncBitPos(true);
			*id = m_owner->m_keyToId.get(m_bitPosCur++);
			if (retKey) {
				retKey->assign(m_iter->word());
			}
			int ret = (m_iter->word() == key) ? 0 : 1;
			if (m_bitPosCur == m_bitPosUpp)
				syncBitPos(m_iter->incr());
			return ret;
		}
		m_bitPosCur = size_t(-1);
		m_bitPosUpp = size_t(-1);
		m_hasNext = false;
		return -1;
	}
};

class NestLoudsTrieIndex::DupableIndexIterBackward : public IndexIterator {
	std::unique_ptr<ADFA_LexIterator> m_iter;
	const NestLoudsTrieIndex* m_owner;
	size_t m_bitPosCur;
	size_t m_bitPosLow;
	bool m_hasNext;

	void syncBitPos(bool hasNext) {
		if (hasNext) {
			size_t state = m_iter->word_state();
			size_t dawgIdx = m_owner->m_dfa->state_to_word_id(state);
			m_bitPosLow = m_owner->m_recBits.select1(dawgIdx);
			m_bitPosCur = m_owner->m_recBits.zero_seq_len(m_bitPosLow + 1)
						+ m_bitPosLow + 1;
		}
		m_hasNext = hasNext;
	}

public:
	DupableIndexIterBackward(const NestLoudsTrieIndex* owner) {
		m_iter.reset(owner->m_dfa->adfa_make_iter());
		m_owner = owner;
		this->reset();
	}

	void reset() override {
		m_bitPosCur = size_t(-1);
		m_bitPosLow = size_t(-1);
		syncBitPos(m_iter->seek_end());
	}

	bool increment(llong* id, valvec<byte>* key) override {
		if (m_hasNext) {
			assert(m_bitPosCur > m_bitPosLow);
			*id = m_owner->m_keyToId.get(--m_bitPosCur);
			if (key)
				key->assign(m_iter->word());
			if (m_bitPosCur == m_bitPosLow)
				syncBitPos(m_iter->decr());
			return true;
		}
		return false;
	}

	int seekLowerBound(fstring key, llong* id, valvec<byte>* retKey) override {
		bool hasForwardLowerBound = m_iter->seek_lower_bound(key);
		if (hasForwardLowerBound && m_iter->word() == key) {
			syncBitPos(true);
			*id = m_owner->m_keyToId.get(--m_bitPosCur);
			if (retKey)
				retKey->assign(key);
			if (m_bitPosCur == m_bitPosLow)
				syncBitPos(m_iter->decr());
			return 0;
		}
		syncBitPos(m_iter->decr());
		return this->increment(id, retKey) ? 1 : -1;
	}
};

IndexIterator* NestLoudsTrieIndex::createIndexIterForward(DbContext*) const {
	if (this->m_isUnique)
		return new UniqueIndexIterForward(this);
	else
		return new DupableIndexIterForward(this);
}
IndexIterator* NestLoudsTrieIndex::createIndexIterBackward(DbContext*) const {
	if (this->m_isUnique)
		return new UniqueIndexIterBackward(this);
	else
		return new DupableIndexIterBackward(this);
}

}}} // namespace nark::db::dfadb
