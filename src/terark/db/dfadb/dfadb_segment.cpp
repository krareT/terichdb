#include "dfadb_segment.hpp"
#include "nlt_index.hpp"
#include "nlt_store.hpp"
#include <terark/db/fixed_len_store.hpp>
#include <terark/fast_zip_blob_store.hpp>
#include <mutex>
#include <float.h>

namespace terark { namespace db { namespace dfadb {

DfaDbReadonlySegment::DfaDbReadonlySegment() {
}
DfaDbReadonlySegment::~DfaDbReadonlySegment() {
}

ReadableIndex*
DfaDbReadonlySegment::openIndex(const Schema& schema, PathRef path) const {
	if (boost::filesystem::exists(path + ".nlt")) {
		std::unique_ptr<NestLoudsTrieIndex> store(new NestLoudsTrieIndex(schema));
		store->load(path);
		return store.release();
	}
	return ReadonlySegment::openIndex(schema, path);
}

static void patchStrVec(SortableStrVec& strVec, size_t fixlen) {
	const size_t rows = strVec.str_size() / fixlen;
	assert(strVec.str_size() % rows == 0);
	strVec.m_index.resize_no_init(rows);
	for (size_t i = 0; i < rows; ++i) {
		strVec.m_index[i].seq_id = i;
		strVec.m_index[i].length = fixlen;
		strVec.m_index[i].offset = fixlen * i;
	}
}

ReadableIndex*
DfaDbReadonlySegment::buildIndex(const Schema& schema, SortableStrVec& indexData)
const {
	ReadableIndex* index0 = ReadonlySegment::buildIndex(schema, indexData);
	if (index0) {
		return index0;
	}
	const size_t fixlen = schema.getFixedRowLen();
	if (fixlen) {
		patchStrVec(indexData, fixlen);
	}
	std::unique_ptr<NestLoudsTrieIndex> index(new NestLoudsTrieIndex(schema));
	index->build(schema, indexData);
	return index.release();
}

ReadableStore*
DfaDbReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	ReadableStore* store = ReadonlySegment::buildStore(schema, storeData);
	if (store) {
		return store;
	}
	std::unique_ptr<NestLoudsTrieStore> nlt(new NestLoudsTrieStore(schema));
	if (storeData.m_index.size() == 0) {
		const size_t fixlen = schema.getFixedRowLen();
		assert(fixlen > 0);
		patchStrVec(storeData, fixlen);
	}
	nlt->build(schema, storeData);
	return nlt.release();
}

ReadableStore*
DfaDbReadonlySegment::buildDictZipStore(const Schema& schema,
										PathRef dir,
										StoreIterator& inputIter,
										const bm_uint_t* isDel,
										const febitvec* isPurged)
const {
	std::unique_ptr<NestLoudsTrieStore> nlt(new NestLoudsTrieStore(schema));
	auto fpath = dir / ("colgroup-" + schema.m_name + ".nlt");
	nlt->build_by_iter(schema, fpath, inputIter, isDel, isPurged);
	return nlt.release();
}

std::mutex& DictZip_reduceMemMutex(); // defined in nlt_store.cpp

void
DfaDbReadonlySegment::compressSingleColgroup(ReadableSegment* input, DbContext* ctx) {
	llong  prevId = -1, id = -1;
	llong  logicRowNum = input->m_isDel.size(), newRowNum = 0;
	assert(logicRowNum > 0);
	auto tmpDir = m_segDir + ".tmp";
	valvec<byte> val;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx));
	SortableStrVec valueVec;
	const Schema& valueSchema = m_schema->getColgroupSchema(0);
	std::unique_ptr<DictZipBlobStore> zds;
	std::unique_ptr<DictZipBlobStore::ZipBuilder> builder;
	FixedLenStorePtr store;
	if (valueSchema.should_use_FixedLenStore()) {
		store = new FixedLenStore(tmpDir, valueSchema);
	}
	else if (valueSchema.m_dictZipSampleRatio >= 0.0) {
		double sRatio = valueSchema.m_dictZipSampleRatio;
		double avgLen = double(input->dataInflateSize()) / logicRowNum;
		if ((sRatio > FLT_EPSILON) || (sRatio >= 0 && avgLen > 100)) {
			zds.reset(new DictZipBlobStore());
			builder.reset(zds->createZipBuilder());
		}
	}
	size_t sampleLenSum = 0;
	while (iter->increment(&id, &val) && id < logicRowNum) {
		assert(id >= 0);
		assert(id < logicRowNum);
		assert(prevId < id);
		if (!m_isDel[id]) {
			if (builder) {
				builder->addSample(val);
				sampleLenSum += val.size();
			}
			else {
				if (store)
					store->append(val, NULL);
				else
					valueVec.push_back(val);
			}
			newRowNum++;
			m_isDel.beg_end_set1(prevId+1, id);
			prevId = id;
		}
	}
	if (prevId != id) {
		assert(prevId < id);
		assert(m_isDel[id]);
		m_isDel.beg_end_set1(prevId+1, id);
	}
	llong  inputRowNum = id + 1;
	assert(inputRowNum <= logicRowNum);
	if (inputRowNum < logicRowNum) {
		fprintf(stderr
			, "WARN: DfaDbReadonlySegment::compressSingleKeyValue(): realrows=%lld, m_isDel=%lld, some data have lost\n"
			, inputRowNum, logicRowNum);
		input->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
		this->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
	}
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	assert(newRowNum <= inputRowNum);
	assert(size_t(logicRowNum - newRowNum) == m_delcnt);
	if (builder) {
		assert(valueVec.m_index.size() == 0);
		assert(valueVec.m_strpool.size() == 0);
		iter->reset(); // free resources and seek to begin
		std::lock_guard<std::mutex> lock(DictZip_reduceMemMutex());
		auto fpath = tmpDir / ("colgroup-" + valueSchema.m_name + ".nlt");
		if (0 == sampleLenSum) {
			builder->addSample("Hello World");
		}
		builder->prepare(newRowNum, fpath.string());
		while (iter->increment(&id, &val) && id < inputRowNum) {
			if (!m_isDel[id])
				builder->addRecord(val);
		}
		iter = nullptr;
		zds->completeBuild(*builder);
		m_colgroups[0] = new NestLoudsTrieStore(valueSchema, zds.release());
	}
	else if (store) {
		m_colgroups[0] = std::move(store);
	}
	else {
		iter = nullptr;
		m_colgroups[0] = this->buildStore(valueSchema, valueVec);
	}
}

void
DfaDbReadonlySegment::compressSingleKeyValue(ReadableSegment* input, DbContext* ctx) {
	llong  prevId = -1, id = -1;
	llong  logicRowNum = input->m_isDel.size(), newRowNum = 0;
	assert(logicRowNum > 0);
	auto tmpDir = m_segDir + ".tmp";
	ColumnVec columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> buf;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx));
	SortableStrVec keyVec, valueVec;
	const Schema& rowSchema = m_schema->getRowSchema();
	const Schema& keySchema = m_schema->getIndexSchema(0);
	const Schema& valueSchema = m_schema->getColgroupSchema(1);
	std::unique_ptr<DictZipBlobStore> zds;
	std::unique_ptr<DictZipBlobStore::ZipBuilder> builder;
	FixedLenStorePtr store;
	if (valueSchema.should_use_FixedLenStore()) {
		store = new FixedLenStore(tmpDir, valueSchema);
	}
	else if (valueSchema.m_dictZipSampleRatio >= 0.0) {
		double sRatio = valueSchema.m_dictZipSampleRatio;
		double avgLen = double(input->dataInflateSize()) / logicRowNum;
		if ((sRatio > FLT_EPSILON) || (sRatio >= 0 && avgLen > 120)) {
			zds.reset(new DictZipBlobStore());
			builder.reset(zds->createZipBuilder());
		}
	}
	size_t sampleLenSum = 0;
	valvec<byte_t> key, val;
	while (iter->increment(&id, &buf) && id < logicRowNum) {
		assert(id >= 0);
		assert(id < logicRowNum);
		assert(prevId < id);
		if (!m_isDel[id]) {
			rowSchema.parseRow(buf, &columns);
			keySchema.selectParent(columns, &key);
			valueSchema.selectParent(columns, &val);
			if (keySchema.getFixedRowLen() > 0) {
				keyVec.m_strpool.append(key);
			} else {
				keyVec.push_back(key);
			}
			if (builder) {
				builder->addSample(val);
				sampleLenSum += val.size();
			}
			else {
				if (store)
					store->append(val, NULL);
				else
					valueVec.push_back(val);
			}
			newRowNum++;
			m_isDel.beg_end_set1(prevId+1, id);
			prevId = id;
		}
	}
	if (prevId != id) {
		assert(prevId < id);
		assert(m_isDel[id]);
		m_isDel.beg_end_set1(prevId+1, id);
	}
	llong  inputRowNum = id + 1;
	assert(inputRowNum <= logicRowNum);
	if (inputRowNum < logicRowNum) {
		fprintf(stderr
			, "WARN: DfaDbReadonlySegment::compressSingleKeyValue(): realrows=%lld, m_isDel=%lld, some data have lost\n"
			, inputRowNum, logicRowNum);
		input->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
		this->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
	}
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	assert(newRowNum <= inputRowNum);
	assert(size_t(logicRowNum - newRowNum) == m_delcnt);
	if (builder) {
		iter->reset(); // free resources and seek to begin
	} else {
		iter = nullptr;
	}
	m_indices[0] = buildIndex(keySchema, keyVec); // memory heavy
	m_colgroups[0] = m_indices[0]->getReadableStore();
	keyVec.clear();
	if (builder) {
		assert(valueVec.m_index.size() == 0);
		assert(valueVec.m_strpool.size() == 0);
		std::lock_guard<std::mutex> lock(DictZip_reduceMemMutex());
		auto fpath = tmpDir / ("colgroup-" + valueSchema.m_name + ".nlt");
		if (0 == sampleLenSum) {
			builder->addSample("Hello World");
		}
		builder->prepare(newRowNum, fpath.string());
		while (iter->increment(&id, &buf) && id < inputRowNum) {
			if (!m_isDel[id]) {
				rowSchema.parseRow(buf, &columns);
				valueSchema.selectParent(columns, &val);
				builder->addRecord(val);
			}
		}
		iter = nullptr;
		zds->completeBuild(*builder);
		m_colgroups[1] = new NestLoudsTrieStore(valueSchema, zds.release());
	}
	else if (store) {
		m_colgroups[1] = std::move(store);
	}
	else {
		m_colgroups[1] = this->buildStore(valueSchema, valueVec);
	}
}

}}} // namespace terark::db::dfadb
