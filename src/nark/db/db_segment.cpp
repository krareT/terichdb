#include "db_table.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>
#include <nark/util/mmap.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db {

SegmentSchema::SegmentSchema() {
}
SegmentSchema::~SegmentSchema() {
}
void SegmentSchema::copySchema(const SegmentSchema& y) {
	*this = y;
}

void SegmentSchema::compileSchema() {
	m_nonIndexRowSchema.reset(new Schema());
	m_indexSchemaSet->compileSchemaSet(m_rowSchema.get());
	febitvec hasIndex(m_rowSchema->columnNum(), false);
	for (size_t i = 0; i < m_indexSchemaSet->m_nested.end_i(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		const size_t colnum = schema.columnNum();
		for (size_t j = 0; j < colnum; ++j)
			hasIndex.set1(schema.parentColumnId(j));
	}
	for (size_t i = 0; i < hasIndex.size(); ++i) {
		if (!hasIndex[i]) {
			fstring    colname = m_rowSchema->getColumnName(i);
			ColumnMeta colmeta = m_rowSchema->getColumnMeta(i);
			m_nonIndexRowSchema->m_columnsMeta.insert_i(colname, colmeta);
		}
	}
	m_nonIndexRowSchema->compile(m_rowSchema.get());
}

ReadableSegment::ReadableSegment() {
	m_delcnt = 0;
	m_tobeDel = false;
}
ReadableSegment::~ReadableSegment() {
	if (m_isDelMmap) {
		size_t bitBytes = m_isDel.capacity()/8;
		mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
		m_isDel.risk_release_ownership();
	}
	assert(!m_segDir.empty());
	if (m_tobeDel && !m_segDir.empty()) {
		boost::filesystem::remove_all(m_segDir);
	}
}

void ReadableSegment::deleteSegment() {
	assert(!m_segDir.empty());
	m_tobeDel = true;
}

llong ReadableSegment::numDataRows() const {
	return m_isDel.size();
}

void ReadableSegment::save(fstring dir) const {
	std::string isDelFpath = dir + "/isDel";
	NativeDataOutput<FileStream> file;
	file.open(isDelFpath.c_str(), "wb");
	file << uint64_t(m_isDel.size());
	file << uint64_t(m_delcnt);
	file.ensureWrite(m_isDel.bldata(), m_isDel.mem_size());
}

void ReadableSegment::load(fstring dir) {
	std::string isDelFpath = dir + "/isDel";
	size_t bytes = 0;
	bool writable = true;
	m_isDelMmap = (byte*)mmap_load(isDelFpath.c_str(), &bytes, writable);
	uint64_t rowNum = ((uint64_t*)m_isDelMmap)[0];
	uint64_t delcnt = ((uint64_t*)m_isDelMmap)[1];
	m_isDel.risk_mmap_from(m_isDelMmap + 16, bytes - 16);
	assert(m_isDel.size() >= rowNum);
	m_isDel.risk_set_size(size_t(rowNum));
	m_delcnt = size_t(delcnt);
}

///////////////////////////////////////////////////////////////////////////////

ReadonlySegment::ReadonlySegment() {
	m_dataMemSize = 0;
	m_totalStorageSize = 0;
	m_maxPartDataSize = 2LL * 1024*1024*1024;
	m_parts.reserve(16);
	m_rowNumVec.reserve(16);
}
ReadonlySegment::~ReadonlySegment() {
}
ReadableIndexPtr
ReadonlySegment::getReadableIndex(size_t nth) const {
	return m_indices[nth];
}

llong ReadonlySegment::dataStorageSize() const {
	return m_dataMemSize;
}
llong ReadonlySegment::totalStorageSize() const {
	return m_totalStorageSize;
}

void ReadonlySegment::getValueAppend(llong id, valvec<byte>* val, DbContext* txn) const {
	assert(&txn != nullptr);
	llong rows = m_isDel.size();
	if (id < 0) {
		THROW_STD(invalid_argument, "invalid id=%lld", id);
	}
	if (id >= rows) {
		THROW_STD(invalid_argument, "invalid id=%lld, rows=%lld", id, rows);
	}
	if (m_parts.empty()) {
		getValueImpl(0, id, val, txn);
	} else {
		size_t upp = nark::upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
		getValueImpl(upp-1, id, val, txn);
	}
}

void
ReadonlySegment::getValueImpl(size_t partIdx, size_t id,
							  valvec<byte>* val, DbContext* ctx)
const {
	val->risk_set_size(0);
	ctx->buf1.risk_set_size(0);
	// m_indices also store index keys, so index keys will not be stored
	// in m_parts(nonIndex store)

	// getValueAppend to ctx->buf1
	ctx->offsets.risk_set_size(0);
	ctx->offsets.push_back(0);
	for (size_t i = 0; i < m_indices.size(); ++i) {
		if (m_indexSchemaSet->m_keepSchema[i]) {
			m_indices[i]->getValueAppend(id, &ctx->buf1, ctx);
		}
		ctx->offsets.push_back(ctx->buf1.size());
	}
	if (!m_parts.empty()) { // get nonIndex store
		llong subId = id - m_rowNumVec[partIdx];
		m_parts[partIdx]->getValueAppend(subId, &ctx->buf1, ctx);
		ctx->offsets.push_back(ctx->buf1.size());
	}

	// parseRowAppend to ctx->cols1
	ctx->cols1.risk_set_size(0);
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& iSchema = *m_indexSchemaSet->m_nested.elem_at(i);
		size_t off0 = ctx->offsets[i], off1 = ctx->offsets[i+1];
		if (m_indexSchemaSet->m_keepSchema[i]) {
			fstring indexRow(ctx->buf1.data() + off0, off1 - off0);
			iSchema.parseRowAppend(indexRow, &ctx->cols1);
		}
		else { // keep array slots
			assert(off0 == off1);
			ctx->cols1.resize(ctx->cols1.size() + iSchema.columnNum());
		}
	}
	assert(ctx->cols1.size() == m_indexSchemaSet->m_keepColumn.size());
	if (!m_parts.empty()) { // get nonIndex store
		size_t off0 = ctx->offsets.ende(2), off1 = ctx->offsets.ende(1);
		fstring indexRow(ctx->buf1.data() + off0, off1 - off0);
		m_nonIndexRowSchema->parseRowAppend(indexRow, &ctx->cols1);
	}

	// combine columns to ctx->cols2
	size_t baseColumnId = 0;
	ctx->cols2.resize_fill(m_rowSchema->columnNum());
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& iSchema = *m_indexSchemaSet->m_nested.elem_at(i);
		for (size_t j = 0; j < iSchema.columnNum(); ++j) {
			if (m_indexSchemaSet->m_keepColumn[baseColumnId + j]) {
				size_t parentColId = iSchema.parentColumnId(j);
				ctx->cols2[parentColId] = ctx->cols1[baseColumnId + j];
			}
		}
		baseColumnId += iSchema.columnNum();
	}
	if (!m_parts.empty()) { // get nonIndex store
		for(size_t j = 0; j < m_nonIndexRowSchema->columnNum(); ++j) {
			size_t parentColId = m_nonIndexRowSchema->parentColumnId(j);
			ctx->cols2[parentColId] = ctx->cols1[baseColumnId + j];
		}
	}

#if !defined(NDEBUG)
	for (size_t i = 0; i < ctx->cols2.size(); ++i) {
		assert(!ctx->cols2[i].empty());
	}
#endif

	// combine to val
	m_rowSchema->combineRow(ctx->cols2, val);
}

class ReadonlySegment::MyStoreIterator : public StoreIterator {
	size_t m_partIdx = 0;
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterator(const ReadonlySegment* owner, const DbContextPtr& ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ReadonlySegment*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		if (owner->m_parts.empty()) {
			if (size_t(m_id) < owner->m_isDel.size()) {
				*id = m_id++;
				owner->getValueImpl(0, *id, val, m_ctx.get());
				return true;
			}
			return false;
		}
		assert(m_partIdx < owner->m_parts.size());
		if (nark_likely(m_id < owner->m_rowNumVec[m_partIdx + 1])) {
			*id = m_id++;
			owner->getValueImpl(m_partIdx, *id, val, m_ctx.get());
			return true;
		}
		else {
			if (m_partIdx + 1 < owner->m_parts.size()) {
				m_partIdx++;
				if (nark_likely(size_t(m_id) < owner->m_isDel.size())) {
					*id = m_id++;
					owner->getValueImpl(m_partIdx, *id, val, m_ctx.get());
					return true;
				}
			}
		}
		return false;
	}
};
StoreIteratorPtr ReadonlySegment::createStoreIter(DbContext* ctx) const {
	return new MyStoreIterator(this, ctx);
}

void
ReadonlySegment::mergeFrom(const valvec<const ReadonlySegment*>& input, DbContext* ctx) {
	m_indices.resize(input[0]->m_indices.size());
	valvec<byte> buf;
	SortableStrVec strVec;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		size_t fixedIndexRowLen = indexSchema->getFixedRowLen();
		for (size_t j = 0; j < input.size(); ++j) {
			auto seg = input[j];
			const ReadableStore* indexStore = seg->m_indices[i].get();
			llong num = indexStore->numDataRows();
			for (llong id = 0; id < num; ++id) {
				if (!seg->m_isDel[id]) {
					indexStore->getValue(id, &buf, ctx);
					if (fixedIndexRowLen) {
						assert(buf.size() == fixedIndexRowLen);
						strVec.m_strpool.append(buf);
					} else
						strVec.push_back(buf);
				}
			}
		}
		m_indices[i] = this->buildIndex(indexSchema, strVec);
		strVec.clear();
	}
	for (size_t i = 0; i < input.size(); ++i) {
		auto seg = input[i];
		llong baseId = 0;
		for (size_t j = 0; j < seg->m_parts.size(); ++j) {
			const ReadableStore* dataStore = seg->m_parts[j].get();
			llong numRows = dataStore->numDataRows();
			for (llong subId = 0; subId < numRows; ++subId) {
				if (strVec.mem_size() >= this->m_maxPartDataSize) {
					m_parts.push_back(buildStore(strVec));
					strVec.clear();
				}
				llong id = baseId + subId;
				if (!seg->m_isDel[id]) {
					dataStore->getValue(subId, &buf, ctx);
					strVec.push_back(buf);
				}
			}
			baseId += numRows;
		}
	}
	if (strVec.size()) {
		m_parts.push_back(buildStore(strVec));
		strVec.clear();
	}
	m_rowNumVec.resize(0);
	llong baseId = 0;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		m_rowNumVec.push_back(baseId);
		baseId += m_parts[i]->numDataRows();
	}
	m_rowNumVec.push_back(baseId);
}

namespace {
	class FileDataIO {
		FileStream m_fp;
		NativeDataOutput<OutputBuffer> m_obuf;
		size_t m_fixedLen;
		FileDataIO(const FileDataIO&) = delete;
	public:
		FileDataIO(size_t fixedLen) {
			m_fp.attach(tmpfile());
			m_obuf.attach(&m_fp);
			m_fixedLen = fixedLen;
		}
		void dioWrite(const valvec<byte>& rowData) {
			assert(rowData.size() > 0);
			if (0 == m_fixedLen) {
				m_obuf << var_size_t(rowData.size());
			} else {
				assert(rowData.size() == m_fixedLen);
				if (rowData.size() != m_fixedLen)
					THROW_STD(runtime_error, "index RowLen=%zd != FixedRowLen=%zd"
						, rowData.size(), m_fixedLen);
			}
			m_obuf.ensureWrite(rowData.data(), rowData.size());
		}
		void completeWrite() {
			m_obuf.flush();
			m_fp.rewind();
		}
		FileStream& fp() { return m_fp; }
		size_t fixedLen() const { return m_fixedLen; }
	};
}

void
ReadonlySegment::convFrom(const ReadableSegment& input, DbContext* ctx)
{
	assert(input.numDataRows() > 0);
	assert(m_parts.size() == 0);
	size_t indexNum = m_indexSchemaSet->m_nested.end_i();
	valvec<FileDataIO> indexTempFiles(indexNum, valvec_reserve());
	for (size_t i = 0; i < indexNum; ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		indexTempFiles.unchecked_emplace_back(indexSchema->getFixedRowLen());
	}
	valvec<fstring> columns(m_rowSchema->columnNum(), valvec_reserve());
	valvec<byte> buf, projRowBuf;
	size_t nonIndexColNum = m_nonIndexRowSchema->columnNum();
	size_t nonIndexFixLen = m_nonIndexRowSchema->getFixedRowLen();
	SortableStrVec strVec;
	llong inputRowNum = input.numDataRows();
	assert(size_t(inputRowNum) == input.m_isDel.size());
	StoreIteratorPtr iter(input.createStoreIter(ctx));
	llong id = -1;
	llong newRowNum = 0;
	m_isDel = input.m_isDel; // make a copy, input.m_isDel[*] may be changed
	while (iter->increment(&id, &buf)) {
		assert(id >= 0);
		assert(id < inputRowNum);
		if (m_isDel[id]) continue;

		m_rowSchema->parseRow(buf, &columns);
		const Schema* indexSchema = nullptr;
		for (size_t i = 0; i < indexNum; ++i) {
			indexSchema = &*m_indexSchemaSet->m_nested.elem_at(i);
			indexSchema->selectParent(columns, &projRowBuf);
			indexTempFiles[i].dioWrite(projRowBuf);
		}
		// if all columns are indexed, then nonIndexColNum is 0
		if (nonIndexColNum) {
			m_nonIndexRowSchema->selectParent(columns, &projRowBuf);
			if (nonIndexFixLen) {
				strVec.m_strpool.append(projRowBuf);
			} else {
				strVec.push_back(projRowBuf); // new id is strVec[i].seq_id
			}
		}
		if (strVec.mem_size() >= this->m_maxPartDataSize) {
			assert(0 != nonIndexColNum);
			m_parts.push_back(buildStore(strVec));
			strVec.clear();
		}
		newRowNum++;
	}
	if (strVec.m_strpool.size() > 0) {
		assert(0 != nonIndexColNum);
		m_parts.push_back(buildStore(strVec));
		strVec.clear();
	}
	assert(strVec.m_index.size() == 0);
	assert(strVec.m_strpool.size() == 0);

	// build index from temporary index files
	for (size_t i = 0; i < indexTempFiles.size(); ++i) {
		indexTempFiles[i].completeWrite();
	}
	m_indices.resize(indexNum);
	for (size_t i = 0; i < indexTempFiles.size(); ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		size_t indexColumnNum = indexSchema->columnNum();
		if (indexTempFiles[i].fixedLen() == 0) {
			NativeDataInput<InputBuffer> dio;
			indexTempFiles[i].fp().disbuf();
			dio.attach(&indexTempFiles[i].fp());
			for (llong id = 0; id < newRowNum; id++) {
				dio >> buf;
				assert(buf.size() > 0);
				strVec.push_back(buf);
			}
		}
		else {
			assert(strVec.m_index.size() == 0);
			size_t size = indexTempFiles[i].fixedLen() * newRowNum;
			strVec.m_strpool.resize_no_init(size);
			indexTempFiles[i].fp().ensureRead(strVec.m_strpool.data(), size);
		}
		m_indices[i] = this->buildIndex(indexSchema, strVec);
		strVec.clear();
	}
	{
		assert(newRowNum <= inputRowNum);
		tbb::queuing_rw_mutex::scoped_lock lock(ctx->m_tab->m_rwMutex, false);
		size_t old_delcnt = inputRowNum - newRowNum;
		if (old_delcnt < input.m_delcnt) { // rows were deleted during build
			size_t i = 0;
			for (size_t j = 0; j < size_t(inputRowNum); ++j) {
				if (!m_isDel[j])
					 m_isDel.set(i, input.m_isDel[j]), ++i;
			}
			assert(i == newRowNum);
			m_isDel.resize(newRowNum);
		}
		m_delcnt = input.m_delcnt - old_delcnt;
	}
	if (nonIndexColNum) {
		m_rowNumVec.erase_all();
		m_rowNumVec.push_back(0);
		llong sum = 0;
		llong baseId = 0;
		for (auto& part : m_parts) {
			sum += part->dataStorageSize();
			m_rowNumVec.push_back(baseId += part->numDataRows());
		}
		m_dataMemSize = sum;
	}
	else {
		m_rowNumVec.clear();
		m_dataMemSize = 0;
	}
}

void ReadonlySegment::save(fstring prefix) const {
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema.joinColumnNames(',');
		std::string p2 = prefix + "/index-" + colnames;
		m_indices[i]->save(p2);
	}
	AutoGrownMemIO buf;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		buf.rewind();
		buf.printf("%s/store-%04ld", prefix.c_str(), long(i));
		m_parts[i]->save(buf.c_str());
	}
	ReadableSegment::save(prefix);
}

void ReadonlySegment::load(fstring dir) {
	if (!m_indices.empty()) {
		THROW_STD(invalid_argument, "m_indices must be empty");
	}
	if (!m_parts.empty()) {
		THROW_STD(invalid_argument, "m_parts must be empty");
	}
	for (size_t i = 0; i < m_indexSchemaSet->m_nested.end_i(); ++i) {
		SchemaPtr schema = m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema->joinColumnNames(',');
		std::string path = dir + "/index-" + colnames;
		m_indices.push_back(this->openIndex(path, schema));
	}
	namespace fs = boost::filesystem;
	for (auto& x : fs::directory_iterator(fs::path(dir.c_str()))) {
		std::string fname = x.path().filename().string();
		long partIdx = -1;
		if (!fstring(fname).startsWith("store")) {
			continue;
		}
		if (sscanf(fname.c_str(), "store-%ld", &partIdx) <= 0) {
			fprintf(stderr, "WARN: bad store filename = %s\n", fname.c_str());
			continue;
		}
		if (partIdx < 0) {
			THROW_STD(invalid_argument,
				"bad partIdx in fname = %s", fname.c_str());
		}
		if (m_parts.size() <= size_t(partIdx)) {
			m_parts.resize(partIdx+1);
		}
		m_parts[partIdx] = this->openPart(x.path().string());
	}
	m_rowNumVec.resize_no_init(m_parts.size() + 1);
	llong id = 0;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		m_rowNumVec[i] = id;
		id += m_parts[i]->numDataRows();
	}
	m_rowNumVec.back() = id;
	ReadableSegment::load(dir);
}

///////////////////////////////////////////////////////////////////////////////

WritableSegment::WritableSegment() {
}
WritableSegment::~WritableSegment() {
}

WritableStore* WritableSegment::getWritableStore() {
	return this;
}
ReadableIndexPtr
WritableSegment::getReadableIndex(size_t nth) const {
	assert(nth < m_indices.size());
	return m_indices[nth].get();
}

llong WritableSegment::totalIndexSize() const {
	llong size = 0;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		size += m_indices[i]->indexStorageSize();
	}
	return size;
}

void WritableSegment::openIndices(fstring dir) {
	if (!m_indices.empty()) {
		THROW_STD(invalid_argument, "m_indices must be empty");
	}
	m_indices.resize(this->getIndexNum());
	for (size_t i = 0; i < this->getIndexNum(); ++i) {
		SchemaPtr schema = m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema->joinColumnNames(',');
		std::string path = dir + "/index-" + colnames;
		m_indices[i] = this->openIndex(path, schema);
	}
}

void WritableSegment::saveIndices(fstring dir) const {
	assert(m_indices.size() == this->getIndexNum());
	for (size_t i = 0; i < m_indices.size(); ++i) {
		SchemaPtr schema = m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema->joinColumnNames(',');
		std::string path = dir + "/index-" + colnames;
		m_indices[i]->save(path);
	}
}

///////////////////////////////////////////////////////////////////////////////

SmartWritableSegment::~SmartWritableSegment() {
}

void
SmartWritableSegment::getValueAppend(llong id, valvec<byte>* val, DbContext* txn)
const {
	assert(txn != nullptr);
	// m_indices also store index keys
//	DbContextPtr dummy;
	assert(0);
	// should similar to ReadonlySegment::getValueAppend(...)
}

class SmartWritableSegment::MyStoreIterator : public StoreIterator {
	size_t m_id;
	mutable DbContextPtr m_ctx;
public:
	MyStoreIterator(const SmartWritableSegment* owner, DbContext* ctx) {
		m_store.reset(const_cast<SmartWritableSegment*>(owner));
		m_id = 0;
		m_ctx.reset(ctx);
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		if (m_id < owner->m_isDel.size()) {
			*id = m_id;
			owner->getValue(m_id, val, &*m_ctx);
			m_id++;
			return true;
		}
		return false;
	}
};

StoreIteratorPtr SmartWritableSegment::createStoreIter(DbContext* ctx) const {
	return new MyStoreIterator(this, ctx);
}

void SmartWritableSegment::save(fstring dir) const {
	saveIndices(dir);
	std::string storePath = dir + "/nonIndexStore";
	m_nonIndexStore->save(storePath);
}

void SmartWritableSegment::load(fstring dir) {
	openIndices(dir);
	std::string storePath = dir + "/nonIndexStore";
	m_nonIndexStore->load(storePath);
}

llong SmartWritableSegment::dataStorageSize() const {
	return m_nonIndexStore->dataStorageSize();
}

llong SmartWritableSegment::totalStorageSize() const {
	return totalIndexSize() + m_nonIndexStore->dataStorageSize();
}

} } // namespace nark::db
