#include "db_table.hpp"
#include "intkey_index.hpp"
#include "zip_int_store.hpp"
#include "fixed_len_key_index.hpp"
#include "fixed_len_store.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>
#include <nark/util/mmap.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

namespace nark { namespace db {

namespace fs = boost::filesystem;

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
		for (size_t j = 0; j < colnum; ++j) {
			hasIndex.set1(schema.parentColumnId(j));
		}
	}

	// remove columns in colgroups which is also in indices
	valvec<SchemaPtr> colgroups(m_colgroupSchemaSet->m_nested.end_i());
	for (size_t i = 0; i < m_colgroupSchemaSet->m_nested.end_i(); ++i) {
		colgroups[i] = m_colgroupSchemaSet->m_nested.elem_at(i);
	}
	m_colgroupSchemaSet->m_nested.erase_all();
	for (size_t i = 0; i < colgroups.size(); ++i) {
		SchemaPtr& schema = colgroups[i];
		schema->m_columnsMeta.shrink_after_erase_if_kv(
			[&](fstring colname, const ColumnMeta&) {
			size_t pos = m_rowSchema->m_columnsMeta.find_i(colname);
			assert(pos < m_rowSchema->m_columnsMeta.end_i());
			bool ret = hasIndex[pos];
			hasIndex.set1(pos); // now it is column stored
			return ret;
		});
	}
	for (size_t i = 0; i < colgroups.size(); ++i) {
		SchemaPtr& schema = colgroups[i];
		if (!schema->m_columnsMeta.empty())
			m_colgroupSchemaSet->m_nested.insert_i(schema);
	}
	m_colgroupSchemaSet->compileSchemaSet(m_rowSchema.get());

	for (size_t i = 0; i < hasIndex.size(); ++i) {
		if (!hasIndex[i]) {
			fstring    colname = m_rowSchema->getColumnName(i);
			ColumnMeta colmeta = m_rowSchema->getColumnMeta(i);
			m_nonIndexRowSchema->m_columnsMeta.insert_i(colname, colmeta);
		}
	}
	if (m_nonIndexRowSchema->columnNum() > 0)
		m_nonIndexRowSchema->compile(m_rowSchema.get());
}

ReadableSegment::ReadableSegment() {
	m_delcnt = 0;
	m_tobeDel = false;
	m_isDirty = false;
}
ReadableSegment::~ReadableSegment() {
	if (m_isDelMmap) {
		size_t bitBytes = m_isDel.capacity()/8;
		mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
		m_isDel.risk_release_ownership();
	}
	else if (m_isDirty && !m_tobeDel && !m_segDir.empty()) {
		saveIsDel(m_segDir);
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

void ReadableSegment::saveIsDel(fstring dir) const {
	assert(m_isDel.popcnt() == m_delcnt);
	if (m_isDelMmap && dir == m_segDir) {
		// need not to save, mmap is sys memory
		return;
	}
	fs::path isDelFpath = fs::path(dir.str()) / "isDel";
	NativeDataOutput<FileStream> file;
	file.open(isDelFpath.string().c_str(), "wb");
	file << uint64_t(m_isDel.size());
	file.ensureWrite(m_isDel.bldata(), m_isDel.mem_size());
}

void ReadableSegment::loadIsDel(fstring dir) {
	if (m_isDelMmap) {
		m_isDel.risk_release_ownership();
		m_isDelMmap = nullptr;
	}
	else {
		m_isDel.clear(); // free memory
	}
	m_delcnt = 0;
	fs::path isDelFpath = fs::path(dir.str()) / "isDel";
	size_t bytes = 0;
	bool writable = true;
	std::string fpath = isDelFpath.string();
	m_isDelMmap = (byte*)mmap_load(fpath, &bytes, writable);
	uint64_t rowNum = ((uint64_t*)m_isDelMmap)[0];
	m_isDel.risk_mmap_from(m_isDelMmap + 8, bytes - 8);
	assert(m_isDel.size() >= rowNum);
	m_isDel.risk_set_size(size_t(rowNum));
	m_delcnt = m_isDel.popcnt();
}

void ReadableSegment::unmapIsDel() {
	febitvec isDel(m_isDel);
	size_t bitBytes = m_isDel.capacity()/8;
	mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
	m_isDel.risk_release_ownership();
	m_isDel.swap(isDel);
	m_isDelMmap = nullptr;
}

void ReadableSegment::openIndices(fstring segDir) {
	if (!m_indices.empty()) {
		THROW_STD(invalid_argument, "m_indices must be empty");
	}
	m_indices.resize(this->getIndexNum());
	fs::path segDirPath = segDir.str();
	for (size_t i = 0; i < this->getIndexNum(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema.joinColumnNames(',');
		fs::path path = segDirPath / ("index-" + colnames);
		m_indices[i] = this->openIndex(schema, path.string());
	}
}

void ReadableSegment::saveIndices(fstring segDir) const {
	assert(m_indices.size() == this->getIndexNum());
	fs::path segDirPath = segDir.str();
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		std::string colnames = schema.joinColumnNames(',');
		fs::path path = segDirPath / ("index-" + colnames);
		m_indices[i]->save(path.string());
	}
}

llong ReadableSegment::totalIndexSize() const {
	llong size = 0;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		size += m_indices[i]->indexStorageSize();
	}
	return size;
}

void ReadableSegment::load(fstring segDir) {
	assert(segDir.size() > 0);
	this->loadIsDel(segDir);
	this->openIndices(segDir);
	this->loadRecordStore(segDir);
}

void ReadableSegment::save(fstring segDir) const {
	assert(segDir.size() > 0);
	if (m_tobeDel) {
		return; // not needed
	}
	this->saveIsDel(segDir);
	this->saveIndices(segDir);
	this->saveRecordStore(segDir);
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

llong ReadonlySegment::dataStorageSize() const {
	return m_dataMemSize;
}
llong ReadonlySegment::totalStorageSize() const {
	return m_totalStorageSize;
}

void ReadonlySegment::getValueAppend(llong id, valvec<byte>* val, DbContext* txn) const {
	assert(txn != nullptr);
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
		const Schema& iSchema = *m_indexSchemaSet->m_nested.elem_at(i);
		if (iSchema.m_keepCols.has_any1()) {
			m_indices[i]->getReadableStore()->
				getValueAppend(id, &ctx->buf1, ctx);
		}
		ctx->offsets.push_back(ctx->buf1.size());
	}
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		m_colgroups[i]->getValueAppend(id, &ctx->buf1, ctx);
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
		if (iSchema.m_keepCols.has_any1()) {
			fstring indexRow(ctx->buf1.data() + off0, off1 - off0);
			iSchema.parseRowAppend(indexRow, &ctx->cols1);
		}
		else { // keep array slots
			assert(off0 == off1);
			ctx->cols1.resize(ctx->cols1.size() + iSchema.columnNum());
		}
	}
	assert(ctx->cols1.size() == m_indexSchemaSet->m_flattenColumnNum);
	size_t indexNum = m_indices.size();
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		size_t off0 = ctx->offsets[indexNum + i + 0];
		size_t off1 = ctx->offsets[indexNum + i + 1];
		fstring colgroupData(ctx->buf1.data() + off0, off1 - off0);
		const Schema& schema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		schema.parseRowAppend(colgroupData, &ctx->cols1);
	}
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
			if (iSchema.m_keepCols[j]) {
				size_t parentColId = iSchema.parentColumnId(j);
				ctx->cols2[parentColId] = ctx->cols1[baseColumnId + j];
			}
		}
		baseColumnId += iSchema.columnNum();
	}
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		const Schema& iSchema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		for(size_t j = 0; j < iSchema.columnNum(); ++j) {
			size_t parentColId = iSchema.parentColumnId(j);
			ctx->cols2[parentColId] = ctx->cols1[baseColumnId + j];
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
//		assert(!ctx->cols2[i].empty()); // can be empty
	}
#endif

	// combine to val
	m_rowSchema->combineRow(ctx->cols2, val);
}

class ReadonlySegment::MyStoreIterForward : public StoreIterator {
	size_t m_partIdx = 0;
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const ReadonlySegment* owner, const DbContextPtr& ctx)
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
	bool seekExact(llong id, valvec<byte>* val) override {
		m_id = id;
		llong id2 = -1;
		return increment(&id2, val);
	}
	void reset() override {
		m_partIdx = 0;
		m_id = 0;
	}
};
class ReadonlySegment::MyStoreIterBackward : public StoreIterator {
	size_t m_partIdx;
	llong  m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterBackward(const ReadonlySegment* owner, const DbContextPtr& ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ReadonlySegment*>(owner));
		m_id = owner->m_isDel.size();
		m_partIdx = owner->m_parts.size();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		if (0 == m_partIdx) {
			assert(owner->m_parts.empty());
			if (m_id > 0) {
				*id = --m_id;
				owner->getValueImpl(0, *id, val, m_ctx.get());
				return true;
			}
			return false;
		}
		assert(m_partIdx <= owner->m_parts.size());
		assert(m_partIdx >= 1);
		if (nark_likely(m_id > owner->m_rowNumVec[m_partIdx-1])) {
			*id = --m_id;
			owner->getValueImpl(m_partIdx-1, *id, val, m_ctx.get());
			return true;
		}
		else {
			if (m_partIdx > 1) {
				--m_partIdx;
				if (nark_likely(m_id > 0)) {
					*id = --m_id;
					owner->getValueImpl(m_partIdx, *id, val, m_ctx.get());
					return true;
				}
			}
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		m_id = id;
		llong id2 = -1;
		return increment(&id2, val);
	}
	void reset() override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		m_partIdx = owner->m_parts.size();
		m_id = owner->m_isDel.size();
	}
};
StoreIterator* ReadonlySegment::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, ctx);
}
StoreIterator* ReadonlySegment::createStoreIterBackward(DbContext* ctx) const {
	return new MyStoreIterBackward(this, ctx);
}

void
ReadonlySegment::mergeFrom(const valvec<const ReadonlySegment*>& input, DbContext* ctx) {
	m_indices.resize(input[0]->m_indices.size());
	valvec<byte> buf;
	SortableStrVec strVec;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& indexSchema = *m_indexSchemaSet->m_nested.elem_at(i);
		size_t fixedIndexRowLen = indexSchema.getFixedRowLen();
		for (size_t j = 0; j < input.size(); ++j) {
			auto seg = input[j];
			auto indexStore = seg->m_indices[i]->getReadableStore();
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
					m_parts.push_back(buildStore(*m_nonIndexRowSchema, strVec));
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
		m_parts.push_back(buildStore(*m_nonIndexRowSchema, strVec));
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
		//	assert(rowData.size() > 0); // can be empty
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

		void collectData(size_t newRowNum, SortableStrVec& strVec) {
			if (m_fixedLen == 0) {
				NativeDataInput<InputBuffer> dio;
				m_fp.disbuf();
				dio.attach(&m_fp);
				valvec<byte> buf;
				for (size_t id = 0; id < newRowNum; id++) {
					dio >> buf;
				//	assert(buf.size() > 0); // can be empty
					strVec.push_back(buf);
				}
			}
			else {
				assert(strVec.m_index.size() == 0);
				size_t size = m_fixedLen * newRowNum;
				strVec.m_strpool.resize_no_init(size);
				m_fp.ensureRead(strVec.m_strpool.data(), size);
			}
		}
	};

	class TempFileList : public valvec<FileDataIO> {
		const SchemaSet& m_schemaSet;
	public:
		TempFileList(const SchemaSet& schemaSet) : m_schemaSet(schemaSet) {
			this->reserve(schemaSet.m_nested.end_i());
			for (size_t i = 0; i < schemaSet.m_nested.end_i(); ++i) {
				SchemaPtr schema = schemaSet.m_nested.elem_at(i);
				this->unchecked_emplace_back(schema->getFixedRowLen());
			}
		}
		void writeColgroups(const valvec<fstring>& columns, valvec<byte>& workBuf) {
			size_t colgroupNum = this->size();
			for (size_t i = 0; i < colgroupNum; ++i) {
				const Schema& schema = *m_schemaSet.m_nested.elem_at(i);
				schema.selectParent(columns, &workBuf);
				this->p[i].dioWrite(workBuf);
			}
		}
		void completeWrite() {
			size_t colgroupNum = this->size();
			for (size_t i = 0; i < colgroupNum; ++i) {
				this->p[i].completeWrite();
			}
		}
	};
}

void
ReadonlySegment::convFrom(const ReadableSegment& input, DbContext* ctx)
{
	assert(input.numDataRows() > 0);
	assert(m_parts.size() == 0);
	size_t indexNum = m_indexSchemaSet->m_nested.end_i();
	TempFileList indexTempFiles(*m_indexSchemaSet);
	TempFileList colgroupTempFiles(*m_colgroupSchemaSet);

	valvec<fstring> columns(m_rowSchema->columnNum(), valvec_reserve());
	valvec<byte> buf, projRowBuf;
	size_t nonIndexColNum = m_nonIndexRowSchema->columnNum();
	size_t nonIndexFixLen = m_nonIndexRowSchema->getFixedRowLen();
	SortableStrVec strVec;
	llong inputRowNum = input.numDataRows();
	assert(size_t(inputRowNum) == input.m_isDel.size());
	StoreIteratorPtr iter(input.createStoreIterForward(ctx));
	llong id = -1;
	llong newRowNum = 0;
	m_isDel = input.m_isDel; // make a copy, input.m_isDel[*] may be changed
	while (iter->increment(&id, &buf)) {
		assert(id >= 0);
		assert(id < inputRowNum);
		if (m_isDel[id]) continue;

		m_rowSchema->parseRow(buf, &columns);
		indexTempFiles.writeColgroups(columns, projRowBuf);
		colgroupTempFiles.writeColgroups(columns, projRowBuf);
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
			m_parts.push_back(buildStore(*m_nonIndexRowSchema, strVec));
			strVec.clear();
		}
		newRowNum++;
	}
	if (strVec.m_strpool.size() > 0) {
		assert(0 != nonIndexColNum);
		m_parts.push_back(buildStore(*m_nonIndexRowSchema, strVec));
		strVec.clear();
	}
	assert(strVec.m_index.size() == 0);
	assert(strVec.m_strpool.size() == 0);

	// build index from temporary index files
	indexTempFiles.completeWrite();
	m_indices.resize(indexNum);
	for (size_t i = 0; i < indexTempFiles.size(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		indexTempFiles[i].collectData(newRowNum, strVec);
		m_indices[i] = this->buildIndex(schema, strVec);
		strVec.clear();
	}
	colgroupTempFiles.completeWrite();
	m_colgroups.resize(m_colgroupSchemaSet->m_nested.end_i());
	for (size_t i = 0; i < colgroupTempFiles.size(); ++i) {
		const Schema& schema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		colgroupTempFiles[i].collectData(newRowNum, strVec);
		m_colgroups[i] = this->buildStore(schema, strVec);
		strVec.clear();
	}
	fs::create_directories(m_segDir);
	this->save(m_segDir);

// reload as mmap
	m_isDel.clear();
	m_indices.erase_all();
	m_colgroups.erase_all();
	m_parts.erase_all();
	m_rowNumVec.erase_all();
	this->load(m_segDir);

	{
		assert(newRowNum <= inputRowNum);
		MyRwLock lock(ctx->m_tab->m_rwMutex, false);
		size_t old_delcnt = inputRowNum - newRowNum;
		if (old_delcnt < input.m_delcnt) { // rows were deleted during build
			size_t i = 0;
			for (size_t j = 0; j < size_t(inputRowNum); ++j) {
				if (!m_isDel[j])
					 m_isDel.set(i, input.m_isDel[j]), ++i;
			}
			assert(i == size_t(newRowNum));
			m_isDel.resize(size_t(newRowNum));
		}
		m_delcnt = input.m_delcnt - old_delcnt;
		fprintf(stderr,
			"INFO: ReadonlySegment::convFrom: delcnt[old=%zd input2=%zd new=%zd]",
			old_delcnt, input.m_delcnt, m_delcnt);
		assert(m_isDel.popcnt() == m_delcnt);
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

void ReadonlySegment::saveRecordStore(fstring dir) const {
	fs::path dirp = dir.str();
	AutoGrownMemIO buf;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		buf.rewind();
		buf.printf("%s/store-%04ld", dir.c_str(), long(i));
		m_parts[i]->save(buf.c_str());
	}
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		const Schema& schema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		std::string colsList = schema.joinColumnNames();
		fs::path fpath = dirp / ("colgroup-" + colsList);
		m_colgroups[i]->save(fpath.string());
	}
}

void ReadonlySegment::loadRecordStore(fstring dir) {
	if (!m_parts.empty()) {
		THROW_STD(invalid_argument, "m_parts must be empty");
	}
	if (!m_colgroups.empty()) {
		THROW_STD(invalid_argument, "m_colgroups must be empty");
	}
	fs::path segDir = dir.str();
	m_colgroups.resize(m_colgroupSchemaSet->m_nested.end_i());
	for (size_t i = 0; i < m_colgroupSchemaSet->m_nested.end_i(); ++i) {
		const Schema& schema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		std::string colsList = schema.joinColumnNames();
		fs::path fpath = segDir / ("colgroup-" + colsList);
		m_colgroups[i] = openStore(schema, fpath.string());
	}
	for (auto& x : fs::directory_iterator(segDir)) {
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
		fs::path stemPath = segDir / x.path().stem();
		m_parts[partIdx] = openStore(*m_nonIndexRowSchema, stemPath.string());
	}
	if (m_parts.size()) {
		m_rowNumVec.resize_no_init(m_parts.size() + 1);
		llong id = 0;
		for (size_t i = 0; i < m_parts.size(); ++i) {
			m_rowNumVec[i] = id;
			id += m_parts[i]->numDataRows();
		}
		m_rowNumVec.back() = id;
	}
}

ReadableStore*
ReadonlySegment::openStore(const Schema& schema, fstring path) const {
	if (boost::filesystem::exists(path + ".zint")) {
		std::unique_ptr<ZipIntStore> store(new ZipIntStore());
		store->load(path);
		return store.release();
	}
	return nullptr;
}

ReadableIndex*
ReadonlySegment::openIndex(const Schema& schema, fstring path) const {
	if (boost::filesystem::exists(path + ".zint")) {
		std::unique_ptr<ZipIntKeyIndex> store(new ZipIntKeyIndex());
		store->load(path);
		return store.release();
	}
	if (boost::filesystem::exists(path + ".fixlen")) {
		std::unique_ptr<FixedLenKeyIndex> store(new FixedLenKeyIndex());
		store->load(path);
		return store.release();
	}
	return nullptr;
}

ReadableIndex*
ReadonlySegment::buildIndex(const Schema& schema, SortableStrVec& indexData)
const {
	const size_t fixlen = schema.getFixedRowLen();
	if (schema.columnNum() == 1 && schema.getColumnMeta(0).isInteger()) {
		try {
			std::unique_ptr<ZipIntKeyIndex> index(new ZipIntKeyIndex());
			index->build(schema.getColumnMeta(0).type, indexData);
			return index.release();
		}
		catch (const std::exception&) {
			// ignore and fall through
		}
	}
	if (fixlen && fixlen <= 16) {
		std::unique_ptr<FixedLenKeyIndex> index(new FixedLenKeyIndex());
		index->build(schema, indexData);
		return index.release();
	}
	return nullptr; // derived class should override
}

ReadableStore*
ReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	const size_t fixlen = schema.getFixedRowLen();
	if (schema.columnNum() == 1 && schema.getColumnMeta(0).isInteger()) {
		assert(fixlen > 0);
		try {
			std::unique_ptr<ZipIntStore> store(new ZipIntStore());
			store->build(schema.getColumnMeta(0).type, storeData);
			return store.release();
		}
		catch (const std::exception&) {
			// ignore and fall through
			fprintf(stderr, "try to build ZipIntStore: on %s failed, fallback to FixedLenStore\n",
				schema.joinColumnNames().c_str());
		}
	}
	if (fixlen && fixlen <= 16) {
		std::unique_ptr<FixedLenStore> store(new FixedLenStore());
		store->build(schema, storeData);
		return store.release();
	}
	return nullptr;
}

///////////////////////////////////////////////////////////////////////////////

WritableSegment::WritableSegment() {
}
WritableSegment::~WritableSegment() {
	if (!m_tobeDel)
		flushSegment();
}

WritableStore* WritableSegment::getWritableStore() {
	return this;
}

void WritableSegment::flushSegment() {
	if (m_isDirty) {
		save(m_segDir);
		m_isDirty = false;
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

class SmartWritableSegment::MyStoreIterForward : public StoreIterator {
	size_t m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const SmartWritableSegment* owner, DbContext* ctx) {
		m_store.reset(const_cast<SmartWritableSegment*>(owner));
		m_id = 0;
		m_ctx.reset(ctx);
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		if (m_id < owner->m_isDel.size()) {
			*id = m_id;
			owner->getValue(m_id, val, m_ctx.get());
			m_id++;
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		m_id = id;
		if (owner->m_isDel[id]) {
			return false;
		}
		owner->getValue(id, val, m_ctx.get());
		return true;
	}
	void reset() override {
		m_id = 0;
	}
};
class SmartWritableSegment::MyStoreIterBackward : public StoreIterator {
	size_t m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterBackward(const SmartWritableSegment* owner, DbContext* ctx) {
		m_store.reset(const_cast<SmartWritableSegment*>(owner));
		m_id = owner->m_isDel.size();
		m_ctx.reset(ctx);
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		if (m_id > 0) {
			*id = --m_id;
			owner->getValue(m_id, val, &*m_ctx);
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		m_id = id;
		if (owner->m_isDel[id]) {
			return false;
		}
		owner->getValue(id, val, m_ctx.get());
		return true;
	}
	void reset() override {
		auto owner = static_cast<const SmartWritableSegment*>(m_store.get());
		m_id = owner->m_isDel.size();
	}
};

StoreIterator* SmartWritableSegment::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, ctx);
}
StoreIterator* SmartWritableSegment::createStoreIterBackward(DbContext* ctx) const {
	return new MyStoreIterBackward(this, ctx);
}

void SmartWritableSegment::saveRecordStore(fstring dir) const {
	fs::path storePath = fs::path(dir.str()) / "nonIndexStore";
	m_nonIndexStore->save(storePath.string());
}

void SmartWritableSegment::loadRecordStore(fstring dir) {
	fs::path storePath = fs::path(dir.str()) / "nonIndexStore";
	m_nonIndexStore->load(storePath.string());
}

llong SmartWritableSegment::dataStorageSize() const {
	return m_nonIndexStore->dataStorageSize();
}

llong SmartWritableSegment::totalStorageSize() const {
	return totalIndexSize() + m_nonIndexStore->dataStorageSize();
}

} } // namespace nark::db
