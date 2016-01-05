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
#include <nark/util/linebuf.hpp>
#include <nark/util/sortable_strvec.hpp>

#define NARK_DB_ENABLE_DFA_META
#if defined(NARK_DB_ENABLE_DFA_META)
#include <nark/fsa/nest_trie_dawg.hpp>
#endif

#include "json.hpp"

namespace nark { namespace db {

namespace fs = boost::filesystem;

const llong DEFAULT_readonlyDataMemSize = 2LL * 1024 * 1024 * 1024;
const llong DEFAULT_maxWrSegSize        = 3LL * 1024 * 1024 * 1024;

SegmentSchema::SegmentSchema() {
	m_readonlyDataMemSize = DEFAULT_readonlyDataMemSize;
	m_maxWrSegSize = DEFAULT_maxWrSegSize;
}
SegmentSchema::~SegmentSchema() {
}

void SegmentSchema::compileSchema() {
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
	m_colgroupSchemaSet->m_nested = m_indexSchemaSet->m_nested;
	for (size_t i = 0; i < colgroups.size(); ++i) {
		SchemaPtr& schema = colgroups[i];
		if (!schema->m_columnsMeta.empty())
			m_colgroupSchemaSet->m_nested.insert_i(schema);
	}
	m_colgroupSchemaSet->compileSchemaSet(m_rowSchema.get());

	SchemaPtr restAll(new Schema());
	for (size_t i = 0; i < hasIndex.size(); ++i) {
		if (!hasIndex[i]) {
			fstring    colname = m_rowSchema->getColumnName(i);
			ColumnMeta colmeta = m_rowSchema->getColumnMeta(i);
			restAll->m_columnsMeta.insert_i(colname, colmeta);
		}
	}
	if (restAll->columnNum() > 0) {
		restAll->m_name = ".RestAll";
		restAll->compile(m_rowSchema.get());
		restAll->m_keepCols.fill(true);
		m_colgroupSchemaSet->m_nested.insert_i(restAll);
	}
}

void SegmentSchema::loadJsonFile(fstring fname) {
	LineBuf alljson;
	alljson.read_all(fname.c_str());
	loadJsonString(alljson);
}

void SegmentSchema::loadJsonString(fstring jstr) {
	using nark::json;
	const json meta = json::parse(jstr.p
					// UTF8 BOM Check, fixed in nlohmann::json
					// + (fstring(alljson.p, 3) == "\xEF\xBB\xBF" ? 3 : 0)
					);
	const json& rowSchema = meta["RowSchema"];
	const json& cols = rowSchema["columns"];
	m_rowSchema.reset(new Schema());
	m_colgroupSchemaSet.reset(new SchemaSet());
	for (auto iter = cols.cbegin(); iter != cols.cend(); ++iter) {
		const auto& col = iter.value();
		std::string name = iter.key();
		std::string type = col["type"];
		std::transform(type.begin(), type.end(), type.begin(), &::tolower);
		if ("nested" == type) {
			fprintf(stderr, "TODO: nested column: %s is not supported now, save it to $$\n", name.c_str());
			continue;
		}
		ColumnMeta colmeta;
		colmeta.type = Schema::parseColumnType(type);
		if (ColumnType::Fixed == colmeta.type) {
			colmeta.fixedLen = col["length"];
		}
		auto found = col.find("uType");
		if (col.end() != found) {
			int uType = found.value();
			colmeta.uType = byte(uType);
		}
		found = col.find("colstore");
		if (col.end() != found) {
			bool colstore = found.value();
			if (colstore) {
				// this colstore has the only-one 'name' field
				SchemaPtr schema(new Schema());
				schema->m_columnsMeta.insert_i(name, colmeta);
				schema->m_name = name;
				m_colgroupSchemaSet->m_nested.insert_i(schema);
			}
		}
		auto ib = m_rowSchema->m_columnsMeta.insert_i(name, colmeta);
		if (!ib.second) {
			THROW_STD(invalid_argument, "duplicate RowName=%s", name.c_str());
		}
	}
	m_rowSchema->compile();
	auto iter = meta.find("ReadonlyDataMemSize");
	if (meta.end() == iter) {
		m_readonlyDataMemSize = DEFAULT_readonlyDataMemSize;
	} else {
		m_readonlyDataMemSize = *iter;
	}
	iter = meta.find("MaxWrSegSize");
	if (meta.end() == iter) {
		m_maxWrSegSize = DEFAULT_maxWrSegSize;
	} else {
		m_maxWrSegSize = *iter;
	}
	const json& tableIndex = meta["TableIndex"];
	if (!tableIndex.is_array()) {
		THROW_STD(invalid_argument, "json TableIndex must be an array");
	}
	m_indexSchemaSet.reset(new SchemaSet());
	for (const auto& index : tableIndex) {
		SchemaPtr indexSchema(new Schema());
		const std::string& strFields = index["fields"];
		indexSchema->m_name = strFields;
		std::vector<std::string> fields;
		fstring(strFields).split(',', &fields);
		if (fields.size() > Schema::MaxProjColumns) {
			THROW_STD(invalid_argument, "Index Columns=%zd exceeds Max=%zd",
				fields.size(), Schema::MaxProjColumns);
		}
		for (const std::string& colname : fields) {
			const size_t k = m_rowSchema->getColumnId(colname);
			if (k == m_rowSchema->columnNum()) {
				THROW_STD(invalid_argument,
					"colname=%s is not in RowSchema", colname.c_str());
			}
			indexSchema->m_columnsMeta.
				insert_i(colname, m_rowSchema->getColumnMeta(k));
		}
		auto ib = m_indexSchemaSet->m_nested.insert_i(indexSchema);
		if (!ib.second) {
			THROW_STD(invalid_argument,
				"duplicate index: %s", strFields.c_str());
		}
		auto found = index.find("ordered");
		if (index.end() == found)
			indexSchema->m_isOrdered = true; // default
		else
			indexSchema->m_isOrdered = found.value();

		found = index.find("unique");
		if (index.end() == found)
			indexSchema->m_isUnique = false; // default
		else
			indexSchema->m_isUnique = found.value();
	}
	compileSchema();
}

void SegmentSchema::saveJsonFile(fstring jsonFile) const {
	abort(); // not completed yet
	using nark::json;
	json meta;
	json& rowSchema = meta["RowSchema"];
	json& cols = rowSchema["columns"];
	cols = json::array();
	for (size_t i = 0; i < m_rowSchema->columnNum(); ++i) {
		ColumnType coltype = m_rowSchema->getColumnType(i);
		std::string colname = m_rowSchema->getColumnName(i).str();
		std::string strtype = Schema::columnTypeStr(coltype);
		json col;
		col["name"] = colname;
		col["type"] = strtype;
		if (ColumnType::Fixed == coltype) {
			col["length"] = m_rowSchema->getColumnMeta(i).fixedLen;
		}
		cols.push_back(col);
	}
	json& indexSet = meta["TableIndex"];
	for (size_t i = 0; i < m_indexSchemaSet->m_nested.end_i(); ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		json indexCols;
		for (size_t j = 0; j < schema.columnNum(); ++j) {
			indexCols.push_back(schema.getColumnName(j).str());
		}
		indexSet.push_back(indexCols);
	}
	std::string jsonStr = meta.dump(2);
	FileStream fp(jsonFile.c_str(), "w");
	fp.ensureWrite(jsonStr.data(), jsonStr.size());
}

#if defined(NARK_DB_ENABLE_DFA_META)
void SegmentSchema::loadMetaDFA(fstring metaFile) {
	std::unique_ptr<MatchingDFA> metaConf(MatchingDFA::load_from(metaFile));
	std::string val;
	size_t segNum = 0, minWrSeg = 0;
	if (metaConf->find_key_uniq_val("TotalSegNum", &val)) {
		segNum = lcast(val);
	} else {
		THROW_STD(invalid_argument, "metaconf dfa: TotalSegNum is missing");
	}
	if (metaConf->find_key_uniq_val("MinWrSeg", &val)) {
		minWrSeg = lcast(val);
	} else {
		THROW_STD(invalid_argument, "metaconf dfa: MinWrSeg is missing");
	}
	if (metaConf->find_key_uniq_val("MaxWrSegSize", &val)) {
		m_maxWrSegSize = lcast(val);
	} else {
		m_maxWrSegSize = DEFAULT_maxWrSegSize;
	}
	if (metaConf->find_key_uniq_val("ReadonlyDataMemSize", &val)) {
		m_readonlyDataMemSize = lcast(val);
	} else {
		m_readonlyDataMemSize = DEFAULT_readonlyDataMemSize;
	}

	valvec<fstring> F;
	MatchContext ctx;
	m_rowSchema.reset(new Schema());
	if (!metaConf->step_key_l(ctx, "RowSchema")) {
		THROW_STD(invalid_argument, "metaconf dfa: RowSchema is missing");
	}
	metaConf->for_each_value(ctx, [&](size_t klen, size_t, fstring val) {
		val.split('\t', &F);
		if (F.size() < 3) {
			THROW_STD(invalid_argument, "RowSchema Column definition error");
		}
		size_t     columnId = lcast(F[0]);
		fstring    colname = F[1];
		ColumnMeta colmeta;
		colmeta.type = Schema::parseColumnType(F[2]);
		if (ColumnType::Fixed == colmeta.type) {
			colmeta.fixedLen = lcast(F[3]);
		}
		auto ib = m_rowSchema->m_columnsMeta.insert_i(colname, colmeta);
		if (!ib.second) {
			THROW_STD(invalid_argument, "duplicate column name: %.*s",
				colname.ilen(), colname.data());
		}
		if (ib.first != columnId) {
			THROW_STD(invalid_argument, "bad columnId: %lld", llong(columnId));
		}
	});
	ctx.reset();
	if (!metaConf->step_key_l(ctx, "TableIndex")) {
		THROW_STD(invalid_argument, "metaconf dfa: TableIndex is missing");
	}
	metaConf->for_each_value(ctx, [&](size_t klen, size_t, fstring val) {
		val.split(',', &F);
		if (F.size() < 1) {
			THROW_STD(invalid_argument, "TableIndex definition error");
		}
		SchemaPtr schema(new Schema());
		for (size_t i = 0; i < F.size(); ++i) {
			fstring colname = F[i];
			size_t colId = m_rowSchema->getColumnId(colname);
			if (colId >= m_rowSchema->columnNum()) {
				THROW_STD(invalid_argument,
					"index column name=%.*s is not found in RowSchema",
					colname.ilen(), colname.c_str());
			}
			ColumnMeta colmeta = m_rowSchema->getColumnMeta(colId);
			schema->m_columnsMeta.insert_i(colname, colmeta);
		}
		auto ib = m_indexSchemaSet->m_nested.insert_i(schema);
		if (!ib.second) {
			THROW_STD(invalid_argument, "invalid index schema");
		}
	});
	compileSchema();
}

void SegmentSchema::saveMetaDFA(fstring fname) const {
	SortableStrVec meta;
	AutoGrownMemIO buf;
	size_t pos;
//	pos = buf.printf("TotalSegNum\t%ld", long(m_segments.s));
	pos = buf.printf("RowSchema\t");
	for (size_t i = 0; i < m_rowSchema->columnNum(); ++i) {
		buf.printf("%04ld", long(i));
		meta.push_back(fstring(buf.begin(), buf.tell()));
		buf.seek(pos);
	}
	NestLoudsTrieDAWG_SE_512 trie;
}
#endif

/////////////////////////////////////////////////////////////////////////////

MultiPartStore::MultiPartStore() {
}

MultiPartStore::~MultiPartStore() {
}

llong MultiPartStore::dataStorageSize() const {
	size_t size = 0;
	for (auto& part : m_parts)
		size += part->dataStorageSize();
	return size;
}

llong MultiPartStore::numDataRows() const {
	return m_rowNumVec.back();
}

void
MultiPartStore::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	assert(m_parts.size() + 1 == m_rowNumVec.size());
	llong maxId = m_rowNumVec.back();
	if (id >= maxId) {
		THROW_STD(out_of_range, "id %lld, maxId = %lld", id, maxId);
	}
	size_t upp = upper_bound_a(m_rowNumVec, id);
	assert(upp < m_rowNumVec.size());
	llong baseId = m_rowNumVec[upp-1];
	m_parts[upp-1]->getValueAppend(id - baseId, val, ctx);
}

class MultiPartStore::MyStoreIterForward : public StoreIterator {
	size_t m_partIdx = 0;
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const MultiPartStore* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<MultiPartStore*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		assert(m_partIdx < owner->m_parts.size());
		if (nark_likely(m_id < owner->m_rowNumVec[m_partIdx + 1])) {
			// do nothing
		}
		else if (m_partIdx + 1 < owner->m_parts.size()) {
			m_partIdx++;
		}
		else {
			return false;
		}
		*id = m_id++;
		llong baseId = owner->m_rowNumVec[m_partIdx];
		llong subId = *id - baseId;
		owner->m_parts[m_partIdx]->getValueAppend(subId, val, m_ctx.get());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		if (id < 0 || id >= owner->m_rowNumVec.back()) {
			return false;
		}
		size_t upp = upper_bound_a(owner->m_rowNumVec, id);
		llong  baseId = owner->m_rowNumVec[upp-1];
		llong  subId = id - baseId;
		owner->m_parts[upp-1]->getValueAppend(subId, val, m_ctx.get());
		m_id = id+1;
		m_partIdx = upp-1;
		return true;
	}
	void reset() override {
		m_partIdx = 0;
		m_id = 0;
	}
};

class MultiPartStore::MyStoreIterBackward : public StoreIterator {
	size_t m_partIdx;
	llong  m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterBackward(const MultiPartStore* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<MultiPartStore*>(owner));
		m_partIdx = owner->m_parts.size();
		m_id = owner->m_rowNumVec.back();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		if (owner->m_parts.empty()) {
			return false;
		}
		assert(m_partIdx > 0);
		if (nark_likely(m_id > owner->m_rowNumVec[m_partIdx-1])) {
			// do nothing
		}
		else if (m_partIdx > 1) {
			--m_partIdx;
		}
		else {
			return false;
		}
		*id = --m_id;
		llong baseId = owner->m_rowNumVec[m_partIdx-1];
		llong subId = *id - baseId;
		owner->m_parts[m_partIdx-1]->getValueAppend(subId, val, m_ctx.get());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		if (id < 0 || id >= owner->m_rowNumVec.back()) {
			return false;
		}
		size_t upp = upper_bound_a(owner->m_rowNumVec, id);
		llong  baseId = owner->m_rowNumVec[upp-1];
		llong  subId = id - baseId;
		owner->m_parts[upp-1]->getValueAppend(subId, val, m_ctx.get());
		m_partIdx = upp;
		m_id = id;
		return true;
	}
	void reset() override {
		auto owner = static_cast<const MultiPartStore*>(m_store.get());
		m_partIdx = owner->m_parts.size();
		m_id = owner->m_rowNumVec.back();
	}
};
StoreIterator* MultiPartStore::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, ctx);
}
StoreIterator* MultiPartStore::createStoreIterBackward(DbContext* ctx) const {
	return new MyStoreIterBackward(this, ctx);
}

void MultiPartStore::load(PathRef path) {
	abort();
}

void MultiPartStore::save(PathRef path) const {
	char szNum[16];
	for (size_t i = 0; i < m_parts.size(); ++i) {
		snprintf(szNum, sizeof(szNum), ".%04zd", i);
		m_parts[i]->save(path + szNum);
	}
}

void MultiPartStore::syncRowNumVec() {
	m_rowNumVec.resize_no_init(m_parts.size() + 1);
	llong rows = 0;
	for (size_t i = 0; i < m_parts.size(); ++i) {
		m_rowNumVec[i] = rows;
		rows += m_parts[i]->numDataRows();
	}
	m_rowNumVec.back() = rows;
}

/////////////////////////////////////////////////////////////////////////////

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

void ReadableSegment::saveIsDel(PathRef dir) const {
	assert(m_isDel.popcnt() == m_delcnt);
	if (m_isDelMmap && dir == PathRef(m_segDir)) {
		// need not to save, mmap is sys memory
		return;
	}
	fs::path isDelFpath = dir / "isDel";
	NativeDataOutput<FileStream> file;
	file.open(isDelFpath.string().c_str(), "wb");
	file << uint64_t(m_isDel.size());
	file.ensureWrite(m_isDel.bldata(), m_isDel.mem_size());
}

void ReadableSegment::loadIsDel(PathRef dir) {
	if (m_isDelMmap) {
		m_isDel.risk_release_ownership();
		m_isDelMmap = nullptr;
	}
	else {
		m_isDel.clear(); // free memory
	}
	m_delcnt = 0;
	m_isDelMmap = loadIsDel_aux(dir, m_isDel);
	m_delcnt = m_isDel.popcnt();
}

byte* ReadableSegment::loadIsDel_aux(PathRef segDir, febitvec& isDel) const {
	fs::path isDelFpath = segDir / "isDel";
	size_t bytes = 0;
	bool writable = true;
	std::string fpath = isDelFpath.string();
	byte* isDelMmap = (byte*)mmap_load(fpath, &bytes, writable);
	uint64_t rowNum = ((uint64_t*)isDelMmap)[0];
	isDel.risk_mmap_from(isDelMmap + 8, bytes - 8);
	assert(m_isDel.size() >= rowNum);
	isDel.risk_set_size(size_t(rowNum));
	return isDelMmap;
}

void ReadableSegment::unmapIsDel() {
	febitvec isDel(m_isDel);
	size_t bitBytes = m_isDel.capacity()/8;
	mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
	m_isDel.risk_release_ownership();
	m_isDel.swap(isDel);
	m_isDelMmap = nullptr;
}

void ReadableSegment::openIndices(PathRef segDir) {
	if (!m_indices.empty()) {
		THROW_STD(invalid_argument, "m_indices must be empty");
	}
	m_indices.resize(m_schema->getIndexNum());
	for (size_t i = 0; i < m_schema->getIndexNum(); ++i) {
		const Schema& schema = m_schema->getIndexSchema(i);
		fs::path path = segDir / ("index-" + schema.m_name);
		m_indices[i] = this->openIndex(schema, path.string());
	}
}

void ReadableSegment::saveIndices(PathRef segDir) const {
	assert(m_indices.size() == m_schema->getIndexNum());
	for (size_t i = 0; i < m_indices.size(); ++i) {
		const Schema& schema = m_schema->getIndexSchema(i);
		fs::path path = segDir / ("index-" + schema.m_name);
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

void ReadableSegment::load(PathRef segDir) {
	assert(!segDir.empty());
	this->loadIsDel(segDir);
	this->openIndices(segDir);
	this->loadRecordStore(segDir);
}

void ReadableSegment::save(PathRef segDir) const {
	assert(!segDir.empty());
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
	if (id < 0 || id >= rows) {
		THROW_STD(invalid_argument, "invalid id=%lld, rows=%lld", id, rows);
	}
	getValueImpl(id, val, txn);
}

void
ReadonlySegment::getValueImpl(size_t id, valvec<byte>* val, DbContext* ctx)
const {
	val->risk_set_size(0);
	ctx->buf1.risk_set_size(0);

	// getValueAppend to ctx->buf1
	ctx->offsets.risk_set_size(0);
	ctx->offsets.push_back(0);
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		const Schema& iSchema = m_schema->getColgroupSchema(i);
		if (iSchema.m_keepCols.has_any1()) {
			m_colgroups[i]->getValueAppend(id, &ctx->buf1, ctx);
		}
		ctx->offsets.push_back(uint32_t(ctx->buf1.size()));
	}

	// parseRowAppend to ctx->cols1
	ctx->cols1.risk_set_size(0);
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		const Schema& iSchema = m_schema->getColgroupSchema(i);
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
	assert(ctx->cols1.size() == m_schema->m_colgroupSchemaSet->m_flattenColumnNum);

	// combine columns to ctx->cols2
	size_t baseColumnId = 0;
	ctx->cols2.resize_fill(m_schema->columnNum());
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		const Schema& iSchema = m_schema->getColgroupSchema(i);
		for (size_t j = 0; j < iSchema.columnNum(); ++j) {
			if (iSchema.m_keepCols[j]) {
				size_t parentColId = iSchema.parentColumnId(j);
				ctx->cols2[parentColId] = ctx->cols1[baseColumnId + j];
			}
		}
		baseColumnId += iSchema.columnNum();
	}

#if !defined(NDEBUG)
	for (size_t i = 0; i < ctx->cols2.size(); ++i) {
//		assert(!ctx->cols2[i].empty()); // can be empty
	}
#endif

	// combine to val
	m_schema->m_rowSchema->combineRow(ctx->cols2, val);
}

void
ReadonlySegment::selectColumns(llong recId,
							   const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext* ctx)
const {
	colsData->erase_all();
	ctx->offsets.resize_fill(2 * m_colgroups.size(), uint32_t(-1));
	ctx->buf1.erase_all();
	size_t rowColnum = m_schema->m_rowSchema->columnNum();
	for(size_t i = 0; i < colsNum; ++i) {
		assert(colsId[i] < rowColnum);
		auto cp = m_schema->m_colproject[colsId[i]];
		size_t colgroupId = cp.colgroupId;
		size_t oldsize = ctx->buf1.size();
		if (ctx->offsets[2*colgroupId] == uint32_t(-1)) {
			ctx->offsets[2*colgroupId] = oldsize;
			m_colgroups[colgroupId]->getValueAppend(recId, &ctx->buf1, ctx);
			ctx->offsets[2*colgroupId+1] = ctx->buf1.size() - oldsize;
		}
	}
	for(size_t i = 0; i < colsNum; ++i) {
		auto cp = m_schema->m_colproject[colsId[i]];
		size_t colgroupId = cp.colgroupId;
		assert(ctx->offsets[2*colgroupId] != uint32_t(-1));
		const Schema& schema = m_schema->getColgroupSchema(colgroupId);
		const size_t offset = ctx->offsets[2*colgroupId];
		const size_t length = ctx->offsets[2*colgroupId + 1];
		if (offset != uint32_t(-1)) {
			fstring d(ctx->buf1.data() + offset, length);
			schema.parseRowAppend(d, &ctx->cols1);
		} else {
			ctx->cols1.grow(schema.columnNum());
		}
	}
	size_t colseq = 0;
	for(size_t i = 0; i < colsNum; ++i) {
		auto cp = m_schema->m_colproject[colsId[i]];
		size_t colgroupId = cp.colgroupId;
		assert(ctx->offsets[2*colgroupId] != uint32_t(-1));
		const Schema& schema = m_schema->getColgroupSchema(colgroupId);
		const size_t offset = ctx->offsets[2*colgroupId];
		const size_t length = ctx->offsets[2*colgroupId + 1];
		if (i < colsNum-1) {
			fstring d = ctx->cols1[colseq + cp.subColumnId];
			schema.projectToNorm(d, cp.subColumnId, colsData);
		}
		colseq += schema.columnNum();
	}
}

void
ReadonlySegment::selectOneColumn(llong recId, size_t columnId,
								 valvec<byte>* colsData, DbContext* ctx)
const {
	assert(columnId < m_schema->m_rowSchema->columnNum());
	auto cp = m_schema->m_colproject[columnId];
	size_t colgroupId = cp.colgroupId;
	const Schema& schema = m_schema->getColgroupSchema(colgroupId);
	if (schema.columnNum() == 1) {
		m_colgroups[colgroupId]->getValue(recId, colsData, ctx);
	}
	else {
		m_colgroups[colgroupId]->getValue(recId, &ctx->buf1, ctx);
		schema.parseRow(ctx->buf1, &ctx->cols1);
		colsData->erase_all();
		colsData->append(ctx->cols1[cp.subColumnId]);
	}
}

class ReadonlySegment::MyStoreIterForward : public StoreIterator {
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const ReadonlySegment* owner, const DbContextPtr& ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ReadonlySegment*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		if (size_t(m_id) < owner->m_isDel.size()) {
			*id = m_id++;
			owner->getValueImpl(*id, val, m_ctx.get());
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		m_id = id;
		llong id2 = -1;
		return increment(&id2, val);
	}
	void reset() override {
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
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		if (m_id > 0) {
			*id = --m_id;
			owner->getValueImpl(*id, val, m_ctx.get());
			return true;
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
		const Schema& indexSchema = m_schema->getIndexSchema(i);
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

		void prepairRead(NativeDataInput<InputBuffer>& dio) {
			m_fp.disbuf();
			dio.resetbuf();
			dio.attach(&m_fp);
		}
		size_t
		collectData(NativeDataInput<InputBuffer>& dio,
					size_t newRowNum, SortableStrVec& strVec,
					size_t maxMemSize = size_t(-1)) {
			if (m_fixedLen == 0) {
				valvec<byte> buf;
				size_t i = 0;
				while (i < newRowNum && strVec.mem_size() < maxMemSize) {
					dio >> buf;
				//	assert(buf.size() > 0); // can be empty
					strVec.push_back(buf);
					i++;
				}
				return strVec.size();
			}
			else {
				// ignore maxMemSize
				assert(strVec.m_index.size() == 0);
				size_t size = m_fixedLen * newRowNum;
				strVec.m_strpool.resize_no_init(size);
				m_fp.ensureRead(strVec.m_strpool.data(), size);
				return newRowNum;
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
	size_t indexNum = m_schema->getIndexNum();
	TempFileList colgroupTempFiles(*m_schema->m_colgroupSchemaSet);

	valvec<fstring> columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> buf, projRowBuf;
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

		m_schema->m_rowSchema->parseRow(buf, &columns);
		colgroupTempFiles.writeColgroups(columns, projRowBuf);
		newRowNum++;
	}

	// build index from temporary index files
	colgroupTempFiles.completeWrite();
	m_indices.resize(indexNum);
	m_colgroups.resize(m_schema->getColgroupNum());
	NativeDataInput<InputBuffer> dio;
	for (size_t i = 0; i < indexNum; ++i) {
		const Schema& schema = m_schema->getIndexSchema(i);
		colgroupTempFiles[i].prepairRead(dio);
		colgroupTempFiles[i].collectData(dio, newRowNum, strVec);
		m_indices[i] = this->buildIndex(schema, strVec);
		m_colgroups[i] = const_cast<ReadableStore*>(m_indices[i]->getReadableStore());
		strVec.clear();
	}
	for (size_t i = indexNum; i < colgroupTempFiles.size(); ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		size_t maxMem = m_schema->m_readonlyDataMemSize;
		llong rows = 0;
		valvec<ReadableStorePtr> parts;
		colgroupTempFiles[i].prepairRead(dio);
		while (rows < newRowNum) {
			size_t rest = newRowNum - rows;
			rows += colgroupTempFiles[i].collectData(dio, rest, strVec, maxMem);
			parts.push_back(this->buildStore(schema, strVec));
			strVec.clear();
		}
		if (parts.size() == 1) {
			m_colgroups[i] = parts[0];
		} else {
			std::unique_ptr<MultiPartStore> store(new MultiPartStore());
			store->m_parts.swap(parts);
			store->syncRowNumVec();
			m_colgroups[i] = store.release();
		}
	}
	fs::create_directories(m_segDir);
	this->save(m_segDir);

// reload as mmap
	m_isDel.clear();
	m_indices.erase_all();
	m_colgroups.erase_all();
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
			"INFO: ReadonlySegment::convFrom: delcnt[old=%zd input2=%zd new=%zd]\n",
			old_delcnt, input.m_delcnt, m_delcnt);
		assert(m_isDel.popcnt() == m_delcnt);
	}
	m_dataMemSize = 0;
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		m_dataMemSize += m_colgroups[i]->dataStorageSize();
	}
}

void ReadonlySegment::saveRecordStore(PathRef segDir) const {
	size_t indexNum = m_schema->getIndexNum();
	size_t colgroupNum = m_schema->getColgroupNum();
	for (size_t i = indexNum; i < colgroupNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		fs::path fpath = segDir / ("colgroup-" + schema.m_name);
		m_colgroups[i]->save(fpath.string());
	}
}

void ReadonlySegment::loadRecordStore(PathRef segDir) {
	if (!m_colgroups.empty()) {
		THROW_STD(invalid_argument, "m_colgroups must be empty");
	}
	// indices must be loaded first
	assert(m_indices.size() == m_schema->getIndexNum());

	size_t indexNum = m_schema->getIndexNum();
	size_t colgroupNum = m_schema->getColgroupNum();
	m_colgroups.resize(colgroupNum);
	for (size_t i = 0; i < indexNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		assert(m_indices[i]); // index must have be loaded
		auto store = m_indices[i]->getReadableStore();
		assert(nullptr != store);
		m_colgroups[i] = const_cast<ReadableStore*>(store);
	}
	SortableStrVec files;
	for(auto ent : fs::directory_iterator(segDir)) {
		files.push_back(ent.path().filename().string());
	}
	files.sort();
	for (size_t i = indexNum; i < colgroupNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		std::string prefix = "colgroup-" + schema.m_name;
		size_t lo = lower_bound_0<SortableStrVec&>(files, files.size(), prefix);
		if (lo >= files.size() || !files[lo].startsWith(prefix)) {
			THROW_STD(invalid_argument, "missing: %s",
				(segDir / prefix).string().c_str());
		}
		fstring fname = files[lo];
		if (fname.substr(prefix.size()).startsWith(".0000.")) {
			std::unique_ptr<MultiPartStore> mstore(new MultiPartStore());
			size_t j = lo;
			while (j < files.size() && (fname = files[j]).startsWith(prefix)) {
				size_t partIdx = lcast(fname.substr(prefix.size()+1));
				assert(partIdx == j - lo);
				if (partIdx != j - lo) {
					THROW_STD(invalid_argument, "missing part: %s.%zd",
						(segDir / prefix).string().c_str(), j - lo);
				}
				mstore->m_parts.push_back(ReadableStore::openStore(segDir, fname));
				++j;
			}
			mstore->syncRowNumVec();
			m_colgroups[i] = mstore.release();
		}
		else {
			m_colgroups[i] = ReadableStore::openStore(segDir, fname);
		}
	}
}

ReadableIndex*
ReadonlySegment::openIndex(const Schema& schema, PathRef path) const {
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
				schema.m_name.c_str());
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

void WritableSegment::selectColumns(llong recId,
									const size_t* colsId, size_t colsNum,
									valvec<byte>* colsData, DbContext* ctx)
const {
	colsData->erase_all();
	this->getValue(recId, &ctx->buf1, ctx);
	const Schema& schema = *m_schema->m_rowSchema;
	schema.parseRow(ctx->buf1, &ctx->cols1);
	assert(ctx->cols1.size() == schema.columnNum());
	auto cols = ctx->cols1.data();
	for(size_t i = 0; i < colsNum; ++i) {
		size_t columnId = colsId[i];
		assert(columnId < schema.columnNum());
		if (i < colsNum)
			schema.projectToNorm(cols[columnId], columnId, colsData);
		else
			schema.projectToLast(cols[columnId], columnId, colsData);
	}
}

void WritableSegment::selectOneColumn(llong recId, size_t columnId,
									  valvec<byte>* colsData, DbContext* ctx)
const {
	colsData->erase_all();
	this->getValue(recId, &ctx->buf1, ctx);
	const Schema& schema = *m_schema->m_rowSchema;
	assert(columnId < schema.columnNum());
	schema.parseRow(ctx->buf1, &ctx->cols1);
	assert(ctx->cols1.size() == schema.columnNum());
	schema.projectToLast(ctx->cols1[columnId], columnId, colsData);
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

void SmartWritableSegment::saveRecordStore(PathRef dir) const {
	abort();
}

void SmartWritableSegment::loadRecordStore(PathRef dir) {
}

llong SmartWritableSegment::dataStorageSize() const {
	abort();
	return 0;
}

llong SmartWritableSegment::totalStorageSize() const {
	abort();
	return totalIndexSize() + 0;
}

} } // namespace nark::db
