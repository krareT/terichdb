#include "db_table.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>
//#include <nark/util/ini_parser.hpp>

namespace nark {

ReadableSegment::~ReadableSegment() {
}
llong ReadableSegment::numDataRows() const {
	return m_isDel.size();
}

ReadonlySegment::ReadonlySegment() {
	m_dataMemSize = 0;
	m_totalStorageSize = 0;
	m_maxPartDataSize = 2LL * 1024*1024*1024;
	m_parts.reserve(16);
	m_rowNumVec.reserve(16);
	m_rowNumVec.push_back(0);
}
ReadonlySegment::~ReadonlySegment() {
}
const ReadableIndex*
ReadonlySegment::getReadableIndex(size_t nth) const {
	return m_indices[nth].get();
}

llong ReadonlySegment::dataStorageSize() const {
	return m_dataMemSize;
}
llong ReadonlySegment::totalStorageSize() const {
	return m_totalStorageSize;
}

void ReadonlySegment::getValue(llong id, valvec<byte>* val, BaseContextPtr& txn) const {
	assert(txn.get() != nullptr);
	assert(dynamic_cast<ReadonlyStoreContext*>(txn.get()) != nullptr);
	auto rdctx = static_cast<ReadonlyStoreContext*>(txn.get());
	llong rows = m_rowNumVec.back();
	if (id < 0) {
		THROW_STD(invalid_argument, "invalid id=%lld", id);
	}
	if (id >= rows) {
		THROW_STD(invalid_argument, "invalid id=%lld, rows=%lld", id, rows);
	}
	size_t upp = nark::upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	llong subId = id - m_rowNumVec[upp-1];
	getValueImpl(upp-1, id, subId, val, &rdctx->buf);
}

void ReadonlySegment::
getValueImpl(size_t partIdx, size_t id, llong subId, valvec<byte>* val, valvec<byte>* buf)
const {
	val->resize(0);
	// m_indices is also store index keys, so index keys will not be stored
	// in m_parts(the main data store)
	BaseContextPtr dummy;
	for (auto& index : m_indices) {
		index->getValue(id, buf, dummy);
		val->append(*buf);
	}
	m_parts[partIdx]->getValue(subId, buf, dummy); // get the main store
	val->append(*buf);
}

class ReadonlySegment::MyStoreIterator : public ReadableStore::StoreIterator {
	size_t m_partIdx = 0;
	llong  m_id = -1;
	mutable valvec<byte> m_buf;
public:
	explicit MyStoreIterator(const ReadonlySegment* owner) {
		m_store.reset(const_cast<ReadonlySegment*>(owner));
	}
	bool increment() override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		assert(m_partIdx < owner->m_parts.size());
		if (m_id >= owner->m_rowNumVec[m_partIdx + 1]) {
			if (m_partIdx + 1 < owner->m_parts.size()) {
				m_partIdx++;
			} else {
				if (m_id >= owner->m_rowNumVec.back())
					return false;
			}
		}
		m_id++;
		return true;
	}
	void getKeyVal(llong* idKey, valvec<byte>* val) const override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		assert(m_partIdx < owner->m_parts.size());
		assert(m_id >= 0);
		assert(m_id < owner->m_rowNumVec[m_partIdx+1]);
		*idKey = m_id;
		llong subId = m_id - owner->m_rowNumVec[m_partIdx];
		owner->getValueImpl(m_partIdx, m_id, subId, val, &m_buf);
	}
};
ReadableStore::StoreIteratorPtr ReadonlySegment::createStoreIter() const {
	return new MyStoreIterator(this);
}
BaseContextPtr ReadonlySegment::createStoreContext() const {
	return new ReadonlyStoreContext();
}

void ReadonlySegment::mergeFrom(const valvec<const ReadonlySegment*>& input)
{
	m_indices.resize(input[0]->m_indices.size());
	valvec<byte> buf;
	SortableStrVec strVec;
	BaseContextPtr dummy;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		size_t fixedIndexRowLen = indexSchema->getFixedRowLen();
		for (size_t j = 0; j < input.size(); ++j) {
			auto seg = input[j];
			const ReadableStore* indexStore = seg->m_indices[i].get();
			llong num = indexStore->numDataRows();
			for (llong id = 0; id < num; ++id) {
				if (!seg->m_isDel[id]) {
					indexStore->getValue(id, &buf, dummy);
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
					dataStore->getValue(subId, &buf, dummy);
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
		void dioWrite(const valvec<ColumnData>& cols) {
			size_t len = 0;
			for (size_t i = 0; i < cols.size(); ++i) {
				len += cols[i].all_size();
			}
			if (0 == m_fixedLen) {
				m_obuf << var_size_t(len);
			} else {
				assert(len == m_fixedLen);
				if (len != m_fixedLen)
					THROW_STD(runtime_error, "index RowLen=%ld != FixedRowLen=%ld"
						, long(len), long(m_fixedLen));
			}
			for (size_t i = 0; i < cols.size(); ++i) {
				m_obuf.ensureWrite(cols[i].all_data(), cols[i].all_size());
			}
		}
		void completeWrite() {
			m_obuf.flush();
			m_fp.rewind();
		}
		FileStream& fp() { return m_fp; }
		size_t fixedLen() const { return m_fixedLen; }
	};
}

void ReadonlySegment::convFrom(const ReadableSegment& input, const Schema& schema)
{
	size_t indexNum = m_indexSchemaSet->m_nested.end_i();
	valvec<FileDataIO> indexTempFiles(indexNum, valvec_reserve());
	for (size_t i = 0; i < indexNum; ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		indexTempFiles.unchecked_emplace_back(indexSchema->getFixedRowLen());
	}
	basic_fstrvec<size_t> indexColumnIdVec;
	indexColumnIdVec.reserve(indexNum);
	valvec<size_t> columnIdMap(m_dataSchema->columnNum(), size_t(-1));
	for (size_t i = 0; i < m_dataSchema->columnNum(); ++i) {
		fstring columnName = m_dataSchema->getColumnName(i);
		size_t columnId = schema.getColumnId(columnName);
		if (columnId < schema.columnNum()) {
			columnIdMap[i] = columnId;
		}
	}
	for (size_t i = 0; i < indexNum; ++i) {
		const Schema& indexSchema = *m_indexSchemaSet->m_nested.elem_at(i);
		indexColumnIdVec.push_back();
		for (size_t j = 0; j < indexSchema.columnNum(); ++j) {
			fstring columnName = indexSchema.getColumnName(j);
			size_t columnId = schema.getColumnId(columnName);
			if (columnId >= schema.columnNum()) {
				THROW_STD(invalid_argument
					, "column %s is missing in writable schema"
					, columnName.c_str()
					);
			}
			indexColumnIdVec.back_append(columnId);
		}
	}

	valvec<ColumnData> columns(schema.columnNum(), valvec_reserve());
	valvec<ColumnData> indexColumns;
	valvec<byte> buf;
	SortableStrVec strVec;
	llong inputRowNum = input.numDataRows();
	assert(size_t(inputRowNum) == input.m_isDel.size());
	StoreIteratorPtr iter(input.createStoreIter());
	while (iter->increment()) {
		llong id = -1;
		iter->getKeyVal(&id, &buf);
		assert(id >= 0);
		assert(id < inputRowNum);
		if (input.m_isDel[id]) continue;

		strVec.push_back(""); // new id is strVec[i].seq_id
		schema.parseRow(buf, &columns);
		for (size_t i = 0; i < indexNum; ++i) {
			const Schema& indexSchema = *m_indexSchemaSet->m_nested.elem_at(i);
			const size_t* colmap = indexColumnIdVec.beg_of(i);
			indexColumns.erase_all();
			for (size_t j = 0; j < indexSchema.columnNum(); ++j) {
				size_t columnId = colmap[j];
				indexColumns.push_back(columns[columnId]);
			}
			indexTempFiles[i].dioWrite(indexColumns);
		}
		for (size_t i = 0; i < m_dataSchema->columnNum(); i++) {
			size_t columnId = columnIdMap[i];
			strVec.back_append(columns[columnId]);
		}
		if (strVec.mem_size() >= this->m_maxPartDataSize) {
			m_parts.push_back(buildStore(strVec));
			strVec.clear();
		}
	}
	if (strVec.size() > 0) {
		m_parts.push_back(buildStore(strVec));
		strVec.clear();
	}

	// build index from temporary index files
	for (size_t i = 0; i < indexTempFiles.size(); ++i) {
		indexTempFiles[i].completeWrite();
	}
	m_indices.resize(indexNum);
	for (size_t i = 0; i < indexTempFiles.size(); ++i) {
		SchemaPtr indexSchema = m_indexSchemaSet->m_nested.elem_at(i);
		size_t indexColumnNum = indexSchema->columnNum();
		ColumnMeta firstColumnMeta = indexSchema->m_columnsMeta.val(0);
		if (indexTempFiles[i].fixedLen() == 0) {
			NativeDataInput<InputBuffer> dio;
			dio.attach(&indexTempFiles[i].fp());
			for (llong id = 0; id < inputRowNum; id++) {
				dio >> buf;
				strVec.push_back(buf);
			}
		}
		else {
			size_t size = indexTempFiles[i].fixedLen() * inputRowNum;
			strVec.m_strpool.resize_no_init(size);
			indexTempFiles[i].fp().ensureRead(strVec.m_strpool.data(), size);
		}
		m_indices[i] = this->buildIndex(indexSchema, strVec);
		strVec.clear();
	}
	llong sum = 0;
	for (auto part : m_parts) sum += part->dataStorageSize();
	m_dataMemSize = sum;
}

///////////////////////////////////////////////////////////////////////////////

WritableStore* WritableSegment::getWritableStore() {
	return this;
}
const ReadableIndex* WritableSegment::getReadableIndex(size_t nth) const {
	assert(nth < m_indices.size());
	return m_indices[nth].get();
}

///////////////////////////////////////////////////////////////////////////////
TableContext::TableContext() {
}

TableContext::~TableContext() {
}

///////////////////////////////////////////////////////////////////////////////

const llong DEFAULT_readonlyDataMemSize = 2LL * 1024 * 1024 * 1024;
const llong DEFAULT_maxWrSegSize        = 3LL * 1024 * 1024 * 1024;
const size_t DEFAULT_maxSegNum = 4095;

CompositeTable::CompositeTable() {
	m_readonlyDataMemSize = DEFAULT_readonlyDataMemSize;
	m_maxWrSegSize = DEFAULT_maxWrSegSize;
	m_tableScanningRefCount = 0;

	m_segments.reserve(DEFAULT_maxSegNum);
	m_rowNumVec.reserve(DEFAULT_maxSegNum+1);
	m_rowNumVec.push_back(0);
}

void
CompositeTable::createTable(fstring dir, fstring name,
							SchemaPtr rowSchema, SchemaSetPtr indexSchemaSet)
{
	assert(!dir.empty());
	assert(!name.empty());
	assert(rowSchema->columnNum() > 0);
	assert(indexSchemaSet->m_nested.end_i() > 0);
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	m_rowSchema = rowSchema;
	m_indexSchemaSet = indexSchemaSet;
	m_indexProjects.offsets.reserve(indexSchemaSet->m_nested.end_i());
	m_nonIndexRowSchema.reset(new Schema());
	febitvec hasIndex(m_rowSchema->columnNum(), false);
	for (size_t i = 0; i < indexSchemaSet->m_nested.end_i(); ++i) {
		const Schema& schema = *indexSchemaSet->m_nested.elem_at(i);
		m_indexProjects.push_back();
		for (size_t j = 0; j < schema.columnNum(); ++i) {
			fstring colname = schema.getColumnName(i);
			size_t k = rowSchema->getColumnId(colname);
			if (k >= rowSchema->columnNum()) {
				THROW_STD(invalid_argument
					, "indexColumn=%s is not found in rowSchema"
					, colname.c_str());
			}
			m_indexProjects.back_append(k);
			hasIndex.set1(k);
		}
	}
	for (size_t i = 0; i < hasIndex.size(); ++i) {
		if (!hasIndex[i]) {
			fstring    colname = rowSchema->getColumnName(i);
			ColumnMeta colmeta = rowSchema->getColumnMeta(i);
			m_nonIndexRowSchema->m_columnsMeta.insert_i(colname, colmeta);
		}
	}
	m_dir = dir.str();
	m_name = name.str();

	AutoGrownMemIO buf;
	buf.printf("%s/%s/wr-%04d", dir.c_str(), name.c_str(), 0);
	fstring dirBaseName = (const char*)buf.begin();
	m_wrSeg = this->createWritableSegment(dirBaseName);
	m_segments.push_back(m_wrSeg);
}

void CompositeTable::openTable(fstring dir, fstring name) {
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	m_dir = dir.str();
	m_name = name.str();
	AutoGrownMemIO buf(1024);
	buf.printf("%s/%s/dbmeta.dfa", dir.c_str(), name.c_str(), 0);
	fstring metaFile = (const char*)buf.begin();
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
	for (size_t i = 0; i < minWrSeg; ++i) { // load readonly segments
		buf.rewind();
		buf.printf("%s/%s/rd-%04d", dir.c_str(), name.c_str(), int(i));
		fstring dirBaseName = (const char*)buf.begin();
		ReadableSegmentPtr seg(this->openReadonlySegment(dirBaseName));
		m_segments.push_back(seg);
	}
	for (size_t i = minWrSeg; i < segNum ; ++i) { // load writable segments
		buf.rewind();
		buf.printf("%s/%s/wr-%04d", dir.c_str(), name.c_str(), int(i));
		fstring dirBaseName = (const char*)buf.begin();
		ReadableSegmentPtr seg(this->openWritableSegment(dirBaseName));
		m_segments.push_back(seg);
	}
	if (minWrSeg < segNum && m_segments.back()->totalStorageSize() < m_maxWrSegSize) {
		auto seg = dynamic_cast<WritableSegment*>(m_segments.back().get());
		assert(NULL != seg);
		m_wrSeg.reset(seg);
	}
	else {
		buf.rewind();
		buf.printf("%s/%s/wr-%04d", dir.c_str(), name.c_str(), int(segNum));
		fstring dirBaseName = (const char*)buf.begin();
		m_wrSeg = this->createWritableSegment(dirBaseName);
	}
}

class CompositeTable::MyStoreIterator : public ReadableStore::StoreIterator {
	size_t  m_segIdx = 0;
	llong   m_subId = -1;
	StoreIteratorPtr m_curSegIter;
public:
	explicit MyStoreIterator(const CompositeTable* tab) {
		this->m_store.reset(const_cast<CompositeTable*>(tab));
		{
		// MyStoreIterator creation is rarely used, lock it by m_rwMutex
			tbb::queuing_rw_mutex::scoped_lock lock(tab->m_rwMutex, true);
			tab->m_tableScanningRefCount++;
			lock.downgrade_to_reader();
			assert(tab->m_segments.size() > 0);
			m_curSegIter = tab->m_segments[0]->createStoreIter();
		}
	}
	~MyStoreIterator() {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		{
			tbb::queuing_rw_mutex::scoped_lock lock(tab->m_rwMutex, true);
			tab->m_tableScanningRefCount--;
		}
	}
	bool increment() override {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		while (incrementImpl()) {
			llong subId = -1;
			m_curSegIter->getKeyVal(&subId, nullptr);
			assert(subId >= 0);
			assert(subId < tab->m_segments[m_segIdx]->numDataRows());
			if (m_segIdx < tab->m_segments.size()-1) {
				if (!tab->m_segments[m_segIdx]->m_isDel[subId])
					return true;
			}
			else {
				tbb::queuing_rw_mutex::scoped_lock lock(tab->m_rwMutex, false);
				if (!tab->m_segments[m_segIdx]->m_isDel[subId])
					return true;
			}
		}
		return false;
	}
	bool incrementImpl() {
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		if (!m_curSegIter->increment()) {
			tbb::queuing_rw_mutex::scoped_lock lock(tab->m_rwMutex, false);
			if (m_segIdx < tab->m_segments.size()-1) {
				m_segIdx++;
				tab->m_segments[m_segIdx]->createStoreIter().swap(m_curSegIter);
				bool ret = m_curSegIter->increment();
				assert(ret || tab->m_segments.size()-1 == m_segIdx);
				return ret;
			}
		}
		return true;
	}
	void getKeyVal(llong* idKey, valvec<byte>* val) const override {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		assert(m_segIdx < tab->m_segments.size());
		llong subId = -1;
		m_curSegIter->getKeyVal(&subId, val);
		assert(subId >= 0);
		*idKey = tab->m_rowNumVec[m_segIdx] + subId;
	}
};

ReadableStore::StoreIteratorPtr CompositeTable::createStoreIter() const {
	return new MyStoreIterator(this);
}

BaseContextPtr CompositeTable::createStoreContext() const {
	TableContextPtr ctx(new TableContext());
	size_t indexNum = this->getIndexNum();
	ctx->wrIndexContext.resize(indexNum);
	for (size_t i = 0; i < indexNum; ++i) {
		ctx->wrIndexContext[i] = m_wrSeg->m_indices[i]->createIndexContext();
	}
	ctx->wrStoreContext = m_wrSeg->createStoreContext();
	ctx->readonlyContext.reset(new ReadonlySegment::ReadonlyStoreContext());
	return ctx;
}

llong CompositeTable::totalStorageSize() const {
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	llong size = m_readonlyDataMemSize + m_wrSeg->dataStorageSize();
	for (size_t i = 0; i < getIndexNum(); ++i) {
		for (size_t i = 0; i < m_segments.size(); ++i) {
			size += m_segments[i]->totalStorageSize();
		}
	}
	size += m_wrSeg->totalStorageSize();
	return size;
}

llong CompositeTable::numDataRows() const {
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	return m_rowNumVec.back() + m_wrSeg->numDataRows();
}

llong CompositeTable::dataStorageSize() const {
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	return m_readonlyDataMemSize + m_wrSeg->dataStorageSize();
}

void
CompositeTable::getValue(llong id, valvec<byte>* val, BaseContextPtr& txn)
const {
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
	if (seg->getWritableStore())
		seg->getValue(subId, val, ttx.wrStoreContext);
	else
		seg->getValue(subId, val, ttx.readonlyContext);
}

void
CompositeTable::maybeCreateNewSegment(tbb::queuing_rw_mutex::scoped_lock& lock) {
	if (m_wrSeg->dataStorageSize() >= m_maxWrSegSize) {
		if (m_segments.size() == m_segments.capacity()) {
			THROW_STD(invalid_argument,
				"Reaching maxSegNum=%d", int(m_segments.capacity()));
		}
		llong newMaxRowNum = m_rowNumVec.back();
		m_rowNumVec.push_back(newMaxRowNum);
		AutoGrownMemIO buf(256);
		buf.printf("%s/%s/wr-%04d",
			m_dir.c_str(), m_name.c_str(), int(m_segments.size()));
		fstring dirBaseName = (const char*)buf.begin();
		WritableSegmentPtr seg = createWritableSegment(dirBaseName);
		lock.upgrade_to_writer();
		m_wrSeg = seg;
		m_segments.push_back(m_wrSeg);
		lock.downgrade_to_reader();
	}
}

llong
CompositeTable::insertRow(fstring row, bool syncIndex, BaseContextPtr& txn) {
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	return insertRowImpl(row, syncIndex, txn, lock);
}

llong
CompositeTable::insertRowImpl(fstring row, bool syncIndex,
							  BaseContextPtr& txn,
							  tbb::queuing_rw_mutex::scoped_lock& lock)
{
	maybeCreateNewSegment(lock);
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	llong wrBaseId = m_rowNumVec.back();
	llong subId;
	if (syncIndex) {
		m_rowSchema->parseRow(row, &ttx.cols1);
	}
	lock.upgrade_to_writer();
	if (m_deletedWrIdSet.empty() || m_tableScanningRefCount) {
		subId = m_wrSeg->append(row, txn);
		assert(subId == (llong)m_wrSeg->m_isDel.size());
		m_wrSeg->m_isDel.push_back(false);
		m_rowNumVec.back() = subId;
	}
	else {
		subId = m_deletedWrIdSet.pop_val();
		m_wrSeg->insert(subId, row, ttx.wrStoreContext);
		m_wrSeg->m_isDel.set0(subId);
	}
	if (syncIndex) {
		size_t indexNum = m_wrSeg->m_indices.size();
		for (size_t i = 0; i < indexNum; ++i) {
			auto wrIndex = m_wrSeg->m_indices[i].get();
			getIndexKey(i, ttx.cols1, &ttx.key1);
			wrIndex->insert(ttx.key1, subId, ttx.wrIndexContext[i]);
		}
	}
	llong id = wrBaseId + subId;
	return id;
}

llong
CompositeTable::replaceRow(llong id, fstring row, bool syncIndex,
						   BaseContextPtr& txn)
{
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size());
	assert(id < m_rowNumVec.back());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	if (j == m_rowNumVec.size()-1) { // id is in m_wrSeg
		if (syncIndex) {
			valvec<byte> &oldrow = ttx.row1, &oldkey = ttx.key1;
			valvec<byte> &newrow = ttx.row2, &newkey = ttx.key2;
			valvec<ColumnData>& oldcols = ttx.cols1;
			valvec<ColumnData>& newcols = ttx.cols2;
			m_wrSeg->getValue(subId, &oldrow, ttx.wrStoreContext);
			m_rowSchema->parseRow(oldrow, &oldcols);
			m_rowSchema->parseRow(newrow, &newcols);
			size_t indexNum = m_wrSeg->m_indices.size();
			lock.upgrade_to_writer();
			for (size_t i = 0; i < indexNum; ++i) {
				getIndexKey(i, oldcols, &oldkey);
				getIndexKey(i, newcols, &newkey);
				if (!valvec_equalTo(oldkey, newkey)) {
					auto wrIndex = m_wrSeg->m_indices[i].get();
					wrIndex->remove(oldkey, subId, ttx.wrIndexContext[i]);
					wrIndex->insert(newkey, subId, ttx.wrIndexContext[i]);
				}
			}
		} else {
			lock.upgrade_to_writer();
		}
		m_wrSeg->replace(subId, row, ttx.wrStoreContext);
		return id; // id is not changed
	}
	else {
		lock.upgrade_to_writer();
		m_wrSeg->m_isDel.set1(subId); // this is an atom op on x86(use bts)
		lock.downgrade_to_reader();
		return insertRowImpl(row, syncIndex, txn, lock); // id is changed
	}
}

void
CompositeTable::removeRow(llong id, bool syncIndex, BaseContextPtr& txn) {
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	if (j == m_rowNumVec.size()) {
		if (syncIndex) {
			valvec<byte> &row = ttx.row1, &key = ttx.key1;
			valvec<ColumnData>& columns = ttx.cols1;
			m_wrSeg->getValue(subId, &row, ttx.wrStoreContext);
			m_rowSchema->parseRow(row, &columns);
			lock.upgrade_to_writer();
			for (size_t i = 0; i < m_wrSeg->m_indices.size(); ++i) {
				auto wrIndex = m_wrSeg->m_indices[i].get();
				getIndexKey(i, columns, &key);
				wrIndex->remove(key, ttx.wrIndexContext[i]);
			}
		}
		m_wrSeg->remove(subId, ttx.wrStoreContext);
	}
	else {
		lock.upgrade_to_writer();
		m_wrSeg->m_isDel.set1(subId);
	}
}

void
CompositeTable::indexInsert(size_t indexId, fstring indexKey, llong id,
							BaseContextPtr& txn)
{
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	assert(id >= 0);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
	llong minWrRowNum = m_rowNumVec.back() + m_wrSeg->numDataRows();
	if (id < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", id, minWrRowNum);
	}
	llong subId = id - minWrRowNum;
	m_wrSeg->m_indices[indexId]->
		insert(indexKey, subId, ttx.wrIndexContext[indexId]);
}

void
CompositeTable::indexRemove(size_t indexId, fstring indexKey, llong id,
							BaseContextPtr& txn)
{
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
	llong minWrRowNum = m_rowNumVec.back() + m_wrSeg->numDataRows();
	if (id < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", id, minWrRowNum);
	}
	llong subId = id - minWrRowNum;
	m_wrSeg->m_indices[indexId]->
		remove(indexKey, subId, ttx.wrIndexContext[indexId]);
}

void
CompositeTable::indexReplace(size_t indexId, fstring indexKey,
							 llong oldId, llong newId,
							 BaseContextPtr& txn)
{
	assert(dynamic_cast<TableContext*>(txn.get()) != nullptr);
	TableContext& ttx = static_cast<TableContext&>(*txn);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	if (oldId == newId) {
		return;
	}
	tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
	llong minWrRowNum = m_rowNumVec.back() + m_wrSeg->numDataRows();
	if (oldId < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", oldId, minWrRowNum);
	}
	if (newId < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", newId, minWrRowNum);
	}
	llong oldSubId = oldId - minWrRowNum;
	llong newSubId = newId - minWrRowNum;
	m_wrSeg->m_indices[indexId]->
		replace(indexKey, oldSubId, newSubId, ttx.wrIndexContext[indexId]);
}

size_t CompositeTable::columnNum() const {
	return m_wrSeg->m_rowSchema->columnNum();
}

void
CompositeTable::getIndexKey(size_t indexId,
							const valvec<ColumnData>& columns,
							valvec<byte>* key)
const {
	assert(m_indexProjects.size() == m_wrSeg->m_indices.size());
	auto proj = m_indexProjects[indexId];
	const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(indexId);
	assert(proj.second - proj.first == schema.columnNum());
	if (schema.columnNum() == 1) {
		fstring k = columns[*proj.first];
		key->assign(k.udata(), k.size());
		return;
	}
	key->resize(0);
	for (auto i = proj.first; i < proj.second-1; ++i) {
		const ColumnData& col = columns[*i];
		key->append(col.all_data(), col.all_size());
	}
	const ColumnData& col = columns[proj.second[-1]];
	key->append(col.data(), col.size());
}

bool CompositeTable::compact() {
	ReadonlySegmentPtr newSeg;
	ReadableSegmentPtr srcSeg;
	AutoGrownMemIO buf(1024);
	size_t firstWrSegIdx, lastWrSegIdx;
	fstring dirBaseName;
	{
		tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, false);
		if (m_tableScanningRefCount > 0) {
			return false;
		}
		if (m_segments.size() < 2) {
			return false;
		}
		// don't include m_segments.back(), it is the working wrseg: m_wrSeg
		lastWrSegIdx = m_segments.size() - 1;
		firstWrSegIdx = lastWrSegIdx;
		for (; firstWrSegIdx > 0; firstWrSegIdx--) {
			if (m_segments[firstWrSegIdx-1]->getWritableStore() == nullptr)
				break;
		}
		if (firstWrSegIdx == lastWrSegIdx) {
			goto MergeReadonlySeg;
		} else {
			const char* dir = m_dir.c_str();
			const char* name = m_name.c_str();
			buf.printf("%s/%s/rd-%04d", dir, name, int(firstWrSegIdx));
			srcSeg = m_segments[firstWrSegIdx];
		}
	}
	dirBaseName = (const char*)buf.begin();
	newSeg = createReadonlySegment(dirBaseName);
	newSeg->convFrom(*srcSeg, *m_rowSchema);
	{
		tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
		m_segments[firstWrSegIdx] = newSeg;
	}

MergeReadonlySeg:
	// now don't merge
	return true;
}

} // namespace nark
