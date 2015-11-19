#define NARK_DB_ENABLE_DFA_META
#if defined(NARK_DB_ENABLE_DFA_META)
#include <nark/fsa/nest_trie_dawg.hpp>
#endif
#include "db_table.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/util/linebuf.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>
#include <nlohmann/json.hpp>
#include <boost/filesystem.hpp>

namespace nark {

namespace fs = boost::filesystem;

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
CompositeTable::createTable(fstring dir,
							SchemaPtr rowSchema,
							SchemaSetPtr indexSchemaSet)
{
	assert(!dir.empty());
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

	AutoGrownMemIO buf;
	m_wrSeg = this->createWritableSegment(getSegPath("wr", 0, buf));
	m_segments.push_back(m_wrSeg);
}

void CompositeTable::load(fstring dir) {
	if (!m_segments.empty()) {
		THROW_STD(invalid_argument, "Invalid: m_segment.size=%ld is not empty",
			long(m_segments.size()));
	}
	if (m_rowSchema && m_rowSchema->columnNum()) {
		THROW_STD(invalid_argument, "Invalid: rowSchemaColumns=%ld is not empty",
			long(m_rowSchema->columnNum()));
	}
	m_dir = dir.str();
	loadMetaJson(dir);
	for (auto& x : fs::directory_iterator(fs::path(m_dir))) {
		std::string fname = x.path().filename().string();
		long segIdx = -1;
		ReadableSegmentPtr seg;
		if (sscanf(fname.c_str(), "wr-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			seg = openWritableSegment(x.path().string());
		}
		else if (sscanf(fname.c_str(), "rd-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			seg = createReadonlySegment();
			seg->load(x.path().string());
		}
		else {
			continue;
		}
		if (m_segments.size() <= size_t(segIdx)) {
			m_segments.resize(segIdx + 1);
		}
		m_segments[segIdx] = seg;
	}
	if (m_segments.size() == 0) {
		// THROW_STD(invalid_argument, "no any segment found");
		// allow user create an table dir which just contains json meta file
		AutoGrownMemIO buf;
		m_wrSeg = createWritableSegment(getSegPath("wr", 0, buf));
		m_segments.push_back(m_wrSeg);
	}
	else {
		auto seg = dynamic_cast<WritableSegment*>(m_segments.back().get());
		assert(NULL != seg);
		m_wrSeg.reset(seg); // old wr seg at end
	}
}

#if defined(NARK_DB_ENABLE_DFA_META)
void CompositeTable::loadMetaDFA(fstring dir) {
	std::string metaFile = dir + "/dbmeta.dfa";
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
	m_segments.reserve(std::max(DEFAULT_maxSegNum, segNum*2));
	m_rowNumVec.reserve(std::max(DEFAULT_maxSegNum+1, segNum*2 + 1));

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
	llong rowNum = 0;
	AutoGrownMemIO buf(1024);
	for (size_t i = 0; i < minWrSeg; ++i) { // load readonly segments
		ReadableSegmentPtr seg(createReadonlySegment());
		seg->load(getSegPath("rd", i, buf));
		rowNum += seg->numDataRows();
		m_segments.push_back(seg);
		m_rowNumVec.push_back(rowNum);
	}
	for (size_t i = minWrSeg; i < segNum ; ++i) { // load writable segments
		ReadableSegmentPtr seg(openWritableSegment(getSegPath("wr", i, buf)));
		rowNum += seg->numDataRows();
		m_segments.push_back(seg);
		m_rowNumVec.push_back(rowNum);
	}
	if (minWrSeg < segNum && m_segments.back()->totalStorageSize() < m_maxWrSegSize) {
		auto seg = dynamic_cast<WritableSegment*>(m_segments.back().get());
		assert(NULL != seg);
		m_wrSeg.reset(seg); // old wr seg at end
	}
	else {
		m_wrSeg = createWritableSegment(getSegPath("wr", segNum, buf));
		m_segments.push_back(m_wrSeg); // new empty wr seg at end
		m_rowNumVec.push_back(rowNum); // m_rowNumVec[-2] == m_rowNumVec[-1]
	}
}

void CompositeTable::saveMetaDFA(fstring dir) const {
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

void CompositeTable::loadMetaJson(fstring dir) {
	std::string jsonFile = dir + "/dbmeta.json";
	LineBuf alljson;
	alljson.read_all(jsonFile.c_str());

	using nlohmann::json;
	const json meta = json::parse(alljson.p);
	const json& rowSchema = meta["RowSchema"];
	const json& cols = rowSchema["columns"];
	if (!cols.is_array()) {
		THROW_STD(invalid_argument, "json RowSchema/columns must be an array");
	}
	m_rowSchema.reset(new Schema());
	for (auto iter = cols.cbegin(); iter != cols.cend(); ++iter) {
		const auto& col = *iter;
		std::string name = col["name"];
		std::string type = col["type"];
		std::transform(type.begin(), type.end(), type.begin(), &::tolower);
		ColumnMeta colmeta;
		colmeta.type = Schema::parseColumnType(type);
		if (ColumnType::Fixed == colmeta.type) {
			colmeta.fixedLen = col["length"];
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
		for (const auto& col : index) {
			const std::string& colname = col;
			const size_t k = m_rowSchema->getColumnId(colname);
			if (k == m_rowSchema->columnNum()) {
				THROW_STD(invalid_argument,
					"colname=%s is not in RowSchema", colname.c_str());
			}
			indexSchema->m_columnsMeta.
				insert_i(colname, m_rowSchema->getColumnMeta(k));
		}
		indexSchema->compile();
		m_indexSchemaSet->m_nested.insert_i(indexSchema);
	}
	m_indexSchemaSet->compileSchemaSet();
}

void CompositeTable::saveMetaJson(fstring dir) const {
	using nlohmann::json;
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
	std::string jsonFile = dir + "/dbmeta.json";
	std::string jsonStr = meta.dump(2);
	FileStream fp(jsonFile.c_str(), "w");
	fp.ensureWrite(jsonStr.data(), jsonStr.size());
}

class CompositeTable::MyStoreIterator : public StoreIterator {
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

StoreIteratorPtr CompositeTable::createStoreIter() const {
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
		if (lock.upgrade_to_writer() ||
			// if upgrade_to_writer fails, it means the lock has been
			// temporary released and re-acquired, so we need check
			// the condition again
			m_wrSeg->dataStorageSize() >= m_maxWrSegSize)
		{
			if (m_segments.size() == m_segments.capacity()) {
				THROW_STD(invalid_argument,
					"Reaching maxSegNum=%d", int(m_segments.capacity()));
			}
			// createWritableSegment should be fast, other wise the lock time
			// may be too long
			AutoGrownMemIO buf(512);
			size_t newSegIdx = m_segments.size();
			m_wrSeg = myCreateWritableSegment(newSegIdx, buf);
			m_segments.push_back(m_wrSeg);
			llong newMaxRowNum = m_rowNumVec.back();
			m_rowNumVec.push_back(newMaxRowNum);
		}
		lock.downgrade_to_reader();
	}
}

WritableSegmentPtr
CompositeTable::myCreateWritableSegment(size_t segIdx, AutoGrownMemIO& buf)
const {
	fstring segDir = getSegPath("wr", segIdx, buf);
	fs::create_directories(segDir.c_str());
	return createWritableSegment(segDir);
}
WritableSegmentPtr
CompositeTable::myCreateWritableSegment(fstring segDir) const {
	fs::create_directories(segDir.c_str());
	return createWritableSegment(segDir);
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
	if (syncIndex) {
		m_rowSchema->parseRow(row, &ttx.cols1);
	}
	lock.upgrade_to_writer();
	llong subId;
	llong wrBaseId = m_rowNumVec.end()[-2];
	if (m_wrSeg->m_deletedWrIdSet.empty() || m_tableScanningRefCount) {
		subId = m_wrSeg->append(row, txn);
		assert(subId == (llong)m_wrSeg->m_isDel.size());
		m_wrSeg->m_isDel.push_back(false);
		m_rowNumVec.back() = subId;
	}
	else {
		subId = m_wrSeg->m_deletedWrIdSet.pop_val();
		m_wrSeg->replace(subId, row, ttx.wrStoreContext);
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
	assert(j > 0);
	assert(j < m_rowNumVec.size());
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
		// mark old subId as deleted
		m_segments[j-1]->m_isDel.set1(subId); // is atom on x86(use bts)
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
	assert(j < m_rowNumVec.size());
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
				wrIndex->remove(key, subId, ttx.wrIndexContext[i]);
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
	assert(oldId != newId);
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
		}
	}
	for (size_t i = firstWrSegIdx; i < lastWrSegIdx; ++i) {
		srcSeg = m_segments[firstWrSegIdx];
		newSeg = createReadonlySegment();
		newSeg->convFrom(*srcSeg, *m_rowSchema);
		fstring segDir = getSegPath("rd", i, buf);
		fs::create_directories(segDir.c_str());
		newSeg->save(segDir);
		{
			tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
			m_segments[firstWrSegIdx] = newSeg;
		}
		fs::remove_all(getSegPath("wr", i, buf).c_str());
	}

MergeReadonlySeg:
	// now don't merge
	return true;
}

fstring
CompositeTable::getSegPath(fstring type, size_t segIdx, AutoGrownMemIO& buf)
const {
	return getSegPath2(m_dir, type, segIdx, buf);
}

fstring
CompositeTable::getSegPath2(fstring dir, fstring type, size_t segIdx,
							AutoGrownMemIO& buf)
const {
	buf.rewind();
	size_t len = buf.printf("%s/%s-%04ld",
			dir.c_str(), type.c_str(), long(segIdx));
	return fstring(buf.c_str(), len);
}

void CompositeTable::save(fstring dir) const {
	if (dir == m_dir) {
		fprintf(stderr, "WARN: save self(%s), skipped\n", dir.c_str());
		return;
	}
	try {
		tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
		m_tableScanningRefCount++;
		size_t segNum = m_segments.size();

		// save segments except m_wrSeg
		lock.release(); // doesn't need any lock
		AutoGrownMemIO buf(1024);
		for (size_t segIdx = 0; segIdx < segNum-1; ++segIdx) {
			auto seg = m_segments[segIdx];
			if (seg->getWritableStore())
				seg->save(getSegPath2(dir, "wr", segIdx, buf));
			else
				seg->save(getSegPath2(dir, "rd", segIdx, buf));
		}

		// save the remained segments, new segment may created during
		// time pieriod of saving previous segments
		lock.acquire(m_rwMutex, false); // need read lock
		size_t segNum2 = m_segments.size();
		for (size_t segIdx = segNum-1; segIdx < segNum2; ++segIdx) {
			auto seg = m_segments[segIdx];
			assert(seg->getWritableStore());
			seg->save(getSegPath2(dir, "wr", segIdx, buf));
		}
		lock.upgrade_to_writer();
		m_tableScanningRefCount--;
	}
	catch (...) {
		tbb::queuing_rw_mutex::scoped_lock lock(m_rwMutex, true);
		m_tableScanningRefCount--;
		throw;
	}
	saveMetaJson(dir);
}

} // namespace nark
