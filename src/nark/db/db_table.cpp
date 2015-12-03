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
#include <nark/util/fstrvec.hpp>
#include <nark/util/sortable_strvec.hpp>
#include <boost/filesystem.hpp>

#include "json.hpp"

namespace nark { namespace db {

namespace fs = boost::filesystem;

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
	m_dir = dir.str();
	compileSchema();

	AutoGrownMemIO buf;
	m_wrSeg = this->myCreateWritableSegment(getSegPath("wr", 0, buf));
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
			std::string segDir = x.path().string();
			auto wseg = openWritableSegment(segDir);
			wseg->m_segDir = segDir;
			seg = wseg;
		}
		else if (sscanf(fname.c_str(), "rd-%ld", &segIdx) > 0) {
			if (segIdx < 0) {
				THROW_STD(invalid_argument, "invalid segment: %s", fname.c_str());
			}
			seg = myCreateReadonlySegment(x.path().string());
			seg->load(seg->m_segDir);
		}
		else {
			continue;
		}
		if (m_segments.size() <= size_t(segIdx)) {
			m_segments.resize(segIdx + 1);
		}
		m_segments[segIdx] = seg;
	}
	if (m_segments.size() == 0 || !m_segments.back()->getWritableStore()) {
		// THROW_STD(invalid_argument, "no any segment found");
		// allow user create an table dir which just contains json meta file
		AutoGrownMemIO buf;
		size_t segIdx = m_segments.size();
		m_wrSeg = myCreateWritableSegment(getSegPath("wr", segIdx, buf));
		m_segments.push_back(m_wrSeg);
	}
	else {
		auto seg = dynamic_cast<WritableSegment*>(m_segments.back().get());
		assert(NULL != seg);
		m_wrSeg.reset(seg); // old wr seg at end
	}
	m_rowNumVec.resize_no_init(m_segments.size() + 1);
	llong baseId = 0;
	for (size_t i = 0; i < m_segments.size(); ++i) {
		m_rowNumVec[i] = baseId;
		baseId += m_segments[i]->numDataRows();
	}
	m_rowNumVec.back() = baseId; // the end guard
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
	compileSchema();
	llong rowNum = 0;
	AutoGrownMemIO buf(1024);
	for (size_t i = 0; i < minWrSeg; ++i) { // load readonly segments
		ReadableSegmentPtr seg(myCreateReadonlySegment(getSegPath("rd", i, buf)));
		seg->load(seg->m_segDir);
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
		m_wrSeg = myCreateWritableSegment(getSegPath("wr", segNum, buf));
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
	using nark::json;
	const json meta = json::parse(alljson.p + // UTF8 BOM Check
						(fstring(alljson.p, 3) == "\xEF\xBB\xBF" ? 3 : 0));
	const json& rowSchema = meta["RowSchema"];
	const json& cols = rowSchema["columns"];
	m_rowSchema.reset(new Schema());
	for (auto iter = cols.cbegin(); iter != cols.cend(); ++iter) {
		const auto& col = iter.value();
		std::string name = iter.key();
		std::string type = col["type"];
		std::transform(type.begin(), type.end(), type.begin(), &::tolower);
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

		if (indexSchema->m_isUnique)
			m_uniqIndices.push_back(ib.first);
		else
			m_multIndices.push_back(ib.first);
	}
	compileSchema();
}

void CompositeTable::saveMetaJson(fstring dir) const {
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
	std::string jsonFile = dir + "/dbmeta.json";
	std::string jsonStr = meta.dump(2);
	FileStream fp(jsonFile.c_str(), "w");
	fp.ensureWrite(jsonStr.data(), jsonStr.size());
}

struct CompareBy_baseId {
	template<class T>
	typename boost::enable_if_c<(sizeof(((T*)0)->baseId) >= 4), bool>::type
	operator()(const T& x, llong y) const { return x.baseId < y; }
	template<class T>
	typename boost::enable_if_c<(sizeof(((T*)0)->baseId) >= 4), bool>::type
	operator()(llong x, const T& y) const { return x < y.baseId; }
	bool operator()(llong x, llong y) const { return x < y; }
};

class CompositeTable::MyStoreIterator : public StoreIterator {
	size_t m_segIdx = 0;
	DbContextPtr m_ctx;
	struct OneSeg {
		ReadableSegmentPtr seg;
		StoreIteratorPtr   iter;
		llong  baseId;
	};
	valvec<OneSeg> m_segs;
public:
	explicit MyStoreIterator(const CompositeTable* tab, DbContext* ctx) {
		this->m_store.reset(const_cast<CompositeTable*>(tab));
		this->m_ctx.reset(ctx);
		{
		// MyStoreIterator creation is rarely used, lock it by m_rwMutex
			MyRwLock lock(tab->m_rwMutex, false);
			m_segs.resize(tab->m_segments.size() + 1);
			for (size_t i = 0; i < m_segs.size()-1; ++i) {
				m_segs[i].seg = tab->m_segments[i];
				m_segs[i].baseId = tab->m_rowNumVec[i];
			}
			m_segs.back().baseId = tab->m_rowNumVec.back();
			lock.upgrade_to_writer();
			tab->m_tableScanningRefCount++;
			assert(tab->m_segments.size() > 0);
		}
	}
	~MyStoreIterator() {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		{
			MyRwLock lock(tab->m_rwMutex, true);
			tab->m_tableScanningRefCount--;
		}
	}
	bool increment(llong* id, valvec<byte>* val) override {
		assert(dynamic_cast<const CompositeTable*>(m_store.get()));
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		llong subId = -1;
		while (incrementNoCheckDel(&subId, val)) {
			assert(subId >= 0);
			assert(subId < m_segs[m_segIdx].seg->numDataRows());
			llong baseId = m_segs[m_segIdx].baseId;
			if (m_segIdx < m_segs.size()-2) {
				if (!m_segs[m_segIdx].seg->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
			else {
				MyRwLock lock(tab->m_rwMutex, false);
				if (!tab->m_segments[m_segIdx]->m_isDel[subId]) {
					*id = baseId + subId;
					assert(*id < tab->numDataRows());
					return true;
				}
			}
		}
		return false;
	}
	inline bool incrementNoCheckDel(llong* subId, valvec<byte>* val) {
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		if (nark_unlikely(!m_segs[m_segIdx].iter))
			 m_segs[m_segIdx].iter = m_segs[m_segIdx].seg->createStoreIterForward(&*m_ctx);
		if (!m_segs[m_segIdx].iter->increment(subId, val)) {
			MyRwLock lock(tab->m_rwMutex, false);
			m_segs.resize(tab->m_segments.size() + 1);
			for (size_t i = 0; i < m_segs.size()-1; ++i) {
				if (m_segs[i].seg != tab->m_segments[i]) {
					m_segs[i].seg = tab->m_segments[i];
					m_segs[i].iter.reset();
					m_segs[i].baseId = tab->m_rowNumVec[i];
				}
			}
			m_segs.back().baseId = tab->m_rowNumVec.back();
			if (m_segIdx < m_segs.size()-2) {
				m_segIdx++;
				auto& cur = m_segs[m_segIdx];
				if (nark_unlikely(!cur.iter))
					cur.iter = cur.seg->createStoreIterForward(&*m_ctx);
				bool ret = cur.iter->increment(subId, val);
				if (ret) {
					assert(*subId < m_segs[m_segIdx].seg->numDataRows());
				}
				return ret;
			}
			return false;
		}
		assert(*subId < m_segs[m_segIdx].seg->numDataRows());
		return true;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto tab = static_cast<const CompositeTable*>(m_store.get());
		MyRwLock lock(tab->m_rwMutex, false);
		size_t upp = upper_bound_0(m_segs.data(), m_segs.size(), id, CompareBy_baseId());
		if (upp < m_segs.size()) {
			m_segIdx = upp-1;
			llong subId = id - m_segs[upp-1].baseId;
			auto& cur = m_segs[upp-1];
			if (!cur.seg->m_isDel[subId]) {
				if (nark_unlikely(!cur.iter))
					cur.iter = cur.seg->createStoreIterForward(&*m_ctx);
				return cur.iter->seekExact(subId, val);
			}
		}
		return false;
	}
	void reset() override {
		for (size_t i = 0; i < m_segs.size()-1; ++i) {
			m_segs[i].iter->reset();
		}
		m_segs.ende(1).baseId = m_segs.ende(2).baseId +
								m_segs.ende(2).seg->numDataRows();
		m_segIdx = 0;
	}
};

StoreIterator* CompositeTable::createStoreIterForward(DbContext* ctx) const {
	assert(m_rowSchema);
	assert(m_indexSchemaSet);
	return new MyStoreIterator(this, ctx);
}

llong CompositeTable::totalStorageSize() const {
	MyRwLock lock(m_rwMutex, false);
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
	return m_rowNumVec.back();
}

llong CompositeTable::dataStorageSize() const {
	MyRwLock lock(m_rwMutex, false);
	return m_readonlyDataMemSize + m_wrSeg->dataStorageSize();
}

void
CompositeTable::getValueAppend(llong id, valvec<byte>* val, DbContext* txn)
const {
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size() + 1);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = m_segments[j-1].get();
	seg->getValueAppend(subId, val, txn);
}

void
CompositeTable::maybeCreateNewSegment(MyRwLock& lock) {
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
			m_wrSeg = myCreateWritableSegment(getSegPath("wr", newSegIdx, buf));
			m_segments.push_back(m_wrSeg);
			llong newMaxRowNum = m_rowNumVec.back();
			m_rowNumVec.push_back(newMaxRowNum);
		}
		lock.downgrade_to_reader();
	}
}

ReadonlySegment*
CompositeTable::myCreateReadonlySegment(fstring segDir) const {
	std::unique_ptr<ReadonlySegment> seg(createReadonlySegment(segDir));
	seg->m_segDir = segDir.str();
	seg->m_rowSchema = m_rowSchema;
	seg->m_indexSchemaSet = m_indexSchemaSet;
	seg->m_nonIndexRowSchema = m_nonIndexRowSchema;
	return seg.release();
}

WritableSegment*
CompositeTable::myCreateWritableSegment(fstring segDir) const {
	fs::create_directories(segDir.c_str());
	std::unique_ptr<WritableSegment> seg(createWritableSegment(segDir));
	seg->m_segDir = segDir.str();
	seg->m_rowSchema = m_rowSchema;
	seg->m_indexSchemaSet = m_indexSchemaSet;
	seg->m_nonIndexRowSchema = m_nonIndexRowSchema;
	if (seg->m_indices.empty()) {
		seg->m_indices.resize(m_indexSchemaSet->m_nested.end_i());
		for (size_t i = 0; i < seg->m_indices.size(); ++i) {
			const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
			std::string colnames = schema.joinColumnNames(',');
			std::string indexPath = segDir + "/index-" + colnames;
			seg->m_indices[i] = seg->createIndex(indexPath, schema);
		}
	}
	return seg.release();
}

llong
CompositeTable::insertRow(fstring row, DbContext* txn) {
	if (txn->syncIndex) { // parseRow doesn't need lock
		m_rowSchema->parseRow(row, &txn->cols1);
	}
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	return insertRowImpl(row, txn, lock);
}

llong
CompositeTable::insertRowImpl(fstring row, DbContext* txn, MyRwLock& lock) {
	maybeCreateNewSegment(lock);
	if (txn->syncIndex) {
		size_t oldSegNum = m_segments.size();
	//	lock.release(); // seg[0, oldSegNum-1) need read lock?
		if (!insertCheckSegDup(0, oldSegNum-1, txn))
			return -1;
	//	lock.acquire(m_rwMutex, true); // write lock
		if (!lock.upgrade_to_writer()) {
			// check for new added segment(should be very rare)
			if (oldSegNum != m_segments.size()) {
				if (!insertCheckSegDup(oldSegNum-1, m_segments.size()-1, txn))
					return -1;
			}
		}
	}
	else {
		lock.upgrade_to_writer();
	}
	llong subId;
	llong wrBaseId = m_rowNumVec.end()[-2];
	if (m_wrSeg->m_deletedWrIdSet.empty() || m_tableScanningRefCount) {
		subId = m_wrSeg->append(row, txn);
		assert(subId == (llong)m_wrSeg->m_isDel.size());
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				m_wrSeg->remove(subId, txn);
				return -1; // fail
			}
		}
		m_wrSeg->m_isDel.push_back(false);
		m_rowNumVec.back() = wrBaseId + subId + 1;
	}
	else {
		subId = m_wrSeg->m_deletedWrIdSet.back();
		if (txn->syncIndex) {
			if (!insertSyncIndex(subId, txn)) {
				return -1; // fail
			}
		}
		m_wrSeg->m_deletedWrIdSet.pop_back();
		m_wrSeg->replace(subId, row, txn);
		m_wrSeg->m_isDel.set0(subId);
		m_wrSeg->m_delcnt--;
	}
	llong id = wrBaseId + subId;
	return wrBaseId + subId;
}

bool
CompositeTable::insertCheckSegDup(size_t begSeg, size_t endSeg, DbContext* txn) {
	if (begSeg == endSeg)
		return true;
	for (size_t segIdx = begSeg; segIdx < endSeg; ++segIdx) {
		auto seg = &*m_segments[segIdx];
		for(size_t i = 0; i < m_uniqIndices.size(); ++i) {
			size_t indexId = m_uniqIndices[i];
			const Schema& iSchema = getIndexSchema(indexId);
			auto rIndex = seg->getReadableIndex(indexId);
			assert(iSchema.m_isUnique);
			iSchema.selectParent(txn->cols1, &txn->key1);
			if (rIndex->exists(txn->key1)) {
				// std::move makes it no temps
				txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
							+ ", in freezen seg: " + seg->m_segDir;
			//	txn->errMsg += ", rowData=";
			//	txn->errMsg += m_rowSchema->toJsonStr(row);
				return false;
			}
		}
	}
	return true;
}

bool CompositeTable::insertSyncIndex(llong subId, DbContext* txn) {
	// first try insert unique index
	size_t i = 0;
	for (; i < m_uniqIndices.size(); ++i) {
		size_t indexId = m_uniqIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = getIndexSchema(indexId);
		assert(iSchema.m_isUnique);
		iSchema.selectParent(txn->cols1, &txn->key1);
		if (!wrIndex->insert(txn->key1, subId, txn)) {
			txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
						+ ", in writing seg: " + m_wrSeg->m_segDir;
			goto Fail;
		}
	}
	// insert non-unique index
	for (i = 0; i < m_multIndices.size(); ++i) {
		size_t indexId = m_multIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = getIndexSchema(indexId);
		assert(!iSchema.m_isUnique);
		iSchema.selectParent(txn->cols1, &txn->key1);
		wrIndex->insert(txn->key1, subId, txn);
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = m_uniqIndices[i];
		auto wrIndex = m_wrSeg->m_indices[indexId].get();
		const Schema& iSchema = getIndexSchema(indexId);
		iSchema.selectParent(txn->cols1, &txn->key1);
		wrIndex->remove(txn->key1, subId, txn);
	}
	return false;
}

llong
CompositeTable::replaceRow(llong id, fstring row, DbContext* txn) {
	m_rowSchema->parseRow(row, &txn->cols1); // new row
	MyRwLock lock(m_rwMutex, false);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	assert(id < m_rowNumVec.back());
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j > 0);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	auto seg = &*m_segments[j-1];
	bool directUpgrade = true;
	if (txn->syncIndex) {
		size_t oldSegNum = m_segments.size();
		if (seg->m_isDel[subId]) { // behave as insert
			if (!insertCheckSegDup(0, oldSegNum-1, txn))
				return -1;
			if (!lock.upgrade_to_writer()) {
				// check for new added segment(should be very rare)
				if (oldSegNum != m_segments.size()) {
					if (!insertCheckSegDup(oldSegNum-1, m_segments.size()-1, txn))
						return -1;
				}
				directUpgrade = false;
			}
		}
		else {
			seg->getValue(subId, &txn->row2, txn);
			m_rowSchema->parseRow(txn->row2, &txn->cols2); // old row

			if (!replaceCheckSegDup(0, oldSegNum-1, txn))
				return -1;
			if (!lock.upgrade_to_writer()) {
				// check for new added segment(should be very rare)
				if (oldSegNum != m_segments.size()) {
					if (!replaceCheckSegDup(oldSegNum-1, m_segments.size()-1, txn))
						return -1;
				}
				directUpgrade = false;
			}
		}
	}
	else {
		directUpgrade = lock.upgrade_to_writer();
	}
	if (!directUpgrade) {
		j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
		assert(j > 0);
		assert(j < m_rowNumVec.size());
		baseId = m_rowNumVec[j-1];
		subId = id - baseId;
		seg = &*m_segments[j-1];
	}
	if (j == m_rowNumVec.size()-1) { // id is in m_wrSeg
		if (txn->syncIndex) {
			replaceSyncIndex(subId, txn, lock);
		}
		m_wrSeg->replace(subId, row, txn);
		return id; // id is not changed
	}
	else {
		// mark old subId as deleted
		seg->m_isDel.set1(subId);
		seg->m_delcnt++;
		lock.downgrade_to_reader();
		return insertRowImpl(row, txn, lock); // id is changed
	}
}

bool
CompositeTable::replaceCheckSegDup(size_t begSeg, size_t endSeg, DbContext* txn) {
	for(size_t i = 0; i < m_uniqIndices.size(); ++i) {
		size_t indexId = m_uniqIndices[i];
		const Schema& iSchema = getIndexSchema(indexId);
		for (size_t segIdx = begSeg; segIdx < endSeg; ++segIdx) {
			auto seg = &*m_segments[segIdx];
			auto rIndex = seg->getReadableIndex(indexId);
			assert(iSchema.m_isUnique);
			iSchema.selectParent(txn->cols1, &txn->key1);
			if (rIndex->exists(txn->key1)) {
				// std::move makes it no temps
				txn->errMsg = "DupKey=" + iSchema.toJsonStr(txn->key1)
							+ ", in freezen seg: " + seg->m_segDir;
			//	txn->errMsg += ", rowData=";
			//	txn->errMsg += m_rowSchema->toJsonStr(row);
				return false;
			}
		}
	}
	return true;
}

bool
CompositeTable::replaceSyncIndex(llong subId, DbContext* txn, MyRwLock& lock) {
	size_t i = 0;
	for (; i < m_uniqIndices.size(); ++i) {
		size_t indexId = m_uniqIndices[i];
		const Schema& iSchema = getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = m_wrSeg->m_indices[indexId].get();
			if (!wrIndex->insert(txn->key1, subId, txn)) {
				goto Fail;
			}
		}
	}
	for (i = 0; i < m_uniqIndices.size(); ++i) {
		size_t indexId = m_uniqIndices[i];
		const Schema& iSchema = getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = m_wrSeg->m_indices[indexId].get();
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	for (i = 0; i < m_multIndices.size(); ++i) {
		size_t indexId = m_multIndices[i];
		const Schema& iSchema = getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = m_wrSeg->m_indices[indexId].get();
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
			if (!wrIndex->insert(txn->key1, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	return true;
Fail:
	for (size_t j = i; j > 0; ) {
		--j;
		size_t indexId = m_uniqIndices[j];
		const Schema& iSchema = getIndexSchema(indexId);
		iSchema.selectParent(txn->cols2, &txn->key2); // old
		iSchema.selectParent(txn->cols1, &txn->key1); // new
		if (!valvec_equalTo(txn->key1, txn->key2)) {
			auto wrIndex = &*m_wrSeg->m_indices[indexId];
			if (!wrIndex->remove(txn->key2, subId, txn)) {
				assert(0);
				THROW_STD(invalid_argument, "should be a bug");
			}
		}
	}
	return false;
}

bool
CompositeTable::removeRow(llong id, DbContext* txn) {
	assert(txn != nullptr);
	MyRwLock lock(m_rwMutex, true);
	assert(m_rowNumVec.size() == m_segments.size()+1);
	size_t j = upper_bound_0(m_rowNumVec.data(), m_rowNumVec.size(), id);
	assert(j < m_rowNumVec.size());
	llong baseId = m_rowNumVec[j-1];
	llong subId = id - baseId;
	assert(m_segments[j-1]->m_isDel.is0(subId));
	if (m_segments[j-1]->m_isDel.is0(subId)) {
	}
	if (j == m_rowNumVec.size()) {
		if (txn->syncIndex) {
			valvec<byte> &row = txn->row1, &key = txn->key1;
			valvec<fstring>& columns = txn->cols1;
			m_wrSeg->getValue(subId, &row, txn);
			m_rowSchema->parseRow(row, &columns);
			for (size_t i = 0; i < m_wrSeg->m_indices.size(); ++i) {
				auto wrIndex = m_wrSeg->m_indices[i].get();
				const Schema& iSchema = *m_indexSchemaSet->m_nested.elem_at(i);
				iSchema.selectParent(columns, &key);
				wrIndex->remove(key, subId, txn);
			}
		}
		m_wrSeg->remove(subId, txn);
		if (m_wrSeg->m_isDel.size()-1 == subId) {
			m_wrSeg->m_isDel.resize(m_wrSeg->m_isDel.size()-1);
		}
	}
	else {
		m_wrSeg->m_isDel.set1(subId);
		m_wrSeg->m_delcnt++;
	}
	return true;
}

bool
CompositeTable::indexInsert(size_t indexId, fstring indexKey, llong id,
							DbContext* txn)
{
	assert(txn != nullptr);
	assert(id >= 0);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	MyRwLock lock(m_rwMutex, true);
	llong minWrRowNum = m_rowNumVec.back() + m_wrSeg->numDataRows();
	if (id < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", id, minWrRowNum);
	}
	llong subId = id - minWrRowNum;
	return m_wrSeg->m_indices[indexId]->insert(indexKey, subId, txn);
}

bool
CompositeTable::indexRemove(size_t indexId, fstring indexKey, llong id,
							DbContext* txn)
{
	assert(txn != nullptr);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	MyRwLock lock(m_rwMutex, true);
	llong minWrRowNum = m_rowNumVec.back() + m_wrSeg->numDataRows();
	if (id < minWrRowNum) {
		THROW_STD(invalid_argument,
			"Invalid rowId=%lld, minWrRowNum=%lld", id, minWrRowNum);
	}
	llong subId = id - minWrRowNum;
	return m_wrSeg->m_indices[indexId]->remove(indexKey, subId, txn);
}

bool
CompositeTable::indexReplace(size_t indexId, fstring indexKey,
							 llong oldId, llong newId,
							 DbContext* txn)
{
	assert(txn != nullptr);
	if (indexId >= m_indexSchemaSet->m_nested.end_i()) {
		THROW_STD(invalid_argument,
			"Invalid indexId=%lld, indexNum=%lld",
			llong(indexId), llong(m_indexSchemaSet->m_nested.end_i()));
	}
	assert(oldId != newId);
	if (oldId == newId) {
		return true;
	}
	MyRwLock lock(m_rwMutex, true);
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
	return m_wrSeg->m_indices[indexId]->replace(indexKey, oldSubId, newSubId, txn);
}

class TableIndexIterUnOrdered : public IndexIterator {
	CompositeTablePtr m_tab;
	DbContextPtr m_ctx;
	size_t m_indexId;
	size_t m_segIdx;
	struct OneSeg {
		ReadableSegmentPtr seg;
		IndexIteratorPtr   iter;
	};
	valvec<OneSeg> m_segs;

	void init(CompositeTable* tab, size_t indexId) {
		m_tab.reset(tab);
		m_indexId = indexId;
		m_segIdx = -1;
		m_ctx = tab->createDbContext();
		{
			MyRwLock lock(tab->m_rwMutex);
			tab->m_tableScanningRefCount++;
		}
		refresh();
	}

	void refresh() {
		MyRwLock lock(m_tab->m_rwMutex, false);
		m_segs.resize(m_tab->m_segments.size());
		for (size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			assert(m_tab->m_segments[i]);
			if (cur.seg == m_tab->m_segments[i]) {
				if (cur.iter)
					cur.iter->reset(nullptr);
			} else {
				cur.seg = m_tab->m_segments[i];
				cur.iter = nullptr;
			}
		}
	}

public:
	TableIndexIterUnOrdered(const CompositeTable* tab, size_t indexId) {
		init(const_cast<CompositeTable*>(tab), indexId);
	}
	~TableIndexIterUnOrdered() {
		MyRwLock lock(m_tab->m_rwMutex);
		m_tab->m_tableScanningRefCount--;
	}
	void reset(PermanentablePtr p2) override {
		CompositeTablePtr tab;
		if (!p2 || m_tab == (tab = dynamic_cast<CompositeTable*>(&*p2))) {
			m_segIdx = -1;
			refresh();
			return;
		}
		{
			MyRwLock lock(m_tab->m_rwMutex);
			m_tab->m_tableScanningRefCount--;
		}
		init(tab.detach(), m_indexId);
	}
	bool increment(llong* id, valvec<byte>* key) override {
		if (nark_unlikely(size_t(-1) == m_segIdx)) {
			m_segIdx = 0;
		}
		if (nark_unlikely(!m_segs[m_segIdx].iter)) {
			m_segs[m_segIdx].iter = m_segs[m_segIdx].seg->
				getReadableIndex(m_indexId)->createIndexIter(&*m_ctx);
		}
		llong subId = -1;
		while (incrementNoCheckDel(&subId, key)) {
			if (!isDeleted(subId)) {
				llong baseId = m_tab->m_rowNumVec[m_segIdx];
				*id = baseId + subId;
				return true;
			}
		}
		return false;
	}
	bool incrementNoCheckDel(llong* subId, valvec<byte>* key) {
		bool ret = m_segs[m_segIdx].iter->increment(subId, key);
		if (nark_unlikely(!ret)) {
			if (m_segs.size()-1 == m_segIdx) {
				assert(m_segs.size() <= m_tab->m_segments.size());
				if (m_segs.size() == m_tab->m_segments.size()) {
					return false;
				}
				refresh();
			}
			m_segIdx++;
			ret = m_segs[m_segIdx].iter->increment(subId, key);
		}
		return ret;
	}
	bool decrement(llong* id, valvec<byte>* key) override {
		if (nark_unlikely(size_t(-1) == m_segIdx)) {
			MyRwLock lock(m_tab->m_rwMutex, false);
			m_segIdx = m_segs.size() - 1;
		}
		llong subId = -1;
		while (decrementNoCheckDel(&subId, key)) {
			if (!isDeleted(subId)) {
				llong baseId = m_tab->m_rowNumVec[m_segIdx];
				*id = baseId + subId;
				return true;
			}
		}
		return false;
	}
	bool isDeleted(llong subId) {
		if (m_tab->m_segments.size()-1 == m_segIdx) {
			MyRwLock lock(m_tab->m_rwMutex, false);
			return m_segs[m_segIdx].seg->m_isDel[subId];
		} else {
			return m_segs[m_segIdx].seg->m_isDel[subId];
		}
	}
	bool decrementNoCheckDel(llong* subId, valvec<byte>* key) {
		bool ret = m_segs[m_segIdx].iter->increment(subId, key);
		if (nark_unlikely(!ret)) {
			if (m_segs.size()-1 == m_segIdx) {
				assert(m_segs.size() <= m_tab->m_segments.size());
				if (m_segs.size() == m_tab->m_segments.size()) {
					return false;
				}
				refresh();
			}
			m_segIdx++;
			ret = m_segs[m_segIdx].iter->increment(subId, key);
		}
		return ret;
	}
	bool seekExact(fstring key) override {
		auto schema = m_tab->m_indexSchemaSet->m_nested.elem_at(m_indexId);
		size_t fixlen = schema->getFixedRowLen();
		assert(fixlen == 0 || key.size() == fixlen);
		if (fixlen && key.size() != fixlen) {
			THROW_STD(invalid_argument,
				"bad key, len=%d is not same as fixed-len=%d",
				key.ilen(), int(fixlen));
		}
		for (size_t i = m_segs.size(); i > 0; --i) {
			size_t segIdx = i - 1;
			if (m_segs[segIdx].iter->seekExact(key)) {
				if (m_segIdx != segIdx && size_t(-1) != m_segIdx) {
					m_segs[m_segIdx].iter->reset(nullptr);
				}
				m_segIdx = segIdx;
				return true;
			}
		}
		return false;
	}
	bool seekLowerBound(fstring key) override {
		return seekExact(key);
	}
};

class TableIndexIterOrdered : public IndexIterator {
	// bi-directional multiple segment iterator can not use heap or loser-tree
	CompositeTablePtr m_tab;
	DbContextPtr m_ctx;
	size_t m_indexId;
	struct OneSeg {
		ReadableSegmentPtr seg;
		IndexIteratorPtr   iter;
		valvec<byte>       data;
		llong              subId;
	};
	valvec<OneSeg> m_segs;
	valvec<byte> m_keyBuf;
	class SetKeyCompare {
		TableIndexIterOrdered* owner;
		const Schema* schema;
	public:
		bool operator()(size_t x, size_t y) const {
			const auto& xkey = owner->m_segs[x].data;
			const auto& ykey = owner->m_segs[y].data;
			if (xkey.empty()) {
				if (ykey.empty())
					return x < y;
				else
					return true; // xkey < ykey
			}
			if (ykey.empty())
				return false;
			int r = schema->compareData(xkey, ykey);
			if (r) return r < 0;
			else   return x < y;
		}
		SetKeyCompare(TableIndexIterOrdered* o, const Schema* s)
			: owner(o), schema(s) {}
	};
	friend class SetKeyCompare;
	std::set<size_t, SetKeyCompare> m_set;

	void init(CompositeTable* tab, size_t indexId) {
		m_tab.reset(tab);
		m_indexId = indexId;
		m_ctx = tab->createDbContext();
		{
			MyRwLock lock(tab->m_rwMutex);
			tab->m_tableScanningRefCount++;
		}
		refresh();
	}

	void refresh() {
		MyRwLock lock(m_tab->m_rwMutex, false);
		m_segs.resize(m_tab->m_segments.size());
		for (size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			assert(m_tab->m_segments[i]);
			if (cur.seg != m_tab->m_segments[i]) {
				cur.seg = m_tab->m_segments[i];
				cur.iter = cur.seg->getReadableIndex(m_indexId)->createIndexIter(&*m_ctx);
				if (!cur.data.empty()) {
					m_set.erase(i);
					cur.iter->seekLowerBound(cur.data);
				}
				if (cur.iter->increment(&cur.subId, &cur.data)) {
					m_set.insert(i);
				}
			}
		}
	}

public:
	TableIndexIterOrdered(const CompositeTable* tab, size_t indexId)
	 : m_set(SetKeyCompare(this, &*tab->m_indexSchemaSet->m_nested.elem_at(indexId)))
	{
		assert(tab->m_indexSchemaSet->m_nested.elem_at(indexId)->m_isOrdered);
		init(const_cast<CompositeTable*>(tab), indexId);
	}
	~TableIndexIterOrdered() {
		MyRwLock lock(m_tab->m_rwMutex);
		m_tab->m_tableScanningRefCount--;
	}
	void reset(PermanentablePtr p2) override {
		if (!p2) {
			refresh();
			return;
		}
		auto tab = dynamic_cast<CompositeTable*>(p2.get());
		if (m_tab.get() == tab) {
			refresh();
			return;
		}
		{
			MyRwLock lock(m_tab->m_rwMutex);
			m_tab->m_tableScanningRefCount--;
		}
		m_set.clear();
		m_segs.clear();
		init(tab, m_indexId);
	}
	bool increment(llong* id, valvec<byte>* key) override {
		while (!m_set.empty()) {
			llong subId;
			size_t segIdx = incrementNoCheckDel(&subId);
			if (!isDeleted(segIdx, subId)) {
				assert(subId < m_segs[segIdx].seg->numDataRows());
				llong baseId = m_tab->m_rowNumVec[segIdx];
				*id = baseId + subId;
				assert(*id < m_tab->numDataRows());
				if (key)
					key->swap(m_keyBuf);
				return true;
			}
		}
		return false;
	}
	size_t incrementNoCheckDel(llong* subId) {
		assert(!m_set.empty());
		size_t segIdx = *m_set.begin();
		m_set.erase(m_set.begin());
		auto& cur = m_segs[segIdx];
		if (cur.iter->increment(subId, &m_keyBuf)) {
			m_set.insert(segIdx);
		} else {
			m_keyBuf.erase_all();
		}
		std::swap(cur.subId, *subId); // wa! it's cool
		m_keyBuf.swap(cur.data);          // wa! it's cool
		return segIdx;
	}
	bool decrement(llong* id, valvec<byte>* key) override {
		while (!m_set.empty()) {
			llong subId;
			size_t segIdx = decrementNoCheckDel(&subId);
			if (!isDeleted(segIdx, subId)) {
				assert(subId < m_segs[segIdx].seg->numDataRows());
				llong baseId = m_tab->m_rowNumVec[segIdx];
				*id = baseId + subId;
				if (key)
					key->swap(m_keyBuf);
				return true;
			}
		}
		return false;
	}
	size_t decrementNoCheckDel(llong* subId) {
		assert(!m_set.empty());
		auto last = m_set.end();
		size_t segIdx = *--last;
		m_set.erase(last);
		auto& cur = m_segs[segIdx];
		if (cur.iter->decrement(subId, &m_keyBuf)) {
			m_set.insert(segIdx);
		} else {
			m_keyBuf.erase_all();
		}
		std::swap(cur.subId, *subId); // wa! it's cool
		m_keyBuf.swap(cur.data);      // wa! it's cool
		return segIdx;
	}
	bool isDeleted(size_t segIdx, llong subId) {
		if (m_tab->m_segments.size()-1 == segIdx) {
			MyRwLock lock(m_tab->m_rwMutex, false);
			return m_segs[segIdx].seg->m_isDel[subId];
		} else {
			return m_segs[segIdx].seg->m_isDel[subId];
		}
	}
	bool seekExact(fstring key) override {
		return seekLowerBound(key);
	}
	bool seekLowerBound(fstring key) override {
		const Schema& schema = *m_tab->m_indexSchemaSet->m_nested.elem_at(m_indexId);
		size_t fixlen = schema.getFixedRowLen();
		assert(fixlen == 0 || key.size() == fixlen);
		if (fixlen && key.size() != fixlen) {
			THROW_STD(invalid_argument,
				"bad key, len=%d is not same as fixed-len=%d",
				key.ilen(), int(fixlen));
		}
		m_set.clear();
		bool ret = false;
		for(size_t i = 0; i < m_segs.size(); ++i) {
			auto& cur = m_segs[i];
			if (cur.iter->seekLowerBound(key)) {
				ret = true;
			}
			if (cur.iter->increment(&cur.subId, &cur.data)) {
				m_set.insert(i);
			}
		}
		return ret;
	}
};

IndexIteratorPtr CompositeTable::createIndexIter(size_t indexId) const {
	assert(indexId < m_indexSchemaSet->m_nested.end_i());
	const Schema& iSchema = *m_indexSchemaSet->m_nested.elem_at(indexId);
	if (iSchema.m_isOrdered)
		return new TableIndexIterOrdered(this, indexId);
	else
		return new TableIndexIterUnOrdered(this, indexId);
}

IndexIteratorPtr CompositeTable::createIndexIter(fstring indexCols) const {
	size_t indexId = m_indexSchemaSet->m_nested.find_i(indexCols);
	if (m_indexSchemaSet->m_nested.end_i() == indexId) {
		THROW_STD(invalid_argument, "index: %s not exists", indexCols.c_str());
	}
	return createIndexIter(indexId);
}

bool CompositeTable::compact() {
	ReadonlySegmentPtr newSeg;
	ReadableSegmentPtr srcSeg;
	AutoGrownMemIO buf(1024);
	size_t firstWrSegIdx, lastWrSegIdx;
	fstring dirBaseName;
	DbContextPtr ctx(createDbContext());
	{
		MyRwLock lock(m_rwMutex, false);
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
		fstring segDir = getSegPath("rd", i, buf);
		newSeg = myCreateReadonlySegment(segDir);
		newSeg->convFrom(*srcSeg, &*ctx);
		{
			MyRwLock lock(m_rwMutex, true);
			m_segments[firstWrSegIdx] = newSeg;
		}
		srcSeg->deleteSegment();
	}

MergeReadonlySeg:
	// now don't merge
	return true;
}

void CompositeTable::clear() {
	MyRwLock lock(m_rwMutex, true);
	for (size_t i = 0; i < m_segments.size(); ++i) {
		m_segments[i]->deleteSegment();
		m_segments[i] = nullptr;
	}
	m_segments.clear();
	m_rowNumVec.clear();
}

void CompositeTable::flush() {
	valvec<ReadableSegmentPtr> segsCopy;
	{
		MyRwLock lock(m_rwMutex, false);
		segsCopy.assign(m_segments);
	}
	for (size_t i = 0; i < segsCopy.size(); ++i) {
		auto seg = segsCopy[i].get();
		auto wStore = seg->getWritableStore();
		if (wStore) {
			auto wSeg = dynamic_cast<WritableSegment*>(seg);
			wSeg->flushSegment();
		}
	}
}

std::string CompositeTable::toJsonStr(fstring row) const {
	return m_rowSchema->toJsonStr(row);
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
		MyRwLock lock(m_rwMutex, true);
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
		MyRwLock lock(m_rwMutex, true);
		m_tableScanningRefCount--;
		throw;
	}
	saveMetaJson(dir);
}

} } // namespace nark::db
