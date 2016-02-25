#include "db_table.hpp"
#include "db_segment.hpp"
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

#define NARK_DB_ENABLE_DFA_META
#if defined(NARK_DB_ENABLE_DFA_META)
#include <nark/fsa/nest_trie_dawg.hpp>
#endif

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
#endif
#include <fcntl.h>
#include <float.h>

#include "json.hpp"

#include <boost/scope_exit.hpp>

namespace nark { namespace db {

namespace fs = boost::filesystem;


ReadableSegment::ReadableSegment() {
	m_delcnt = 0;
	m_tobeDel = false;
	m_isDirty = false;
	m_bookDeletion = false;
	m_withPurgeBits = false;
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
	m_indices.clear(); // destroy index objects
	assert(!m_segDir.empty());
	if (m_tobeDel && !m_segDir.empty()) {
		fprintf(stderr, "INFO: remove: %s\n", m_segDir.string().c_str());
		try { boost::filesystem::remove_all(m_segDir); }
		catch (const std::exception& ex) {
			fprintf(stderr
				, "ERROR: ReadableSegment::~ReadableSegment(): ex.what = %s\n"
				, ex.what());
		// windows can not delete a hardlink when another hardlink
		// to the same file is in use
		//	FEBIRD_IF_DEBUG(abort(),;);
		}
	}
}

ReadonlySegment* ReadableSegment::getReadonlySegment() {
	return nullptr;
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
	if (m_isDelMmap && dir == m_segDir) {
		// need not to save, mmap is sys memory
		return;
	}
	fs::path isDelFpath = dir / "isDel";
	fs::path tmpFpath = isDelFpath + ".tmp";
	{
		NativeDataOutput<FileStream> file;
		file.open(tmpFpath.string().c_str(), "wb");
		file << uint64_t(m_isDel.size());
		file.ensureWrite(m_isDel.bldata(), m_isDel.mem_size());
	}
	fs::rename(tmpFpath, isDelFpath);
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
	assert(isDel.size() >= rowNum);
	isDel.risk_set_size(size_t(rowNum));
	return isDelMmap;
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
	this->saveRecordStore(segDir);
	this->saveIndices(segDir);
	this->saveIsDel(segDir);
}

///////////////////////////////////////////////////////////////////////////////

ReadonlySegment::ReadonlySegment() {
	m_dataMemSize = 0;
	m_totalStorageSize = 0;
	m_dataInflateSize = 0;
	m_isPurgedMmap = 0;
}
ReadonlySegment::~ReadonlySegment() {
	if (m_isPurgedMmap) {
		mmap_close(m_isPurgedMmap, m_isPurged.mem_size());
		m_isPurged.risk_release_ownership();
	}
	m_colgroups.clear();
}

ReadonlySegment* ReadonlySegment::getReadonlySegment() {
	return this;
}

llong ReadonlySegment::dataInflateSize() const {
	return m_dataMemSize;
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
	getValueByLogicId(id, val, txn);
}

// logic id is immutable
inline
size_t ReadonlySegment::getPhysicId(size_t logicId) const {
	if (m_isPurged.empty()) {
		return logicId;
	} else {
		return m_isPurged.rank0(logicId);
	}
}

size_t ReadonlySegment::getLogicId(size_t physicId) const {
	if (m_isPurged.empty()) {
		return physicId;
	} else {
		assert(physicId < m_isPurged.max_rank0());
		return m_isPurged.select0(physicId);
	}
}

void
ReadonlySegment::getValueByLogicId(size_t id, valvec<byte>* val, DbContext* ctx)
const {
	getValueByPhysicId(getPhysicId(id), val, ctx);
}

void
ReadonlySegment::getValueByPhysicId(size_t id, valvec<byte>* val, DbContext* ctx)
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
	assert(recId >= 0);
	recId = getPhysicId(size_t(recId));
	colsData->erase_all();
	ctx->offsets.resize_fill(2 * m_colgroups.size(), uint32_t(-1));
	ctx->buf1.erase_all();
	for(size_t i = 0; i < colsNum; ++i) {
		assert(colsId[i] < m_schema->m_rowSchema->columnNum());
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
	assert(recId >= 0);
	recId = getPhysicId(size_t(recId));
	assert(columnId < m_schema->m_rowSchema->columnNum());
	auto cp = m_schema->m_colproject[columnId];
	size_t colgroupId = cp.colgroupId;
	const Schema& schema = m_schema->getColgroupSchema(colgroupId);
//	printf("colprojects = %zd, colgroupId = %zd, schema.cols = %zd\n"
//		, m_schema->m_colproject.size(), colgroupId, schema.columnNum());
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
	MyStoreIterForward(const ReadonlySegment* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ReadonlySegment*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ReadonlySegment*>(m_store.get());
		while (size_t(m_id) < owner->m_isDel.size() && owner->m_isDel[m_id])
			m_id++;
		if (size_t(m_id) < owner->m_isDel.size()) {
			*id = m_id++;
			owner->getValueByLogicId(*id, val, m_ctx.get());
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
		while (m_id > 0 && owner->m_isDel[m_id-1])
			 --m_id;
		if (m_id > 0) {
			*id = --m_id;
			owner->getValueByLogicId(*id, val, m_ctx.get());
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

namespace {
	class FileDataIO : public ReadableStore {
		class FileStoreIter;
		FileStream m_fp;
		NativeDataOutput<OutputBuffer> m_obuf;
		size_t m_fixedLen;
		size_t m_fileSize;
		size_t m_dataSize;
		size_t m_rows;
		FileDataIO(const FileDataIO&) = delete;
	public:
		FileDataIO(size_t fixedLen) {
			intrusive_ptr_add_ref(this); // Trick: will never be deleted
			m_fp.attach(tmpfile());
			m_obuf.attach(&m_fp);
			m_fixedLen = fixedLen;
			m_fileSize = 0;
			m_dataSize = 0;
			m_rows = 0;
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
			m_rows++;
			m_dataSize += rowData.size();
		}
		void completeWrite() {
			m_obuf.flush();
			m_fp.rewind();
			m_fileSize = m_fp.size();
		}
		FileStream& fp() { return m_fp; }
		size_t fixedLen() const { return m_fixedLen; }

		void prepairRead(NativeDataInput<InputBuffer>& dio) {
			m_fp.disbuf();
			m_fp.rewind();
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

		llong dataInflateSize() const override { return m_dataSize; }
		llong dataStorageSize() const override { return m_fileSize; }
		llong numDataRows() const override { return m_rows; }

		double avgLen() const { return (m_fileSize + 0.1) / (m_rows + 0.1); }

		StoreIterator* createStoreIterForward(DbContext*) const override;
		StoreIterator* createStoreIterBackward(DbContext*) const override {
			THROW_STD(invalid_argument, "Not Implemented");
			return NULL;
		}
		void getValueAppend(llong id, valvec<byte>* rec, DbContext*)
		const override {
			assert(id >= 0);
#ifdef _MSC_VER
#else
			size_t flen = m_fixedLen;
			if (flen) {
				byte* p = rec->grow_no_init(flen);
				intptr_t nRead = pread(fileno(m_fp), p, flen, flen*id);
				return;
			}
#endif
			THROW_STD(invalid_argument, "Not Implemented");
		}
		void save(PathRef) const {
			THROW_STD(invalid_argument, "Not Implemented");
		}
		void load(PathRef) {
			THROW_STD(invalid_argument, "Not Implemented");
		}
	};

	class FileDataIO::FileStoreIter : public StoreIterator {
		NativeDataInput<InputBuffer> m_ibuf;
		llong m_id;
		llong m_rows;
		size_t m_fixedLen;
	public:
		explicit FileStoreIter(FileDataIO* store) {
			m_store.reset(store);
			store->prepairRead(m_ibuf);
			m_id = 0;
			m_rows = store->numDataRows();
			m_fixedLen = store->fixedLen();
		}
		bool increment(llong* id, valvec<byte>* val) override {
			if (m_id < m_rows) {
				if (m_fixedLen) {
					val->resize_no_init(m_fixedLen);
					m_ibuf.ensureRead(val->data(), m_fixedLen);
				}
				else {
					m_ibuf >> *val;
				}
				*id = m_id++;
				return true;
			}
			return false;
		}
		bool seekExact(llong  id, valvec<byte>* val) override {
			THROW_STD(invalid_argument, "Not Implemented");
			return false;
		}
		void reset() override {
			dynamic_cast<FileDataIO*>(m_store.get())->prepairRead(m_ibuf);
			m_id = 0;
		}
	};

	StoreIterator* FileDataIO::createStoreIterForward(DbContext*) const {
		return new FileStoreIter(const_cast<FileDataIO*>(this));
	}	

	class TempFileList : public valvec<FileDataIO> {
		const SchemaSet& m_schemaSet;
		valvec<byte> m_projRowBuf;
	public:
		TempFileList(const SchemaSet& schemaSet) : m_schemaSet(schemaSet) {
			this->reserve(schemaSet.m_nested.end_i());
			for (size_t i = 0; i < schemaSet.m_nested.end_i(); ++i) {
				SchemaPtr schema = schemaSet.m_nested.elem_at(i);
				this->unchecked_emplace_back(schema->getFixedRowLen());
			}
		}
		void writeColgroups(const valvec<fstring>& columns) {
			size_t colgroupNum = this->size();
			for (size_t i = 0; i < colgroupNum; ++i) {
				const Schema& schema = *m_schemaSet.m_nested.elem_at(i);
				schema.selectParent(columns, &m_projRowBuf);
				this->p[i].dioWrite(m_projRowBuf);
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

///@param iter record id from iter is physical id
///@param isDel new logical deletion mark
///@param isPurged physical deletion mark
///@note  physical deleted records must also be logical deleted
ReadableStore*
ReadonlySegment::buildDictZipStore(const Schema&, PathRef, StoreIterator& iter,
								   const bm_uint_t* isDel, const febitvec* isPurged) const {
	THROW_STD(invalid_argument,
		"Not Implemented, Only Implemented by DfaDbReadonlySegment");
}

/*
namespace {

// needs select0, it is slow
class PurgeMappingReadableStoreIterForward : public StoreIterator {
	StoreIteratorPtr m_iter;
	const rank_select_se* m_purgeBits;
public:
	PurgeMappingReadableStoreIterForward(const StoreIteratorPtr& iter, const rank_select_se& purgeBits) {
		assert(!purgeBits.empty());
		m_iter = iter;
		m_store = iter->m_store;
		m_purgeBits = &purgeBits;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		while (m_iter->increment(id, val)) {
			if (!m_purgeBits->is1(size_t(*id))) {
				*id = m_purgeBits->select0(size_t(*id));
				return true;
			}
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		if (m_iter->seekExact(id, val)) {
			if (!m_purgeBits->is1(size_t(*id))) {
				*id = m_purgeBits->select0(size_t(*id));
				return true;
			}
		}
		return false;
	}
	void reset() { m_iter->reset(); }
};

}
*/

void
ReadonlySegment::convFrom(CompositeTable* tab, size_t segIdx)
{
	auto tmpDir = m_segDir + ".tmp";
	fs::create_directories(tmpDir);

	DbContextPtr ctx(tab->createDbContext());
	ReadableSegmentPtr input;
	{
		MyRwLock lock(tab->m_rwMutex, true);
		input = tab->m_segments[segIdx];
		input->m_bookDeletion = true;
	}
	m_isDel = input->m_isDel; // make a copy, input->m_isDel[*] may be changed
//	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	llong logicRowNum = input->m_isDel.size();
	llong newRowNum = 0;
	assert(logicRowNum > 0);
	size_t indexNum = m_schema->getIndexNum();
	TempFileList colgroupTempFiles(*m_schema->m_colgroupSchemaSet);
{
	valvec<fstring> columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> buf;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx.get()));
	llong id = -1;
	while (iter->increment(&id, &buf) && id < logicRowNum) {
		assert(id >= 0);
		assert(id < logicRowNum);
		if (!m_isDel[id]) {
			m_schema->m_rowSchema->parseRow(buf, &columns);
			colgroupTempFiles.writeColgroups(columns);
			newRowNum++;
		}
	}
	llong inputRowNum = id + 1;
	assert(inputRowNum <= logicRowNum);
	if (inputRowNum < logicRowNum) {
		fprintf(stderr
			, "WARN: inputRows[real=%lld saved=%lld], some data have lost\n"
			, inputRowNum, logicRowNum);
		input->m_isDel.set1(inputRowNum, logicRowNum - inputRowNum);
		this->m_isDel.set1(inputRowNum, logicRowNum - inputRowNum);
	}
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	assert(newRowNum <= inputRowNum);
	assert(size_t(logicRowNum - newRowNum) == m_delcnt);
}
	// build index from temporary index files
	colgroupTempFiles.completeWrite();
	m_indices.resize(indexNum);
	m_colgroups.resize(m_schema->getColgroupNum());
	NativeDataInput<InputBuffer> dio;
	SortableStrVec strVec;
	for (size_t i = 0; i < indexNum; ++i) {
		const Schema& schema = m_schema->getIndexSchema(i);
		colgroupTempFiles[i].prepairRead(dio);
		colgroupTempFiles[i].collectData(dio, newRowNum, strVec);
		m_indices[i] = this->buildIndex(schema, strVec);
		m_colgroups[i] = m_indices[i]->getReadableStore();
		strVec.clear();
	}
	for (size_t i = indexNum; i < colgroupTempFiles.size(); ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		// dictZipLocalMatch is true by default
		// dictZipLocalMatch == false is just for experiment
		// dictZipLocalMatch should always be true in production
		// dictZipSampleRatio < 0 indicate don't use dictZip
		if (schema.m_dictZipLocalMatch && schema.m_dictZipSampleRatio >= 0.0) {
			double sRatio = schema.m_dictZipSampleRatio;
			double avgLen = colgroupTempFiles[i].avgLen();
			if (sRatio > 0 || (sRatio < FLT_EPSILON && avgLen > 100)) {
				StoreIteratorPtr
				iter = colgroupTempFiles[i].createStoreIterForward(NULL);
				m_colgroups[i] = buildDictZipStore(schema, tmpDir, *iter, NULL, NULL);
				continue;
			}
		}
		size_t maxMem = m_schema->m_readonlyDataMemSize;
		llong rows = 0;
		valvec<ReadableStorePtr> parts;
		colgroupTempFiles[i].prepairRead(dio);
		while (rows < newRowNum) {
			size_t rest = size_t(newRowNum - rows);
			rows += colgroupTempFiles[i].collectData(dio, rest, strVec, maxMem);
			parts.push_back(this->buildStore(schema, strVec));
			strVec.clear();
		}
		m_colgroups[i] = parts.size()==1 ? parts[0] : new MultiPartStore(parts);
	}

	completeAndReload(tab, segIdx, &*input);

	fs::rename(tmpDir, m_segDir);
	input->deleteSegment();
}

void
ReadonlySegment::completeAndReload(class CompositeTable* tab, size_t segIdx,
								   class ReadableSegment* input) {
	m_dataMemSize = 0;
	m_dataInflateSize = 0;
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		m_dataMemSize += m_colgroups[i]->dataStorageSize();
		m_dataInflateSize += m_colgroups[i]->dataInflateSize();
	}

	m_isPurged.assign(m_isDel);
	m_isPurged.build_cache(true, false); // need select0
	m_withPurgeBits = true;
	auto tmpDir = m_segDir + ".tmp";
	this->save(tmpDir);

	// reload as mmap
	m_isDel.clear();
	m_isPurged.clear();
	m_indices.erase_all();
	m_colgroups.erase_all();
	this->load(tmpDir);
	assert(this->m_isDel.size() == input->m_isDel.size());

	MyRwLock lock(tab->m_rwMutex, false);
	if (m_delcnt < input->m_delcnt) { // rows were deleted during build
		m_delcnt = input->m_delcnt;
		assert(input->m_bookDeletion);
		if (input->m_deletionList.size() * 1024 < m_isDel.size()) {
			auto dlist = input->m_deletionList.data();
			auto isDel = this->m_isDel.bldata();
			for (size_t i = 0, n = input->m_deletionList.size(); i < n; ++i) {
				nark_bit_set1(isDel, dlist[i]);
			}
#if !defined(NDEBUG)
			size_t computed_delcnt1 = this->m_isDel.popcnt();
			size_t computed_delcnt2 = input->m_isDel.popcnt();
			assert(computed_delcnt1 == input->m_delcnt);
			assert(computed_delcnt2 == input->m_delcnt);
#endif
		}
		else {
			m_isDel.risk_memcpy(input->m_isDel);
#if !defined(NDEBUG)
			size_t computed_delcnt1 = this->m_isDel.popcnt();
			size_t computed_delcnt2 = input->m_isDel.popcnt();
			assert(computed_delcnt1 == input->m_delcnt);
			assert(computed_delcnt2 == input->m_delcnt);
#endif
		}
	}
	lock.upgrade_to_writer();
	tab->m_segments[segIdx] = this;
}

static inline
void pushRecord(SortableStrVec& strVec, const ReadableStore& store,
				const bm_uint_t* isDel, llong logicId, llong physicId,
				size_t fixlen, DbContext* ctx) {
	assert(physicId <= logicId);
	if (!nark_bit_test(isDel, logicId)) {
		size_t oldsize = strVec.str_size();
		store.getValueAppend(physicId, &strVec.m_strpool, ctx);
		if (!fixlen) {
			SortableStrVec::SEntry ent;
			ent.offset = oldsize;
			ent.length = strVec.str_size() - oldsize;
			ent.seq_id = uint32_t(strVec.m_index.size());
			strVec.m_index.push_back(ent);
		}
	}
}
static
fs::path renameToBackupFromDir(PathRef segDir) {
	fs::path backupDir;
	for (int tmpNum = 0; ; ++tmpNum) {
		char szBuf[32];
		snprintf(szBuf, sizeof(szBuf), ".backup-%d", tmpNum);
		backupDir = segDir + szBuf;
		if (!fs::exists(backupDir))
			break;
		fprintf(stderr, "ERROR: existed %s\n", backupDir.string().c_str());
	}
	try { fs::rename(segDir, backupDir); }
	catch (const std::exception& ex) {
		std::string strDir = segDir.string();
		fprintf(stderr
			, "ERROR: rename(%s, %s.backup), ex.what = %s\n"
			, strDir.c_str(), strDir.c_str(), ex.what());
		abort();
	}
	return backupDir;
}

void
ReadonlySegment::purgeDeletedRecords(CompositeTable* tab, size_t segIdx) {
	DbContextPtr ctx(tab->createDbContext());
	ReadonlySegmentPtr input;
	{
		MyRwLock lock(tab->m_rwMutex, true);
		input = dynamic_cast<ReadonlySegment*>(&*tab->m_segments[segIdx]);
		tab->m_purgeStatus = CompositeTable::PurgeStatus::purging;
		input->m_bookDeletion = true;
	}
	fprintf(stderr, "INFO: purging %s\n", input->m_segDir.string().c_str());
	m_isDel = input->m_isDel; // make a copy, input->m_isDel[*] may be changed
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	m_indices.resize(m_schema->getIndexNum());
	m_colgroups.resize(m_schema->getColgroupNum());
	auto tmpSegDir = m_segDir + ".tmp";
	fs::create_directories(tmpSegDir);
{
/*
	size_t logicRows = input->m_isDel.size();
	size_t physicId = 0;
	AutoFree<uint32_t> physicToLogic(logicRows, UINT32_MAX);
	for (size_t logicId = 0; logicId < logicRows; ++logicId) {
	}
*/
	for (size_t i = 0; i < m_indices.size(); ++i) {
		m_indices[i] = purgeIndex(i, input.get(), ctx.get());
		m_colgroups[i] = m_indices[i]->getReadableStore();
	}
}
	for (size_t i = m_indices.size(); i < m_colgroups.size(); ++i) {
		m_colgroups[i] = purgeColgroup(i, input.get(), ctx.get(), tmpSegDir);
	}
	completeAndReload(tab, segIdx, &*input);

	assert(input->m_segDir == this->m_segDir);
	fs::path backupDir = renameToBackupFromDir(input->m_segDir);
	{
		MyRwLock lock(tab->m_rwMutex, true);
		input->m_segDir = backupDir;
		input->deleteSegment(); // will delete backupDir
		tab->m_segments[segIdx] = this;
	}
	try { fs::rename(tmpSegDir, m_segDir); }
	catch (const std::exception& ex) {
		fs::rename(backupDir, m_segDir);
		std::string strDir = m_segDir.string();
		fprintf(stderr
			, "ERROR: rename(%s.tmp, %s), ex.what = %s\n"
			, strDir.c_str(), strDir.c_str(), ex.what());
		abort();
	}
}

ReadableIndexPtr
ReadonlySegment::purgeIndex(size_t indexId, ReadonlySegment* input, DbContext* ctx) {
	llong inputRowNum = input->m_isDel.size();
	assert(inputRowNum > 0);
	const bm_uint_t* isDel = m_isDel.bldata();
	SortableStrVec strVec;
	const Schema& schema = m_schema->getIndexSchema(indexId);
	const size_t  fixlen = schema.getFixedRowLen();
	const auto& store = *input->m_indices[indexId]->getReadableStore();
	StoreIteratorPtr iter = store.createStoreIterForward(ctx);
	if (false && iter) { // for performance, don't use iter
		valvec<byte_t> rec;
		llong physicId;
		while (iter->increment(&physicId, &rec)) {
			size_t logicId = input->getLogicId(size_t(physicId));
			if (!nark_bit_test(isDel, logicId)) {
				if (fixlen)
					strVec.m_strpool.append(rec);
				else
					strVec.push_back(rec);
			}
		}
	}
	else {
		const bm_uint_t* purgeBits = input->m_isPurged.bldata();
		llong physicId = 0;
		for(llong logicId = 0; logicId < inputRowNum; ++logicId) {
			if (!nark_bit_test(purgeBits, logicId)) {
				pushRecord(strVec, store, isDel, logicId, physicId, fixlen, ctx);
				physicId++;
			}
		}
	}
	return this->buildIndex(schema, strVec);
}

ReadableStorePtr
ReadonlySegment::purgeColgroup(size_t colgroupId, ReadonlySegment* input, DbContext* ctx, PathRef tmpSegDir) {
	assert(m_isDel.size() == input->m_isDel.size());
	SortableStrVec strVec;
	const bm_uint_t* isDel = m_isDel.bldata();
	llong inputRowNum = input->m_isDel.size();
	const Schema& schema = m_schema->getColgroupSchema(colgroupId);
	const auto& colgroup = *input->m_colgroups[colgroupId];
	if (schema.m_dictZipLocalMatch && schema.m_dictZipSampleRatio >= 0.0) {
		double sRatio = schema.m_dictZipSampleRatio;
		double avgLen = 1.0 * colgroup.dataInflateSize() / colgroup.numDataRows();
		if (sRatio > 0 || (sRatio < FLT_EPSILON && avgLen > 100)) {
			StoreIteratorPtr iter = colgroup.createStoreIterForward(ctx);
			if (!iter) {
				iter = colgroup.createDefaultStoreIterForward(ctx);
			}
			return buildDictZipStore(schema, tmpSegDir, *iter, isDel, &input->m_isPurged);
		}
	}
	size_t fixlen = schema.getFixedRowLen();
	size_t maxMem = size_t(m_schema->m_readonlyDataMemSize);
	valvec<ReadableStorePtr> parts;
	auto partsPushRecord = [&](const ReadableStore& store, llong logicId, llong physicId) {
		if (nark_unlikely(strVec.mem_size() >= maxMem)) {
			parts.push_back(this->buildStore(schema, strVec));
			strVec.clear();
		}
		pushRecord(strVec, store, isDel, logicId, physicId, fixlen, ctx);
	};
	const bm_uint_t* oldpurgeBits = input->m_isPurged.bldata();
	assert(!oldpurgeBits || input->m_isPurged.size() == m_isDel.size());
	if (auto cgparts = dynamic_cast<const MultiPartStore*>(&colgroup)) {
		llong logicId = 0;
		llong basePhysicId = 0;
		for (size_t j = 0; j < cgparts->numParts(); ++j) {
			auto& partStore = cgparts->getPart(j);
			llong partRows = partStore.numDataRows();
			llong subPhysicId = 0;
			while (logicId < inputRowNum && subPhysicId < partRows) {
				if (!oldpurgeBits || !nark_bit_test(oldpurgeBits, logicId)) {
					llong physicId = basePhysicId + subPhysicId;
					partsPushRecord(partStore, logicId, physicId);
					subPhysicId++;
				}
				logicId++;
			}
			assert(subPhysicId == partRows);
			basePhysicId += partRows;
		}
	}
	else {
		llong physicId = 0;
		for(llong logicId = 0; logicId < inputRowNum; ++logicId) {
			if (!oldpurgeBits || !nark_bit_test(oldpurgeBits, logicId)) {
				partsPushRecord(colgroup, logicId, physicId);
				physicId++;
			}
		}
#if !defined(NDEBUG)
		if (oldpurgeBits) { assert(physicId == input->m_isPurged.max_rank0()); }
		else			  { assert(physicId == m_isDel.size()); }
#endif
	}
	if (strVec.str_size() > 0) {
		parts.push_back(this->buildStore(schema, strVec));
	}
	return parts.size()==1 ? parts[0] : new MultiPartStore(parts);
}

void ReadonlySegment::load(PathRef segDir) {
	ReadableSegment::load(segDir);
	removePurgeBitsForCompactIdspace(segDir);
}

void ReadonlySegment::removePurgeBitsForCompactIdspace(PathRef segDir) {
	assert(m_isPurgedMmap == NULL);
	assert(m_isPurged.empty());
	PathRef purgeFpath = segDir / "IsPurged.rs";
	if (!fs::exists(purgeFpath)) {
		return;
	}
	size_t bytes = 0;
	m_isPurgedMmap = (byte*)mmap_load(purgeFpath.string(), &bytes);
	m_isPurged.risk_mmap_from(m_isPurgedMmap, bytes);
	assert(m_isPurged.size() == m_isDel.size());
	assert(m_withPurgeBits); // for self test debug
	if (m_withPurgeBits) {
		// logical record id will be m_isPurged.select0(physical id)
		return;
	}
	// delete IsPurged and compact bitmap m_isDel
	size_t oldRows = m_isDel.size();
	size_t newRows = m_isPurged.max_rank0();
	size_t newId = 0;
	febitvec newIsDel(newRows, false);
	for (size_t oldId = 0; oldId < oldRows; ++oldId) {
		if (!m_isPurged[newId]) {
			if (m_isDel[oldId])
				newIsDel.set1(newId);
			++newId;
		}
		else {
			assert(m_isDel[oldId]);
		}
	}
	assert(newId == newRows);
	m_isDel.risk_set_size(newRows);
	m_isDel.risk_memcpy(newIsDel);
	*(uint64_t*)m_isDelMmap = newRows;
//	mmap_close(m_isDelMmap, 8 + m_isDel.mem_size());
//	m_isDel.risk_release_ownership();
	mmap_close(m_isPurgedMmap, bytes);
	m_isPurged.risk_release_ownership();
	fs::remove(purgeFpath);
}

void ReadonlySegment::savePurgeBits(PathRef segDir) const {
	if (m_isPurgedMmap && segDir == m_segDir)
		return;
	PathRef purgeFpath = segDir / "IsPurged.rs";
	FileStream fp(purgeFpath.string().c_str(), "wb");
	fp.ensureWrite(m_isPurged.data(), m_isPurged.mem_size());
}

void ReadonlySegment::save(PathRef segDir) const {
	assert(!segDir.empty());
	if (m_tobeDel) {
		return;
	}
	savePurgeBits(segDir);
	ReadableSegment::save(segDir);
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
		assert(m_indices[i]); // index must have be loaded
		auto store = m_indices[i]->getReadableStore();
		assert(nullptr != store);
		m_colgroups[i] = store;
	}
	SortableStrVec files;
	for(auto ent : fs::directory_iterator(segDir)) {
		std::string  fname = ent.path().filename().string();
		if (!fstring(fname).endsWith("-dict")) {
			files.push_back(fname);
		}
	}
	files.sort();
	for (size_t i = indexNum; i < colgroupNum; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		std::string prefix = "colgroup-" + schema.m_name;
		size_t lo = files.lower_bound(prefix);
		if (lo >= files.size() || !files[lo].startsWith(prefix)) {
			THROW_STD(invalid_argument, "missing: %s",
				(segDir / prefix).string().c_str());
		}
		fstring fname = files[lo];
		if (fname.substr(prefix.size()).startsWith(".0000.")) {
			valvec<ReadableStorePtr> parts;
			size_t j = lo;
			while (j < files.size() && (fname = files[j]).startsWith(prefix)) {
				size_t partIdx = lcast(fname.substr(prefix.size()+1));
				assert(partIdx == j - lo);
				if (partIdx != j - lo) {
					THROW_STD(invalid_argument, "missing part: %s.%zd",
						(segDir / prefix).string().c_str(), j - lo);
				}
				parts.push_back(ReadableStore::openStore(segDir, fname));
				++j;
			}
			m_colgroups[i] = new MultiPartStore(parts);
		}
		else {
			m_colgroups[i] = ReadableStore::openStore(segDir, fname);
		}
	}
}

void ReadonlySegment::closeFiles() {
	if (m_isDelMmap) {
		size_t bitBytes = m_isDel.capacity()/8;
		mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
		m_isDelMmap = nullptr;
		m_isDel.risk_release_ownership();
	}
	m_indices.clear();
	m_colgroups.clear();
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

void WritableSegment::pushIsDel(bool val) {
	const size_t ChunkBits = FEBIRD_IF_DEBUG(4*1024, 1*1024*1024);
	if (nark_unlikely(nullptr == m_isDelMmap)) {
		assert(m_isDel.size() == 0);
		assert(m_isDel.capacity() == 0);
		m_isDel.resize_fill(ChunkBits - 64, 0); // 64 is for uint64 header
		saveIsDel(m_segDir);
		m_isDel.clear();
		m_isDelMmap = loadIsDel_aux(m_segDir, m_isDel);
		((uint64_t*)m_isDelMmap)[0] = 0;
		m_isDel.risk_set_size(0);
		m_delcnt = 0;
	}
	else if (nark_unlikely(m_isDel.size() == m_isDel.capacity())) {
		assert((64 + m_isDel.size()) % ChunkBits == 0);
		size_t newCap = ((64+m_isDel.size()+2*ChunkBits-1) & ~(ChunkBits-1));
		mmap_close(m_isDelMmap, sizeof(uint64_t) + m_isDel.mem_size());
		m_isDelMmap = nullptr;
		m_isDel.risk_release_ownership();
		std::string fpath = (m_segDir / "isDel").string();
#ifdef _MSC_VER
	{
		Auto_close_fd fd(::_open(fpath.c_str(), O_CREAT|O_BINARY|O_RDWR));
		if (fd < 0) {
			THROW_STD(logic_error
				, "FATAL: ::_open(%s, O_CREAT|O_BINARY|O_RDWR) = %s"
				, fpath.c_str(), strerror(errno));
		}
		int err = ::_chsize_s(fd, newCap/8);
		if (err) {
			THROW_STD(logic_error, "FATAL: ::_chsize_s(%s, %zd) = %s"
				, fpath.c_str(), newCap/8, strerror(errno));
		}
	}
#else
		int err = ::truncate(fpath.c_str(), newCap/8);
		if (err) {
			THROW_STD(logic_error, "FATAL: ::truncate(%s, %zd) = %s"
				, fpath.c_str(), newCap/8, strerror(errno));
		}
#endif
		m_isDelMmap = loadIsDel_aux(m_segDir, m_isDel);
		assert(nullptr != m_isDelMmap);
	}
	assert(m_isDel.size() < m_isDel.capacity());
	assert(m_isDel.size() == size_t(((uint64_t*)m_isDelMmap)[0]));
	m_isDel.unchecked_push_back(val);
	((uint64_t*)m_isDelMmap)[0] = m_isDel.size();
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
