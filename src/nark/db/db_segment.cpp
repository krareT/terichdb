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
#endif
#include <fcntl.h>
#include <float.h>

#include "json.hpp"

namespace nark { namespace db {

namespace fs = boost::filesystem;
/////////////////////////////////////////////////////////////////////////////

MultiPartStore::MultiPartStore(valvec<ReadableStorePtr>& parts) {
	m_parts.swap(parts);
	syncRowNumVec();
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
	size_t upp = upper_bound_a(m_rowNumVec, uint32_t(id));
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
		m_rowNumVec[i] = uint32_t(rows);
		rows += m_parts[i]->numDataRows();
	}
	m_rowNumVec.back() = uint32_t(rows);
}

/////////////////////////////////////////////////////////////////////////////

ReadableSegment::ReadableSegment() {
	m_delcnt = 0;
	m_tobeDel = false;
	m_isDirty = false;
	m_bookDeletion = false;
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
}
ReadonlySegment::~ReadonlySegment() {
	m_colgroups.clear();
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
	const size_t indexNum = m_schema->getIndexNum();
	const size_t colgroupNum = m_schema->getColgroupNum();
	m_indices.resize(indexNum);
	m_colgroups.resize(colgroupNum);
	valvec<byte> buf;
	for (size_t i = 0; i < m_indices.size(); ++i) {
		SortableStrVec strVec;
		const Schema& indexSchema = m_schema->getIndexSchema(i);
		size_t fixedIndexRowLen = indexSchema.getFixedRowLen();
		for (size_t j = 0; j < input.size(); ++j) {
			auto seg = input[j];
			auto indexStore = seg->m_indices[i]->getReadableStore();
			assert(nullptr != indexStore);
			llong rows = seg->numDataRows();
			for (llong id = 0; id < rows; ++id) {
				if (!seg->m_isDel[size_t(id)]) {
					indexStore->getValue(id, &buf, ctx);
					if (fixedIndexRowLen) {
						assert(buf.size() == fixedIndexRowLen);
						strVec.m_strpool.append(buf);
					} else
						strVec.push_back(buf);
				}
			}
		}
		ReadableIndex* index = this->buildIndex(indexSchema, strVec);
		m_indices[i] = index;
		m_colgroups[i] = index->getReadableStore();
	}

	for (size_t i = indexNum; i < colgroupNum; ++i) {

	}
}

namespace {
	class FileDataIO : public ReadableStore {
		class FileStoreIter;
		FileStream m_fp;
		NativeDataOutput<OutputBuffer> m_obuf;
		size_t m_fixedLen;
		size_t m_fileSize;
		size_t m_rows;
		FileDataIO(const FileDataIO&) = delete;
	public:
		FileDataIO(size_t fixedLen) {
			intrusive_ptr_add_ref(this); // Trick: will never be deleted
			m_fp.attach(tmpfile());
			m_obuf.attach(&m_fp);
			m_fixedLen = fixedLen;
			m_fileSize = 0;
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

		llong dataStorageSize() const override { return m_fileSize; }
		llong numDataRows() const override { return m_rows; }

		double avgLen() const { return (m_fileSize + 0.1) / (m_rows + 0.1); }

		StoreIterator* createStoreIterForward(DbContext*) const override;
		StoreIterator* createStoreIterBackward(DbContext*) const override {
			THROW_STD(invalid_argument, "Not Implemented");
			return NULL;
		}
		void getValueAppend(llong, valvec<byte>*, DbContext*) const override {
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

ReadableStore*
ReadonlySegment::buildDictZipStore(const Schema&, StoreIterator& inputIter) const {
	THROW_STD(invalid_argument,
		"Not Implemented, Only Implemented by DfaDbReadonlySegment");
}

void
ReadonlySegment::convFrom(CompositeTable* tab, size_t segIdx)
{
	DbContextPtr ctx(tab->createDbContext());
	ReadableSegmentPtr input;
	{
		MyRwLock lock(tab->m_rwMutex, false);
		input = tab->m_segments[segIdx];
		m_isDel = input->m_isDel; // make a copy, input->m_isDel[*] may be changed
		m_delcnt = input->m_delcnt;
	}
	llong savedInputRowNum = input->m_isDel.size();
	assert(savedInputRowNum > 0);
	size_t indexNum = m_schema->getIndexNum();
	TempFileList colgroupTempFiles(*m_schema->m_colgroupSchemaSet);

	valvec<fstring> columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> buf, projRowBuf;
	SortableStrVec strVec;
	llong inputRowNum = 0;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx.get()));
	llong id = -1;
	llong newRowNum = 0;
	while (iter->increment(&id, &buf)) {
		inputRowNum++;
		assert(id >= 0);
		assert(id < savedInputRowNum);
		if (m_isDel[id]) continue;

		m_schema->m_rowSchema->parseRow(buf, &columns);
		colgroupTempFiles.writeColgroups(columns, projRowBuf);
		newRowNum++;
	}
	if (inputRowNum < savedInputRowNum) {
		fprintf(stderr
			, "WARN: inputRows[real=%lld saved=%lld], some data have lossed\n"
			, inputRowNum, savedInputRowNum);
		input->m_isDel.risk_set_size(inputRowNum);
		this->m_isDel.risk_set_size(inputRowNum);
	}
	assert(newRowNum <= inputRowNum);
	assert(size_t(inputRowNum - newRowNum) == m_delcnt);

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
				m_colgroups[i] = this->buildDictZipStore(schema, *iter);
				continue;
			}
		}
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
		m_colgroups[i] = parts.size()==1 ? parts[0] : new MultiPartStore(parts);
	}
	m_dataMemSize = 0;
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		m_dataMemSize += m_colgroups[i]->dataStorageSize();
	}

	auto tmpDir = m_segDir + ".tmp";
	fs::create_directories(tmpDir);
	this->save(tmpDir);

	// reload as mmap
	m_isDel.clear();
	m_indices.erase_all();
	m_colgroups.erase_all();
	this->load(tmpDir);
	{
		MyRwLock lock(tab->m_rwMutex, false);
		if (m_delcnt < input->m_delcnt) { // rows were deleted during build
			size_t i = 0;
			for (size_t j = 0; j < size_t(inputRowNum); ++j) {
				if (!m_isDel[j])
					 m_isDel.set(i, input->m_isDel[j]), ++i;
			}
			assert(i == size_t(newRowNum));
			size_t new_delcnt = input->m_delcnt - m_delcnt;
			fprintf(stderr,
				"INFO: ReadonlySegment::convFrom: delcnt[old=%zd input2=%zd new=%zd] done\n",
				m_delcnt, input->m_delcnt, new_delcnt);
			m_isDel.risk_set_size(size_t(newRowNum));
			m_delcnt = new_delcnt;
			assert(m_isDel.popcnt() == m_delcnt);
		}
		else if (m_delcnt) {
			m_isDel.resize_fill(size_t(newRowNum), false);
			m_delcnt = 0;
		}
		((uint64_t*)m_isDelMmap)[0] = newRowNum;
		tab->m_segments[segIdx] = this;
	}
	fs::rename(tmpDir, m_segDir);
	input->deleteSegment();
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
