#include "db_table.hpp"
#include "db_segment.hpp"
#include "intkey_index.hpp"
#include "zip_int_store.hpp"
#include "fixed_len_key_index.hpp"
#include "fixed_len_store.hpp"
#include "appendonly.hpp"
#include <terark/util/autoclose.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/truncate_file.hpp>
//#include <boost/dll.hpp>

//#define TERARK_DB_ENABLE_DFA_META
#if defined(TERARK_DB_ENABLE_DFA_META)
#include <terark/fsa/nest_trie_dawg.hpp>
#endif

#if defined(_MSC_VER)
	#include <io.h>
#else
	#include <unistd.h>
	#include <dlfcn.h>
#endif
#include <fcntl.h>
#include <float.h>

#include "json.hpp"

#include <boost/scope_exit.hpp>

//#define SLOW_DEBUG_CHECK

namespace terark { namespace db {

namespace fs = boost::filesystem;

typedef hash_strmap< std::function<ReadableSegment*()>
					, fstring_func::hash_align
					, fstring_func::equal_align
					, ValueInline, SafeCopy
					>
		SegmentFactory;
static	SegmentFactory& s_segmentFactory() {
	static SegmentFactory instance;
	return instance;
}

ReadableSegment::
RegisterSegmentFactory::
RegisterSegmentFactory(std::initializer_list<fstring> names, const SegmentCreator& creator) {
	SegmentFactory& factory = s_segmentFactory();
	fstring clazz = *names.begin();
	for (fstring name : names) {
		auto ib = factory.insert_i(name, creator);
		assert(ib.second);
		if (!ib.second) {
			THROW_STD(invalid_argument
				, "duplicate segment name %s for class: %s"
				, name.c_str(), clazz.c_str());
		}
	}
}

struct AutoLoadSegmentLibraries : hash_strmap<void*> {
	AutoLoadSegmentLibraries() {
		load_dll("terark-db-dfadb");
		load_dll("terark-db-trbdb");
		load_dll("terark-db-wiredtiger");
	}
	void load_dll(fstring libname) {
#if defined(_WIN32) || defined(_WIN64)
		std::string realnames[1] = {
			libname + TERARK_IF_DEBUG("-d", "-r") ".dll",
		};
#elif defined(__DARWIN_C_LEVEL)
		std::string realnames[2] = {
			"lib" + libname + TERARK_IF_DEBUG("-d", "-r") ".dylib",
			"lib" + libname + TERARK_IF_DEBUG("-d", "-r") ".so",
		};
#else
		std::string realnames[1] = {
			"lib" + libname + TERARK_IF_DEBUG("-d", "-r") ".so",
		};
#endif
		void* lib = NULL;
		for (const std::string& realname : realnames) {
#if defined(_WIN32) || defined(_WIN64)
			lib = (void*)LoadLibraryA(realname.c_str());
#else
			lib = dlopen(realname.c_str(), RTLD_NOW);
			if (lib)
				break;
			fprintf(stderr, "dlopen(%s) = %s\n", realname.c_str(), dlerror());
#endif
		}
		if (lib) {
			auto ib = this->insert_i(libname, lib);
			if (!ib.second) {
				THROW_STD(invalid_argument
					, "duplicate lib: \"%s\", it has been loaded"
					, libname.c_str());
			}
		}
		else {
			fprintf(stderr, "WARN: dynamic load DLL: %s failed\n", libname.c_str());
		}
	}
};

const hash_strmap<void*>& g_AutoLoadSegmentDLLs() {
	static AutoLoadSegmentLibraries libtab;
	return libtab;
}

ReadableSegment*
ReadableSegment::createSegment(fstring clazz, PathRef segDir, SchemaConfig* sc) {
	(void)g_AutoLoadSegmentDLLs(); // auto load dll
	const SegmentFactory& factory = s_segmentFactory();
	const size_t idx = factory.find_i(clazz);
	if (idx < factory.end_i()) {
		ReadableSegment* seg = factory.val(idx)();
		assert(seg);
		seg->m_segDir = segDir;
		seg->m_schema = sc;
		return seg;
	}
	THROW_STD(invalid_argument, "unknown segment class: %s", clazz.c_str());
}

ReadableSegment::ReadableSegment() {
	m_delcnt = 0;
	m_tobeDel = false;
	m_isDirty = false;
	m_isFreezed = false;
	m_hasLockFreePointSearch = true;
	m_bookUpdates = false;
	m_withPurgeBits = false;
    m_onProcess = false;
	m_isPurgedMmap = nullptr;
}
ReadableSegment::~ReadableSegment() {
	if (m_isDelMmap) {
		closeIsDel();
	}
	else if (m_isDirty && !m_tobeDel && !m_segDir.empty()) {
		saveIsDel(m_segDir);
	}
	m_indices.clear(); // destroy index objects
	m_colgroups.clear();
	m_deletionTime = nullptr;
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
		//	TERARK_IF_DEBUG(abort(),;);
		}
	}
}

ColgroupSegment* ReadableSegment::getColgroupSegment() const {
	return nullptr;
}
ReadonlySegment* ReadableSegment::getReadonlySegment() const {
	return nullptr;
}
WritableSegment* ReadableSegment::getWritableSegment() const {
	return nullptr;
}
PlainWritableSegment* ReadableSegment::getPlainWritableSegment() const {
	return nullptr;
}

void ReadableSegment::deleteSegment() {
	assert(!m_segDir.empty());
	m_tobeDel = true;
}

llong ReadableSegment::numDataRows() const {
	return m_isDel.size();
}

void ReadableSegment::setStorePath(PathRef path) {
    size_t const colgroups_size = m_colgroups.size();
    for(size_t i = 0; i < colgroups_size; ++i)
    {
        auto store = m_colgroups[i].get();
        if (store) {
            const Schema& schema = m_schema->getColgroupSchema(i);
            store->setStorePath(path / "colgroup-" + schema.m_name);
        }
    }
}

void ReadableSegment::saveIsDel(PathRef dir) const {
	assert(m_isDel.popcnt() == m_delcnt);
	if (m_isDelMmap && dir == m_segDir) {
		// need not to save, mmap is sys memory
		return;
	}
	fs::path isDelFpath = dir / "IsDel";
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
	fs::path isDelFpath = segDir / "IsDel";
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

void ReadableSegment::closeIsDel() {
	if (m_isDelMmap) {
		size_t bitBytes = m_isDel.capacity()/8;
		mmap_close(m_isDelMmap, sizeof(uint64_t) + bitBytes);
		m_isDel.risk_release_ownership();
		m_isDelMmap = NULL;
	}
	else {
		m_isDel.clear();
	}
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
	if (m_schema->m_snapshotSchema) {
		fs::path fpath = segDir / "deletion-time.fixlen";
		auto deltime = new FixedLenStore(*m_schema->m_snapshotSchema);
		if (this->getReadonlySegment()) {
			deltime->unneedsLock();
		}
		m_deletionTime = deltime;
		m_deletionTime->load(fpath);
		size_t physicRows = this->getPhysicRows();
		if (size_t(m_deletionTime->numDataRows()) != physicRows) {
			THROW_STD(invalid_argument
				, "m_deletionTime->numDataRows() = %lld, m_isDel.size() = %zd, must be the same"
				, m_deletionTime->numDataRows(), physicRows);
		}
	}
}

void ReadableSegment::save(PathRef segDir) const {
	assert(!segDir.empty());
	if (m_tobeDel) {
		return; // not needed
	}
	this->saveRecordStore(segDir);
	this->saveIndices(segDir);
	this->saveIsDel(segDir);
	if (m_deletionTime) {
		assert(getPhysicRows() == size_t(m_deletionTime->numDataRows()));
		m_deletionTime->save(segDir / "deletion-time");
	}
}

size_t ReadableSegment::getPhysicRows() const {
	if (m_isPurged.size())
		return m_isPurged.max_rank0();
	else
		return m_isDel.size();
}

// logic id is immutable
// inline
size_t ReadableSegment::getPhysicId(size_t logicId) const {
	if (m_isPurged.empty()) {
		return logicId;
	} else {
		assert(this->getReadonlySegment() != NULL);
		assert(m_isPurged.size() == m_isDel.size());
		assert(logicId < m_isDel.size());
		return m_isPurged.rank0(logicId);
	}
}

size_t ReadableSegment::getLogicId(size_t physicId) const {
	if (m_isPurged.empty()) {
		return physicId;
	}
	else {
		assert(this->getReadonlySegment() != NULL);
		assert(m_isPurged.size() == m_isDel.size());
		assert(physicId < m_isPurged.max_rank0());
		return m_isPurged.select0(physicId);
	}
}

void ReadableSegment::addtoUpdateList(size_t logicId) {
	assert(m_isFreezed);
	if (!m_bookUpdates) {
		return;
	}
	const size_t rows = m_isDel.size();
	if ((m_updateList.unused() > 0 || m_updateList.size() < rows / 256) && m_updateBits.empty()) {
		m_updateList.push_back(logicId);
	}
	else if (!m_updateBits.empty()) {
		assert(m_updateBits.size() == rows + 1);
		m_updateBits.set1(logicId);
	}
	else {
		// reserve an extra bit as the guard
		m_updateBits.resize(rows + 1, false);
		bm_uint_t* bits = m_updateBits.bldata();
		uint32_t* idvec = m_updateList.data();
		size_t    idnum = m_updateList.size();
		for(size_t i = 0; i < idnum; ++i) {
			size_t id = idvec[i];
			assert(id < rows);
			terark_bit_set1(bits, id);
		}
		terark_bit_set1(bits, logicId);
		// set the last bit to 1 as the guard
		terark_bit_set1(bits, rows);
		m_updateList.clear();
	}
}

///////////////////////////////////////////////////////////////////////////////

ColgroupSegment::ColgroupSegment() {
	m_dataMemSize = 0;
	m_totalStorageSize = 0;
	m_dataInflateSize = 0;
}
ColgroupSegment::~ColgroupSegment() {
	assert(nullptr == m_isPurgedMmap);
	m_colgroups.clear();
}

ColgroupSegment* ColgroupSegment::getColgroupSegment() const {
	return const_cast<ColgroupSegment*>(this);
}

ReadonlySegment::ReadonlySegment() {
	m_isFreezed = true;
}
ReadonlySegment::~ReadonlySegment() {
	if (m_isPurgedMmap) {
		mmap_close(m_isPurgedMmap, m_isPurged.mem_size());
		m_isPurged.risk_release_ownership();
		m_isPurgedMmap = nullptr;
	}
	m_colgroups.clear();
}

ReadonlySegment* ReadonlySegment::getReadonlySegment() const {
	return const_cast<ReadonlySegment*>(this);
}

llong ColgroupSegment::dataInflateSize() const {
	return m_dataMemSize;
}
llong ColgroupSegment::dataStorageSize() const {
	return m_dataMemSize;
}
llong ColgroupSegment::totalStorageSize() const {
	return m_totalStorageSize;
}

void ReadonlySegment::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx) const {
	assert(ctx != nullptr);
	llong rows = m_isDel.size();
	if (terark_unlikely(id < 0 || id >= rows)) {
		THROW_STD(out_of_range, "invalid id=%lld, rows=%lld", id, rows);
	}
	getValueByPhysicId(getPhysicId(id), val, ctx);
}

void
ColgroupWritableSegment::getValueAppend(llong id, valvec<byte>* val, DbContext* ctx)
const {
	assert(ctx != nullptr);
	assert(m_isPurged.empty());
	llong rows = m_isDel.size();
	if (terark_unlikely(id < 0 || id >= rows)) {
		THROW_STD(out_of_range, "invalid id=%lld, rows=%lld", id, rows);
	}
	getValueByPhysicId(id, val, ctx);
}

void
ColgroupSegment::getValueByPhysicId(size_t id, valvec<byte>* val, DbContext* ctx)
const {
    auto cols1 = ctx->cols.get();
    auto cols2 = ctx->cols.get();
    auto buf1 = ctx->bufs.get();
	val->risk_set_size(0);
    cols1->erase_all();
    buf1->risk_set_size(0);

	// getValueAppend to ctx->buf1
	const size_t colgroupNum = m_colgroups.size();
	for (size_t i = 0; i < colgroupNum; ++i) {
		const Schema& iSchema = m_schema->getColgroupSchema(i);
		if (iSchema.m_keepCols.has_any1()) {
			size_t oldsize = buf1->size();
			m_colgroups[i]->getValueAppend(id, buf1.get(), ctx);
			iSchema.parseRowAppend(*buf1, oldsize, cols1.get());
		}
		else {
			cols1->grow(iSchema.columnNum());
		}
	}
	assert(cols1->size() == m_schema->m_colgroupSchemaSet->m_flattenColumnNum);

	// combine columns to ctx->cols2
	size_t baseColumnId = 0;
	cols2->m_base = cols1->m_base;
	cols2->m_cols.resize_fill(m_schema->columnNum());
	for (size_t i = 0; i < colgroupNum; ++i) {
		const Schema& iSchema = m_schema->getColgroupSchema(i);
		for (size_t j = 0; j < iSchema.columnNum(); ++j) {
			if (iSchema.m_keepCols[j]) {
				size_t parentColId = iSchema.parentColumnId(j);
				cols2->m_cols[parentColId] = cols1->m_cols[baseColumnId + j];
			}
		}
		baseColumnId += iSchema.columnNum();
	}

#if !defined(NDEBUG)
	for (size_t i = 0; i < cols2->size(); ++i) {
		assert(cols2->m_cols[i].isValid());
	}
#endif

	// combine to val
	m_schema->m_rowSchema->combineRow(*cols2, val);
}

void
ColgroupWritableSegment::indexSearchExactAppend(size_t mySegIdx, size_t indexId,
										fstring key, valvec<llong>* recIdvec,
										DbContext* ctx) const {
	assert(m_isPurged.empty());
	size_t oldsize = recIdvec->size();
	auto index = m_indices[indexId].get();
	index->searchExactAppend(key, recIdvec, ctx);
	if (recIdvec->size() == oldsize) {
		return;
	}
	size_t newsize = oldsize;
	size_t recIdvecSize = recIdvec->size();
	llong* recIdvecData = recIdvec->data();
	SpinRwLock lock;
	if (!m_isFreezed) {
		lock.acquire(m_segMutex, false);
	}
	if (m_deletionTime) {
		auto deltime = (const llong*)m_deletionTime->getRecordsBasePtr();
		auto snapshotVersion = ctx->m_mySnapshotVersion;
		for(size_t k = oldsize; k < recIdvecSize; ++k) {
			llong logicId = recIdvecData[k];
			if (deltime[logicId] > snapshotVersion)
				recIdvecData[newsize++] = logicId;
		}
	}
	else {
		auto isDel = m_isDel.bldata();
		for(size_t k = oldsize; k < recIdvecSize; ++k) {
			llong logicId = recIdvecData[k];
			if (!terark_bit_test(isDel, logicId))
				recIdvecData[newsize++] = logicId;
		}
	}
	recIdvec->risk_set_size(newsize);
}

void
ReadonlySegment::indexSearchExactAppend(size_t mySegIdx, size_t indexId,
										fstring key, valvec<llong>* recIdvec,
										DbContext* ctx) const {
	size_t oldsize = recIdvec->size();
	auto index = m_indices[indexId].get();
	index->searchExactAppend(key, recIdvec, ctx);
	if (recIdvec->size() == oldsize) {
		return;
	}
	size_t newsize = oldsize;
	llong* recIdvecData = recIdvec->data();
	if (m_deletionTime) {
		auto deltime = (const llong*)m_deletionTime->getRecordsBasePtr();
		auto snapshotVersion = ctx->m_mySnapshotVersion;
		if (m_isPurged.empty()) {
			for(size_t k = oldsize; k < recIdvec->size(); ++k) {
				llong logicId = recIdvecData[k];
				if (deltime[logicId] > snapshotVersion)
					recIdvecData[newsize++] = logicId;
			}
		}
		else {
			assert(m_isPurged.size() == m_isDel.size());
			assert(this->getReadonlySegment() != NULL);
			for(size_t k = oldsize; k < recIdvec->size(); ++k) {
				size_t physicId = (size_t)recIdvecData[k];
				assert(physicId < m_isPurged.max_rank0());
				size_t logicId = m_isPurged.select0(physicId);
				if (deltime[physicId] > snapshotVersion)
					recIdvecData[newsize++] = logicId;
			}
		}
	}
	else {
		if (m_isPurged.empty()) {
			for(size_t k = oldsize; k < recIdvec->size(); ++k) {
				llong logicId = recIdvecData[k];
				if (!m_isDel[logicId])
					recIdvecData[newsize++] = logicId;
			}
		}
		else {
			assert(m_isPurged.size() == m_isDel.size());
			assert(this->getReadonlySegment() != NULL);
			for(size_t k = oldsize; k < recIdvec->size(); ++k) {
				size_t physicId = (size_t)recIdvecData[k];
				assert(physicId < m_isPurged.max_rank0());
				size_t logicId = m_isPurged.select0(physicId);
				if (!m_isDel[logicId])
					recIdvecData[newsize++] = logicId;
			}
		}
	}
	recIdvec->risk_set_size(newsize);
}

void
ReadonlySegment::selectColumns(llong recId,
							   const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext* ctx)
const {
	assert(recId >= 0);
	llong physicId = getPhysicId(size_t(recId));
	return selectColumnsByPhysicId(physicId, colsId, colsNum, colsData, ctx);
}

void
ColgroupWritableSegment::selectColumns(llong recId,
							   const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext* ctx)
const {
	assert(recId >= 0);
	assert(m_isPurged.empty());
	if (m_isFreezed) {
		return selectColumnsByPhysicId(recId, colsId, colsNum, colsData, ctx);
	} else {
		SpinRwLock lock(m_segMutex, false);
		return selectColumnsByPhysicId(recId, colsId, colsNum, colsData, ctx);
	}
}

void
ColgroupSegment::selectColumnsByPhysicId(llong physicId,
							   const size_t* colsId, size_t colsNum,
							   valvec<byte>* colsData, DbContext* ctx)
const {
	assert(physicId >= 0);
    auto cols = ctx->cols.get();
    auto buf = ctx->bufs.get();
	colsData->erase_all();
	buf->erase_all();
	ctx->offsets.resize_fill(m_colgroups.size(), UINT32_MAX);
	auto offsets = ctx->offsets.data();
	for(size_t i = 0; i < colsNum; ++i) {
		assert(colsId[i] < m_schema->m_rowSchema->columnNum());
		auto cp = m_schema->m_colproject[colsId[i]];
		size_t colgroupId = cp.colgroupId;
		size_t oldsize = buf->size();
		const Schema& schema = m_schema->getColgroupSchema(colgroupId);
		if (offsets[colgroupId] == UINT32_MAX) {
			offsets[colgroupId] = cols->size();
			m_colgroups[colgroupId]->getValueAppend(physicId, buf.get(), ctx);
			schema.parseRowAppend(*buf, oldsize, cols.get());
		}
		fstring d = (*cols)[offsets[colgroupId] + cp.subColumnId];
		if (i < colsNum-1)
			schema.projectToNorm(d, cp.subColumnId, colsData);
		else
			schema.projectToLast(d, cp.subColumnId, colsData);
	}
}

void
ReadonlySegment::selectOneColumn(llong recId, size_t columnId,
								 valvec<byte>* colsData, DbContext* ctx)
const {
	assert(recId >= 0);
	llong physicId = getPhysicId(size_t(recId));
	selectOneColumnByPhysicId(physicId, columnId, colsData, ctx);
}

void
ColgroupWritableSegment::selectOneColumn(llong recId, size_t columnId,
								 valvec<byte>* colsData, DbContext* ctx)
const {
	assert(recId >= 0);
	assert(m_isPurged.empty());
	if (m_isFreezed) {
		selectOneColumnByPhysicId(recId, columnId, colsData, ctx);
	} else {
		SpinRwLock lock(m_segMutex, false);
		selectOneColumnByPhysicId(recId, columnId, colsData, ctx);
	}
}

void
ColgroupSegment::selectOneColumnByPhysicId(llong physicId, size_t columnId,
								 valvec<byte>* colsData, DbContext* ctx)
const {
	assert(physicId >= 0);
	assert(columnId < m_schema->m_rowSchema->columnNum());
	auto cp = m_schema->m_colproject[columnId];
	size_t colgroupId = cp.colgroupId;
	const Schema& schema = m_schema->getColgroupSchema(colgroupId);
//	printf("colprojects = %zd, colgroupId = %zd, schema.cols = %zd\n"
//		, m_schema->m_colproject.size(), colgroupId, schema.columnNum());
	if (schema.columnNum() == 1) {
		m_colgroups[colgroupId]->getValue(physicId, colsData, ctx);
	}
	else {
        auto cols = ctx->cols.get();
        auto buf = ctx->bufs.get();
		m_colgroups[colgroupId]->getValue(physicId, buf.get(), ctx);
		schema.parseRow(*buf, cols.get());
		colsData->erase_all();
		colsData->append((*cols)[cp.subColumnId]);
	}
}

void ReadonlySegment::selectColgroups(llong recId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
	assert(recId >= 0);
	llong physicId = getPhysicId(size_t(recId));
	selectColgroupsByPhysicId(physicId, cgIdvec, cgIdvecSize, cgDataVec, ctx);
}

void ColgroupWritableSegment::selectColgroups(llong recId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
	assert(recId >= 0);
	assert(m_isPurged.empty());
	if (m_isFreezed) {
		selectColgroupsByPhysicId(recId, cgIdvec, cgIdvecSize, cgDataVec, ctx);
	} else {
		SpinRwLock lock(m_segMutex, false);
		selectColgroupsByPhysicId(recId, cgIdvec, cgIdvecSize, cgDataVec, ctx);
	}
}

void ColgroupSegment::selectColgroupsByPhysicId(llong physicId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
	for(size_t i = 0; i < cgIdvecSize; ++i) {
		size_t cgId = cgIdvec[i];
		if (cgId >= m_schema->getColgroupNum()) {
			THROW_STD(out_of_range, "cgId = %zd, cgNum = %zd"
				, cgId, m_schema->getColgroupNum());
		}
		m_colgroups[cgId]->getValue(physicId, &cgDataVec[i], ctx);
	}
}

class ColgroupSegment::MyStoreIterForward : public StoreIterator {
	llong  m_id = 0;
	DbContextPtr m_ctx;
public:
	MyStoreIterForward(const ColgroupSegment* owner, DbContext* ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ColgroupSegment*>(owner));
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ColgroupSegment*>(m_store.get());
		size_t rows = owner->m_isDel.size();
		while (size_t(m_id) < rows && owner->m_isDel[m_id])
			m_id++;
		if (terark_likely(size_t(m_id) < rows)) {
			*id = m_id++;
			owner->getValue(*id, val, m_ctx.get());
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const ColgroupSegment*>(m_store.get());
		llong rows = owner->m_isDel.size();
		assert(id >= 0);
		m_id = id + 1;
		if (id < rows) {
			// do not check m_isDel, always success!
			owner->getValue(id, val, m_ctx.get());
			return true;
		}
		fprintf(stderr, "ERROR: %s: id = %lld, rows = %lld\n"
			, BOOST_CURRENT_FUNCTION, id, rows);
		return false;
	}
	void reset() override {
		m_id = 0;
	}
};
class ColgroupSegment::MyStoreIterBackward : public StoreIterator {
	llong  m_id;
	DbContextPtr m_ctx;
public:
	MyStoreIterBackward(const ColgroupSegment* owner, const DbContextPtr& ctx)
	  : m_ctx(ctx) {
		m_store.reset(const_cast<ColgroupSegment*>(owner));
		m_id = owner->m_isDel.size();
	}
	bool increment(llong* id, valvec<byte>* val) override {
		auto owner = static_cast<const ColgroupSegment*>(m_store.get());
		while (m_id > 0 && owner->m_isDel[m_id-1])
			 --m_id;
		if (terark_likely(m_id > 0)) {
			*id = --m_id;
			owner->getValue(*id, val, m_ctx.get());
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		auto owner = static_cast<const ColgroupSegment*>(m_store.get());
		llong rows = owner->m_isDel.size();
		assert(id >= 0);
		if (id < rows) {
			m_id = id; // is not (id-1)
			// do not check m_isDel, always success!
			owner->getValue(id, val, m_ctx.get());
			return true;
		}
		m_id = rows;
		fprintf(stderr, "ERROR: %s: id = %lld, rows = %lld\n"
			, BOOST_CURRENT_FUNCTION, id, rows);
		return false;
	}
	void reset() override {
		auto owner = static_cast<const ColgroupSegment*>(m_store.get());
		m_id = owner->m_isDel.size();
	}
};
StoreIterator* ColgroupSegment::createStoreIterForward(DbContext* ctx) const {
	return new MyStoreIterForward(this, ctx);
}
StoreIterator* ColgroupSegment::createStoreIterBackward(DbContext* ctx) const {
	return new MyStoreIterBackward(this, ctx);
}

class TempFileList {
	const SchemaSet& m_schemaSet;
	valvec<byte> m_projRowBuf;
	valvec<ReadableStorePtr> m_readers;
	valvec<AppendableStore*> m_appenders;
	TERARK_IF_DEBUG(ColumnVec m_debugCols;,;);
public:
	TempFileList(PathRef segDir, const SchemaSet& schemaSet)
		: m_schemaSet(schemaSet)
	{
		size_t cgNum = schemaSet.m_nested.end_i();
		m_readers.resize(cgNum);
		m_appenders.resize(cgNum);
		for (size_t i = 0; i < cgNum; ++i) {
			const Schema& schema = *schemaSet.m_nested.elem_at(i);
			if (schema.getFixedRowLen()) {
				auto store = new FixedLenStore(segDir, schema);
				store->unneedsLock();
				m_readers[i] = store;
			}
			else {
				m_readers[i] = new SeqReadAppendonlyStore(segDir, schema);
			}
			m_appenders[i] = m_readers[i]->getAppendableStore();
		}
	}
	void writeColgroups(const ColumnVec& columns) {
		size_t colgroupNum = m_readers.size();
		for (size_t i = 0; i < colgroupNum; ++i) {
			const Schema& schema = *m_schemaSet.m_nested.elem_at(i);
			schema.selectParent(columns, &m_projRowBuf);
#if !defined(NDEBUG)
			schema.parseRow(m_projRowBuf, &m_debugCols);
			assert(m_debugCols.size() == schema.columnNum());
			for(size_t j = 0; j < m_debugCols.size(); ++j) {
				size_t k = schema.parentColumnId(j);
				assert(k < columns.size());
				assert(m_debugCols[j] == columns[k]);
			}
#endif
			m_appenders[i]->append(m_projRowBuf, NULL);
		}
	}
	void completeWrite() {
		size_t colgroupNum = m_readers.size();
		for (size_t i = 0; i < colgroupNum; ++i) {
			m_appenders[i]->shrinkToFit();
		}
	}
	ReadableStore* getStore(size_t cgId) const {
		return m_readers[cgId].get();
	}
	size_t size() const { return m_readers.size(); }
	size_t
	collectData(size_t cgId, StoreIterator* iter, SortableStrVec& strVec,
				size_t maxMemSize = size_t(-1)) const {
		assert(strVec.m_index.size() == 0);
		assert(strVec.m_strpool.size() == 0);
		const Schema& schema = *m_schemaSet.getSchema(cgId);
		const llong   rows = iter->getStore()->numDataRows();
		const size_t  fixlen = schema.getFixedRowLen();
		if (fixlen == 0) {
			valvec<byte> buf;
			llong  recId = INT_MAX; // for fail fast
			while (strVec.mem_size() < maxMemSize && iter->increment(&recId, &buf)) {
				assert(recId < rows);
				strVec.push_back(buf);
			}
			return strVec.size();
		}
		else { // ignore maxMemSize
			size_t size = fixlen * rows;
			strVec.m_strpool.resize_no_init(size);
			byte_t* basePtr = iter->getStore()->getRecordsBasePtr();
			memcpy(strVec.m_strpool.data(), basePtr, size);
			return rows;
		}
	}
};

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

ReadableStore*
ReadonlySegment::purgeDictZipStore(const Schema&, PathRef pathWithPrefix, const ReadableStore* inputStore, 
                                   const bm_uint_t* isDel, const rank_select_se* isPueged, size_t baseId) const {
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
ReadonlySegment::compressMultipleColgroups(ReadableSegment* input, DbContext* ctx) {
	llong logicRowNum = input->m_isDel.size();
	llong newRowNum = 0;
	assert(logicRowNum > 0);
	size_t indexNum = m_schema->getIndexNum();
	auto tmpDir = m_segDir + ".tmp";
	TempFileList colgroupTempFiles(tmpDir, *m_schema->m_colgroupSchemaSet);
{
	ColumnVec columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> buf;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx));
	llong prevId = -1, id = -1;
	while (iter->increment(&id, &buf) && id < logicRowNum) {
		assert(id >= 0);
		assert(id < logicRowNum);
		assert(prevId < id);
		if (!m_isDel[id]) {
			m_schema->m_rowSchema->parseRow(buf, &columns);
			colgroupTempFiles.writeColgroups(columns);
			newRowNum++;
			m_isDel.beg_end_set1(prevId + 1, id);
			prevId = id;
		}
	}
	if (prevId != id) {
		assert(prevId < id);
		assert(m_isDel[id]);
		m_isDel.beg_end_set1(prevId+1, id);
	}
	llong inputRowNum = id + 1;
	assert(inputRowNum <= logicRowNum);
	if (inputRowNum < logicRowNum) {
		fprintf(stderr
			, "WARN: ReadonlySegment::compressMultipleColgroups(): realrows=%lld, m_isDel=%lld, some data have lost\n"
			, inputRowNum, logicRowNum);
		input->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
		this->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
	}
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	assert(newRowNum <= inputRowNum);
	assert(size_t(logicRowNum - newRowNum) == m_delcnt);
}
	// build index from temporary index files
	colgroupTempFiles.completeWrite();
	for (size_t i = 0; i < indexNum; ++i) {
		SortableStrVec strVec;
		const Schema& schema = m_schema->getIndexSchema(i);
		auto tmpStore = colgroupTempFiles.getStore(i);
		StoreIteratorPtr iter = tmpStore->ensureStoreIterForward(NULL);
		colgroupTempFiles.collectData(i, iter.get(), strVec);
		m_indices[i] = this->buildIndex(schema, strVec);
		m_colgroups[i] = m_indices[i]->getReadableStore();
		if (!schema.m_enableLinearScan) {
			iter.reset();
			tmpStore->deleteFiles();
		}
	}
	for (size_t i = indexNum; i < colgroupTempFiles.size(); ++i) {
		if (0 == newRowNum) {
			m_colgroups[i] = new EmptyIndexStore();
			continue;
		}
		const Schema& schema = m_schema->getColgroupSchema(i);
		auto tmpStore = colgroupTempFiles.getStore(i);
		if (schema.should_use_FixedLenStore()) {
			m_colgroups[i] = tmpStore;
			continue;
		}
		// dictZipSampleRatio < 0 indicate don't use dictZip
		if (schema.m_dictZipSampleRatio >= 0.0) {
			double sRatio = schema.m_dictZipSampleRatio;
			double avgLen = double(tmpStore->dataInflateSize()) / newRowNum;
			if (sRatio > 0 || (sRatio < FLT_EPSILON && avgLen > 100)) {
				StoreIteratorPtr iter = tmpStore->ensureStoreIterForward(NULL);
				m_colgroups[i] = buildDictZipStore(schema, tmpDir, *iter, NULL, NULL);
				iter.reset();
				tmpStore->deleteFiles();
				continue;
			}
		}
		size_t maxMem = m_schema->m_compressingWorkMemSize;
		llong rows = 0;
		MultiPartStorePtr parts = new MultiPartStore();
		StoreIteratorPtr iter = tmpStore->ensureStoreIterForward(NULL);
		while (rows < newRowNum) {
			SortableStrVec strVec;
			rows += colgroupTempFiles.collectData(i, iter.get(), strVec, maxMem);
			parts->addpart(this->buildStore(schema, strVec));
		}
		m_colgroups[i] = parts->finishParts();
		iter.reset();
		tmpStore->deleteFiles();
	}
}

void
ReadonlySegment::compressSingleKeyIndex(ReadableSegment* input, DbContext* ctx) {
	llong logicRowNum = input->m_isDel.size();
	llong newRowNum = 0;
	assert(logicRowNum > 0);
	auto tmpDir = m_segDir + ".tmp";
	ColumnVec columns(m_schema->columnNum(), valvec_reserve());
	valvec<byte> key;
	StoreIteratorPtr iter(input->createStoreIterForward(ctx));
	llong prevId = -1, id = -1;
	SortableStrVec keyVec;
	const Schema& keySchema = m_schema->getIndexSchema(0);
	while (iter->increment(&id, &key) && id < logicRowNum) {
		assert(id >= 0);
		assert(id < logicRowNum);
		assert(prevId < id);
		if (!m_isDel[id]) {
			if (keySchema.getFixedRowLen() > 0) {
				keyVec.m_strpool.append(key);
			} else {
				keyVec.push_back(key);
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
	llong inputRowNum = id + 1;
	assert(inputRowNum <= logicRowNum);
	if (inputRowNum < logicRowNum) {
		fprintf(stderr
			, "WARN: DfaDbReadonlySegment::compressSingleKeyValue(): realrows=%lld, m_isDel=%lld, some data have lost\n"
			, inputRowNum, logicRowNum);
		input->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
		this->m_isDel.beg_end_set1(inputRowNum, logicRowNum);
	}
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	m_indices[0] = buildIndex(keySchema, keyVec); // memory heavy
	m_colgroups[0] = m_indices[0]->getReadableStore();
}

void
ReadonlySegment::compressSingleColgroup(ReadableSegment* input, DbContext* ctx) {
	compressMultipleColgroups(input, ctx); // fallback
}
void
ReadonlySegment::compressSingleKeyValue(ReadableSegment* input, DbContext* ctx) {
	compressMultipleColgroups(input, ctx); // fallback
}

void
ReadonlySegment::convFrom(DbTable* tab, size_t segIdx) {
	auto tmpDir = m_segDir + ".tmp";
	fs::create_directories(tmpDir);

	DbContextPtr ctx;
	ReadableSegmentPtr input;
    size_t mergeSeqNum;
	{
		MyRwLock lock(tab->m_rwMutex, false);
		ctx.reset(tab->createDbContextNoLock());
		input = tab->m_segments[segIdx];
        mergeSeqNum = tab->m_mergeSeqNum;
	}
	assert(input->getWritableStore() != nullptr);
	assert(input->m_isFreezed);
	assert(input->m_updateList.empty());
	assert(input->m_bookUpdates == false);
	input->m_updateList.reserve(1024);
	input->m_bookUpdates = true;
	m_isDel = input->m_isDel; // make a copy, input->m_isDel[*] may be changed

	const size_t indexNum = m_schema->getIndexNum();
	const size_t colgroupNum = m_schema->getColgroupNum();

	m_indices.resize(indexNum);
	m_colgroups.resize(colgroupNum);

	if (colgroupNum == 1 && indexNum == 0) {
		// single-value-only
		compressSingleColgroup(input.get(), ctx.get());
	}
	else if (colgroupNum == 1 && indexNum == 1) {
		// single-key-only
		compressSingleKeyIndex(input.get(), ctx.get());
	}
	else if (colgroupNum == 2 && indexNum == 1) {
		// key-value
		compressSingleKeyValue(input.get(), ctx.get());
	}
	else {
		compressMultipleColgroups(input.get(), ctx.get());
	}
    
    MyRwLock lock(tab->m_rwMergeMutex, false);
    size_t finalSegIdx = segIdx;
    if (mergeSeqNum != tab->m_mergeSeqNum) {
		MyRwLock lock(tab->m_rwMutex, false);
        bool miss = true;
        for (size_t i = 0, e = tab->m_segments.size(); i < e; ++i) {
            if (tab->m_segments[i]->getColgroupSegment() != input.get())
                continue;
            finalSegIdx = i;
            miss = false;
            break;
        }
        if (miss) {
            fprintf(stderr
			    , "ERROR: missing segment after merge ! %s\n"
                , m_segDir.string().c_str());
		    abort();
        }
    }
	completeAndReload(tab, segIdx, input.get());
    if (finalSegIdx != segIdx)
        m_segDir = tab->getSegPath("rd", finalSegIdx);
	fs::rename(tmpDir, m_segDir);
    setStorePath(m_segDir);
	input->deleteSegment();
}

void
ReadonlySegment::completeAndReload(DbTable* tab, size_t segIdx,
								   ReadableSegment* input) {
	m_dataMemSize = 0;
	m_dataInflateSize = 0;
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		m_dataMemSize += m_colgroups[i]->dataStorageSize();
		m_dataInflateSize += m_colgroups[i]->dataInflateSize();
	}

	if (this->m_delcnt) {
		m_isPurged.assign(m_isDel);
		m_isPurged.build_cache(true, false); // need select0
		m_withPurgeBits = true;
	}
#if !defined(NDEBUG)
	for (size_t cgId = 0; cgId < m_colgroups.size(); ++cgId) {
		auto store = m_colgroups[cgId].get();
		size_t physicRows1 = (size_t)this->getPhysicRows();
		size_t physicRows2 = (size_t)store->numDataRows();
		assert(physicRows1 == physicRows2);
	}
#endif
	auto tmpDir = m_segDir + ".tmp";
	this->save(tmpDir);

	// reload as mmap
	m_isDel.clear();
	m_isPurged.clear();
	m_indices.erase_all();
	m_colgroups.erase_all();
	this->load(tmpDir);
	assert(this->m_isDel.size() == input->m_isDel.size());
	assert(this->m_isDel.popcnt() == this->m_delcnt);
	assert(this->m_isPurged.max_rank1() == this->m_delcnt);

	valvec<uint32_t> updateList;
	febitvec         updateBits;
	auto syncNewDeletionMark = [&]() {
		assert(input->m_bookUpdates);
		{
			SpinRwLock inputLock(input->m_segMutex, true);
			updateList.swap(input->m_updateList);
			updateBits.swap(input->m_updateBits);
		}
		if (updateList.size() > 0) {
			// this is the likely branch when lock(tab->m_rwMutex)
			assert(updateBits.size() == 0);
			std::sort(updateList.begin(), updateList.end());
			updateList.trim(
				std::unique(updateList.begin(),
							updateList.end())
			);
			auto dlist = updateList.data();
			auto isDel = this->m_isDel.bldata();
			size_t dlistSize = updateList.size();
			for (size_t i = 0; i < dlistSize; ++i) {
				assert(dlist[i] < m_isDel.size());
				size_t logicId = dlist[i];
				if (input->m_isDel[logicId])
					terark_bit_set1(isDel, logicId);
				else
					this->syncUpdateRecordNoLock(0, logicId, input);
			}
		}
		else if (updateBits.size() > 0) {
			assert(updateBits.size() == m_isDel.size()+1);
			size_t logicId = updateBits.zero_seq_len(0);
			while (logicId < m_isDel.size()) {
				if (!input->m_isDel[logicId]) {
					this->syncUpdateRecordNoLock(0, logicId, input);
				}
				logicId += 1 + updateBits.zero_seq_len(logicId + 1);
			}
			m_isDel.risk_memcpy(input->m_isDel);
		}
		else {
			// have nothing to update
			assert(updateList.size() == 0); // for set break point
		}
		// m_updateBits and m_updateList is safe to change in reader lock here
		updateBits.erase_all();
		updateList.erase_all();
	};
	syncNewDeletionMark(); // no lock
	MyRwLock lock(tab->m_rwMutex, false);
	assert(tab->m_segments[segIdx].get() == input);
	syncNewDeletionMark(); // reader locked
	lock.upgrade_to_writer();
	syncNewDeletionMark(); // writer locked
	m_delcnt = input->m_delcnt;
#if defined(SLOW_DEBUG_CHECK)
	{
		size_t computed_delcnt1 = this->m_isDel.popcnt();
		size_t computed_delcnt2 = input->m_isDel.popcnt();
		assert(computed_delcnt1 == input->m_delcnt);
		assert(computed_delcnt2 == input->m_delcnt);
	}
	valvec<byte> r1, r2;
	DbContextPtr ctx = tab->createDbContextNoLock();
	const Schema& rowSchema = *m_schema->m_rowSchema;
	for(size_t i = 0, rows = m_isDel.size(); i < rows; ++i) {
		if (!input->m_isDel[i]) {
			assert(!this->m_isDel[i]);
			this->getValue(i, &r1, ctx.get());
			input->getValue(i, &r2, ctx.get());
			int cmp = rowSchema.compareData(r1, r2);
			if (0 != cmp) {
				std::string js1 = rowSchema.toJsonStr(r1);
				std::string js2 = rowSchema.toJsonStr(r2);
				fprintf(stderr, "recId: %zd\n\tjs1[len=%zd]=%s\n\tjs2[len=%zd]=%s\n"
					, i, r1.size(), js1.c_str(), r2.size(), js2.c_str());
			}
			assert(0 == cmp);
			assert(m_isPurged.empty() || !m_isPurged[i]);
		} else {
			assert(this->m_isDel[i]);
		}
	}
	if (m_isPurged.size() > 0) {
		assert(m_isDel.size() == m_isPurged.size());
		for(size_t i = 0, rows = m_isDel.size(); i < rows; ++i) {
			if (m_isPurged[i]) {
				assert(m_isDel[i]);
				assert(input->m_isDel[i]);
			}
		}
	}
#endif
	assert(tab->m_segments[segIdx].get() == input);
	tab->m_segments[segIdx] = this;
	tab->m_segArrayUpdateSeq++;
}

// dstBaseId is for merge update
void
ReadonlySegment::syncUpdateRecordNoLock(size_t dstBaseId, size_t logicId,
										const ReadableSegment* input) {
	assert(input->m_isDel.is0(logicId));
	assert(this->m_isDel.is0(dstBaseId + logicId));
	auto dstPhysicId = this->getPhysicId(dstBaseId + logicId);
	auto srcPhysicId = input->getPhysicId(logicId);
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto&schema = m_schema->getColgroupSchema(colgroupId);
		auto dstColstore = this->m_colgroups[colgroupId].get();
		auto srcColstore = input->m_colgroups[colgroupId].get();
		assert(nullptr != dstColstore);
		assert(nullptr != srcColstore);
		auto fixlen = schema.getFixedRowLen();
		auto dstDataPtr = dstColstore->getRecordsBasePtr() + fixlen * dstPhysicId;
		auto srcDataPtr = srcColstore->getRecordsBasePtr() + fixlen * srcPhysicId;
		memcpy(dstDataPtr, srcDataPtr, fixlen);
	}
	if (m_deletionTime) {
		auto dstDataPtr = (uint64_t*)this->m_deletionTime->getRecordsBasePtr();
		auto srcDataPtr = (uint64_t*)input->m_deletionTime->getRecordsBasePtr();
		dstDataPtr[dstPhysicId] = srcDataPtr[srcPhysicId];
	}
}

static inline
void pushRecord(SortableStrVec& strVec, const ReadableStore& store,
				llong physicId, size_t fixlen, DbContext* ctx) {
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
static
fs::path renameToBackupFromDir(PathRef segDir) {
	fs::path backupDir;
	for (int tmpNum = 0; ; ++tmpNum) {
		char szBuf[32];
		snprintf(szBuf, sizeof(szBuf), ".backup-%d", tmpNum);
		backupDir = segDir + szBuf;
		if (!fs::exists(backupDir))
			break;
		// if a ReadonlySegmentPtr is living somewhere, the backup dir
		// would not have been deleted, this is a rare but valid case
		fprintf(stderr, "WARN: rare but valid: existed %s\n"
			, backupDir.string().c_str());
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

template<class BrainDeadThreadId>
std::string
ThreadIdToString(BrainDeadThreadId id) {
	std::ostringstream oss;
	oss << id;
	return oss.str();
}

void
ReadonlySegment::purgeDeletedRecords(DbTable* tab, size_t segIdx) {
	DbContextPtr ctx(tab->createDbContext());
	ColgroupSegmentPtr input;
    size_t mergeSeqNum;
	{
		MyRwLock lock(tab->m_rwMutex, false);
		input = tab->m_segments[segIdx]->getColgroupSegment();
        mergeSeqNum = tab->m_mergeSeqNum;
		assert(NULL != input);
		assert(input->m_isFreezed);
		assert(!input->m_bookUpdates);
		input->m_updateList.reserve(1024);
		input->m_bookUpdates = true;
	}
	std::string strDir = m_segDir.string();
	m_isDel = input->m_isDel; // make a copy, input->m_isDel[*] may be changed
	m_delcnt = m_isDel.popcnt(); // recompute delcnt
	m_indices.resize(m_schema->getIndexNum());
	m_colgroups.resize(m_schema->getColgroupNum());
	auto tmpSegDir = m_segDir + ".tmp";
	fs::create_directories(tmpSegDir);
	try {
		for (size_t i = 0; i < m_indices.size(); ++i) {
			m_indices[i] = purgeIndex(i, input.get(), ctx.get());
			m_colgroups[i] = m_indices[i]->getReadableStore();
		}
		for (size_t i = m_indices.size(); i < m_colgroups.size(); ++i) {
			m_colgroups[i] = purgeColgroup(i, input.get(), ctx.get(), tmpSegDir);
		}
	}
	catch (const std::exception& ex) {
		fs::remove_all(tmpSegDir);
		THROW_STD(logic_error, "generate new segment %s failed: %s"
			, tmpSegDir.string().c_str(), ex.what());
	}
    MyRwLock lock(tab->m_rwMergeMutex, false);
    size_t finalSegIdx = segIdx;
    if (mergeSeqNum != tab->m_mergeSeqNum) {
		MyRwLock lock(tab->m_rwMutex, false);
        bool miss = true;
        for (size_t i = 0, e = tab->m_segments.size(); i < e; ++i) {
            if (tab->m_segments[i]->getColgroupSegment() != input.get())
                continue;
            finalSegIdx = i;
            miss = false;
            break;
        }
        if (miss) {
            fprintf(stderr
			    , "ERROR: missing segment after merge ! %s\n"
                , m_segDir.string().c_str());
		    abort();
        }
    }
	completeAndReload(tab, finalSegIdx, input.get());
    if (finalSegIdx != segIdx)
        m_segDir = tab->getSegPath("rd", finalSegIdx);
    if (input->getWritableSegment()) {
        fs::rename(tmpSegDir, m_segDir);
        setStorePath(m_segDir);
		input->deleteSegment();
    }
    else {
        if (fs::is_symlink(m_segDir)) {
	        fs::path backupDir = renameToBackupFromDir(tmpSegDir);
            fs::path Rela = ".." / input->m_segDir.parent_path().filename() / input->m_segDir.filename();
            if (fs::read_symlink(m_segDir) == Rela) {
                fs::remove(m_segDir);
            }
            else {
		        THROW_STD(logic_error
			        , "ERROR: error symlink(%s)"
			        , strDir.c_str());
            }
		    fs::rename(backupDir, m_segDir);
            setStorePath(m_segDir);
		    input->deleteSegment();
        }
        else {
	        fs::path backupDir = renameToBackupFromDir(input->m_segDir);
	        try {
                fs::rename(tmpSegDir, m_segDir);
                setStorePath(m_segDir);
            }
	        catch (const std::exception& ex) {
		        fs::rename(backupDir, m_segDir);
		        THROW_STD(logic_error
			        , "ERROR: rename(%s.tmp, %s), ex.what = %s"
			        , strDir.c_str(), strDir.c_str(), ex.what());
	        }
	        {
		        MyRwLock lock(tab->m_rwMutex, true);
		        input->m_segDir.swap(backupDir);
		        input->deleteSegment(); // will delete backupDir
	        }
        }
    }
}

ReadableIndexPtr
ReadonlySegment::purgeIndex(size_t indexId, ColgroupSegment* input, DbContext* ctx) {
	llong inputRowNum = input->m_isDel.size();
	assert(inputRowNum > 0);
	if (m_isDel.size() == m_delcnt) {
		return new EmptyIndexStore();
	}
	const bm_uint_t* isDel = m_isDel.bldata();
	SortableStrVec strVec;
	const Schema& schema = m_schema->getIndexSchema(indexId);
	const size_t  fixlen = schema.getFixedRowLen();
	if (0 == fixlen && schema.m_enableLinearScan) {
		ReadableStorePtr store = new SeqReadAppendonlyStore(input->m_segDir, schema);
		StoreIteratorPtr iter = store->createStoreIterForward(ctx);
		const bm_uint_t* purgeBits = input->m_isPurged.bldata();
		valvec<byte_t> rec;
		llong physicId = 0;
		for(llong logicId = 0; logicId < inputRowNum; ++logicId) {
			if (!purgeBits || !terark_bit_test(purgeBits, logicId)) {
				bool hasRow = iter->increment(&physicId, &rec);
				TERARK_RT_assert(hasRow, std::logic_error);
				TERARK_RT_assert(physicId <= logicId, std::logic_error);
				strVec.push_back(rec);
			}
		}
	}
	else
	{
		const auto& store = *input->m_indices[indexId]->getReadableStore();
		const bm_uint_t* purgeBits = input->m_isPurged.bldata();
		llong physicId = 0;
		for(llong logicId = 0; logicId < inputRowNum; ++logicId) {
			if (!purgeBits || !terark_bit_test(purgeBits, logicId)) {
				if (!terark_bit_test(isDel, logicId)) {
					pushRecord(strVec, store, physicId, fixlen, ctx);
				}
				physicId++;
			}
		}
	}
	return this->buildIndex(schema, strVec);
}

ReadableStorePtr
ReadonlySegment::purgeColgroup(size_t colgroupId, ColgroupSegment* input, DbContext* ctx, PathRef tmpSegDir) {
	assert(m_isDel.size() == input->m_isDel.size());
    ReadableStore* store = input->m_colgroups[colgroupId].get();
    if (dynamic_cast<MultiPartStore*>(store)) {
        size_t newPartIdx = 0;
	    return purgeColgroupMultiPart(colgroupId, m_isDel, m_delcnt, input, ctx, tmpSegDir, newPartIdx);
    }
    double cheapPurgeMultiple = ctx->m_tab->getSchemaConfig().m_cheapPurgeMultiple;
    if (store->dataDictSize() * cheapPurgeMultiple >= store->dataFileSize())
        return purgeColgroupRebuild(colgroupId, m_isDel, m_delcnt, input, ctx, tmpSegDir);
    const Schema& schema = m_schema->getColgroupSchema(colgroupId);
    return purgeDictZipStore(schema,
                             tmpSegDir / "colgroup-" + schema.m_name,
                             store,
                             m_isDel.bldata(),
                             input->m_isPurged.empty() ? nullptr : &input->m_isPurged,
                             0);
}

ReadableStorePtr ReadonlySegment::purgeColgroupMultiPart(size_t colgroupId,
                                                         const febitvec& newIsDel,
                                                         size_t newDelcnt,
                                                         ColgroupSegment* input,
                                                         DbContext* ctx,
                                                         PathRef destSegDir,
                                                         size_t& newPartIdx) {
    const Schema& schema = m_schema->getColgroupSchema(colgroupId);
    auto prefix = "colgroup-" + schema.m_name;
    auto genPrefix = [&]{
	    char szNum[16];
		snprintf(szNum, sizeof(szNum), ".%04zd", newPartIdx++);
        return prefix + szNum;
    };
    assert(!schema.should_use_FixedLenStore());
    double cheapPurgeMultiple = ctx->m_tab->getSchemaConfig().m_cheapPurgeMultiple;
    ReadableStore* store = input->m_colgroups[colgroupId].get();
    MultiPartStore* multiStore = dynamic_cast<MultiPartStore*>(store);
    if (multiStore == nullptr) {
        if (store->dataDictSize() * cheapPurgeMultiple >= store->dataFileSize()) {
            ReadableStorePtr resStore;
            auto tmpDir = destSegDir / "temp-store";
            fs::create_directory(tmpDir);
            resStore = purgeColgroupRebuild(colgroupId, newIsDel, newDelcnt, input, ctx, tmpDir);
            resStore->save(tmpDir / prefix);
            DbTable::moveStoreFiles(tmpDir, destSegDir, prefix, newPartIdx);
            resStore->setStorePath(destSegDir);
            fs::remove_all(tmpDir);
            return resStore;
        }
        return purgeDictZipStore(schema,
                                 destSegDir / genPrefix(),
                                 store,
                                 newIsDel.bldata(),
                                 input->m_isPurged.empty() ? nullptr : &input->m_isPurged,
                                 0);
    }
    auto tmpDir = destSegDir / "temp-store";
    fs::create_directory(tmpDir);
    MultiPartStorePtr resMultiStore = new MultiPartStore();
    double maxCount = 1.0 * multiStore->numDataRows() / multiStore->numParts();
    double minCount = maxCount * 4 / 7;
    enum
    {
        DontRebuild = 0,
        CanRebuild = 1,
        ShouldRebuild = 2,
        NeedRebuild = 3,
    };
    auto getRebuildInfo = [&](ReadableStore *store)->size_t {
        if (store->dataDictSize() * cheapPurgeMultiple >= store->dataFileSize())
            return NeedRebuild;
        if (store->numDataRows() < minCount)
            return ShouldRebuild;
        if (store->numDataRows() < maxCount)
            return CanRebuild;
        return DontRebuild;
    };
    llong baseId = 0;
    for (size_t i = 0; i < multiStore->numParts(); ) {
        size_t j = i + 1;
        auto srcPart = multiStore->getPart(i);
        size_t status = getRebuildInfo(srcPart);
        if (status >= CanRebuild) {
            size_t totalInflateSize = srcPart->dataInflateSize(), totalDataRows = srcPart->numDataRows();
            for (; j < multiStore->numParts(); ++j) {
                auto itPart = multiStore->getPart(j);
                size_t itStatus = getRebuildInfo(itPart);
                if (itStatus == DontRebuild)
                    break;
                status = std::max(status, itStatus);
                totalInflateSize += itPart->dataInflateSize();
                totalDataRows += itPart->numDataRows();
            }
            if (status == NeedRebuild || (status == ShouldRebuild && j - i > 1)) {
                if (schema.m_dictZipSampleRatio >= 0.0 &&
                    (schema.m_dictZipSampleRatio > FLT_EPSILON || 1.0 * totalInflateSize / totalDataRows > 100)) {
                    MultiPartStorePtr subMultiStore = new MultiPartStore();
                    for (; i < j; ++i)
                        subMultiStore->addpart(multiStore->getPart(i));
                    ForwardPartStoreIterator iter(subMultiStore->finishParts(),
                                                  baseId,
                                                  newIsDel.bldata(),
                                                  input->m_isPurged.empty() ? nullptr : &input->m_isPurged,
                                                  ctx);
			        auto newPart = buildDictZipStore(schema, tmpDir, iter, nullptr, nullptr);
                    DbTable::moveStoreFiles(tmpDir, destSegDir, prefix, newPartIdx);
                    resMultiStore->addpart(newPart);
                    baseId += totalDataRows;
                }
                else {
                    std::unique_ptr<SeqReadAppendonlyStore> seqStore;
                    if (schema.m_enableLinearScan)
                        seqStore.reset(new SeqReadAppendonlyStore(tmpDir, schema));
                    SortableStrVec strVec;
                    size_t fixlen = schema.getFixedRowLen();
                    size_t maxMem = size_t(m_schema->m_compressingWorkMemSize);
                    auto partsPushRecord = [&](const ReadableStore& store, llong physicId) {
                        if (terark_unlikely(strVec.mem_size() >= maxMem)) {
                            auto newPart = this->buildStore(schema, strVec);
                            newPart->save(destSegDir / genPrefix());
                            resMultiStore->addpart(newPart);
                            strVec.clear();
                        }
                        size_t oldsize = strVec.size();
                        pushRecord(strVec, store, physicId, fixlen, ctx);
                        if (seqStore)
                            seqStore->append(fstring(strVec.m_strpool).substr(oldsize), NULL);
                    };
                    const bm_uint_t* oldpurgeBits = input->m_isPurged.bldata();
                    const bm_uint_t* isDel = newIsDel.bldata();
                    assert(!oldpurgeBits || input->m_isPurged.size() == newIsDel.size());
                    do {
                        auto isPurged = input->m_isPurged.empty() ? nullptr : &input->m_isPurged;
                        size_t baseId_of_isDel = size_t(baseId);
                        size_t recNum = size_t(srcPart->numDataRows());
	                    for(size_t oldPhysicId = 0; oldPhysicId < recNum; oldPhysicId++) {
                            size_t logicId = isPurged
                                ? isPurged->select0(baseId_of_isDel + oldPhysicId)
                                : baseId_of_isDel + oldPhysicId;
		                    if (!terark_bit_test(isDel, logicId)) {
			                    partsPushRecord(*srcPart, oldPhysicId);
		                    }
	                    }
                        baseId += srcPart->numDataRows();
                        ++i;
                    } while((i < j) && (srcPart = multiStore->getPart(i)));
                    if (strVec.str_size() > 0) {
                        auto newPart = this->buildStore(schema, strVec);
                        newPart->save(destSegDir / genPrefix());
                        resMultiStore->addpart(newPart);
                    }
                }
                continue;
            }
        }
        auto new_part = purgeDictZipStore(schema,
                                          destSegDir / genPrefix(),
                                          srcPart,
                                          newIsDel.bldata(),
                                          input->m_isPurged.empty() ? nullptr : &input->m_isPurged,
                                          size_t(baseId));
        baseId += srcPart->numDataRows();
        resMultiStore->addpart(new_part);
        ++i;
    }
    fs::remove_all(tmpDir);
    resMultiStore->finishParts();
    resMultiStore->setStorePath(destSegDir / prefix);
    return resMultiStore;
}

// should be a static/factory method in the future refactory
ReadableStorePtr
ReadonlySegment::purgeColgroupRebuild(size_t colgroupId,
                                      const febitvec& newIsDel,
                                      size_t newDelcnt,
                                      ColgroupSegment* input,
                                      DbContext* ctx,
                                      PathRef tmpSegDir) {
	assert(newIsDel.size() == input->m_isDel.size());
	assert(newIsDel.popcnt() == newDelcnt);
	if (newIsDel.size() == newDelcnt) {
		return new EmptyIndexStore();
	}
	const bm_uint_t* isDel = newIsDel.bldata();
	const llong inputRowNum = input->m_isDel.size();
	const Schema& schema = m_schema->getColgroupSchema(colgroupId);
	const auto& colgroup = *input->m_colgroups[colgroupId];
	if (schema.should_use_FixedLenStore()) {
		FixedLenStorePtr store = new FixedLenStore(tmpSegDir, schema);
		store->unneedsLock();
		store->reserveRows(newIsDel.size() - newDelcnt);
		llong physicId = 0;
		const bm_uint_t* isPurged = input->m_isPurged.bldata();
		valvec<byte> buf;
		for (llong logicId = 0; logicId < inputRowNum; logicId++) {
			if (!isPurged || !terark_bit_test(isPurged, logicId)) {
				if (!terark_bit_test(isDel, logicId)) {
					colgroup.getValue(physicId, &buf, ctx);
					assert(buf.size() == schema.getFixedRowLen());
					store->append(buf, ctx);
				}
				physicId++;
			}
		}
		assert(!isPurged || llong(input->m_isPurged.max_rank0()) == physicId);
		assert(llong(newIsDel.size() - newDelcnt) == store->numDataRows());
		return store;
	}
	if (schema.m_dictZipSampleRatio >= 0.0) {
		double avgLen = 1.0 * colgroup.dataInflateSize() / colgroup.numDataRows();
		if (schema.m_dictZipSampleRatio > FLT_EPSILON || avgLen > 100) {
			StoreIteratorPtr iter = colgroup.ensureStoreIterForward(ctx);
			auto store = buildDictZipStore(schema, tmpSegDir, *iter, isDel, &input->m_isPurged);
			assert(llong(newIsDel.size() - newDelcnt) == store->numDataRows());
			return store;
		}
	}
	std::unique_ptr<SeqReadAppendonlyStore> seqStore;
	if (schema.m_enableLinearScan) {
		seqStore.reset(new SeqReadAppendonlyStore(tmpSegDir, schema));
	}
	SortableStrVec strVec;
	size_t fixlen = schema.getFixedRowLen();
	size_t maxMem = size_t(m_schema->m_compressingWorkMemSize);
	MultiPartStorePtr parts = new MultiPartStore();
	auto partsPushRecord = [&](const ReadableStore& store, llong physicId) {
		if (terark_unlikely(strVec.mem_size() >= maxMem)) {
			parts->addpart(this->buildStore(schema, strVec));
			strVec.clear();
		}
		size_t oldsize = strVec.size();
		pushRecord(strVec, store, physicId, fixlen, ctx);
		if (seqStore)
			seqStore->append(fstring(strVec.m_strpool).substr(oldsize), NULL);
	};
	const bm_uint_t* oldpurgeBits = input->m_isPurged.bldata();
	assert(!oldpurgeBits || input->m_isPurged.size() == newIsDel.size());
	if (auto cgparts = dynamic_cast<const MultiPartStore*>(&colgroup)) {
		llong logicId = 0;
		for (size_t j = 0; j < cgparts->numParts(); ++j) {
			auto& partStore = *cgparts->getPart(j);
			llong partRows = partStore.numDataRows();
			llong subPhysicId = 0;
			while (logicId < inputRowNum && subPhysicId < partRows) {
				if (!oldpurgeBits || !terark_bit_test(oldpurgeBits, logicId)) {
					if (!terark_bit_test(isDel, logicId)) {
						partsPushRecord(partStore, subPhysicId);
					}
					subPhysicId++;
				}
				logicId++;
			}
			assert(subPhysicId == partRows);
		}
	}
	else {
		llong physicId = 0;
		for(llong logicId = 0; logicId < inputRowNum; ++logicId) {
			if (!oldpurgeBits || !terark_bit_test(oldpurgeBits, logicId)) {
				if (!terark_bit_test(isDel, logicId)) {
					partsPushRecord(colgroup, physicId);
				}
				physicId++;
			}
		}
#if !defined(NDEBUG)
		if (oldpurgeBits) { assert(size_t(physicId) == input->m_isPurged.max_rank0()); }
		else			  { assert(size_t(physicId) == newIsDel.size()); }
#endif
	}
	if (strVec.str_size() > 0) {
		parts->addpart(this->buildStore(schema, strVec));
	}
	auto store = parts->finishParts();
	assert(llong(newIsDel.size() - newDelcnt) == store->numDataRows());
	return store;
}

void ReadonlySegment::load(PathRef segDir) {
	ColgroupSegment::load(segDir);
	removePurgeBitsForCompactIdspace(segDir);

	size_t physicRows = this->getPhysicRows();
	for (size_t i = 0; i < m_colgroups.size(); ++i) {
		auto store = m_colgroups[i].get();
		assert(size_t(store->numDataRows()) == physicRows);
		if (size_t(store->numDataRows()) != physicRows) {
			TERARK_THROW(DbException
				, "FATAL: "
					"m_colgroups[%zd]->numDataRows() = %lld, physicRows = %zd"
				, i, store->numDataRows(), physicRows
				);
		}
	}
}

void ReadonlySegment::removePurgeBitsForCompactIdspace(PathRef segDir) {
//	assert(m_isDel.size() > 0);
	assert(m_isDelMmap != NULL);
	assert(m_isPurgedMmap == NULL);
	assert(m_isPurged.empty());
	PathRef purgeFpath = segDir / "IsPurged.rs";
	if (!fs::exists(purgeFpath)) {
		return;
	}
	fs::path formalFile = segDir / "IsDel";
	fs::path backupFile = segDir / "IsDel.backup";
	size_t isPurgedMmapBytes = 0;
	m_isPurgedMmap = (byte*)mmap_load(purgeFpath.string(), &isPurgedMmapBytes);
	m_isPurged.risk_mmap_from(m_isPurgedMmap, isPurgedMmapBytes);
	if (m_isDel.size() != m_isPurged.size()) {
		assert(m_isDel.size() < m_isPurged.size());
		// maybe last calling of this function was interupted
		if (fs::exists(backupFile)) {
			closeIsDel();
			fs::remove(formalFile);
			fs::rename(backupFile, formalFile);
			loadIsDel(segDir);
		}
	}
//	assert(m_withPurgeBits); // for self test debug
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
		if (!m_isPurged[oldId]) {
			if (m_isDel[oldId])
				newIsDel.set1(newId);
			++newId;
		}
		else {
			assert(m_isDel[oldId]);
		}
	}
	assert(newId == newRows);
	closeIsDel();
	fs::rename(formalFile, backupFile);
	m_isDel.swap(newIsDel);
	m_delcnt = m_isDel.popcnt();
	try {
		saveIsDel(segDir);
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: save %s failed: %s, restore backup\n"
			, formalFile.string().c_str(), ex.what());
		fs::rename(backupFile, formalFile);
		m_isDel.clear(); // by malloc, of newIsDel
		loadIsDel(segDir);
		return;
	}
	m_isDel.clear(); // by malloc, of newIsDel
	loadIsDel(segDir);
	mmap_close(m_isPurgedMmap, isPurgedMmapBytes);
	m_isPurgedMmap = NULL;
	m_isPurged.risk_release_ownership();
	fs::remove(purgeFpath);
	fs::remove(backupFile);
}

void ReadonlySegment::savePurgeBits(PathRef segDir) const {
	if (m_isPurgedMmap && segDir == m_segDir)
		return;
	if (m_isPurged.empty())
		return;
	assert(m_isPurged.size() == m_isDel.size());
	assert(m_isPurged.max_rank1() <= m_delcnt);
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
	ColgroupSegment::save(segDir);
}

void ColgroupSegment::saveRecordStore(PathRef segDir) const {
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
			MultiPartStorePtr parts = new MultiPartStore();
			size_t j = lo;
			while (j < files.size() && (fname = files[j]).startsWith(prefix)) {
				size_t partIdx = lcast(fname.substr(prefix.size()+1));
				assert(partIdx == j - lo);
				if (partIdx != j - lo) {
					THROW_STD(invalid_argument, "missing part: %s.%zd",
						(segDir / prefix).string().c_str(), j - lo);
				}
				parts->addpart(ReadableStore::openStore(schema, segDir, fname));
				++j;
			}
			m_colgroups[i] = parts->finishParts();
			//assert(parts->numParts() > 1);
		}
		else {
			m_colgroups[i] = ReadableStore::openStore(schema, segDir, fname);
		}
	}
}

void ColgroupSegment::closeFiles() {
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
		std::unique_ptr<ZipIntKeyIndex> store(new ZipIntKeyIndex(schema));
		store->load(path);
		return store.release();
	}
	if (boost::filesystem::exists(path + ".fixlen")) {
		std::unique_ptr<FixedLenKeyIndex> store(new FixedLenKeyIndex(schema));
		store->load(path);
		return store.release();
	}
	if (boost::filesystem::exists(path + ".empty")) {
		std::unique_ptr<EmptyIndexStore> store(new EmptyIndexStore());
		store->load(path);
		return store.release();
	}
	return nullptr;
}

ReadableIndex*
ReadonlySegment::buildIndex(const Schema& schema, SortableStrVec& indexData)
const {
	if (indexData.size() == 0 && indexData.str_size() == 0) {
		return new EmptyIndexStore();
	}
	const size_t fixlen = schema.getFixedRowLen();
	if (schema.columnNum() == 1 && schema.getColumnMeta(0).isInteger()) {
		try {
			std::unique_ptr<ZipIntKeyIndex> index(new ZipIntKeyIndex(schema));
			index->build(schema.getColumnMeta(0).type, indexData);
			return index.release();
		}
		catch (const std::exception&) {
			// ignore and fall through
		}
	}
	if (fixlen && fixlen <= 16) {
		std::unique_ptr<FixedLenKeyIndex> index(new FixedLenKeyIndex(schema));
		index->build(schema, indexData);
		return index.release();
	}
	return nullptr; // derived class should override
}

ReadableStore*
ReadonlySegment::buildStore(const Schema& schema, SortableStrVec& storeData)
const {
	assert(!schema.should_use_FixedLenStore());
	if (storeData.size() == 0 && storeData.str_size() == 0) {
		return new EmptyIndexStore();
	}
	if (schema.columnNum() == 1 && schema.getColumnMeta(0).isInteger()) {
		assert(schema.getFixedRowLen() > 0);
		try {
			std::unique_ptr<ZipIntStore> store(new ZipIntStore(schema));
			store->build(schema.getColumnMeta(0).type, storeData);
			return store.release();
		}
		catch (const std::exception&) {
			// ignore and fall through
			fprintf(stderr,
"try to build ZipIntStore: on %s failed, fallback to FixedLenStore\n",
				schema.m_name.c_str());
			std::unique_ptr<FixedLenStore> store(new FixedLenStore(m_segDir, schema));
			store->build(storeData);
			return store.release();
		}
	}
	return nullptr;
}

///////////////////////////////////////////////////////////////////////////////

DbTransaction::~DbTransaction() {
	assert(started != m_status);
}
DbTransaction::DbTransaction() {
	m_status = committed;
}
void DbTransaction::startTransaction(llong recId) {
	assert(started != m_status);
    m_recId = recId;
	do_startTransaction();
    m_status = started;
}
bool DbTransaction::commit() {
	assert(started == m_status);
	if (do_commit()) {
		m_status = committed;
		return true;
	} else {
		m_status = rollbacked;
		return false;
	}
}
void DbTransaction::rollback() {
	assert(started == m_status);
	do_rollback();
	m_status = rollbacked;
}

DefaultRollbackTransaction::~DefaultRollbackTransaction() {
	assert(nullptr != m_txn);
	if (DbTransaction::started == m_txn->m_status) {
		try {
			m_txn->rollback();
		}
		catch (const std::exception& ex) {
			fprintf(stderr, "ERROR: %s: auto rollback failed: %s\n"
				, BOOST_CURRENT_FUNCTION, ex.what());
		}
		catch (...) {
			fprintf(stderr, "ERROR: %s: auto rollback failed by catch ...\n"
				, BOOST_CURRENT_FUNCTION);
		}
	}
}

WritableSegment::WritableSegment() {
}
WritableSegment::~WritableSegment() {
	if (!m_tobeDel)
		flushSegment();
}

void WritableSegment::pushIsDel(bool val) {
	const size_t ChunkBits = TERARK_IF_DEBUG(4*1024, 1*1024*1024);
	if (terark_unlikely(nullptr == m_isDelMmap)) {
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
	else if (terark_unlikely(m_isDel.size() == m_isDel.capacity())) {
#if !defined(NDEBUG)
		assert((64 + m_isDel.size()) % ChunkBits == 0);
		size_t oldsize = m_isDel.size();
		size_t delcnt0 = m_isDel.popcnt();
		assert(delcnt0 == m_delcnt);
#endif
		size_t newCap = ((64+m_isDel.size()+2*ChunkBits-1) & ~(ChunkBits-1));
		closeIsDel();
		std::string fpath = (m_segDir / "IsDel").string();
		truncate_file(fpath, newCap/8);
		m_isDelMmap = loadIsDel_aux(m_segDir, m_isDel);
#if !defined(NDEBUG)
		size_t delcnt1 = m_isDel.popcnt();
		assert(nullptr != m_isDelMmap);
		assert(m_isDel.size() == oldsize);
		assert(delcnt1 == delcnt0);
		assert(delcnt1 == m_delcnt);
#endif
	}
	assert(m_isDel.size() < m_isDel.capacity());
	assert(m_isDel.size() == size_t(((uint64_t*)m_isDelMmap)[0]));
	m_isDel.unchecked_push_back(val);
	((uint64_t*)m_isDelMmap)[0] = m_isDel.size();
}

void WritableSegment::popIsDel() {
	assert(m_isDel.size() >= 1);
	assert(m_isDel.size() == size_t(((uint64_t*)m_isDelMmap)[0]));
	assert(nullptr != m_isDelMmap);
	m_isDel.pop_back();
	((uint64_t*)m_isDelMmap)[0] = m_isDel.size();
}

AppendableStore* WritableSegment::getAppendableStore() { return this; }
UpdatableStore* WritableSegment::getUpdatableStore() { return this; }
WritableStore* WritableSegment::getWritableStore() { return this; }

void
PlainWritableSegment::getValueAppend(llong recId, valvec<byte>* val, DbContext* ctx)
const {
	if (m_schema->m_updatableColgroups.empty()) {
	//	m_wrtStore->getValueAppend(recId, val, ctx);
		this->getWrtStoreData(recId, val, ctx);
	}
	else {
        auto cols1 = ctx->cols.get();
        auto cols2 = ctx->cols.get();
        auto buf1 = ctx->bufs.get();
        cols1->erase_all();
        buf1->erase_all();
	//	m_wrtStore->getValueAppend(recId, &ctx->buf1, ctx);
		this->getWrtStoreData(recId, buf1.get(), ctx);
		const size_t ProtectCnt = 100;
		SpinRwLock lock;
		if (!m_isFreezed && m_isDel.unused() < ProtectCnt) {
			lock.acquire(m_segMutex, false);
		}
		this->getCombineAppend(recId, val, *buf1, *cols1, *cols2);
	}
}

void PlainWritableSegment::initEmptySegment() {
	if (m_indices.empty()) {
		m_indices.resize(m_schema->getIndexNum());
		for (size_t i = 0; i < m_indices.size(); ++i) {
			const Schema& schema = m_schema->getIndexSchema(i);
			m_indices[i] = createIndex(schema, m_segDir);
		}
	}
	if (!m_schema->m_updatableColgroups.empty()) {
		m_colgroups.resize(m_schema->getColgroupNum());
		for (size_t colgroupId : m_schema->m_updatableColgroups) {
			const Schema& schema = m_schema->getColgroupSchema(colgroupId);
			m_colgroups[colgroupId] = new FixedLenStore(m_segDir, schema);
		}
	}
}

void PlainWritableSegment::markFrozen() {
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto store = dynamic_cast<FixedLenStore*>(m_colgroups[colgroupId].get());
		store->unneedsLock();
	}
	m_isFreezed = true;
}
void ColgroupWritableSegment::markFrozen() {
	for (size_t cgId = 0; cgId < m_colgroups.size(); ++cgId) {
        auto readable_store = m_colgroups[cgId].get();
        assert(readable_store);
        auto wtitable_store = readable_store->getWritableStore();
        assert(wtitable_store);
        wtitable_store->shrinkToFit();
        readable_store->markFrozen();
#if 0 && !defined(NDEBUG)
        assert(readable_store->numDataRows() == m_isDel.size());
		auto schema = &m_schema->getColgroupSchema(cgId);
		if (!schema->m_isInplaceUpdatable) {
            valvec<byte> v;
            for(size_t i = 0, e = m_isDel.size(); i < e; ++i) {
                if(m_isDel.is0(i)) {
                    try {
                        readable_store->getValue(i, &v, nullptr);
                    }
                    catch(...) {
                        assert(0);
                    }
                }
                else {
                    do {
                        try {
                            readable_store->getValue(i, &v, nullptr);
                        }
                        catch(...) {
                            break;
                        }
                        assert(0);
                    }
                    while(0);
                }
            }
        }
#endif
	}
	m_isFreezed = true;
}

void
WritableSegment::indexSearchExactAppend(size_t mySegIdx, size_t indexId,
										fstring key, valvec<llong>* recIdvec,
										DbContext* ctx) const {
	assert(mySegIdx < ctx->m_segCtx.size());
	assert(ctx->getSegmentPtr(mySegIdx) == this);
	assert(m_isPurged.empty());
	assert(!m_hasLockFreePointSearch);
	IndexIterator* iter = ctx->getIndexIterNoLock(mySegIdx, indexId);
	llong recId = -1;
    auto key2 = ctx->bufs.get();
	int cmp = iter->seekLowerBound(key, &recId, key2.get());
	if (cmp == 0) {
		// now IndexIterator::m_isUniqueInSchema is just for this quick check
		// faster than m_schema->getIndexSchema(indexId).m_isUnique
		// use lock if m_isDel.unused() is less than ProtectCnt
		const size_t ProtectCnt = 10;
		assert(iter->isUniqueInSchema() == m_schema->getIndexSchema(indexId).m_isUnique);
		if (iter->isUniqueInSchema()) {
			if (this->m_isFreezed || m_isDel.unused() >= ProtectCnt) {
				if (!m_isDel[recId])
					recIdvec->push_back(recId);
			}
			else {
				SpinRwLock lock(this->m_segMutex, false);
				if (!m_isDel[recId])
					recIdvec->push_back(recId);
			}
		}
		else {
			size_t oldsize = recIdvec->size();
			do {
				recIdvec->push_back(recId);
			} while (iter->increment(&recId, key2.get()) && key == *key2);
			size_t i = oldsize, j = oldsize;
			size_t n = recIdvec->size();
			llong* p = recIdvec->data();
			if (this->m_isFreezed || (n < ProtectCnt && m_isDel.unused() > ProtectCnt)) {
				const bm_uint_t* isDel = m_isDel.bldata();
				for (; j < n; ++j) {
					intptr_t id = intptr_t(p[j]);
					if (!terark_bit_test(isDel, id))
						p[i++] = id;
				}
			}
			else { // same code, but with lock, lock as less as possible
				SpinRwLock lock(this->m_segMutex, false);
				const bm_uint_t* isDel = m_isDel.bldata();
				for (; j < n; ++j) {
					intptr_t id = intptr_t(p[j]);
					if (!terark_bit_test(isDel, id))
						p[i++] = id;
				}
			}
			recIdvec->risk_set_size(i);
		}
	}
	iter->reset();
}

void PlainWritableSegment::getCombineAppend(llong recId, valvec<byte>* val,
					valvec<byte>& wrtBuf, ColumnVec& cols1, ColumnVec& cols2)
const {
	auto& sconf = *m_schema;
	assert(m_colgroups.size() == sconf.getColgroupNum());
	cols1.reserve(sconf.columnNum());
	sconf.m_wrtSchema->parseRowAppend(wrtBuf, 0, &cols1);
	for(size_t colgroupId : sconf.m_updatableColgroups) {
		auto& schema = sconf.getColgroupSchema(colgroupId);
		auto cg = m_colgroups[colgroupId].get();
		assert(nullptr != cg);
		size_t oldsize = wrtBuf.size();
		cg->getValueAppend(recId, &wrtBuf, NULL);
		schema.parseRowAppend(wrtBuf, oldsize, &cols1);
	}
	cols2.m_base = wrtBuf.data();
	cols2.m_cols.resize_fill(sconf.columnNum());
	auto  pCols1 = cols1.m_cols.data();
	auto  pCols2 = cols2.m_cols.data();
	auto& wrtSchema = *sconf.m_wrtSchema;
	for(size_t i  = 0, n = wrtSchema.columnNum(); i < n; ++i) {
		size_t j  = wrtSchema.parentColumnId(i);
		pCols2[j] = pCols1[i];
	}
	size_t baseColumnIdx1 = wrtSchema.columnNum();
	for(size_t colgroupId : sconf.m_updatableColgroups) {
		auto & schema = sconf.getColgroupSchema(colgroupId);
		size_t colnum = schema.columnNum();
		for(size_t i  = 0; i < colnum; ++i) {
			size_t j  = schema.parentColumnId(i);
			pCols2[j] = pCols1[baseColumnIdx1 + i];
		}
		baseColumnIdx1 += colnum;
	}
	sconf.m_rowSchema->combineRowAppend(cols2, val);
}

void PlainWritableSegment::selectColumns(llong recId,
									const size_t* colsId, size_t colsNum,
									valvec<byte>* colsData, DbContext* ctx)
const {
	if (m_schema->m_updatableColgroups.empty()) {
		selectColumnsByWhole(recId, colsId, colsNum, colsData, ctx);
	}
	else {
		selectColumnsCombine(recId, colsId, colsNum, colsData, ctx);
	}
}

void PlainWritableSegment::selectColumnsByWhole(llong recId,
									const size_t* colsId, size_t colsNum,
									valvec<byte>* colsData, DbContext* ctx)
const {
	assert(m_schema->m_updatableColgroups.empty());
	colsData->erase_all();
    auto cols = ctx->cols.get();
    auto buf = ctx->bufs.get();
//	this->getValue(recId, buf.get(), ctx);
	this->getWrtStoreData(recId, buf.get(), ctx);
	const Schema& schema = *m_schema->m_rowSchema;
	schema.parseRow(*buf, cols.get());
	assert(cols->size() == schema.columnNum());
	for(size_t i = 0; i < colsNum; ++i) {
		size_t columnId = colsId[i];
		assert(columnId < schema.columnNum());
		if (i < colsNum-1)
			schema.projectToNorm((*cols)[columnId], columnId, colsData);
		else
			schema.projectToLast((*cols)[columnId], columnId, colsData);
	}
}

void PlainWritableSegment::selectColumnsCombine(llong recId,
									const size_t* colsIdvec, size_t colsNum,
									valvec<byte>* colsData, DbContext* ctx)
const {
	colsData->erase_all();
	const SchemaConfig& sconf = *m_schema;
	const Schema& rowSchema = *sconf.m_rowSchema;
	auto cols1 = ctx->cols.get();
    cols1->erase_all();
	for(size_t i = 0; i < colsNum; ++i) {
		size_t columnId = colsIdvec[i];
		assert(columnId < rowSchema.columnNum());
		auto colproj = sconf.m_colproject[columnId];
		auto schema = &sconf.getColgroupSchema(colproj.colgroupId);
		if (schema->m_isInplaceUpdatable) {
			assert(colproj.colgroupId >= sconf.getIndexNum());
			auto store = m_colgroups[colproj.colgroupId].get();
#if !defined(NDEBUG)
			size_t fixlen = schema->getFixedRowLen();
			assert(fixlen > 0);
			assert(nullptr != store);
			auto&  colmeta = schema->getColumnMeta(colproj.subColumnId);
			assert(colmeta.fixedLen > 0);
			assert(colmeta.fixedEndOffset() <= fixlen);
#endif
			store->getValueAppend(recId, colsData, ctx);
		}
		else {
			schema = sconf.m_wrtSchema.get();
			if (cols1->empty()) {
                auto buf1 = ctx->bufs.get();
			//	m_wrtStore->getValue(recId, &ctx->buf1, ctx);
                this->getWrtStoreData(recId, buf1.get(), ctx);
				schema->parseRow(*buf1, cols1.get());
			}
			size_t subColumnId = sconf.m_rowSchemaColToWrtCol[columnId];
			assert(subColumnId < sconf.m_wrtSchema->columnNum());
			fstring coldata = (*cols1)[subColumnId];
			if (i < colsNum-1)
				rowSchema.projectToNorm(coldata, columnId, colsData);
			else
				rowSchema.projectToLast(coldata, columnId, colsData);
		}
	}
}

void PlainWritableSegment::selectOneColumn(llong recId, size_t columnId,
									  valvec<byte>* colsData, DbContext* ctx)
const {
	assert(columnId < m_schema->columnNum());
	auto colproj = m_schema->m_colproject[columnId];
	auto& schema = m_schema->getColgroupSchema(colproj.colgroupId);
	if (schema.m_isInplaceUpdatable) {
		auto store = m_colgroups[colproj.colgroupId].get();
#if !defined(NDEBUG)
		auto fixlen = schema.getFixedRowLen();
		assert(nullptr != store);
		assert(fixlen > 0);
		const auto& colmeta = schema.getColumnMeta(colproj.subColumnId);
		assert(colmeta.fixedLen > 0);
		assert(colmeta.fixedEndOffset() <= fixlen);
#endif
		store->getValue(recId, colsData, ctx);
	}
	else {
        auto cols = ctx->cols.get();
        auto buf = ctx->bufs.get();
		const Schema& wrtSchema = *m_schema->m_wrtSchema;
	//	m_wrtStore->getValue(recId, &ctx->buf1, ctx);
		this->getWrtStoreData(recId, buf.get(), ctx);
		wrtSchema.parseRow(*buf, cols.get());
		assert(cols->size() == wrtSchema.columnNum());
		colsData->erase_all();
		if (m_schema->m_updatableColgroups.empty()) {
			assert(m_schema->m_wrtSchema == m_schema->m_rowSchema);
			assert(m_schema->m_rowSchemaColToWrtCol.empty());
			wrtSchema.projectToLast((*cols)[columnId], columnId, colsData);
		}
		else {
			size_t wrtColumnId = m_schema->m_rowSchemaColToWrtCol[columnId];
			assert(wrtColumnId < wrtSchema.columnNum());
			wrtSchema.projectToLast((*cols)[wrtColumnId], columnId, colsData);
		}
	}
}

void PlainWritableSegment::selectColgroups(llong recId,
						const size_t* cgIdvec, size_t cgIdvecSize,
						valvec<byte>* cgDataVec, DbContext* ctx) const {
	for(size_t i = 0; i < cgIdvecSize; ++i) {
		size_t cgId = cgIdvec[i];
		if (cgId >= m_schema->getColgroupNum()) {
			THROW_STD(out_of_range, "cgId = %zd, cgNum = %zd"
				, cgId, m_schema->getColgroupNum());
		}
		const ReadableStore* store = m_colgroups.empty() ? NULL : m_colgroups[cgId].get();
		if (store) {
			// inplace updatable store
			assert(store->getRecordsBasePtr() != NULL);
			store->getValue(recId, &cgDataVec[i], ctx);
		}
		else {
			const Schema& schema = m_schema->getColgroupSchema(cgId);
			const valvec<size_t>& colsIdvec = schema.getProj();
			selectColumns(recId, colsIdvec.data(), colsIdvec.size(),
						  &cgDataVec[i], ctx);
		}
	}
}

void WritableSegment::flushSegment() {
	if (m_tobeDel) {
		return;
	}
	if (m_isDirty) {
		save(m_segDir);
		m_isDirty = false;
	}
}

void PlainWritableSegment::saveRecordStore(PathRef segDir) const {
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		const Schema& schema = m_schema->getColgroupSchema(colgroupId);
		assert(schema.m_isInplaceUpdatable);
		assert(schema.getFixedRowLen() > 0);
		auto store = m_colgroups[colgroupId];
		assert(nullptr != store);
		store->save(segDir / "colgroup-" + schema.m_name);
	}
	m_wrtStore->save(segDir / "__wrtStore__");
}

void PlainWritableSegment::loadRecordStore(PathRef segDir) {
	assert(m_colgroups.size() == 0);
	m_colgroups.resize(m_schema->getColgroupNum());
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		const Schema& schema = m_schema->getColgroupSchema(colgroupId);
		assert(schema.m_isInplaceUpdatable);
		assert(schema.getFixedRowLen() > 0);
		std::unique_ptr<FixedLenStore> store(new FixedLenStore(segDir, schema));
	//	auto store(std::make_unique<FixedLenStore>(segDir, schema));
		store->openStore();
		m_colgroups[colgroupId] = store.release();
	}
	m_wrtStore->load(segDir / "__wrtStore__");
}

ColgroupSegment* WritableSegment::getColgroupSegment() const {
	THROW_STD(invalid_argument, "this method should not be called");
}
ColgroupSegment* PlainWritableSegment::getColgroupSegment() const {
	// although WritableSegment is derived from ColgroupSegment
	// this function doen't return this as ColgroupSegment
	return nullptr;
}
ColgroupSegment* ColgroupWritableSegment::getColgroupSegment() const {
	return const_cast<ColgroupWritableSegment*>(this);
}

WritableSegment* WritableSegment::getWritableSegment() const {
	return const_cast<WritableSegment*>(this);
}

PlainWritableSegment* PlainWritableSegment::getPlainWritableSegment() const {
	return const_cast<PlainWritableSegment*>(this);
}

llong PlainWritableSegment::totalStorageSize() const {
	llong size = m_wrtStore->dataStorageSize() + totalIndexSize();
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto store = m_colgroups[colgroupId].get();
		assert(nullptr != store);
		size += store->dataStorageSize();
	}
	return size;
}

llong PlainWritableSegment::dataInflateSize() const {
	llong size = m_wrtStore->dataInflateSize();
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto store = m_colgroups[colgroupId].get();
		assert(nullptr != store);
		size += store->dataInflateSize();
	}
	return size;
}

llong PlainWritableSegment::dataStorageSize() const {
	llong size = m_wrtStore->dataStorageSize();
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto store = m_colgroups[colgroupId].get();
		assert(nullptr != store);
		size += store->dataStorageSize();
	}
	return size;
}

class PlainWritableSegment::MyStoreIter : public StoreIterator {
	const SchemaConfig& m_sconf;
	const PlainWritableSegment* m_wrtSeg;
	StoreIteratorPtr m_wrtIter;
	valvec<byte> m_wrtBuf;
	ColumnVec    m_cols1;
	ColumnVec    m_cols2;
public:
	MyStoreIter(const PlainWritableSegment* wrtSeg, StoreIterator* wrtIter,
				DbContext* ctx, const SchemaConfig& sconf)
	  : m_sconf(sconf)
	{
		m_store = const_cast<PlainWritableSegment*>(wrtSeg);
		m_wrtSeg = wrtSeg;
		m_wrtIter = wrtIter;
	}
	bool increment(llong* id, valvec<byte>* val) override {
		// don't test m_isDel, it requires lock
		// inplace readable store also requires lock
		if (m_sconf.m_updatableColgroups.empty()) {
			if (m_wrtIter->increment(id, val)) {
				return true;
			}
			return false;
		}
		if (m_wrtIter->increment(id, &m_wrtBuf)) {
			val->erase_all();
			m_cols1.erase_all();
			m_wrtSeg->getCombineAppend(*id, val, m_wrtBuf, m_cols1, m_cols2);
			return true;
		}
		return false;
	}
	bool seekExact(llong id, valvec<byte>* val) override {
		m_wrtIter->reset();
		if (m_sconf.m_updatableColgroups.empty()) {
			return m_wrtIter->seekExact(id, val);
		}
		m_wrtBuf.erase_all();
		m_cols1.erase_all();
		if (m_wrtIter->seekExact(id, &m_wrtBuf)) {
			val->erase_all();
			m_wrtSeg->getCombineAppend(id, val, m_wrtBuf, m_cols1, m_cols2);
			return true;
		}
		return false;
	}
	void reset() override {
		m_wrtIter->reset();
	}
};

StoreIterator*
PlainWritableSegment::createStoreIterForward(DbContext* ctx) const {
	if (m_schema->m_updatableColgroups.empty()) {
		return m_wrtStore->createStoreIterForward(ctx);
	}
	else {
		auto wrtIter = m_wrtStore->createStoreIterForward(ctx);
		return new MyStoreIter(this, wrtIter, ctx, *m_schema);
	}
}

StoreIterator*
PlainWritableSegment::createStoreIterBackward(DbContext* ctx) const {
	if (m_schema->m_updatableColgroups.empty()) {
		return m_wrtStore->createStoreIterBackward(ctx);
	}
	else {
		auto wrtIter = m_wrtStore->createStoreIterBackward(ctx);
		return new MyStoreIter(this, wrtIter, ctx, *m_schema);
	}
}

//static void splitRowToWrt

llong PlainWritableSegment::append(fstring row, DbContext* ctx) {
	auto store = m_wrtStore->getAppendableStore();
	assert(nullptr != store);
	const SchemaConfig& sconf = *m_schema;
	if (sconf.m_updatableColgroups.empty()) {
		return store->append(row, ctx);
	}
	else {
        auto cols = ctx->cols.get();
        auto buf = ctx->bufs.get();
		sconf.m_rowSchema->parseRow(row, cols.get());
		sconf.m_wrtSchema->selectParent(*cols, buf.get());
		llong id1 = store->append(*buf, ctx);
		for (size_t colgroupId : sconf.m_updatableColgroups) {
			store = m_colgroups[colgroupId]->getAppendableStore();
			assert(nullptr != store);
			const Schema& schema = sconf.getColgroupSchema(colgroupId);
			schema.selectParent(*cols, buf.get());
			llong id2 = store->append(*buf, ctx);
			TERARK_RT_assert(id1 == id2, std::logic_error);
		}
		return id1;
	}
}

void PlainWritableSegment::update(llong id, fstring row, DbContext* ctx) {
	assert(id <= llong(m_isDel.size()));
	auto store = m_wrtStore->getUpdatableStore();
	assert(nullptr != store);
	const SchemaConfig& sconf = *m_schema;
	if (sconf.m_updatableColgroups.empty()) {
		store->update(id, row, ctx);
	}
	else {
        auto cols = ctx->cols.get();
        auto buf = ctx->bufs.get();
		sconf.m_rowSchema->parseRow(row, cols.get());
		sconf.m_wrtSchema->selectParent(*cols, buf.get());
		store->update(id, *buf, ctx);
		for (size_t colgroupId : sconf.m_updatableColgroups) {
			store = m_colgroups[colgroupId]->getUpdatableStore();
			assert(nullptr != store);
			const Schema& schema = sconf.getColgroupSchema(colgroupId);
			schema.selectParent(*cols, buf.get());
			store->update(id, *buf, ctx);
		}
	}
}

void PlainWritableSegment::remove(llong id, DbContext* ctx) {
	m_wrtStore->getWritableStore()->remove(id, ctx);
}

void PlainWritableSegment::shrinkToFit() {
	for (size_t colgroupId : m_schema->m_updatableColgroups) {
		auto store = m_colgroups[colgroupId]->getAppendableStore();
		assert(nullptr != store);
		store->shrinkToFit();
	}
	m_wrtStore->getAppendableStore()->shrinkToFit();
}

void PlainWritableSegment::shrinkToSize(size_t size)
{
    for(size_t colgroupId : m_schema->m_updatableColgroups)
    {
        auto store = m_colgroups[colgroupId]->getAppendableStore();
        assert(nullptr != store);
        store->shrinkToSize(size);
    }
    m_wrtStore->getAppendableStore()->shrinkToSize(size);
}

void PlainWritableSegment::getWrtStoreData(llong subId, valvec<byte>* buf, DbContext* ctx)
const {
	if (m_hasLockFreePointSearch) {
		m_wrtStore->getValue(subId, buf, ctx);
	}
	else {
		ctx->getWrSegWrtStoreData(this, subId, buf);
	}
}

void WritableSegment::delmarkSet0(llong subId) {
	SpinRwLock segLock(m_segMutex, true);
	assert(m_isDel[subId]);
	m_isDirty = true;
	m_isDel.set0(subId);
	m_delcnt--;
	assert(m_isDel.popcnt() == m_delcnt);
}

///////////////////////////////////////////////////////////////////////////////

ColgroupWritableSegment::~ColgroupWritableSegment() {
}

void ColgroupWritableSegment::initEmptySegment() {
	size_t const indices_size = m_schema->getIndexNum();
	size_t const colgroups_size = m_schema->getColgroupNum();
	m_indices.resize(indices_size);
	m_colgroups.resize(colgroups_size);
	for(size_t i = 0; i < indices_size; ++i) {
		const Schema& schema = m_schema->getIndexSchema(i);
		m_indices[i] = createIndex(schema, m_segDir);
		auto store = m_indices[i]->getReadableStore();
		assert(store);
		m_colgroups[i] = store;
	}
	for(size_t i = indices_size; i < colgroups_size; ++i) {
		const Schema& schema = m_schema->getColgroupSchema(i);
		if (schema.m_isInplaceUpdatable) {
			m_colgroups[i] = new FixedLenStore(m_segDir, schema);
		} else {
			m_colgroups[i] = createStore(schema, m_segDir);
		}
	}
}

void ColgroupWritableSegment::saveRecordStore(PathRef dir) const {
	ColgroupSegment::saveRecordStore(dir);
}

void ColgroupWritableSegment::loadRecordStore(PathRef dir) {
//  TODO:
	abort();
}

llong ColgroupWritableSegment::dataStorageSize() const {
	return ColgroupSegment::dataStorageSize();
}

llong ColgroupWritableSegment::totalStorageSize() const {
	return ColgroupSegment::totalStorageSize();
}

} } // namespace terark::db
