#include "db_table.hpp"
#include <nark/util/autoclose.hpp>
#include <nark/io/FileStream.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/fsa/fsa.hpp>
#include <nark/lcast.hpp>

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
	valvec<size_t> columnIdMap(m_nonIndexRowSchema->columnNum(), size_t(-1));
	for (size_t i = 0; i < m_nonIndexRowSchema->columnNum(); ++i) {
		fstring columnName = m_nonIndexRowSchema->getColumnName(i);
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
		for (size_t i = 0; i < m_nonIndexRowSchema->columnNum(); i++) {
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

} // namespace nark
