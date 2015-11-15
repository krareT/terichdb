#include "db_conf.hpp"
//#include <nark/io/DataIO.hpp>
//#include <nark/io/MemStream.hpp>
#include <nark/io/var_int.hpp>
#include <string.h>

namespace nark {

ColumnData::ColumnData(const ColumnMeta& meta, fstring row) {
#define CHECK_DATA_SIZE(len) \
	if (len != row.size()) { \
		THROW_STD(out_of_range, "len=%ld dsize=%ld", \
			long(len), long(row.size())); \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	this->p = row.p;
	this->n = row.n;
	this->type = meta.type;
	switch (meta.type) {
	default:
		THROW_STD(runtime_error, "Invalid data row");
		break;
	case ColumnType::WholeRow: // at most one WholeRow column
		this->n = row.n;
		break;
	case ColumnType::Uint08:
	case ColumnType::Sint08:
		CHECK_DATA_SIZE(1);
		break;
	case ColumnType::Uint16:
	case ColumnType::Sint16:
		CHECK_DATA_SIZE(2);
		break;
	case ColumnType::Uint32:
	case ColumnType::Sint32:
		CHECK_DATA_SIZE(4);
		break;
	case ColumnType::Uint64:
	case ColumnType::Sint64:
		CHECK_DATA_SIZE(8);
		break;
	case ColumnType::Uint128:
	case ColumnType::Sint128:
		CHECK_DATA_SIZE(16);
		break;
	case ColumnType::Float32:
		CHECK_DATA_SIZE(4);
		break;
	case ColumnType::Float64:
		CHECK_DATA_SIZE(8);
		break;
	case ColumnType::Float128:
	case ColumnType::Uuid:    // 16 bytes(128 bits) binary
		CHECK_DATA_SIZE(16);
		break;
	case ColumnType::Fixed:   // Fixed length binary
		CHECK_DATA_SIZE(meta.fixedLen);
		break;
	// has no OOB(out of bound) data when a schema has just one column
	case ColumnType::StrZero: // Zero ended string
		// easy in: zero ending is optional
		this->n = strnlen(row.p, row.n);
		if (this->n == row.n-1 || this->n == row.n) {
			this->postLen = row.n - this->n;
		} else {
			THROW_STD(out_of_range, "type=StrZero, strnlen=%ld dsize=%ld",
				long(this->n), long(row.n));
		}
		break;
	case ColumnType::StrUtf8: // Has no length prefix
	case ColumnType::Binary:  // Has no length prefix
		// do nothing
		break;
	}
}

void Schema::parseRow(fstring row, valvec<ColumnData>* columns) const {
	columns->resize(0);
	parseRowAppend(row, columns);
}

void Schema::parseRowAppend(fstring row, valvec<ColumnData>* columns) const {
	if (m_columnsMeta.end_i() == 1) {
		columns->push_back(ColumnData(m_columnsMeta.val(0), row));
		return;
	}
	const byte* curr = row.udata();
	const byte* last = row.size() + curr;

#define CHECK_CURR_LAST(len) \
	if (curr + len > last) { \
		THROW_STD(out_of_range, "len=%ld remain=%ld", \
			long(len), long(last-curr)); \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	for (size_t i = 0; i < m_columnsMeta.end_i(); ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		ColumnData coldata(colmeta.type);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::WholeRow:
			// at most one WholeRow column, and must be the last column
			assert(m_columnsMeta.end_i()-1 == i);
			if (m_columnsMeta.end_i()-1 != i) {
				THROW_STD(invalid_argument, "WholeRow type must be the last column");
			}
			coldata.n = last - curr;
			curr = last;
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			CHECK_CURR_LAST(1);
			coldata.n = 1;
			curr += 1;
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			CHECK_CURR_LAST(2);
			coldata.n = 2;
			curr += 2;
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			CHECK_CURR_LAST(4);
			coldata.n = 4;
			curr += 4;
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			CHECK_CURR_LAST(8);
			coldata.n = 8;
			curr += 8;
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			CHECK_CURR_LAST(16);
			coldata.n = 16;
			curr += 16;
			break;
		case ColumnType::Float32:
			CHECK_CURR_LAST(4);
			coldata.n = 4;
			curr += 4;
			break;
		case ColumnType::Float64:
			CHECK_CURR_LAST(8);
			coldata.n = 8;
			curr += 8;
			break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			CHECK_CURR_LAST(16);
			coldata.n = 16;
			curr += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			CHECK_CURR_LAST(colmeta.fixedLen);
			coldata.n = colmeta.fixedLen;
			curr += colmeta.fixedLen;
			break;
		case ColumnType::StrZero: // Zero ended string
			coldata.n = strnlen((const char*)curr, last - curr);
			CHECK_CURR_LAST(coldata.n + 1);
			coldata.postLen = 1;
			curr += coldata.n + 1;
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			{
				const byte* next = nullptr;
				coldata.n = load_var_uint64(curr, &next);
				coldata.preLen = next - curr; // length of var_uint
				CHECK_CURR_LAST(coldata.n);
				curr = next + coldata.n;
			}
			break;
		}
		columns->push_back(coldata);
	}
}

ColumnType Schema::getColumnType(size_t columnId) const {
	assert(columnId < m_columnsMeta.end_i());
	if (columnId >= m_columnsMeta.end_i()) {
		THROW_STD(out_of_range
			, "columnId=%ld out of range, columns=%ld"
			, long(columnId), long(m_columnsMeta.end_i()));
	}
	return m_columnsMeta.val(columnId).type;
}

fstring Schema::getColumnName(size_t columnId) const {
	assert(columnId < m_columnsMeta.end_i());
	if (columnId >= m_columnsMeta.end_i()) {
		THROW_STD(out_of_range
			, "columnId=%ld out of range, columns=%ld"
			, long(columnId), long(m_columnsMeta.end_i()));
	}
	return m_columnsMeta.key(columnId);
}

size_t Schema::getColumnId(fstring columnName) const {
	return m_columnsMeta.find_i(columnName);
}

const ColumnMeta& Schema::getColumnMeta(size_t columnId) const {
	assert(columnId < m_columnsMeta.end_i());
	if (columnId >= m_columnsMeta.end_i()) {
		THROW_STD(out_of_range
			, "columnId=%ld out of range, columns=%ld"
			, long(columnId), long(m_columnsMeta.end_i()));
	}
	return m_columnsMeta.val(columnId);
}

size_t Schema::getFixedRowLen() const {
	size_t rowLen = 0;
	for (size_t i = 0; i < m_columnsMeta.end_i(); ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::WholeRow:
			// at most one WholeRow column, and must be the last column
			assert(m_columnsMeta.end_i()-1 == i);
			if (m_columnsMeta.end_i()-1 != i) {
				THROW_STD(invalid_argument, "WholeRow type must be the last column");
			}
			return 0;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			rowLen += 1;
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			rowLen += 2;
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			rowLen += 4;
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			rowLen += 8;
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			rowLen += 16;
			break;
		case ColumnType::Float32:
			rowLen += 4;
			break;
		case ColumnType::Float64:
			rowLen += 8;
			break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			rowLen += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			rowLen += colmeta.fixedLen;
			break;
		case ColumnType::StrZero: // Zero ended string
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			return 0;
		}
	}
	return rowLen;
}

namespace {
	struct ColumnTypeMap : hash_strmap<ColumnType> {
		ColumnTypeMap() {
			auto& colname2val = *this;
			colname2val["WholeRow"] = ColumnType::WholeRow;
			colname2val["Uint08"] = ColumnType::Uint08;
			colname2val["Sint08"] = ColumnType::Sint08;
			colname2val["Uint16"] = ColumnType::Uint16;
			colname2val["Sint16"] = ColumnType::Sint16;
			colname2val["Uint32"] = ColumnType::Uint32;
			colname2val["Sint32"] = ColumnType::Sint32;
			colname2val["Uint64"] = ColumnType::Uint64;
			colname2val["Sint64"] = ColumnType::Sint64;
			colname2val["Uint128"] = ColumnType::Uint128;
			colname2val["Sint128"] = ColumnType::Sint128;
			colname2val["Float32"] = ColumnType::Float32;
			colname2val["Float64"] = ColumnType::Float64;
			colname2val["Float128"] = ColumnType::Float128;
			colname2val["Uuid"] = ColumnType::Uuid;
			colname2val["Fixed"] = ColumnType::Fixed;
			colname2val["StrZero"] = ColumnType::StrZero;
			colname2val["StrUtf8"] = ColumnType::StrUtf8;
			colname2val["Binary"] = ColumnType::Binary;
		}
	};
}
ColumnType Schema::parseColumnType(fstring str) {
	static ColumnTypeMap colname2val;
	size_t i = colname2val.find_i(str);
	if (colname2val.end_i() == i) {
		THROW_STD(invalid_argument,
			"Invalid ColumnType str: %.*s", str.ilen(), str.data());
	} else {
		return colname2val.val(i);
	}
}

// An index can be a composite index, which have multiple columns as key,
// so many indices may share columns, if the column share happens, we just
// need one instance of the column to compose a row from multiple index.
// when iteratively call schema->parseRowAppend(..) of a SchemaSet, all
// columns are concated, duplicated columns may in concated columns.
// m_keepColumn is used to select the using concated columns
// if m_keepColumn[i] is true, then columns[i] should be keeped
// if all columns of an index are not keeped, m_keepSchema[x] is false.
void SchemaSet::compileSchemaSet() {
	size_t numBits = 0;
	for (size_t i = 0; i < m_nested.end_i(); ++i) {
		const Schema* sc = m_nested.elem_at(i).get();
		numBits += sc->columnNum();
	}
	m_keepColumn.resize_fill(numBits, 1);
	m_keepSchema.resize_fill(m_nested.end_i(), 1);
	hash_strmap<int> dedup;
	numBits = 0;
	for (size_t i = 0; i < m_nested.end_i(); ++i) {
		const Schema* sc = m_nested.elem_at(i).get();
		size_t numSkipped = 0;
		for (size_t j = 0; j < sc->m_columnsMeta.end_i(); ++j) {
			fstring columnName = sc->m_columnsMeta.key(j);
			int cnt = dedup[columnName]++;
			if (cnt) {
				m_keepColumn.set0(numBits);
				numSkipped++;
			}
			numBits++;
		}
		if (sc->m_columnsMeta.end_i() == numSkipped) {
			m_keepSchema.set0(i);
		}
	}
}

void SchemaSet::parseNested(const valvec<fstring>& nested, valvec<ColumnData>* flatten)
const {
	flatten->resize(0);

}

///////////////////////////////////////////////////////////////////////////////
size_t SchemaSet::Hash::operator()(const SchemaPtr& x) const {
	size_t h = 8789;
	for (size_t i = 0; i < x->m_columnsMeta.end_i(); ++i) {
		fstring colname = x->m_columnsMeta.key(i);
		size_t h2 = fstring_func::hash()(colname);
		FaboHashCombine(h, h2);
	}
	return h;
}
size_t SchemaSet::Hash::operator()(fstring x) const {
	size_t h = 8789;
	const char* cur = x.begin();
	const char* end = x.end();
	while (end > cur && ',' == end[-1]) --end; // trim trailing ','
	while (cur < end) {
		const char* next = std::find(cur, end, ',');
		fstring colname(cur, next);
		size_t h2 = fstring_func::hash()(colname);
		FaboHashCombine(h, h2);
		cur = next+1;
	}
	return h;
}
bool SchemaSet::Equal::operator()(const SchemaPtr& x, const SchemaPtr& y) const {
	fstring kx = x->m_columnsMeta.whole_strpool();
	fstring ky = y->m_columnsMeta.whole_strpool();
	return fstring_func::equal()(kx, ky);
}
bool SchemaSet::Equal::operator()(const SchemaPtr& x, fstring y) const {
	const char* cur = y.begin();
	const char* end = y.end();
	while (end > cur && ',' == end[-1]) --end; // trim trailing ','
	size_t nth = 0;
	size_t xCols = x->m_columnsMeta.end_i();
	while (cur < end && nth < xCols) {
		const char* next = std::find(cur, end, ',');
		fstring kx(x->m_columnsMeta.key(nth));
		fstring ky(cur, next);
		if (kx != ky)
			return false;
		cur = next+1;
		nth++;
	}
	return nth >= xCols && cur >= end;
}

} // namespace nark


/* currently unused code
class BitVecRangeView {
	const bm_uint_t* m_bitsPtr;
	size_t m_baseIdx;
	size_t m_bitsNum;
public:
	BitVecRangeView(const febitvec& bv, size_t baseIdx, size_t bitsNum) {
		m_bitsPtr = bv.bldata();
		m_baseIdx = baseIdx;
		m_bitsNum = bitsNum;
	}
	bool operator[](size_t i) const {
		assert(i < m_bitsNum);
		return nark_bit_test(m_bitsPtr, i);
	}
	size_t size() const { return m_bitsNum; }
};
*/

