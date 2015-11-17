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
			if (i < m_columnsMeta.end_i() - 1) {
				CHECK_CURR_LAST(coldata.n + 1);
				coldata.postLen = 1;
				curr += coldata.n + 1;
			}
			else { // the last column
				if (coldata.n + 1 < last - curr) {
					// '\0' is optional, if '\0' exists, it must at string end
					THROW_STD(invalid_argument,
						"'\0' in StrZero is not at string end");
				}
				coldata.postLen = last - curr - coldata.n;
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < m_columnsMeta.end_i() - 1) {
				const byte* next = nullptr;
				coldata.n = load_var_uint64(curr, &next);
				coldata.preLen = next - curr; // length of var_uint
				CHECK_CURR_LAST(coldata.n);
				curr = next + coldata.n;
			}
			else { // the last column
				coldata.n = last - curr;
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

const char* Schema::columnTypeStr(ColumnType t) {
	switch (t) {
	default:
		THROW_STD(invalid_argument, "Bad column type = %d", int(t));
	case ColumnType::Uint08:  return "uint08";
	case ColumnType::Sint08:  return "sint08";
	case ColumnType::Uint16:  return "uint16";
	case ColumnType::Sint16:  return "sint16";
	case ColumnType::Uint32:  return "uint32";
	case ColumnType::Sint32:  return "sint32";
	case ColumnType::Uint64:  return "uint64";
	case ColumnType::Sint64:  return "sint64";
	case ColumnType::Uint128: return "uint128";
	case ColumnType::Sint128: return "sint128";
	case ColumnType::Float32: return "float32";
	case ColumnType::Float64: return "float64";
	case ColumnType::Float128:return "float128";
	case ColumnType::Uuid:    return "uuid";
	case ColumnType::Fixed:   return "fixed";
	case ColumnType::StrZero: return "strzero";
	case ColumnType::StrUtf8: return "strutf8";
	case ColumnType::Binary:  return "binary";
	}
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
			colname2val["uint08"] = ColumnType::Uint08;
			colname2val["sint08"] = ColumnType::Sint08;
			colname2val["uint16"] = ColumnType::Uint16;
			colname2val["sint16"] = ColumnType::Sint16;
			colname2val["uint32"] = ColumnType::Uint32;
			colname2val["sint32"] = ColumnType::Sint32;
			colname2val["uint64"] = ColumnType::Uint64;
			colname2val["sint64"] = ColumnType::Sint64;
			colname2val["uint128"] = ColumnType::Uint128;
			colname2val["sint128"] = ColumnType::Sint128;
			colname2val["float32"] = ColumnType::Float32;
			colname2val["float64"] = ColumnType::Float64;
			colname2val["float128"] = ColumnType::Float128;
			colname2val["uuid"] = ColumnType::Uuid;
			colname2val["fixed"] = ColumnType::Fixed;
			colname2val["strzero"] = ColumnType::StrZero;
			colname2val["strutf8"] = ColumnType::StrUtf8;
			colname2val["binary"] = ColumnType::Binary;
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

std::string Schema::joinColumnNames(char delim) const {
	std::string joined;
	joined.reserve(m_columnsMeta.whole_strpool().size());
	for (size_t i = 0; i < m_columnsMeta.end_i(); ++i) {
		fstring colname = m_columnsMeta.key(i);
		joined.append(colname.data(), colname.size());
		joined.push_back(delim);
	}
	joined.pop_back();
	return joined;
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

