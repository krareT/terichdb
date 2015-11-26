#include "db_conf.hpp"
//#include <nark/io/DataIO.hpp>
//#include <nark/io/MemStream.hpp>
//#include <nark/io/StreamBuffer.hpp>
#include <nark/io/var_int.hpp>
//#include <nark/util/sortable_strvec.hpp>
#include <string.h>
#include "json.hpp"

namespace nark { namespace db {

ColumnMeta::ColumnMeta() {
	type = ColumnType::Binary;
	order = SortOrder::UnOrdered;
}

ColumnMeta::ColumnMeta(ColumnType t, SortOrder ord) {
	type = t;
	order = ord;
	switch (t) {
	default:
		THROW_STD(runtime_error, "Invalid data row");
		break;
	case ColumnType::Uint08:
	case ColumnType::Sint08: fixedLen = 1; break;
	case ColumnType::Uint16:
	case ColumnType::Sint16: fixedLen = 2; break;
	case ColumnType::Uint32:
	case ColumnType::Sint32: fixedLen = 4; break;
	case ColumnType::Uint64:
	case ColumnType::Sint64: fixedLen = 8; break;
	case ColumnType::Uint128:
	case ColumnType::Sint128: fixedLen = 16; break;
	case ColumnType::Float32: fixedLen = 4; break;
	case ColumnType::Float64: fixedLen = 8; break;
	case ColumnType::Float128: fixedLen = 16; break;
	case ColumnType::Uuid: fixedLen = 16; break;
	case ColumnType::Fixed:
		fixedLen = 0; // to be set later
		break;
	case ColumnType::StrZero:
	case ColumnType::StrUtf8:
	case ColumnType::Binary:
		fixedLen = 0;
		break;
	}
}

/////////////////////////////////////////////////////////////////////////////

Schema::Schema() {
	m_fixedLen = size_t(-1);
	m_parent = nullptr;
}
Schema::~Schema() {
}

void Schema::compile(const Schema* parent) {
	m_fixedLen = computeFixedRowLen();
	if (parent) {
		compileProject(parent);
	}
}

void Schema::parseRow(fstring row, valvec<fstring>* columns) const {
	assert(size_t(-1) != m_fixedLen);
	columns->resize(0);
	parseRowAppend(row, columns);
}

void Schema::parseRowAppend(fstring row, valvec<fstring>* columns) const {
	assert(size_t(-1) != m_fixedLen);
	const byte* curr = row.udata();
	const byte* last = row.size() + curr;

#define CHECK_CURR_LAST3(curr, last, len) \
	if (curr + len > last) { \
		THROW_STD(out_of_range, "len=%ld remain=%ld", \
			long(len), long(last-curr)); \
	}
#define CHECK_CURR_LAST(len) CHECK_CURR_LAST3(curr, last, len)
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		fstring coldata;
		coldata.p = (const char*)curr;
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
			if (i < colnum - 1) {
				CHECK_CURR_LAST(coldata.n + 1);
				curr += coldata.n + 1;
			}
			else { // the last column
				if (coldata.n + 1 < last - curr) {
					// '\0' is optional, if '\0' exists, it must at string end
					THROW_STD(invalid_argument,
						"'\0' in StrZero is not at string end");
				}
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				const byte* next = nullptr;
				coldata.n = load_var_uint64(curr, &next);
				coldata.p = (const char*)next;
				CHECK_CURR_LAST3(next, last, coldata.n);
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

void Schema::combineRow(const valvec<fstring>& myCols, valvec<byte>* myRowData) const {
	assert(size_t(-1) != m_fixedLen);
	assert(myCols.size() == m_columnsMeta.end_i());
	myRowData->erase_all();
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		const fstring& coldata = myCols[i];
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			assert(1 == coldata.size());
			myRowData->push_back(coldata[0]);
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			assert(2 == coldata.size());
			myRowData->append(coldata.udata(), 2);
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			assert(4 == coldata.size());
			myRowData->append(coldata.udata(), 4);
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			assert(8 == coldata.size());
			myRowData->append(coldata.udata(), 8);
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			assert(16 == coldata.size());
			myRowData->append(coldata.udata(), 16);
			break;
		case ColumnType::Float32:
			assert(4 == coldata.size());
			myRowData->append(coldata.udata(), 4);
			break;
		case ColumnType::Float64:
			assert(8 == coldata.size());
			myRowData->append(coldata.udata(), 8);
			break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			assert(16 == coldata.size());
			myRowData->append(coldata.udata(), 16);
			break;
		case ColumnType::Fixed:   // Fixed length binary
			assert(colmeta.fixedLen == coldata.size());
			myRowData->append(coldata.udata(), colmeta.fixedLen);
			break;
		case ColumnType::StrZero: // Zero ended string
			myRowData->append(coldata.udata(), coldata.size());
			if (i < colnum - 1) {
				myRowData->push_back('\0');
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				size_t oldsize = myRowData->size();
				myRowData->resize_no_init(oldsize + 10);
				byte* p1 = myRowData->data() + oldsize;
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->risk_set_size(oldsize + (p2 - p1));
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		}
	}
}

void Schema::selectParent(const valvec<fstring>& parentCols, valvec<byte>* myRowData) const {
	assert(nullptr != m_parent);
	assert(m_proj.size() == m_columnsMeta.end_i());
	assert(m_parent->columnNum() == parentCols.size());
	myRowData->erase_all();
	size_t colnum = m_proj.size();
	for(size_t i = 0; i < colnum; ++i) {
		size_t j = m_proj[i];
		assert(j < parentCols.size());
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		const fstring& coldata = parentCols[j];
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			assert(1 == coldata.size());
			myRowData->push_back(coldata[0]);
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			assert(2 == coldata.size());
			myRowData->append(coldata.udata(), 2);
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			assert(4 == coldata.size());
			myRowData->append(coldata.udata(), 4);
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			assert(8 == coldata.size());
			myRowData->append(coldata.udata(), 8);
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			assert(16 == coldata.size());
			myRowData->append(coldata.udata(), 16);
			break;
		case ColumnType::Float32:
			assert(4 == coldata.size());
			myRowData->append(coldata.udata(), 4);
			break;
		case ColumnType::Float64:
			assert(8 == coldata.size());
			myRowData->append(coldata.udata(), 8);
			break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			assert(16 == coldata.size());
			myRowData->append(coldata.udata(), 16);
			break;
		case ColumnType::Fixed:   // Fixed length binary
			assert(colmeta.fixedLen == coldata.size());
			myRowData->append(coldata.udata(), colmeta.fixedLen);
			break;
		case ColumnType::StrZero: // Zero ended string
			myRowData->append(coldata.udata(), coldata.size());
			if (i < colnum - 1) {
				myRowData->push_back('\0');
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				size_t oldsize = myRowData->size();
				myRowData->resize_no_init(oldsize + 10);
				byte* p1 = myRowData->data() + oldsize;
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->risk_set_size(oldsize + (p2 - p1));
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		}
	}
}

void Schema::selectParent(const valvec<fstring>& parentCols, valvec<fstring>* myCols) const {
	assert(nullptr != m_parent);
	assert(m_proj.size() == m_columnsMeta.end_i());
	assert(m_parent->columnNum() == parentCols.size());
	myCols->erase_all();
	for(size_t i = 0; i < m_proj.size(); ++i) {
		size_t j = m_proj[i];
		assert(j < parentCols.size());
		myCols->push_back(parentCols[j]);
	}
}

std::string Schema::toJsonStr(fstring row) const {
	assert(size_t(-1) != m_fixedLen);
	const byte* curr = row.udata();
	const byte* last = row.size() + curr;
	nark::json js;
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		fstring colname = m_columnsMeta.key(i);
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			CHECK_CURR_LAST(1);
			js[colname.str()] = int(*curr);
			curr += 1;
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			CHECK_CURR_LAST(2);
			js[colname.str()] = unaligned_load<int16_t>(curr);
			curr += 2;
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			CHECK_CURR_LAST(4);
			js[colname.str()] = unaligned_load<int32_t>(curr);
			curr += 4;
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			CHECK_CURR_LAST(8);
			js[colname.str()] = unaligned_load<int64_t>(curr);
			curr += 8;
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			CHECK_CURR_LAST(16);
			// TODO: int128
			js[colname.str()] = unaligned_load<int64_t>(curr);
			curr += 16;
			break;
		case ColumnType::Float32:
			CHECK_CURR_LAST(4);
			js[colname.str()] = unaligned_load<float>(curr);
			curr += 4;
			break;
		case ColumnType::Float64:
			CHECK_CURR_LAST(8);
			js[colname.str()] = unaligned_load<double>(curr);
			curr += 8;
			break;
		case ColumnType::Float128:
			CHECK_CURR_LAST(16);
			js[colname.str()] = unaligned_load<long double>(curr);
			curr += 16;
			break;
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			js[colname.str()] = std::string((char*)curr, 16);
			curr += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			CHECK_CURR_LAST(colmeta.fixedLen);
			js[colname.str()] = std::string((char*)curr, colmeta.fixedLen);
			curr += colmeta.fixedLen;
			break;
		case ColumnType::StrZero: // Zero ended string
			{
				intptr_t len = strnlen((const char*)curr, last - curr);
				if (i < colnum - 1) {
					CHECK_CURR_LAST(len + 1);
				}
				else { // the last column
					if (len + 1 < last - curr) {
						// '\0' is optional, if '\0' exists, it must at string end
						THROW_STD(invalid_argument,
							"'\0' in StrZero is not at string end");
					}
				}
				js[colname.str()] = std::string((char*)curr, len);
				curr += len + 1;
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			{
				intptr_t len;
				const byte* next = curr;
				if (i < colnum - 1) {
					len = load_var_uint64(curr, &next);
					CHECK_CURR_LAST3(next, last, len);
				}
				else { // the last column
					len = last - curr;
				}
				js[colname.str()] = std::string((char*)next, len);
				curr = next + len;
			}
			break;
		}
	}
	return js.dump();
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

void Schema::compileProject(const Schema* parent) {
	size_t myColsNum = m_columnsMeta.end_i();
	size_t parentColsNum = parent->m_columnsMeta.end_i();
	m_parent = parent;
	m_proj.resize_no_init(myColsNum);
	for (size_t i = 0; i < myColsNum; ++i) {
		fstring colname = m_columnsMeta.key(i);
		size_t j = parent->m_columnsMeta.find_i(colname);
		if (nark_unlikely(j >= parentColsNum)) {
			THROW_STD(invalid_argument,
				"colname=%s is not in parent schema.cols=%s",
				colname.c_str(), parent->joinColumnNames(',').c_str());
		}
		m_proj[i] = j;
	}
}

size_t Schema::computeFixedRowLen() const {
	size_t rowLen = 0;
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
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

int Schema::compareData(fstring x, fstring y) const {
	assert(size_t(-1) != m_fixedLen);
	const byte *xcurr = x.udata(), *xlast = xcurr + x.size();
	const byte *ycurr = y.udata(), *ylast = ycurr + y.size();

#define CompareByType(Type) { \
		CHECK_CURR_LAST3(xcurr, xlast, sizeof(Type)); \
		CHECK_CURR_LAST3(ycurr, ylast, sizeof(Type)); \
		Type xv = unaligned_load<Type>(xcurr); \
		Type yv = unaligned_load<Type>(ycurr); \
		if (xv < yv) return -1; \
		if (xv > yv) return +1; \
		xcurr += sizeof(Type); \
		ycurr += sizeof(Type); \
		break; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Uint08:
			CHECK_CURR_LAST3(xcurr, ylast, 1);
			if (*xcurr != *ycurr)
				return *xcurr - *ycurr;
			xcurr += 1;
			ycurr += 1;
			break;
		case ColumnType::Sint08:
			CHECK_CURR_LAST3(xcurr, ylast, 1);
			if (sbyte(*xcurr) != sbyte(*ycurr))
				return sbyte(*xcurr) - sbyte(*ycurr);
			xcurr += 1;
			ycurr += 1;
			break;
		case ColumnType::Uint16: CompareByType(uint16_t);
		case ColumnType::Sint16: CompareByType( int16_t);
		case ColumnType::Uint32: CompareByType(uint32_t);
		case ColumnType::Sint32: CompareByType( int32_t);
		case ColumnType::Uint64: CompareByType(uint64_t);
		case ColumnType::Sint64: CompareByType( int64_t);
		case ColumnType::Uint128:
			THROW_STD(invalid_argument, "Uint128 is not supported");
		//	CompareByType(unsigned __int128);
		case ColumnType::Sint128:
			THROW_STD(invalid_argument, "Sint128 is not supported");
		//	CompareByType(  signed __int128);
		case ColumnType::Float32: CompareByType(float);
		case ColumnType::Float64: CompareByType(double);
		case ColumnType::Float128: CompareByType(long double);
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			CHECK_CURR_LAST3(xcurr, xlast, 16);
			CHECK_CURR_LAST3(ycurr, ylast, 16);
			{
				int ret = memcmp(xcurr, ycurr, 16);
				if (ret)
					return ret;
			}
			xcurr += 16;
			ycurr += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			CHECK_CURR_LAST3(xcurr, xlast, colmeta.fixedLen);
			CHECK_CURR_LAST3(ycurr, ylast, colmeta.fixedLen);
			{
				int ret = memcmp(xcurr, ycurr, colmeta.fixedLen);
				if (ret)
					return ret;
			}
			xcurr += colmeta.fixedLen;
			ycurr += colmeta.fixedLen;
			break;
		case ColumnType::StrZero: // Zero ended string
			{
				size_t xn = strnlen((const char*)xcurr, xlast - xcurr);
				size_t yn = strnlen((const char*)ycurr, ylast - ycurr);
				if (i < colnum - 1) {
					CHECK_CURR_LAST3(xcurr, xlast, xn + 1);
					CHECK_CURR_LAST3(ycurr, ylast, yn + 1);
				}
				else { // the last column
					if (xn + 1 < size_t(xlast - xcurr)) {
						// '\0' is optional, if '\0' exists, it must at string end
						THROW_STD(invalid_argument,
							"'\0' in StrZero is not at string end");
					}
					if (yn + 1 < size_t(ylast - ycurr)) {
						// '\0' is optional, if '\0' exists, it must at string end
						THROW_STD(invalid_argument,
							"'\0' in StrZero is not at string end");
					}
				}
				int ret = memcmp(xcurr, ycurr, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
				xcurr += xn + 1;
				ycurr += yn + 1;
				break;
			}
			break;
		case ColumnType::StrUtf8: // Prefixed by length(var_uint) in bytes
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			{
				const byte* xnext = nullptr;
				const byte* ynext = nullptr;
				size_t xn, yn;
				if (i < colnum - 1) {
					xn = load_var_uint64(xcurr, &xnext);
					yn = load_var_uint64(ycurr, &ynext);
					CHECK_CURR_LAST3(xnext, xlast, xn);
					CHECK_CURR_LAST3(ynext, ylast, yn);
				}
				else { // the last column
					xn = xlast - xcurr;
					yn = ylast - ycurr;
				}
				int ret = memcmp(xcurr, ycurr, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
				xcurr = xnext + xn;
				ycurr = ynext + yn;
			}
			break;
		}
	}
	return 0;
}

// x, y are pointers to uint32_t
int Schema::QsortCompareFixedLen(const void* x, const void* y, const void* ctx) {
	auto cc = (const CompareByIndexContext*)(ctx);
	const Schema* schema = (const Schema*)(ctx);
	size_t fixedLen = schema->getFixedRowLen();
	size_t xIdx = *(const uint32_t*)(x);
	size_t yIdx = *(const uint32_t*)(y);
	fstring xs(cc->basePtr + fixedLen * xIdx, fixedLen);
	fstring ys(cc->basePtr + fixedLen * yIdx, fixedLen);
	return schema->compareData(xs, ys);
}

// x, y are pointers to uint32_t
int Schema::QsortCompareByIndex(const void* x, const void* y, const void* ctx) {
	auto cc = (const CompareByIndexContext*)(ctx);
	const uint32_t* offsets = cc->offsets;
	size_t xIdx = *(const uint32_t*)(x);
	size_t yIdx = *(const uint32_t*)(y);
	size_t xoff0 = offsets[xIdx], xoff1 = offsets[xIdx+1];
	size_t yoff0 = offsets[yIdx], yoff1 = offsets[yIdx+1];
	fstring xs(cc->basePtr + xoff0, xoff1 - xoff0);
	fstring ys(cc->basePtr + yoff0, yoff1 - yoff0);
	return cc->schema->compareData(xs, ys);
}

// An index can be a composite index, which have multiple columns as key,
// so many indices may share columns, if the column share happens, we just
// need one instance of the column to compose a row from multiple index.
// when iteratively call schema->parseRowAppend(..) of a SchemaSet, all
// columns are concated, duplicated columns may in concated columns.
// m_keepColumn is used to select the using concated columns
// if m_keepColumn[i] is true, then columns[i] should be keeped
// if all columns of an index are not keeped, m_keepSchema[x] is false.
void SchemaSet::compileSchemaSet(const Schema* parent) {
	assert(nullptr != parent);
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
	for (size_t i = 0; i < m_nested.end_i(); ++i) {
		m_nested.elem_at(i)->compile(parent);
	}
}

///////////////////////////////////////////////////////////////////////////////
size_t SchemaSet::Hash::operator()(const SchemaPtr& x) const {
	size_t h = 8789;
	for (size_t i = 0; i < x->m_columnsMeta.end_i(); ++i) {
		fstring colname = x->m_columnsMeta.key(i);
		size_t h2 = fstring_func::hash()(colname);
		h = FaboHashCombine(h, h2);
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
		h = FaboHashCombine(h, h2);
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

} } // namespace nark::db


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

