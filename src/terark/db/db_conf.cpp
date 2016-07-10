#include "db_conf.hpp"
#include <terark/io/DataIO_Basic.hpp>
#include <terark/io/FileStream.hpp>
//#include <terark/io/DataIO.hpp>
//#include <terark/io/MemStream.hpp>
//#include <terark/io/StreamBuffer.hpp>
#include <terark/io/var_int.hpp>
#include <terark/num_to_str.hpp>
//#include <terark/util/sortable_strvec.hpp>
#include <terark/util/linebuf.hpp>
#include <string.h>
#include "json.hpp"
#include <boost/algorithm/string/join.hpp>
//#include <boost/multiprecision/cpp_int.hpp>

//#define TERARKDB_DEDUCE_DATETIME_COLUMN
#ifdef TERARKDB_DEDUCE_DATETIME_COLUMN
#include <re2/re2.h>
#endif

namespace terark { namespace db {

/*
ColumnMeta::ColumnMeta() {
	fixedLen = 0;
	fixedOffset = UINT32_MAX;
	reserved0 = 0;
	reserved1 = 0;
	mysqlType = 0;
	type = ColumnType::Any;
	mongoType = 255; // unknown
}
*/

ColumnMeta::ColumnMeta(ColumnType t) {
	fixedOffset = UINT32_MAX;
	reserved0 = 0;
	reserved1 = 0;
	mysqlType = 0;
	type = t;
	mongoType = 255;
	switch (t) {
	default:
		THROW_STD(runtime_error, "Invalid data row");
		break;
	case ColumnType::Any:
		fixedLen = 0;
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
	case ColumnType::VarSint:
	case ColumnType::VarUint:
	case ColumnType::StrZero:
	case ColumnType::TwoStrZero:
	case ColumnType::Binary:
	case ColumnType::CarBin:
		fixedLen = 0;
		break;
	}
}

#if defined(TERARK_DB_SCHEMA_COMPILER)
ColumnMeta::~ColumnMeta() {
	realtype.clear();
}
#endif

bool ColumnMeta::isInteger() const {
	switch (type) {
	default:
		return false;
	case ColumnType::Uint08:
	case ColumnType::Sint08:
	case ColumnType::Uint16:
	case ColumnType::Sint16:
	case ColumnType::Uint32:
	case ColumnType::Sint32:
	case ColumnType::Uint64:
	case ColumnType::Sint64:
	case ColumnType::VarSint:
	case ColumnType::VarUint:
		return true;
	}
}

bool ColumnMeta::isNumber() const {
	switch (type) {
	default:
		return false;
	case ColumnType::Uint08:
	case ColumnType::Sint08:
	case ColumnType::Uint16:
	case ColumnType::Sint16:
	case ColumnType::Uint32:
	case ColumnType::Sint32:
	case ColumnType::Uint64:
	case ColumnType::Sint64:
	case ColumnType::VarSint:
	case ColumnType::VarUint:
		return true;
	case ColumnType::Float32:
	case ColumnType::Float64:
	case ColumnType::Float128:
	case ColumnType::Decimal128:
		return true;
	}
}

bool ColumnMeta::isString() const {
	switch (type) {
	default:
		return false;
	case ColumnType::StrZero:
	case ColumnType::Binary:
	case ColumnType::CarBin:
		return true;
	}
}

/////////////////////////////////////////////////////////////////////////////
ColumnVec::~ColumnVec() {
}
ColumnVec::ColumnVec() {
	m_base = nullptr;
}
ColumnVec::ColumnVec(size_t cap, valvec_reserve) {
	m_base = nullptr;
	m_cols.reserve(cap);
}
ColumnVec::ColumnVec(const ColumnVec&) = default;
ColumnVec::ColumnVec(ColumnVec&&) = default;
ColumnVec& ColumnVec::operator=(const ColumnVec&) = default;
ColumnVec& ColumnVec::operator=(ColumnVec&&) = default;

/////////////////////////////////////////////////////////////////////////////

const unsigned int DEFAULT_nltNestLevel = 4;

Schema::Schema() {
	m_fixedLen = size_t(-1);
	m_parent = nullptr;
	m_isCompiled = false;
	m_isOrdered = false;
//	m_isPrimary = false;
	m_isUnique  = false;
	m_dictZipSampleRatio = 0.0;
	m_canEncodeToLexByteComparable = false;
	m_needEncodeToLexByteComparable = false;
	m_useFastZip = false;
	m_dictZipLocalMatch = true;
	m_isInplaceUpdatable = false;
	m_enableLinearScan = false;
	m_mmapPopulate = false;
	m_keepCols.fill(true);
	m_minFragLen = 0;
	m_maxFragLen = 0;
	m_sufarrMinFreq = 0;
	m_rankSelectClass = 512;
	m_nltNestLevel = DEFAULT_nltNestLevel;
	m_lastVarLenCol = 0;
	m_restFixLenSum = 0;
}
Schema::~Schema() {
}

void Schema::compile(const Schema* parent) {
	assert(!m_columnsMeta.empty());
	if (m_isCompiled)
		return;
	m_isCompiled = true;
	m_fixedLen = computeFixedRowLen();
	if (m_fixedLen) {
		uint32_t offset = 0;
		for (size_t i = 0; i < m_columnsMeta.end_i(); ++i) {
			auto& colmeta = m_columnsMeta.val(i);
			colmeta.fixedOffset = offset;
			offset += colmeta.fixedLen;
		}
	}
	if (parent) {
		compileProject(parent);
	}
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		if (ColumnType::Fixed == colmeta.type) {
			TERARK_RT_assert(colmeta.fixedLen > 0, std::invalid_argument);
		}
	}
#if 0 // TODO:
	// theoretically, m_lastVarLenCol can be "last non-binary col",
	// StrZero and TwoStrZero are non-binary col, it need reverse scan to
	// compute sum of rest column len, this is slow, complex and error prone
	m_lastVarLenCol = 0;
	m_restFixLenSum = 0;
	for (size_t i = colnum; i > 0; --i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i-1);
		if (0 == colmeta.fixedLen) {
			m_lastVarLenCol = i;
			break;
		}
		m_restFixLenSum += colmeta.fixedLen;
	}
#else
	m_lastVarLenCol = colnum;
#endif
	m_canEncodeToLexByteComparable = true;
	m_needEncodeToLexByteComparable = false;
	for (size_t i = 0; i + 1 < m_lastVarLenCol; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		if (ColumnType::Binary == colmeta.type ||
			ColumnType::CarBin == colmeta.type) {
			m_canEncodeToLexByteComparable = false;
			break;
		}
	}
	for (size_t i = 0; i < m_lastVarLenCol; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		if (colmeta.isNumber()) {
			m_needEncodeToLexByteComparable = true;
			break;
		}
	}
	if (m_name.empty()) {
		m_name = joinColumnNames();
	}
}

void Schema::parseRow(fstring row, ColumnVec* columns) const {
	assert(size_t(-1) != m_fixedLen);
	columns->erase_all();
	parseRowAppend(row, 0, columns);
}

void Schema::parseRowAppend(fstring row, size_t start, ColumnVec* columns) const {
	assert(size_t(-1) != m_fixedLen);
	const byte* base = row.udata();
	const byte* curr = row.udata() + start;
	const byte* last = row.size() + base;
	columns->m_base = base;
#if defined(NDEBUG)
  #define CHECK_CURR_LAST3(curr, last, len) \
	if (terark_unlikely(curr + (len) > last)) { \
		THROW_STD(out_of_range, "len=%ld remain=%ld", \
			long(len), long(last-curr)); \
	}
#else
  #define CHECK_CURR_LAST3(curr, last, len) \
	assert(terark_unlikely(curr + (len) <= last));
#endif

#define CHECK_CURR_LAST(len) CHECK_CURR_LAST3(curr, last, len)
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
#ifndef NDEBUG
		const fstring colname = m_columnsMeta.key(i);
		(void)colname;
#endif
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		size_t collen = 0;
		size_t colpos = curr - base;
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Any:
			abort(); // Any is not implemented yet
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
			CHECK_CURR_LAST(1);
			collen = 1;
			curr += 1;
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			CHECK_CURR_LAST(2);
			collen = 2;
			curr += 2;
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
			CHECK_CURR_LAST(4);
			collen = 4;
			curr += 4;
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
			CHECK_CURR_LAST(8);
			collen = 8;
			curr += 8;
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			CHECK_CURR_LAST(16);
			collen = 16;
			curr += 16;
			break;
		case ColumnType::Float32:
			CHECK_CURR_LAST(4);
			collen = 4;
			curr += 4;
			break;
		case ColumnType::Float64:
			CHECK_CURR_LAST(8);
			collen = 8;
			curr += 8;
			break;
		case ColumnType::Float128:
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			CHECK_CURR_LAST(16);
			collen = 16;
			curr += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			CHECK_CURR_LAST(colmeta.fixedLen);
			collen = colmeta.fixedLen;
			curr += colmeta.fixedLen;
			break;
		case ColumnType::VarSint:
			{
				const byte* next = nullptr;
				load_var_int64(curr, &next);
				collen = next - curr;
				curr = next;
			}
			break;
		case ColumnType::VarUint:
			{
				const byte* next = nullptr;
				load_var_uint64(curr, &next);
				collen = next - curr;
				curr = next;
			}
			break;
		case ColumnType::StrZero: // Zero ended string
			collen = strnlen((const char*)curr, last - curr);
			if (i < colnum - 1) {
				CHECK_CURR_LAST(collen + 1);
				curr += collen + 1;
			}
			else { // the last column
			//	assert(collen == last - curr);
				if (intptr_t(collen + 1) < last - curr) {
					// '\0' is optional, if '\0' exists, it must at string end
					THROW_STD(invalid_argument,
						"'\\0' in StrZero is not at string end");
				}
			}
			break;
		case ColumnType::TwoStrZero: // Two Zero ended strings
			{
				intptr_t n1 = strnlen((char*)curr, last - curr);
				if (i < colnum - 1) {
					CHECK_CURR_LAST(n1+1);
					intptr_t n2 = strnlen((char*)curr+n1+1, last-curr-n1-1);
					CHECK_CURR_LAST(n1+1 + n2+1);
					collen = n1 + 1 + n2; // don't include 2nd '\0'
					curr += n1+1 + n2+1;
				}
				else { // the last column
					if (n1+1 < last - curr) {
						intptr_t n2 = strnlen((char*)curr+n1+1, last-curr-n1-1);
						// 2nd '\0' is optional if exists, it must at string end
						if (n1+1 + n2+1 < last - curr) {
							THROW_STD(invalid_argument,
								"'\\0' in TwoStrZero is not at string end");
						}
						collen = n1 + 1 + n2; // don't include 2nd '\0'
					} else {
						collen = n1; // second string is empty/(not present)
					}
				}
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				const byte* next = nullptr;
				collen = load_var_uint64(curr, &next);
				colpos = next - base;
				CHECK_CURR_LAST3(next, last, collen);
				curr = next + collen;
			}
			else { // the last column
				collen = last - curr;
			}
			break;
		case ColumnType::CarBin: // Prefixed by uint32 len
			if (i < colnum - 1) {
			#if defined(BOOST_BIG_ENDIAN)
				collen = byte_swap(unaligned_load<uint32_t>(curr));
			#else
				collen = unaligned_load<uint32_t>(curr);
			#endif
				colpos += 4;
				CHECK_CURR_LAST3(curr+4, last, collen);
				curr += 4 + collen;
			}
			else { // the last column
				collen = last - curr;
			}
			break;
		}
		columns->push_back(colpos, collen);
	}
}

void
Schema::combineRow(const ColumnVec& myCols, valvec<byte>* myRowData)
const {
	assert(size_t(-1) != m_fixedLen);
	assert(myCols.size() == m_columnsMeta.end_i());
	myRowData->erase_all();
	combineRowAppend(myCols, myRowData);
}
void
Schema::combineRowAppend(const ColumnVec& myCols, valvec<byte>* myRowData)
const {
	assert(size_t(-1) != m_fixedLen);
	assert(myCols.size() == m_columnsMeta.end_i());
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		const fstring coldata = myCols[i];
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Any:
			abort(); // Any is not implemented yet
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
		case ColumnType::VarSint:
		case ColumnType::VarUint:
			myRowData->append(coldata.udata(), coldata.size());
			break;
		case ColumnType::StrZero: // Zero ended string
		case ColumnType::TwoStrZero: // Two Zero ended strings
			myRowData->append(coldata.udata(), coldata.size());
			if (i < colnum - 1) {
				myRowData->push_back('\0');
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				byte* p1 = myRowData->grow_no_init(10);
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->trim(p2);
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		case ColumnType::CarBin:  // Prefixed by uint32 length
			if (i < colnum - 1) {
				uint32_t binlen = (uint32_t)coldata.size();
			#if defined(BOOST_BIG_ENDIAN)
				binlen = byte_swap(binlen);
			#endif
				myRowData->append((byte*)&binlen, 4);
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		}
	}
}

template<bool isLast>
static void
doProject(fstring col, const ColumnMeta& colmeta, valvec<byte>* rowData) {
	switch (colmeta.type) {
	default:
		THROW_STD(runtime_error, "Invalid data row");
		break;
	case ColumnType::Any:
		abort(); // Any is not implemented yet
		break;
	case ColumnType::Uint08:
	case ColumnType::Sint08:
		assert(1 == col.size());
		rowData->push_back(col[0]);
		break;
	case ColumnType::Uint16:
	case ColumnType::Sint16:
		assert(2 == col.size());
		rowData->append(col.udata(), 2);
		break;
	case ColumnType::Uint32:
	case ColumnType::Sint32:
		assert(4 == col.size());
		rowData->append(col.udata(), 4);
		break;
	case ColumnType::Uint64:
	case ColumnType::Sint64:
		assert(8 == col.size());
		rowData->append(col.udata(), 8);
		break;
	case ColumnType::Uint128:
	case ColumnType::Sint128:
		assert(16 == col.size());
		rowData->append(col.udata(), 16);
		break;
	case ColumnType::Float32:
		assert(4 == col.size());
		rowData->append(col.udata(), 4);
		break;
	case ColumnType::Float64:
		assert(8 == col.size());
		rowData->append(col.udata(), 8);
		break;
	case ColumnType::Float128:
	case ColumnType::Uuid:    // 16 bytes(128 bits) binary
		assert(16 == col.size());
		rowData->append(col.udata(), 16);
		break;
	case ColumnType::Fixed:   // Fixed length binary
		assert(colmeta.fixedLen == col.size());
		rowData->append(col.udata(), colmeta.fixedLen);
		break;
	case ColumnType::VarSint:
	case ColumnType::VarUint:
		rowData->append(col.udata(), col.size());
		break;
	case ColumnType::StrZero: // Zero ended string
	case ColumnType::TwoStrZero: // Two Zero ended strings
		rowData->append(col.udata(), col.size());
		if (!isLast) {
			rowData->push_back('\0');
		}
		break;
	case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
		if (!isLast) {
			byte* p1 = rowData->grow_no_init(10);
			byte* p2 = save_var_uint32(p1, uint32_t(col.size()));
			rowData->trim(p2);
		}
		rowData->append(col.data(), col.size());
		break;
	case ColumnType::CarBin:  // Prefixed by uint32 length
		if (!isLast) {
			uint32_t binlen = (uint32_t)col.size();
		#if defined(BOOST_BIG_ENDIAN)
			binlen = byte_swap(binlen);
		#endif
			rowData->append((byte*)&binlen, 4);
		}
		rowData->append(col.data(), col.size());
		break;
	}
}

void
Schema::projectToNorm(fstring col, size_t columnId, valvec<byte>* rowData)
const {
	assert(columnId < m_columnsMeta.end_i());
	const ColumnMeta& colmeta = m_columnsMeta.val(columnId);
	doProject<false>(col, colmeta, rowData);
}
void
Schema::projectToLast(fstring col, size_t columnId, valvec<byte>* rowData)
const {
	assert(columnId < m_columnsMeta.end_i());
	const ColumnMeta& colmeta = m_columnsMeta.val(columnId);
	doProject<true>(col, colmeta, rowData);
}

void
Schema::selectParent(const ColumnVec& parentCols, valvec<byte>* myRowData)
const {
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
		case ColumnType::Any:
			abort(); // Any is not implemented yet
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
		case ColumnType::VarSint:
		case ColumnType::VarUint:
			myRowData->append(coldata.udata(), coldata.size());
			break;
		case ColumnType::StrZero: // Zero ended string
		case ColumnType::TwoStrZero: // Two Zero ended strings
			myRowData->append(coldata.udata(), coldata.size());
			if (i < colnum - 1) {
				myRowData->push_back('\0');
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				byte* p1 = myRowData->grow_no_init(10);
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->trim(p2);
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		case ColumnType::CarBin:  // Prefixed by uint32 length
			if (i < colnum - 1) {
				uint32_t binlen = (uint32_t)coldata.size();
			#if defined(BOOST_BIG_ENDIAN)
				binlen = byte_swap(binlen);
			#endif
				myRowData->append((byte*)&binlen, 4);
			}
			myRowData->append(coldata.data(), coldata.size());
			break;
		}
	}
}

void Schema::selectParent(const ColumnVec& parentCols, ColumnVec* myCols) const {
	assert(nullptr != m_parent);
	assert(m_proj.size() == m_columnsMeta.end_i());
	assert(m_parent->columnNum() == parentCols.size());
	myCols->erase_all();
	for(size_t i = 0; i < m_proj.size(); ++i) {
		size_t j = m_proj[i];
		assert(j < parentCols.size());
		myCols->m_cols.push_back(parentCols.m_cols[j]);
	}
	myCols->m_base = parentCols.m_base;
}

void Schema::byteLexEncode(valvec<byte>& indexKey) const {
	byteLexEncode(indexKey.data(), indexKey.size());
}
void Schema::byteLexDecode(valvec<byte>& indexKey) const {
	byteLexDecode(indexKey.data(), indexKey.size());
}
struct EncodeOffsetCoding {
	template<class Integer>
	static Integer convert(Integer x) {
		x ^= Integer(1) << (sizeof(Integer)*8 - 1);
		BYTE_SWAP_IF_LITTLE_ENDIAN(x);
		return x;
	}
};
struct DecodeOffsetCoding {
	template<class Integer>
	static Integer convert(Integer x) {
		BYTE_SWAP_IF_LITTLE_ENDIAN(x);
		x ^= Integer(1) << (sizeof(Integer)*8 - 1);
		return x;
	}
};
void Schema::byteLexEncode(byte* data, size_t size) const {
	byteLexConvert<EncodeOffsetCoding>(data, size);
}
void Schema::byteLexDecode(byte* data, size_t size) const {
	byteLexConvert<DecodeOffsetCoding>(data, size);
}

template<class Converter>
void Schema::byteLexConvert(byte* data, size_t size) const {
	assert(size_t(-1) != m_fixedLen);
	assert(m_canEncodeToLexByteComparable);
	byte* curr = data;
	byte* last = data + size;
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Any:
			THROW_STD(invalid_argument, "ColumnType::Any can not be lex-converted");
			break;
		case ColumnType::Uint08:
			CHECK_CURR_LAST(1);
			curr += 1;
			break;
		case ColumnType::Sint08:
			CHECK_CURR_LAST(1);
			curr[0] ^= 1 << 7;
			curr += 1;
			break;
		case ColumnType::Uint16:
			CHECK_CURR_LAST(2);
			{
				uint16_t x = unaligned_load<uint16_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				unaligned_save(curr, x);
			}
			curr += 2;
			break;
		case ColumnType::Sint16:
			CHECK_CURR_LAST(2);
			{
				uint16_t x = unaligned_load<uint16_t>(curr);
				x = Converter::convert(x);
				unaligned_save(curr, x);
			}
			curr += 2;
			break;
		case ColumnType::Uint32:
			CHECK_CURR_LAST(4);
			{
				uint32_t x = unaligned_load<uint32_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				unaligned_save(curr, x);
			}
			curr += 4;
			break;
		case ColumnType::Sint32:
		case ColumnType::Float32:
			CHECK_CURR_LAST(4);
			{
				uint32_t x = unaligned_load<uint32_t>(curr);
				x = Converter::convert(x);
				unaligned_save(curr, x);
			}
			curr += 4;
			break;
		case ColumnType::Uint64:
			CHECK_CURR_LAST(8);
			{
				uint64_t x = unaligned_load<uint64_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				unaligned_save(curr, x);
			}
			curr += 8;
			break;
		case ColumnType::Sint64:
		case ColumnType::Float64:
			CHECK_CURR_LAST(8);
			{
				uint64_t x = unaligned_load<uint64_t>(curr);
				x = Converter::convert(x);
				unaligned_save(curr, x);
			}
			curr += 8;
			break;
		case ColumnType::Uint128:
		case ColumnType::Sint128:
			CHECK_CURR_LAST(16);
			// TODO: int128
			assert(0);
			curr += 16;
			break;
		case ColumnType::Float128:
			CHECK_CURR_LAST(16);
			// TODO:
			assert(0);
			curr += 16;
			break;
		case ColumnType::Uuid:    // 16 bytes(128 bits) binary
			curr += 16;
			break;
		case ColumnType::Fixed:   // Fixed length binary
			CHECK_CURR_LAST(colmeta.fixedLen);
			curr += colmeta.fixedLen;
			break;
		case ColumnType::VarSint:
			THROW_STD(invalid_argument, "VarSint can not be lex-coverted");
			break;
		case ColumnType::VarUint:
			THROW_STD(invalid_argument, "VarUint can not be lex-coverted");
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
							"'\\0' in StrZero is not at string end");
					}
				}
				curr += len + 1;
			}
			break;
		case ColumnType::TwoStrZero: // Two Zero ended string
			{
				intptr_t n1 = strnlen((const char*)curr, last - curr);
				if (i < colnum - 1) {
					CHECK_CURR_LAST(n1 + 1);
					intptr_t n2 = strnlen((const char*)curr+n1+1, last-curr-n1-1);
					CHECK_CURR_LAST(n1 + 1 + n2 + 1);
					curr += n1 + 1 + n2 + 1;
				}
				else { // the last column
					// 2nd '\0' is optional, if exists, it must at string end
					if (n1+1 < last-curr) {
						intptr_t n2 = strnlen((char*)curr+n1+1, last-curr-n1-1);
						if (n1+1 + n2+1 < last - curr) {
							THROW_STD(invalid_argument,
								"second '\\0' in TwoStrZero is not at string end");
						}
					}
				}
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			assert(colnum - 1 == i);
			break;
		case ColumnType::CarBin:  // Prefixed by uint32 length
			assert(colnum - 1 == i);
			break;
		}
	}
}

size_t
Schema::parseDelimText(char delim, fstring text, valvec<byte>* row)
const {
	const char* beg = text.begin();
	const char* end = text.end();
	const char* pos = beg;
	size_t nCol = m_columnsMeta.end_i();
	size_t iCol = 0;
	row->erase_all();
	for (; pos < end && iCol < nCol; ++iCol) {
		const char* next = std::find(pos, end, delim);
		char* next2 = nullptr;
		const ColumnMeta& colmeta = m_columnsMeta.val(iCol);
		switch (colmeta.type) {
		default:
			THROW_STD(invalid_argument,
				"type=%s is not supported", columnTypeStr(colmeta.type));
			break;
		case ColumnType::Sint08:
		case ColumnType::Uint08:
			{
				long val = strtol(pos, &next2, 0);
				row->push_back((char)(val));
			}
			break;
		case ColumnType::Sint16:
		case ColumnType::Uint16:
			{
				long val = strtol(pos, &next2, 0);
				unaligned_save<int16_t>(row->grow_no_init(2), int16_t(val));
			}
			break;
		case ColumnType::Sint32:
			{
				long val = strtol(pos, &next2, 0);
				unaligned_save<int32_t>(row->grow_no_init(4), int32_t(val));
			}
			break;
		case ColumnType::Uint32:
			{
				ulong val = strtoul(pos, &next2, 0);
				unaligned_save<uint32_t>(row->grow_no_init(4), uint32_t(val));
			}
			break;
		case ColumnType::Sint64:
			{
				llong val = strtoll(pos, &next2, 0);
				unaligned_save<int64_t>(row->grow_no_init(8), int64_t(val));
			}
			break;
		case ColumnType::Uint64:
			{
				llong val = strtoull(pos, &next2, 0);
				unaligned_save<uint64_t>(row->grow_no_init(8), uint64_t(val));
			}
			break;
		case ColumnType::Float32:
			{
				float val = strtof(pos, &next2);
				unaligned_save<float>(row->grow_no_init(8), val);
			}
			break;
		case ColumnType::Float64:
			{
				double val = strtod(pos, &next2);
				unaligned_save<double>(row->grow_no_init(8), val);
			}
			break;
		case ColumnType::StrZero:
			row->append(pos, next);
			if (iCol < nCol-1) {
				row->push_back('\0');
			}
			break;
		}
		pos = next + 1;
	}
	return iCol;
}

std::string Schema::toJsonStr(fstring row) const {
	return toJsonStr(row.data(), row.size());
}
std::string Schema::toJsonStr(const char* row, size_t rowlen) const {
	assert(size_t(-1) != m_fixedLen);
	if (0 == rowlen) {
		return "emptyJson{}";
	}
	const byte* curr = (const byte*)(row);
	const byte* last = (const byte*)(row) + rowlen;
	terark::json js;
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum; ++i) {
		fstring colname = m_columnsMeta.key(i);
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid data row");
			break;
		case ColumnType::Any:
			THROW_STD(invalid_argument, "ColumnType::Any to json is not implemented");
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
		case ColumnType::VarSint:
			{
				const byte* next = nullptr;
				int64_t x = load_var_int64(curr, &next);
				CHECK_CURR_LAST(next - curr);
				js[colname.str()] = x;
				curr = next;
			}
			break;
		case ColumnType::VarUint:
			{
				const byte* next = nullptr;
				uint64_t x = load_var_uint64(curr, &next);
				CHECK_CURR_LAST(next - curr);
				js[colname.str()] = x;
				curr = next;
			}
			break;
		case ColumnType::StrZero: // Zero ended string
			{
				intptr_t len = strnlen((const char*)curr, last - curr);
				if (i < colnum - 1) {
					CHECK_CURR_LAST(len + 1);
				}
				else { // the last column
					assert(len + 1 >= last - curr);
					if (len + 1 < last - curr) {
						// '\0' is optional, if '\0' exists, it must at string end
						THROW_STD(invalid_argument,
							"'\\0' in StrZero is not at string end");
					}
				}
				js[colname.str()] = std::string((char*)curr, len);
				curr += len + 1;
			}
			break;
		case ColumnType::TwoStrZero: // Two Zero ended string
			{
				intptr_t n1 = strnlen((const char*)curr, last - curr);
				auto& arr = js[colname.str()] = json::array();
				arr.push_back(std::string((char*)curr, n1));
				if (i < colnum - 1) {
					CHECK_CURR_LAST(n1 + 1);
					intptr_t n2 = strnlen((const char*)curr+n1+1, last-curr-n1-1);
					CHECK_CURR_LAST(n1 + 1 + n2 + 1);
					arr.push_back(std::string((char*)curr+n1+1, n2));
					curr += n1 + 1 + n2 + 1;
				}
				else { // the last column
					// 2nd '\0' is optional, if exists, it must at string end
					if (n1+1 < last-curr) {
						intptr_t n2 = strnlen((char*)curr+n1+1, last-curr-n1-1);
						if (n1+1 + n2+1 < last - curr) {
							THROW_STD(invalid_argument,
								"second '\\0' in TwoStrZero is not at string end");
						}
						arr.push_back(std::string((char*)curr+n1+1, n2));
					} else {
						arr.push_back(std::string(""));
					}
				}
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				const byte* next;
				intptr_t len = load_var_uint64(curr, &next);
				CHECK_CURR_LAST3(next, last, len);
				js[colname.str()] = std::string((char*)next, len);
				curr = next + len;
			}
			else { // the last column
				js[colname.str()] = std::string((char*)curr, last-curr);
			}
			break;
		case ColumnType::CarBin:  // Prefixed by uint32 length
			if (i < colnum - 1) {
				uint32_t len = unaligned_load<uint32_t>(curr);
			#if defined(BOOST_BIG_ENDIAN)
				len = byte_swap(len);
			#endif
				CHECK_CURR_LAST3(curr+4, last, len);
				js[colname.str()] = std::string((char*)curr+4, len);
				curr += 4 + len;
			}
			else { // the last column
				js[colname.str()] = std::string((char*)curr, last-curr);
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
	case ColumnType::Any:  return "any";
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
	case ColumnType::VarSint: return "varsint";
	case ColumnType::VarUint: return "varuint";
	case ColumnType::StrZero: return "strzero";
	case ColumnType::TwoStrZero: return "twostrzero";
	case ColumnType::Binary:  return "binary";
	case ColumnType::CarBin:  return "carbin";
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

bool Schema::should_use_FixedLenStore() const {
	if (columnNum() == 1) {
		auto colmeta = m_columnsMeta.val(0);
		if (colmeta.isInteger() && !m_isInplaceUpdatable) {
			// should use ZipIntStore
			return false;
		}
	}
	size_t fixlen = m_fixedLen;
	if (m_isInplaceUpdatable) {
		assert(fixlen > 0);
		return true;
	}
	if (fixlen && fixlen <= 16) {
		return true;
	}
	return false;
}

void Schema::compileProject(const Schema* parent) {
	size_t myColsNum = m_columnsMeta.end_i();
	size_t parentColsNum = parent->m_columnsMeta.end_i();
	m_parent = parent;
	m_proj.resize_no_init(myColsNum);
	for (size_t i = 0; i < myColsNum; ++i) {
		fstring colname = m_columnsMeta.key(i);
		size_t j = parent->m_columnsMeta.find_i(colname);
		if (terark_unlikely(j >= parentColsNum)) {
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
		const fstring     colname = m_columnsMeta.key(i);
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		switch (colmeta.type) {
		default:
			THROW_STD(runtime_error, "Invalid column[name='%s' type=%d]"
				, colname.c_str(), (int)colmeta.type);
			break;
		case ColumnType::Any:
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
		case ColumnType::VarSint:
		case ColumnType::VarUint:
		case ColumnType::StrZero: // Zero ended string
		case ColumnType::TwoStrZero: // Two Zero ended string
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
		case ColumnType::CarBin:  // Prefixed by uint32 length
			return 0;
		}
	}
	return rowLen;
}

namespace {
	struct ColumnTypeMap : hash_strmap<ColumnType> {
		ColumnTypeMap() {
			auto& colname2val = *this;
			colname2val["any"] = ColumnType::Any;
			colname2val["anytype"] = ColumnType::Any;

			colname2val["int8"] = ColumnType::Sint08;
			colname2val["int08"] = ColumnType::Sint08;
			colname2val["int16"] = ColumnType::Sint16;
			colname2val["int32"] = ColumnType::Sint32;
			colname2val["int64"] = ColumnType::Sint64;
			colname2val["int128"] = ColumnType::Sint128;

			colname2val["byte"] = ColumnType::Uint08;
			colname2val["ubyte"] = ColumnType::Uint08;
			colname2val["sbyte"] = ColumnType::Sint08;

			colname2val["uint8"] = ColumnType::Uint08;
			colname2val["sint8"] = ColumnType::Sint08;

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
			colname2val["float"]   = ColumnType::Float32;
			colname2val["float64"] = ColumnType::Float64;
			colname2val["double"]  = ColumnType::Float64;
			colname2val["float128"] = ColumnType::Float128;
			colname2val["uuid"] = ColumnType::Uuid;
			colname2val["guid"] = ColumnType::Uuid;
			colname2val["fixed"] = ColumnType::Fixed;
			colname2val["varsint"] = ColumnType::VarSint;
			colname2val["varuint"] = ColumnType::VarUint;
			colname2val["strzero"] = ColumnType::StrZero;
			colname2val["twostrzero"] = ColumnType::TwoStrZero;
			colname2val["binary"] = ColumnType::Binary;
			colname2val["carbin"] = ColumnType::CarBin;
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

/// Order == +1 : Ascending
/// Order == -1 : Descending
template<class NumberType, int Order>
static int OneColumn_compareNumber(fstring x, fstring y) {
	BOOST_STATIC_ASSERT(Order == +1 || Order == -1);
	assert(x.size() == sizeof(NumberType));
	assert(y.size() == sizeof(NumberType));
	NumberType xv = unaligned_load<NumberType>(x.p);
	NumberType yv = unaligned_load<NumberType>(y.p);
	if (xv < yv) return -Order;
	if (xv > yv) return +Order;
	return 0;
}

template<class NumberType>
static
Schema::OneColumnComparator oneColumnNumberComparator(bool ascending) {
	if (ascending)
		return &OneColumn_compareNumber<NumberType, +1>;
	else
		return &OneColumn_compareNumber<NumberType, -1>;
}

/// Order == +1 : Ascending
/// Order == -1 : Descending
template<int Order>
static int OneColumn_compareTwoStrZero(fstring x, fstring y) {
	BOOST_STATIC_ASSERT(Order == +1 || Order == -1);
	intptr_t xn1 = strnlen((char*)x.p, x.n);
	intptr_t yn1 = strnlen((char*)y.p, y.n);
	int ret = memcmp(x.p, x.p, std::min(xn1,yn1));
	if (ret) {
		return Order == 1 ? ret : -ret;
	} else if (xn1 != yn1) {
		return xn1 < yn1 ? -Order : +Order;
	}
	assert(xn1 == yn1);
	intptr_t xn2 = 0, xnn = xn1;
	intptr_t yn2 = 0, ynn = yn1;
	if (xn1 + 1 < x.n) {
		xn2 = strnlen((char*)x.p+xn1+1, x.n-xn1-1);
		// 2nd '\0' is optional, if '\0' exists, it must at string end
		if (xn1+1 + xn2+1 < x.n) {
			THROW_STD(invalid_argument,
				"'\\0' in StrZero is not at string end");
		}
		xnn += 1 + xn2;
	}
	if (yn1 + 1 < y.n) {
		yn2 = strnlen((char*)y.p+yn1+1, y.n-yn1-1);
		// 2nd '\0' is optional, if '\0' exists, it must at string end
		if (yn1+1 + yn2+1 < y.n) {
			THROW_STD(invalid_argument,
				"'\\0' in StrZero is not at string end");
		}
		ynn += 1 + yn2;
	}
	ret = memcmp(x.p, y.p, std::min(xnn,ynn));
	if (ret) {
		return Order == 1 ? ret : -ret;
	} else if (xnn != ynn) {
		return xnn < ynn ? -Order : +Order;
	}
	return 0;
}

Schema::OneColumnComparator Schema::getOneColumnComparator(bool ascending) const {
	assert(m_columnsMeta.end_i() == 1);
	if (m_columnsMeta.end_i() != 1) {
		THROW_STD(invalid_argument, "invalid colnum = %zd", m_columnsMeta.end_i());
	}
	const ColumnMeta& colmeta = m_columnsMeta.val(0);
	switch (colmeta.type) {
	default:
		THROW_STD(runtime_error, "Invalid data row");
	case ColumnType::Any:
	//	THROW_STD(invalid_arugment, "ColumnType::Any can not be lex");
		abort(); // not implemented yet
		break;
	case ColumnType::Uint08: return oneColumnNumberComparator<uint08_t>(ascending);
	case ColumnType::Sint08: return oneColumnNumberComparator< int08_t>(ascending);
	case ColumnType::Uint16: return oneColumnNumberComparator<uint16_t>(ascending);
	case ColumnType::Sint16: return oneColumnNumberComparator< int16_t>(ascending);
	case ColumnType::Uint32: return oneColumnNumberComparator<uint32_t>(ascending);
	case ColumnType::Sint32: return oneColumnNumberComparator< int32_t>(ascending);
	case ColumnType::Uint64: return oneColumnNumberComparator<uint64_t>(ascending);
	case ColumnType::Sint64: return oneColumnNumberComparator< int64_t>(ascending);
	case ColumnType::Uint128:
		THROW_STD(invalid_argument, "Uint128 is not supported");
	//	CompareByType(boost::multiprecision::uint128_t);
	case ColumnType::Sint128:
		THROW_STD(invalid_argument, "Sint128 is not supported");
	//	CompareByType(boost::multiprecision::int128_t);
	case ColumnType::Float32: return oneColumnNumberComparator<float>(ascending);
	case ColumnType::Float64: return oneColumnNumberComparator<double>(ascending);
	case ColumnType::Float128: return oneColumnNumberComparator<long double>(ascending);
	case ColumnType::Uuid:    // 16 bytes(128 bits) binary
		if (ascending)
			return [](fstring x, fstring y) {
				assert(x.size() == 16);
				assert(y.size() == 16);
				return memcmp(x.p, y.p, 16);
			};
		else
			return [](fstring x, fstring y) {
				assert(x.size() == 16);
				assert(y.size() == 16);
				return memcmp(y.p, x.p, 16);
			};
	case ColumnType::Fixed:   // Fixed length binary
		if (ascending)
			return [](fstring x, fstring y) {
				// can not get fixed length value, omit fixed length check
				assert(x.size() == y.size());
				return memcmp(x.p, y.p, x.size());
			};
		else
			return [](fstring x, fstring y) {
				// can not get fixed length value, omit fixed length check
				assert(x.size() == y.size());
				return memcmp(y.p, x.p, x.size());
			};
	case ColumnType::StrZero: // Zero ended string
	case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
	case ColumnType::CarBin:  // Prefixed by uint32 length
		if (ascending)
			return [](fstring x, fstring y) {
				return fstring_func::compare3()(x, y);
			};
		else
			return [](fstring x, fstring y) {
				return fstring_func::compare3()(y, x);
			};
	case ColumnType::VarSint:
		THROW_STD(invalid_argument, "VarSint is not supported");
	case ColumnType::VarUint:
		THROW_STD(invalid_argument, "VarUint is not supported");
	case ColumnType::TwoStrZero: // Zero ended string
		if (ascending)
			return &OneColumn_compareTwoStrZero<+1>;
		else
			return &OneColumn_compareTwoStrZero<-1>;
	}
	assert(0); // should not go here
	return nullptr; // suppress compiler warnings
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
		case ColumnType::Any:
		//	THROW_STD(invalid_arugment, "ColumnType::Any can not be lex");
			abort(); // not implemented yet
			break;
		case ColumnType::Uint08:
			CHECK_CURR_LAST3(xcurr, xlast, 1);
			CHECK_CURR_LAST3(ycurr, ylast, 1);
			if (*xcurr != *ycurr)
				return *xcurr - *ycurr;
			xcurr += 1;
			ycurr += 1;
			break;
		case ColumnType::Sint08:
			CHECK_CURR_LAST3(xcurr, xlast, 1);
			CHECK_CURR_LAST3(ycurr, ylast, 1);
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
		//	CompareByType(boost::multiprecision::uint128_t);
		case ColumnType::Sint128:
			THROW_STD(invalid_argument, "Sint128 is not supported");
		//	CompareByType(boost::multiprecision::int128_t);
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
							"'\\0' in StrZero is not at string end");
					}
					if (yn + 1 < size_t(ylast - ycurr)) {
						// '\0' is optional, if '\0' exists, it must at string end
						THROW_STD(invalid_argument,
							"'\\0' in StrZero is not at string end");
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
		case ColumnType::VarSint:
			{
				const byte *xnext, *ynext;
				int64_t xv = load_var_int64(xcurr, &xnext);
				int64_t yv = load_var_int64(ycurr, &ynext);
				CHECK_CURR_LAST3(xcurr, xlast, xnext - xcurr);
				CHECK_CURR_LAST3(ycurr, ylast, ynext - ycurr);
				if (xv < yv) return -1;
				if (xv > yv) return +1;
				xcurr = xnext;
				ycurr = ynext;
			}
			break;
		case ColumnType::VarUint:
			{
				const byte *xnext, *ynext;
				uint64_t xv = load_var_uint64(xcurr, &xnext);
				uint64_t yv = load_var_uint64(ycurr, &ynext);
				CHECK_CURR_LAST3(xcurr, xlast, xnext - xcurr);
				CHECK_CURR_LAST3(ycurr, ylast, ynext - ycurr);
				if (xv < yv) return -1;
				if (xv > yv) return +1;
				xcurr = xnext;
				ycurr = ynext;
			}
			break;
		case ColumnType::TwoStrZero: // Zero ended string
			if (i < colnum - 1) {
				intptr_t xn1 = strnlen((char*)xcurr, xlast-xcurr);
				intptr_t yn1 = strnlen((char*)ycurr, ylast-ycurr);
				CHECK_CURR_LAST3(xcurr, xlast, xn1+1);
				CHECK_CURR_LAST3(ycurr, ylast, yn1+1);
				intptr_t xn2 = strnlen((char*)xcurr+xn1+1, xlast-xcurr-xn1-1);
				intptr_t yn2 = strnlen((char*)ycurr+yn1+1, ylast-ycurr-yn1-1);
				intptr_t xnn = xn1+1+xn2+1;
				intptr_t ynn = yn1+1+yn2+1;
				CHECK_CURR_LAST3(xcurr, xlast, xnn);
				CHECK_CURR_LAST3(ycurr, ylast, ynn);
				int ret = memcmp(xcurr, ycurr, std::min(xnn,ynn));
				if (ret) {
					return ret;
				} else if (xnn != ynn) {
					return xnn < ynn ? -1 : +1;
				}
				xcurr += xnn;
				ycurr += ynn;
			}
			else { // the last column
				intptr_t xn1 = strnlen((char*)xcurr, xlast-xcurr);
				intptr_t yn1 = strnlen((char*)ycurr, ylast-ycurr);
				int ret = memcmp(xcurr, ycurr, std::min(xn1,yn1));
				if (ret) {
					return ret;
				} else if (xn1 != yn1) {
					return xn1 < yn1 ? -1 : +1;
				}
				intptr_t xn2 = 0, xnn = xn1;
				intptr_t yn2 = 0, ynn = yn1;
				if (xn1 + 1 < xlast - xcurr) {
					xn2 = strnlen((char*)xcurr+xn1+1, xlast-xcurr-xn1-1);
					// 2nd '\0' is optional, if '\0' exists, it must at string end
					if (xn1+1 + xn2+1 < xlast - xcurr) {
						THROW_STD(invalid_argument,
							"'\\0' in StrZero is not at string end");
					}
					xnn += 1 + xn2;
				}
				if (yn1 + 1 < ylast - ycurr) {
					yn2 = strnlen((char*)ycurr+yn1+1, ylast-ycurr-yn1-1);
					// 2nd '\0' is optional, if '\0' exists, it must at string end
					if (yn1+1 + yn2+1 < ylast - ycurr) {
						THROW_STD(invalid_argument,
							"'\\0' in StrZero is not at string end");
					}
					ynn += 1 + yn2;
				}
				ret = memcmp(xcurr, ycurr, std::min(xnn,ynn));
				if (ret) {
					return ret;
				} else if (xnn != ynn) {
					return xnn < ynn ? -1 : +1;
				}
			}
			break;
		case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
			if (i < colnum - 1) {
				const byte* xnext = nullptr;
				const byte* ynext = nullptr;
				size_t xn = load_var_uint64(xcurr, &xnext);
				size_t yn = load_var_uint64(ycurr, &ynext);
				CHECK_CURR_LAST3(xnext, xlast, xn);
				CHECK_CURR_LAST3(ynext, ylast, yn);
				int ret = memcmp(xnext, ynext, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
				xcurr = xnext + xn;
				ycurr = ynext + yn;
			}
			else { // the last column
				size_t xn = xlast - xcurr;
				size_t yn = ylast - ycurr;
				int ret = memcmp(xcurr, ycurr, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
			}
			break;
		case ColumnType::CarBin:  // Prefixed by uint32 length
			if (i < colnum - 1) {
			#if defined(BOOST_BIG_ENDIAN)
				size_t xn = byte_swap(unaligned_load<uint32_t>(xcurr));
				size_t yn = byte_swap(unaligned_load<uint32_t>(ycurr));
			#else
				size_t xn = unaligned_load<uint32_t>(xcurr);
				size_t yn = unaligned_load<uint32_t>(ycurr);
			#endif
				CHECK_CURR_LAST3(xcurr+4, xlast, xn);
				CHECK_CURR_LAST3(ycurr+4, ylast, yn);
				int ret = memcmp(xcurr+4, ycurr+4, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
				xcurr += 4 + xn;
				ycurr += 4 + yn;
			}
			else { // the last column
				size_t xn = xlast - xcurr;
				size_t yn = ylast - ycurr;
				int ret = memcmp(xcurr, ycurr, std::min(xn, yn));
				if (ret)
					return ret;
				else if (xn != yn)
					return xn < yn ? -1 : +1;
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

SchemaSet::SchemaSet() {
	m_flattenColumnNum = 0;
}
SchemaSet::~SchemaSet() {
}

// An index can be a composite index, which have multiple columns as key,
// so many indices may share columns, if the column share happens, we just
// need one instance of the column to compose a row from multiple index.
void SchemaSet::compileSchemaSet(const Schema* parent) {
	assert(nullptr != parent);
	hash_strmap<int> dedup;
	assert(m_uniqIndexFields == nullptr);
	m_uniqIndexFields = new Schema();
	this->m_flattenColumnNum = 0;
	for (size_t i = 0; i < m_nested.end_i(); ++i) {
		Schema* sc = m_nested.elem_at(i).get();
		size_t numSkipped = 0;
		for (size_t j = 0; j < sc->m_columnsMeta.end_i(); ++j) {
			fstring columnName = sc->m_columnsMeta.key(j);
			int cnt = dedup[columnName]++;
			if (cnt) {
				sc->m_keepCols.set0(j);
				numSkipped++;
			}
			else {
				m_uniqIndexFields->m_columnsMeta.
					insert_i(columnName, sc->m_columnsMeta.val(j));
			}
			this->m_flattenColumnNum++;
		}
		for (size_t j = sc->m_columnsMeta.end_i(); j < Schema::MaxProjColumns; ++j) {
			sc->m_keepCols.set0(j);
		}
	}
	for (size_t i = 0; i < m_nested.end_i(); ++i) {
		m_nested.elem_at(i)->compile(parent);
	}
}

///////////////////////////////////////////////////////////////////////////////
//#define USE_SPLIT_FIELD_NAMES
size_t SchemaSet::Hash::operator()(const SchemaPtr& x) const {
#if defined(USE_SPLIT_FIELD_NAMES)
	size_t h = 8789;
	for (size_t i = 0; i < x->m_columnsMeta.end_i(); ++i) {
		fstring colname = x->m_columnsMeta.key(i);
		size_t h2 = fstring_func::hash()(colname);
		h = FaboHashCombine(h, h2);
	}
	return h;
#else
	return fstring_func::hash()(x->m_name);
#endif
}
size_t SchemaSet::Hash::operator()(fstring x) const {
#if defined(USE_SPLIT_FIELD_NAMES)
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
#else
	return fstring_func::hash()(x);
#endif
}
bool SchemaSet::Equal::operator()(const SchemaPtr& x, const SchemaPtr& y) const {
#if defined(USE_SPLIT_FIELD_NAMES)
	fstring kx = x->m_columnsMeta.whole_strpool();
	fstring ky = y->m_columnsMeta.whole_strpool();
	return fstring_func::equal()(kx, ky);
#else
	return x->m_name == y->m_name;
#endif
}
bool SchemaSet::Equal::operator()(const SchemaPtr& x, fstring y) const {
#if defined(USE_SPLIT_FIELD_NAMES)
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
#else
	return fstring(x->m_name) == y;
#endif
}

///////////////////////////////////////////////////////////////////////////////


const llong  DEFAULT_compressingWorkMemSize = 2LL * 1024 * 1024 * 1024;
const llong  DEFAULT_maxWritingSegmentSize  = 3LL * 1024 * 1024 * 1024;
const size_t DEFAULT_minMergeSegNum         = TERARK_IF_DEBUG(2, 5);
const double DEFAULT_purgeDeleteThreshold   = 0.10;

SchemaConfig::SchemaConfig() {
	m_compressingWorkMemSize = DEFAULT_compressingWorkMemSize;
	m_maxWritingSegmentSize = DEFAULT_maxWritingSegmentSize;
	m_minMergeSegNum = DEFAULT_minMergeSegNum;
	m_purgeDeleteThreshold = DEFAULT_purgeDeleteThreshold;
	m_usePermanentRecordId = false;
	m_enableSnapshot = false;
}
SchemaConfig::~SchemaConfig() {
}

void SchemaConfig::compileSchema() {
	m_indexSchemaSet->compileSchemaSet(m_rowSchema.get());
	febitvec hasIndex(m_rowSchema->columnNum(), false);
	const size_t indexNum = this->getIndexNum();
	for (size_t i = 0; i < indexNum; ++i) {
		const Schema& schema = *m_indexSchemaSet->m_nested.elem_at(i);
		const size_t colnum = schema.columnNum();
		for (size_t j = 0; j < colnum; ++j) {
			hasIndex.set1(schema.parentColumnId(j));
		}
	}

	// remove columns in colgroups which is also in indices
	valvec<SchemaPtr> colgroups(m_colgroupSchemaSet->m_nested.end_i());
	for (size_t i = 0; i < m_colgroupSchemaSet->m_nested.end_i(); ++i) {
		colgroups[i] = m_colgroupSchemaSet->m_nested.elem_at(i);
	}
	for (size_t i = 0; i < colgroups.size(); ++i) {
		Schema& schema = *colgroups[i];
		if (!schema.m_isInplaceUpdatable)
			continue;
		for(size_t j = 0; j < schema.columnNum(); ++j) {
			size_t f = m_rowSchema->getColumnId(schema.getColumnName(j));
			assert(f < m_rowSchema->columnNum());
			if (hasIndex[f]) {
				THROW_STD(invalid_argument,
"colgroup '%s' can not be inplaceUpdatable, because which column '%s' has index"
					, schema.m_name.c_str()
					, schema.getColumnName(j).c_str()
					);
			}
		}
	}
	m_colgroupSchemaSet->m_nested.erase_all();
	for (size_t i = 0; i < colgroups.size(); ++i) {
		SchemaPtr& schema = colgroups[i];
		schema->m_columnsMeta.shrink_after_erase_if_kv(
			[&](fstring colname, const ColumnMeta&) {
			size_t pos = m_rowSchema->m_columnsMeta.find_i(colname);
			assert(pos < m_rowSchema->m_columnsMeta.end_i());
			bool ret = hasIndex[pos];
			hasIndex.set1(pos); // now it is column stored
			return ret;
		});
	}
	m_colgroupSchemaSet->m_nested = m_indexSchemaSet->m_nested;
	for (size_t i = 0; i < colgroups.size(); ++i) {
		SchemaPtr& schema = colgroups[i];
		if (!schema->m_columnsMeta.empty())
			m_colgroupSchemaSet->m_nested.insert_i(schema);
	}

	SchemaPtr restAll(new Schema());
	for (size_t i = 0; i < hasIndex.size(); ++i) {
		if (!hasIndex[i]) {
			fstring    colname = m_rowSchema->getColumnName(i);
			ColumnMeta colmeta = m_rowSchema->getColumnMeta(i);
			restAll->m_columnsMeta.insert_i(colname, colmeta);
		}
	}
	if (restAll->columnNum() > 0) {
		if (restAll->columnNum() > 1) {
			restAll->m_name = ".RestAll";
		}
		m_colgroupSchemaSet->m_nested.insert_i(restAll);
	}
	m_colgroupSchemaSet->compileSchemaSet(m_rowSchema.get());

	m_colproject.resize(m_rowSchema->columnNum(), {UINT32_MAX, UINT32_MAX});
	size_t colgroupNum = m_colgroupSchemaSet->m_nested.end_i();
	for (size_t i = 0; i < colgroupNum; ++i) {
		const Schema& schema = *m_colgroupSchemaSet->m_nested.elem_at(i);
		for (size_t j = 0; j < schema.columnNum(); ++j) {
			fstring colname = schema.m_columnsMeta.key(j);
			size_t columnId = m_rowSchema->m_columnsMeta.find_i(colname);
			assert(columnId < m_rowSchema->columnNum());
			if (UINT32_MAX == m_colproject[columnId].colgroupId) {
				m_colproject[columnId].colgroupId = i;
				m_colproject[columnId].subColumnId = j;
			}
		}
	}
	m_uniqIndices.erase_all();
	m_multIndices.erase_all();
	for (size_t i = 0; i < indexNum; ++i) {
		auto& schema = this->getIndexSchema(i);
		if (schema.m_isUnique)
			m_uniqIndices.push_back(i);
		else
			m_multIndices.push_back(i);
	}
	m_bestUniqueIndexId = size_t(-1);
	for(size_t i = 0; i < m_uniqIndices.size(); ++i) {
		size_t indexId = m_uniqIndices[i];
		auto&  schema = this->getIndexSchema(indexId);
		if (schema.columnNum() == 1 && schema.getColumnMeta(0).isInteger()) {
			m_bestUniqueIndexId = i;
			break;
		}
	}
	m_uniqIndices.shrink_to_fit_malloc_free();
	m_multIndices.shrink_to_fit_malloc_free();

	m_updatableColgroups.erase_all();
	for (size_t i = indexNum; i < m_colgroupSchemaSet->indexNum(); ++i) {
		auto& schema = *m_colgroupSchemaSet->getSchema(i);
		if (schema.m_isInplaceUpdatable) {
			if (schema.getFixedRowLen() == 0) {
				THROW_STD(invalid_argument
					, "colgroup '%s' is not fixed, can not be inplaceUpdatable"
					, schema.m_name.c_str()
					);
			}
			m_updatableColgroups.push_back(i);
		}
	}
	m_updatableColgroups.shrink_to_fit_malloc_free();

	if (m_updatableColgroups.empty()) {
		m_wrtSchema = m_rowSchema;
	}
	else {
		m_wrtSchema = new Schema;
		for (size_t i = 0; i < m_rowSchema->columnNum(); ++i) {
			if (!isInplaceUpdatableColumn(i)) {
				auto colname = m_rowSchema->getColumnName(i);
				auto colmeta = m_rowSchema->getColumnMeta(i);
				m_wrtSchema->m_columnsMeta.insert_i(colname, colmeta);
			}
		}
		m_wrtSchema->compile(m_rowSchema.get());
		m_rowSchemaColToWrtCol.resize_fill(m_rowSchema->columnNum(), size_t(-1));
		for(size_t i = 0; i < m_wrtSchema->columnNum(); ++i) {
			size_t j = m_wrtSchema->parentColumnId(i);
			m_rowSchemaColToWrtCol[j] = i;
		}
	}
}

bool SchemaConfig::isInplaceUpdatableColumn(size_t columnId) const {
	TERARK_RT_assert(columnId < m_rowSchema->columnNum(), std::invalid_argument);
	auto colproj = m_colproject[columnId];
	auto& schema = *m_colgroupSchemaSet->getSchema(colproj.colgroupId);
	return schema.m_isInplaceUpdatable;
}

bool SchemaConfig::isInplaceUpdatableColumn(fstring colname) const {
	const size_t columnId = m_rowSchema->getColumnId(colname);
	if (columnId >= m_rowSchema->columnNum()) {
		THROW_STD(invalid_argument, "colname = '%.*s' is not defined"
			, colname.ilen(), colname.data());
	}
	auto colproj = m_colproject[columnId];
	auto& schema = *m_colgroupSchemaSet->getSchema(colproj.colgroupId);
	return schema.m_isInplaceUpdatable;
}

void SchemaConfig::loadJsonFile(fstring fname) {
	loadJsonString(LineBuf().read_all(fname));
}

template<class Json, class Value>
Value getJsonValue(const Json& js, const std::string& key, const Value& Default) {
	auto iter = js.find(key);
	if (js.end() != iter)
		return static_cast<const Value&>(iter.value());
	else
		return Default;
}

template<class Int>
Int limitInBound(Int Val, Int Min, Int Max) {
	if (Val < Min) return Min;
	if (Val > Max) return Max;
	return Val;
}

static void
parseJsonColgroup(Schema& schema, const terark::json& js, int sufarrMinFreq) {
	schema.m_isInplaceUpdatable = getJsonValue(js, "inplaceUpdatable", false);
	schema.m_dictZipSampleRatio = getJsonValue(js, "dictZipSampleRatio", float(0.0));
	schema.m_dictZipLocalMatch  = getJsonValue(js, "dictZipLocalMatch", true);
	schema.m_nltDelims  = getJsonValue(js, "nltDelims", std::string());
	schema.m_maxFragLen = getJsonValue(js, "maxFragLen", 0);
	schema.m_minFragLen = getJsonValue(js, "minFragLen", 0);
	schema.m_sufarrMinFreq = getJsonValue(js, "sufarrMinFreq", sufarrMinFreq);
	schema.m_mmapPopulate = getJsonValue(js, "mmapPopulate", false);
	//  512: rank_select_se_512
	//  256: rank_select_se_256
	// -256: rank_select_il_256
	schema.m_rankSelectClass = getJsonValue(js, "rs", 512);
	schema.m_useFastZip = getJsonValue(js, "useFastZip", false);
	schema.m_nltNestLevel = (byte)limitInBound(
		getJsonValue(js, "nltNestLevel", DEFAULT_nltNestLevel), 1u, 20u);
}

static
std::vector<std::string>
parseJsonFields(const terark::json& js) {
	std::vector<std::string> fields;
	if (js.is_array()) {
		for(auto j = js.begin(); js.end() != j; ++j) {
			std::string oneField = j.value();
			fields.push_back(oneField);
		}
	}
	else if (js.is_string()) {
		const std::string& strFields = js;
		fstring(strFields).split(',', &fields);
	}
	else {
		THROW_STD(invalid_argument
			, "fields: must be comma separeated string or string array");
	}
	return fields;
}

static bool string_equal_nocase(fstring x, fstring y) {
	if (x.size() != y.size())
		return false;
#if defined(_MSC_VER)
	return _strnicmp(x.data(), y.data(), x.size()) == 0;
#else
	return strncasecmp(x.data(), y.data(), x.size()) == 0;
#endif
}

llong parseSizeValue(fstring str) {
	char* endp = NULL;
	llong val = strtoll(str.c_str(), &endp, 10);
	while (*endp && !isalpha(byte(*endp))) {
		++endp;
	}
	if (string_equal_nocase(endp, "KB") || string_equal_nocase(endp, "K")) {
		return val * 1024;
	}
	if (string_equal_nocase(endp, "MB") || string_equal_nocase(endp, "M")) {
		return val * 1024 * 1024;
	}
	if (string_equal_nocase(endp, "GB") || string_equal_nocase(endp, "G")) {
		return val * 1024 * 1024 * 1024;
	}
	if (string_equal_nocase(endp, "TB") || string_equal_nocase(endp, "T")) {
		return val * 1024 * 1024 * 1024 * 1024;
	}
	if (string_equal_nocase(endp, "PB") || string_equal_nocase(endp, "P")) {
		return val * 1024 * 1024 * 1024 * 1024 * 1024;
	}
	return val;
}

llong getJsonSizeValue(const terark::json& js, const std::string& key, const llong& Default) {
	auto iter = js.find(key);
	if (js.end() != iter) {
		if (iter.value().is_string()) {
			const std::string& str = iter.value();
			return parseSizeValue(str);
		}
		return static_cast<const llong&>(iter.value());
	}
	else {
		return Default;
	}
}

namespace MongoBson {
/**
    the complete list of valid BSON types
    see also bsonspec.org
*/
enum BSONType {
    /** smaller than all other types */
//    MinKey = -1,
    /** end of object */
//    EOO = 0,
    /** double precision floating point value */
    NumberDouble = 1,
    /** character string, stored in utf8 */
    String = 2,
    /** an embedded object */
    Object = 3,
    /** an embedded array */
    Array = 4,
    /** binary data */
    BinData = 5,
    /** Undefined type */
    Undefined = 6,
    /** ObjectId */
    jstOID = 7,
    /** boolean type */
    Bool = 8,
    /** date type */
    Date = 9,
    /** null type */
    jstNULL = 10,
    /** regular expression, a pattern with options */
    RegEx = 11,
    /** deprecated / will be redesigned */
    DBRef = 12,
    /** deprecated / use CodeWScope */
    Code = 13,
    /** a programming language (e.g., Python) symbol */
    Symbol = 14,
    /** javascript code that can execute on the database server, with SavedContext */
    CodeWScope = 15,
    /** 32 bit signed integer */
    NumberInt = 16,
    /** Two 32 bit signed integers */
    bsonTimestamp = 17,
    /** 64 bit integer */
    NumberLong = 18,
    /** 128 bit decimal */
    NumberDecimal = 19,
    /** max type that is not MaxKey */
//    JSTypeMax = Decimal128::enabled ? 19 : 18,
    MongoAny = 25,
    /** larger than all other types */
//    MaxKey = 127
};
}

static int 
getMongoTypeDefault(const std::string& colname, const ColumnMeta& colmeta) {
#ifdef  TERARKDB_DEDUCE_DATETIME_COLUMN
	re2::RE2::Options reOpt;
	reOpt.set_case_sensitive(false);
	re2::RE2 datetimeRegex("(?:date|time|timestamp)[0-9]*$", reOpt);
#endif
	switch (colmeta.type) {
	default:
		THROW_STD(runtime_error,
			"Invalid terark type = %d", int(colmeta.type));
		break;
	case ColumnType::Any:
		abort(); // Any is not implemented yet
		break;
	case ColumnType::Uint08:
	case ColumnType::Sint08:
		return MongoBson::Bool;
	case ColumnType::Uint16:
	case ColumnType::Sint16:
		return MongoBson::NumberInt;
	case ColumnType::Uint32:
	case ColumnType::Sint32:
#ifdef TERARKDB_DEDUCE_DATETIME_COLUMN
		if (re2::RE2::FullMatch(colname, datetimeRegex)) {
			return MongoBson::Date;
		}
#endif
		return MongoBson::NumberInt;
	case ColumnType::Uint64:
	case ColumnType::Sint64:
#ifdef TERARKDB_DEDUCE_DATETIME_COLUMN
		if (re2::RE2::FullMatch(colname, datetimeRegex)) {
			return MongoBson::bsonTimestamp;
		}
#endif
		return MongoBson::NumberLong;
	case ColumnType::Uint128:
	case ColumnType::Sint128:
		return MongoBson::NumberLong;
	case ColumnType::Float32:
	case ColumnType::Float64:
		return MongoBson::NumberDouble;
	case ColumnType::Float128:
		return MongoBson::NumberDecimal;
	case ColumnType::Uuid:    // 16 bytes(128 bits) binary
		return MongoBson::BinData;
	case ColumnType::Fixed:   // Fixed length binary
		if (colname == "_id" && colmeta.fixedLen == 12) {
			return MongoBson::jstOID;
		}
		return MongoBson::BinData;
	case ColumnType::VarSint: abort(); break;
	case ColumnType::VarUint: abort(); break;
	case ColumnType::StrZero: // Zero ended string
		return MongoBson::String;
	case ColumnType::TwoStrZero: // Two Zero ended strings
		return MongoBson::RegEx;
	case ColumnType::Binary:  // Prefixed by length(var_uint) in bytes
		THROW_STD(invalid_argument,
			"Must explicit define mongo bson type for terark's <binary> type");
	case ColumnType::CarBin: // Prefixed by uint32 len
		return MongoBson::Object;
	}
	abort(); // should not goes here
	return -1;
}

static int getMongoTypeChecked(const ColumnMeta& colmeta, fstring mongoTypeName) {
	ColumnType terarkType = colmeta.type;
	if (string_equal_nocase(mongoTypeName, "oid")) {
		if (12 == colmeta.fixedLen) {
			return MongoBson::jstOID;
		}
		THROW_STD(invalid_argument,
			"mongoType oid must map to terark type fixed, length = 12");
	}
	if (string_equal_nocase(mongoTypeName, "bool")) {
		if (colmeta.isInteger()) {
			return MongoBson::Bool;
		}
		THROW_STD(invalid_argument,
			"mongoType bool must map to terark integer types");
	}
	if (string_equal_nocase(mongoTypeName, "int")) {
		if (colmeta.isNumber()) {
			return MongoBson::NumberInt;
		}
		THROW_STD(invalid_argument,
			"mongoType bool must map to terark number types");
	}
	if (string_equal_nocase(mongoTypeName, "long")) {
		if (colmeta.isNumber()) {
			return MongoBson::NumberLong;
		}
		THROW_STD(invalid_argument,
			"mongoType long must map to terark type number types");
	}
	if (string_equal_nocase(mongoTypeName, "double")) {
		if (ColumnType::Float64 != terarkType && ColumnType::Float32 != terarkType) {
			return MongoBson::NumberDouble;
		}
		THROW_STD(invalid_argument,
			"mongoType double must map to terark type double/float32/float64");
	}
	if (string_equal_nocase(mongoTypeName, "date")) {
		if (0 || ColumnType::Sint32 == terarkType || ColumnType::Uint32 == terarkType
			  || ColumnType::Sint64 == terarkType || ColumnType::Uint64 == terarkType
		) {
			return MongoBson::Date;
		}
		THROW_STD(invalid_argument,
			"mongoType date must map to terark type int32/uint32/int64/uint64");
	}
	if (string_equal_nocase(mongoTypeName, "timestamp")) {
		if (ColumnType::Sint64 == terarkType || ColumnType::Uint64 == terarkType) {
			return MongoBson::bsonTimestamp;
		}
		THROW_STD(invalid_argument,
			"mongoType timestamp must map to terark type int64/uint64");
	}
	if (string_equal_nocase(mongoTypeName, "binary") ||
		string_equal_nocase(mongoTypeName, "bindata")) {
		if (ColumnType::CarBin == terarkType) {
			return MongoBson::BinData;
		}
		if (ColumnType::StrZero == terarkType) {
			return MongoBson::BinData;
		}
		THROW_STD(invalid_argument,
			"mongoType binary must map to terark type CarBin");
	}
	if (string_equal_nocase(mongoTypeName, "array")) {
		if (ColumnType::CarBin == terarkType) {
			return MongoBson::Array;
		}
		THROW_STD(invalid_argument,
			"mongoType array must map to terark type CarBin");
	}
	if (string_equal_nocase(mongoTypeName, "object")) {
		if (ColumnType::CarBin == terarkType) {
			return MongoBson::Object;
		}
		THROW_STD(invalid_argument,
			"mongoType object must map to terark type CarBin");
	}
	if (string_equal_nocase(mongoTypeName, "regex")) {
		if (ColumnType::TwoStrZero == terarkType) {
			return MongoBson::bsonTimestamp;
		}
		THROW_STD(invalid_argument,
			"mongoType regex must map to terark type TwoStrZero");
	}
	if (string_equal_nocase(mongoTypeName, "any")) {
		if (ColumnType::Binary == terarkType || ColumnType::CarBin == terarkType ) {
			return MongoBson::MongoAny;
		}
		THROW_STD(invalid_argument,
			"mongoType MongoAny must map to terark type Binary or CarBin");
	}

#define MongoTypeAsTerarkStrZero(MongoType) \
	if (string_equal_nocase(mongoTypeName, "string")) { \
		if (ColumnType::StrZero == terarkType) { \
			return MongoBson::MongoType; \
		} \
		THROW_STD(invalid_argument, \
			"mongoType \"%s\" must map to terark type StrZero", #MongoType); \
	}
	MongoTypeAsTerarkStrZero(String);
	MongoTypeAsTerarkStrZero(DBRef);
	MongoTypeAsTerarkStrZero(Code);
	MongoTypeAsTerarkStrZero(CodeWScope);
	MongoTypeAsTerarkStrZero(Symbol);

	THROW_STD(invalid_argument, "unknown mongo bson type: %.*s"
		, mongoTypeName.ilen(), mongoTypeName.data());
}

// define as macro, to report correct lineno and function name
#define CheckInvalidChars(somename, nameType) \
do { \
  const char* invalidChars = "\\/{}[]()<>?|~`!#%^&*'\"=:; \t\r\n"; \
  for (byte c : somename) { \
    if (strchr(invalidChars, c)) { \
      THROW_STD(invalid_argument, "invalid char(%c) = 0x%02X in %s: %s", \
        c, c, nameType, somename.c_str()); \
    } \
  } \
} while (0)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

void SchemaConfig::loadJsonString(fstring jstr) {
	using terark::json;
	const json meta = json::parse(jstr.p
					// UTF8 BOM Check, fixed in nlohmann::json
					// + (fstring(alljson.p, 3) == "\xEF\xBB\xBF" ? 3 : 0)
					);
	m_tableClass = getJsonValue(meta, "TableClass", std::string("DfaDbTable"));
	const bool checkMongoType = getJsonValue(meta, "CheckMongoType", false);
	const bool checkMysqlType = getJsonValue(meta, "CheckMysqlType", false);
	const json& rowSchema = meta["RowSchema"];
	const json& cols = rowSchema["columns"];
	m_rowSchema.reset(new Schema());
	m_colgroupSchemaSet.reset(new SchemaSet());
	int sufarrMinFreq = getJsonValue(meta, "SufarrCompressMinFreq", 0);
	for (auto iter = cols.cbegin(); iter != cols.cend(); ++iter) {
		const auto& col = iter.value();
		std::string name = iter.key();
		std::string type = col["type"];
		std::transform(type.begin(), type.end(), type.begin(), &::tolower);
		if ("nested" == type) {
			fprintf(stderr, "TODO: nested column: %s is not supported now, save it to $$\n", name.c_str());
			continue;
		}
		CheckInvalidChars(name, "column name");
		ColumnMeta colmeta(Schema::parseColumnType(type));
		if (ColumnType::Fixed == colmeta.type) {
			long fixlen = col["length"];
			if (fixlen <= 0 || fixlen > 65536) {
				THROW_STD(invalid_argument, "invalid fixed length=%ld, must > 0 and <= 65536", fixlen);
			}
			colmeta.fixedLen = (uint32_t)fixlen;
		}
		if (checkMongoType) {
			std::string mongoTypeName = getJsonValue(col, "mongoType", std::string());
			if (mongoTypeName.empty())
				colmeta.mongoType = getMongoTypeDefault(name, colmeta);
			else
				colmeta.mongoType = getMongoTypeChecked(colmeta, mongoTypeName);
		}
		if (checkMysqlType) {
			std::string mysqlTypeName = getJsonValue(col, "mysqlType", std::string());
		}
#if defined(TERARK_DB_SCHEMA_COMPILER)
		colmeta.realtype = getJsonValue(col, "realtype", std::string());
		if (!colmeta.realtype.empty()) {
			if (colmeta.type == ColumnType::CarBin || colmeta.type == ColumnType::Fixed)
			{}
			else {
				THROW_STD(invalid_argument,
					"realtype must defined on CarBin or Fixed columns");
			}
		}
#endif
		auto colstoreIter = col.find("colstore");
		if (col.end() != colstoreIter) {
			// this colstore has the only-one 'name' field
			SchemaPtr schema(new Schema());
			schema->m_columnsMeta.insert_i(name, colmeta);
			schema->m_name = name;
			parseJsonColgroup(*schema, colstoreIter.value(), sufarrMinFreq);
			m_colgroupSchemaSet->m_nested.insert_i(schema);
		}
		auto ib = m_rowSchema->m_columnsMeta.insert_i(name, colmeta);
		if (!ib.second) {
			THROW_STD(invalid_argument, "duplicate RowName=%s", name.c_str());
		}
	}
	if (checkMongoType) {
		size_t nonSchemaField = m_rowSchema->getColumnId("$$");
		if (nonSchemaField >= m_rowSchema->columnNum()) {
			fprintf(stderr,
				"WARN: missing '$$' field for mongodb, auto fields will be disabled\n");
		}
		else if (nonSchemaField != m_rowSchema->columnNum()-1) {
			THROW_STD(invalid_argument,
				"mongodb '$$' field must be the last field\n");
		}
	}
	m_rowSchema->compile();
	
	auto colgroupsIter = meta.find("ColumnGroups");
	if (colgroupsIter == meta.end()) {
		colgroupsIter = meta.find("ColumnGroup");
	}
	if (colgroupsIter == meta.end()) {
		colgroupsIter = meta.find("colgroup");
	}
if (colgroupsIter != meta.end()) {
	const auto& colgroups = colgroupsIter.value();
	valvec<size_t> colsToCgId(m_rowSchema->columnNum(), size_t(-1));
	for (size_t cgId = 0; cgId < m_colgroupSchemaSet->indexNum(); ++cgId) {
		const Schema* cgSchema = m_colgroupSchemaSet->getSchema(cgId);
		for (size_t i = 0; i < cgSchema->columnNum(); ++i) {
			fstring colname = cgSchema->getColumnName(i);
			size_t columnId = m_rowSchema->getColumnId(colname);
			colsToCgId[columnId] = cgId;
		}
	}
	for(auto  iter = colgroups.begin(); colgroups.end() != iter; ++iter) {
		auto& cgname = iter.key();
		auto& colgrp = iter.value();
		if (m_rowSchema->m_columnsMeta.exists(cgname)) {
			THROW_STD(invalid_argument
				, "explicit colgroup name '%s' is dup with a column name"
				, cgname.c_str());
		}
		CheckInvalidChars(cgname, "column group name");
		SchemaPtr schema(new Schema());
		auto fieldsIter = colgrp.find("fields");
		if (colgrp.end() == fieldsIter) {
			fieldsIter = colgrp.find("columns");
		}
		if (colgrp.end() == fieldsIter) {
			THROW_STD(invalid_argument, "'columns' of an colgroup must be defined");
		}
		auto fields = parseJsonFields(fieldsIter.value());
		for(const auto& colname : fields) {
			size_t columnId = m_rowSchema->getColumnId(colname);
			if (columnId >= m_rowSchema->columnNum()) {
				THROW_STD(invalid_argument,
					"colname=%s is not in RowSchema", colname.c_str());
			}
			auto& colmeta  = m_rowSchema->getColumnMeta(columnId);
			auto ib = schema->m_columnsMeta.insert_i(colname, colmeta);
			if (!ib.second) {
				THROW_STD(invalid_argument
					, "colname '%s' is dup in colgoup '%s'"
					, colname.c_str(), cgname.c_str());
			}
			if (size_t(-1) != colsToCgId[columnId]) {
				size_t cgId = colsToCgId[columnId];
				auto& cgname1 = m_colgroupSchemaSet->getSchema(cgId)->m_name;
				THROW_STD(invalid_argument
					, "colname '%s' is dup in colgoup '%s' and colgroup '%s'"
					, colname.c_str(), cgname1.c_str(), cgname.c_str());
			}
			// m_nested.end_i() will be the new colgroupId
			colsToCgId[columnId] = m_colgroupSchemaSet->m_nested.end_i();
		}
		schema->m_name = cgname;
		parseJsonColgroup(*schema, colgrp, sufarrMinFreq);
		auto ib = m_colgroupSchemaSet->m_nested.insert_i(schema);
		if (!ib.second) {
			THROW_STD(invalid_argument, "dup colgroup name '%s'", cgname.c_str());
		}
	}
}

	// changed config key and compatible with old config key
	m_compressingWorkMemSize = getJsonSizeValue(meta, "ReadonlyDataMemSize", DEFAULT_compressingWorkMemSize);
	m_compressingWorkMemSize = getJsonSizeValue(meta, "CompressingWorkMemSize", m_compressingWorkMemSize);
	m_maxWritingSegmentSize = getJsonSizeValue(meta, "MaxWrSegSize", DEFAULT_maxWritingSegmentSize);
	m_maxWritingSegmentSize = getJsonSizeValue(meta, "MaxWritingSegmentSize", m_maxWritingSegmentSize);

	m_minMergeSegNum = getJsonValue(
		meta, "MinMergeSegNum", DEFAULT_minMergeSegNum);
	m_purgeDeleteThreshold = getJsonValue(
		meta, "PurgeDeleteThreshold", DEFAULT_purgeDeleteThreshold);

	m_enableSnapshot = getJsonValue(meta, "EnableSnapshot", false);
{
	// PermanentRecordId means record id will not be changed by table reload
	auto it = meta.find("UsePermanentRecordId");
	if (meta.end() != it) {
	//	m_usePermanentRecordId = getJsonValue(meta, "UsePermanentRecordId", false);
		m_usePermanentRecordId = static_cast<bool>(it.value());
		if (m_enableSnapshot && !m_usePermanentRecordId) {
			THROW_STD(invalid_argument, "When EnableSnapshot, UsePermanentRecordId must be true or remain undefined");
		}
	}
	else { // when EnableSnapshot, UsePermanentRecordId must be true
		m_usePermanentRecordId = m_enableSnapshot;
	}
}
	if (m_enableSnapshot) {
		m_snapshotSchema = new Schema();
		m_snapshotSchema->m_columnsMeta.insert_i("__mvccDeletionTime", ColumnMeta(ColumnType::Uint64));
		m_snapshotSchema->compile();
	}

	auto tableIndex = meta.find("TableIndex");
	if (tableIndex != meta.end() && !tableIndex.value().is_array()) {
		THROW_STD(invalid_argument, "json TableIndex must be an array");
	}
	m_indexSchemaSet.reset(new SchemaSet());
//	bool hasPrimaryIndex = false;

if (tableIndex != meta.end()) {
	for (const auto& index : tableIndex.value()) {
		SchemaPtr indexSchema(new Schema());
		auto fieldsIter = index.find("fields");
		if (index.end() == fieldsIter) {
			fieldsIter = index.find("columns");
		}
		if (index.end() == fieldsIter) {
			THROW_STD(invalid_argument, "'columns' of an index must be defined");
		}
		auto fields = parseJsonFields(fieldsIter.value());
		if (fields.size() > Schema::MaxProjColumns) {
			THROW_STD(invalid_argument, "Index Columns=%zd exceeds Max=%zd",
				fields.size(), Schema::MaxProjColumns);
		}
		auto nameIter = index.find("name");
		if (index.end() != nameIter) {
			std::string nameStr = nameIter.value();
			indexSchema->m_name = std::move(nameStr);
			CheckInvalidChars(nameStr, "index name");
		} else {
			indexSchema->m_name = boost::join(fields, ",");
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
				"duplicate index name: %s", indexSchema->m_name.c_str());
		}
		indexSchema->m_isOrdered = getJsonValue(index, "ordered", true);
//		indexSchema->m_isPrimary = getJsonValue(index, "primary", false);
		indexSchema->m_isUnique  = getJsonValue(index, "unique" , false);
		indexSchema->m_enableLinearScan = getJsonValue(index, "enableLinearScan", false);
		indexSchema->m_rankSelectClass = getJsonValue(index, "rs", 512);
		indexSchema->m_nltNestLevel = (byte)limitInBound(
			getJsonValue(index, "nltNestLevel", DEFAULT_nltNestLevel), 1u, 20u);

/*
		if (indexSchema->m_isPrimary) {
			if (hasPrimaryIndex) {
				THROW_STD(invalid_argument, "ERROR: More than one primary index");
			}
		}
*/
	}
}

/*
	// now primary index concept is not required
	// uncomment related code when primary index become required
	if (!hasPrimaryIndex) {
		THROW_STD(invalid_argument,
			"ERROR: No primary index, a table must have exactly one primary index");
	}
*/
	compileSchema();
}

void SchemaConfig::saveJsonFile(fstring jsonFile) const {
	abort(); // not completed yet
	using terark::json;
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
	std::string jsonStr = meta.dump(2);
	FileStream fp(jsonFile.c_str(), "w");
	fp.ensureWrite(jsonStr.data(), jsonStr.size());
}

#if defined(TERARK_DB_ENABLE_DFA_META)
void SchemaConfig::loadMetaDFA(fstring metaFile) {
	std::unique_ptr<MatchingDFA> metaConf(MatchingDFA::load_from(metaFile));
	std::string val;
	size_t segNum = 0, minWrSeg = 0;
	(void)segNum;
	(void)minWrSeg;
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
	if (metaConf->find_key_uniq_val("MaxWritingSegmentSize", &val)) {
		m_maxWritingSegmentSize = lcast(val);
	} else {
		m_maxWritingSegmentSize = DEFAULT_maxWritingSegmentSize;
	}
	if (metaConf->find_key_uniq_val("CompressingWorkMemSize", &val)) {
		m_compressingWorkMemSize = lcast(val);
	} else {
		m_compressingWorkMemSize = DEFAULT_compressingWorkMemSize;
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
	compileSchema();
}

void SchemaConfig::saveMetaDFA(fstring fname) const {
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

} } // namespace terark::db


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
		return terark_bit_test(m_bitsPtr, i);
	}
	size_t size() const { return m_bitsNum; }
};
*/

