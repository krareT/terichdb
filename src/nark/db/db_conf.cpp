#include "db_conf.hpp"
#include <nark/io/DataIO_Basic.hpp>
//#include <nark/io/DataIO.hpp>
//#include <nark/io/MemStream.hpp>
//#include <nark/io/StreamBuffer.hpp>
#include <nark/io/var_int.hpp>
//#include <nark/util/sortable_strvec.hpp>
#include <string.h>
#include "json.hpp"

namespace nark { namespace db {

ColumnMeta::ColumnMeta() {
	type = ColumnType::Any;
	uType = 255; // unknown
}

ColumnMeta::ColumnMeta(ColumnType t) {
	type = t;
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

/////////////////////////////////////////////////////////////////////////////

Schema::Schema() {
	m_fixedLen = size_t(-1);
	m_parent = nullptr;
	m_isOrdered = false;
	m_canEncodeToLexByteComparable = false;
	m_keepCols.fill(true);
}
Schema::~Schema() {
}

void Schema::compile(const Schema* parent) {
	assert(!m_columnsMeta.empty());
	m_fixedLen = computeFixedRowLen();
	if (parent) {
		compileProject(parent);
	}
	m_canEncodeToLexByteComparable = true;
	size_t colnum = m_columnsMeta.end_i();
	for (size_t i = 0; i < colnum - 1; ++i) {
		const ColumnMeta& colmeta = m_columnsMeta.val(i);
		if (ColumnType::Binary == colmeta.type ||
			ColumnType::CarBin == colmeta.type) {
			m_canEncodeToLexByteComparable = false;
			break;
		}
	}
}

void Schema::parseRow(fstring row, valvec<fstring>* columns) const {
	assert(size_t(-1) != m_fixedLen);
	columns->risk_set_size(0);
	parseRowAppend(row, columns);
}

void Schema::parseRowAppend(fstring row, valvec<fstring>* columns) const {
	assert(size_t(-1) != m_fixedLen);
	const byte* curr = row.udata();
	const byte* last = row.size() + curr;

#define CHECK_CURR_LAST3(curr, last, len) \
	if (nark_unlikely(curr + (len) > last)) { \
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
		case ColumnType::Any:
			abort(); // Any is not implemented yet
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
		case ColumnType::VarSint:
			{
				const byte* next = nullptr;
				load_var_int64(curr, &next);
				coldata.n = next - curr;
				curr = next;
			}
			break;
		case ColumnType::VarUint:
			{
				const byte* next = nullptr;
				load_var_uint64(curr, &next);
				coldata.n = next - curr;
				curr = next;
			}
			break;
		case ColumnType::StrZero: // Zero ended string
			coldata.n = strnlen((const char*)curr, last - curr);
			if (i < colnum - 1) {
				CHECK_CURR_LAST(coldata.n + 1);
				curr += coldata.n + 1;
			}
			else { // the last column
				assert(coldata.n == last - curr);
				if (coldata.n + 1 < last - curr) {
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
					coldata.n = n1 + 1 + n2; // don't include 2nd '\0'
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
						coldata.n = n1 + 1 + n2; // don't include 2nd '\0'
					} else {
						coldata.n = n1; // second string is empty/(not present)
					}
				}
			}
			break;
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
		case ColumnType::CarBin: // Prefixed by uint32 len
			if (i < colnum - 1) {
			#if defined(BOOST_BIG_ENDIAN)
				coldata.n = byte_swap(unaligned_load<uint32_t>(curr));
			#else
				coldata.n = unaligned_load<uint32_t>(curr);
			#endif
				coldata.p = (const char*)curr + 4;
				CHECK_CURR_LAST3(curr+4, last, coldata.n);
				curr += 4 + coldata.n;
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
				size_t oldsize = myRowData->size();
				myRowData->resize_no_init(oldsize + 10);
				byte* p1 = myRowData->data() + oldsize;
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->risk_set_size(oldsize + (p2 - p1));
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
				size_t oldsize = myRowData->size();
				myRowData->resize_no_init(oldsize + 10);
				byte* p1 = myRowData->data() + oldsize;
				byte* p2 = save_var_uint32(p1, uint32_t(coldata.size()));
				myRowData->risk_set_size(oldsize + (p2 - p1));
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

void Schema::byteLexConvert(valvec<byte>& indexKey) const {
	byteLexConvert(indexKey.data(), indexKey.size());
}
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
		case ColumnType::Sint08:
			CHECK_CURR_LAST(1);
			curr += 1;
			break;
		case ColumnType::Uint16:
		case ColumnType::Sint16:
			CHECK_CURR_LAST(2);
			{
				uint16_t x = unaligned_load<uint16_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				x ^= 1 << 15;
				unaligned_save(curr, x);
			}
			curr += 2;
			break;
		case ColumnType::Uint32:
		case ColumnType::Sint32:
		case ColumnType::Float32:
			CHECK_CURR_LAST(4);
			{
				uint32_t x = unaligned_load<uint32_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				x ^= 1 << 15;
				unaligned_save(curr, x);
			}
			curr += 4;
			break;
		case ColumnType::Uint64:
		case ColumnType::Sint64:
		case ColumnType::Float64:
			CHECK_CURR_LAST(8);
			{
				uint64_t x = unaligned_load<uint64_t>(curr);
				BYTE_SWAP_IF_LITTLE_ENDIAN(x);
				x ^= 1 << 15;
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

size_t Schema::parseDelimText(char delim, fstring text, valvec<byte>* row) const {
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
	nark::json js;
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

