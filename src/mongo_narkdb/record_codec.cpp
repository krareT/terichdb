#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#ifdef _MSC_VER
#pragma warning(disable: 4800) // bool conversion
#pragma warning(disable: 4244) // 'return': conversion from '__int64' to 'double', possible loss of data
#pragma warning(disable: 4267) // '=': conversion from 'size_t' to 'int', possible loss of data
#endif

#include "record_codec.h"

#include <mongo/util/log.h>
#include <mongo/bson/bsonobjbuilder.h>
#include <mongo/util/hex.h>
#include <nark/io/DataIO.hpp>
#include <nark/io/MemStream.hpp>

namespace mongo { namespace narkdb {

// all non-schema fields packed into this field as ColumnType::CarBin
const char G_schemaLessFieldName[] = "$$";

using namespace nark;

static void narkEncodeBsonArray(const BSONObj& arr, valvec<char>& encoded);
static void narkEncodeBsonObject(const BSONObj& obj, valvec<char>& encoded);

static
void narkEncodeBsonElemVal(const BSONElement& elem, valvec<char>& encoded) {
	const char* value = elem.value();
	switch (elem.type()) {
	case EOO:
	case Undefined:
	case jstNULL:
	case MaxKey:
	case MinKey:
		break;
	case mongo::Bool:
		encoded.push_back(value[0] ? 1 : 0);
		break;
	case NumberInt:
		encoded.append(value, 4);
		break;
	case bsonTimestamp: // low 32 bit is always positive
	case mongo::Date:
	case NumberDouble:
	case NumberLong:
		encoded.append(value, 8);
		break;
	case jstOID:
	//	log() << "encode: OID=" << toHexLower(value, OID::kOIDSize);
		encoded.append(value, OID::kOIDSize);
		break;
	case Symbol:
	case Code:
	case mongo::String:
	//	log() << "encode: strlen+1=" << elem.valuestrsize() << ", str=" << elem.valuestr();
		encoded.append(value + 4, elem.valuestrsize());
		break;
	case DBRef:
		encoded.append(value + 4, elem.valuestrsize() + OID::kOIDSize);
		break;
	case mongo::Array:
		narkEncodeBsonArray(elem.embeddedObject(), encoded);
		break;
	case Object:
		narkEncodeBsonObject(elem.embeddedObject(), encoded);
		break;
	case CodeWScope:
		encoded.append(value, elem.objsize());
		break;
	case BinData:
		encoded.append(value, 5 + elem.valuestrsize());
		break;
	case RegEx:
		{
			const char* p = value;
			size_t len1 = strlen(p); // regex len
			p += len1 + 1;
			size_t len2 = strlen(p);
			encoded.append(p, len1 + 1 + len2 + 1);
		}
		break;
	default:
		{
			StringBuilder ss;
			ss << "narkEncodeIndexKey(): BSONElement: bad elem.type " << (int)elem.type();
			std::string msg = ss.str();
			massert(10320, msg.c_str(), false);
		}
	}
}

static void narkEncodeBsonArray(const BSONObj& arr, valvec<char>& encoded) {
	int cnt = 0;
	int itemType = 128;
	BSONForEach(item, arr) {
		if (itemType == 128) {
			itemType = item.type();
		} else {
			if (item.type() != itemType) itemType = 129;
		}
		cnt++;
	}
	{
		unsigned char  buf[8];
		encoded.append(buf, nark::save_var_uint32(buf, cnt));
	}
	if (cnt) {
		invariant(itemType != 128);
		encoded.push_back((unsigned char)(itemType));
		BSONForEach(item, arr) {
			if (itemType == 129) { // heterogeneous array items
				encoded.push_back((unsigned char)item.type());
			}
			narkEncodeBsonElemVal(item, encoded);
		}
	}
}

static void narkEncodeBsonObject(const BSONObj& obj, valvec<char>& encoded) {
	BSONForEach(elem, obj) {
		encoded.push_back((unsigned char)(elem.type()));
		encoded.append(elem.fieldName(), elem.fieldNameSize());
		narkEncodeBsonElemVal(elem, encoded);
	}
	encoded.push_back((unsigned char)EOO);
}

SchemaRecordCoder::SchemaRecordCoder() {
}
SchemaRecordCoder::~SchemaRecordCoder() {
}

template<class Vec>
static void Move_AutoGrownMemIO_to_valvec(AutoGrownMemIO& io, Vec& v) {
	BOOST_STATIC_ASSERT(sizeof(typename Vec::value_type) == 1);
	BOOST_STATIC_ASSERT(sizeof(v[0]) == 1);
	v.clear();
	v.risk_set_size(io.tell());
	v.risk_set_data((typename Vec::value_type*)io.buf());
	v.risk_set_capacity(io.size());
	io.risk_release_ownership();
}

// for WritableSegment, param schema is m_rowSchema, param exclude is nullptr
// for ReadonlySegment, param schema is m_nonIndexSchema,
//                      param exclude is m_uniqIndexFields
void SchemaRecordCoder::encode(const Schema* schema, const Schema* exclude,
							   const BSONObj& obj, valvec<char>* encoded) {
	assert(nullptr != schema);
	encoded->resize(0);

	m_fields.erase_all();
	std::string fieldnames;
	BSONForEach(elem, obj) {
		const char* fieldname = elem.fieldName();
		auto ib = m_fields.insert_i(fieldname);
		if (!ib.second) {
			THROW_STD(invalid_argument,
					"bad bson: duplicate fieldname: %s", fieldname);
		}
		invariant(m_fields.elem_at(ib.first).size() == strlen(fieldname));
		fieldnames += fieldname;
		fieldnames.push_back(',');
	}
	fieldnames.pop_back();
	LOG(1)	<< "SchemaRecordCoder::encode: bsonFields=" << fieldnames;

	fieldnames.resize(0);
	for (size_t i = 0; i < m_fields.end_i(); ++i) {
		fstring fieldname = m_fields.elem_at(i);
		fieldnames.append(fieldname.c_str());
		fieldnames.push_back(',');
		invariant(m_fields.find_i(fieldname) == i);
	}
	fieldnames.pop_back();
	LOG(1)	<< "SchemaRecordCoder::encode: m_fields=" << fieldnames;

	m_stored.resize_fill(m_fields.end_i(), false);

	// last is $$ field, the schema-less fields
	size_t schemaColumn
		= schema->m_columnsMeta.end_key(1) == G_schemaLessFieldName
		? schema->m_columnsMeta.end_i() - 1
		: schema->m_columnsMeta.end_i()
		;
	for(size_t i = 0; i < schemaColumn; ++i) {
		const fstring colname = schema->getColumnName(i);
		const auto&   colmeta = schema->getColumnMeta(i);
		assert(colname != G_schemaLessFieldName);
		size_t j = m_fields.find_i(colname);
		if (j >= m_fields.end_i()) {
			LOG(1)	<< "colname=" << colname.str() << " is missing"
					<< ", j=" << j
					<< ", m_fields.end_i()=" << m_fields.end_i()
					<< ", bson=" << obj.toString();
		}
		invariant(j < m_fields.end_i());
		BSONElement elem(m_fields.key(j).data() - 1, colname.size()+1,
						 BSONElement::FieldNameSizeTag());
		assert((unsigned char)elem.type() == colmeta.uType);
		const char* value = elem.value();
		switch (elem.type()) {
		case EOO:
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			break;
		case mongo::Bool:
			encoded->push_back(value[0] ? 1 : 0);
			assert(colmeta.type == nark::db::ColumnType::Uint08);
			break;
		case NumberInt:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			// offset binary encoding for byte lex compare
			{
				int x = ConstDataView(value).read<LittleEndian<int>>();
				int y = x ^ (1 << 31);
				encoded->append((char*)&y, 4);
			}
		#else
			encoded->append(value, 4);
		#endif
			assert(colmeta.type == nark::db::ColumnType::Sint32);
			break;
		case bsonTimestamp: // low 32 bit is always positive
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			// offset binary encoding for byte lex compare
			{
				auto x = ConstDataView(value).read<LittleEndian<long long>>();
				auto y = x ^ (1LL << 63);
				encoded->append((char*)&y, 8);
			}
		#else
			encoded->append(value, 8);
		#endif
			break;
		case jstOID:
		//	log() << "encode: OID=" << toHexLower(value, OID::kOIDSize);
			encoded->append(value, OID::kOIDSize);
			assert(colmeta.type == nark::db::ColumnType::Fixed);
			assert(colmeta.fixedLen == OID::kOIDSize);
			break;
		case Symbol:
		case Code:
		case mongo::String:
		//	log() << "encode: strlen+1=" << elem.valuestrsize() << ", str=" << elem.valuestr();
			assert(colmeta.type == nark::db::ColumnType::StrZero);
			encoded->append(value + 4, elem.valuestrsize());
			break;
		case DBRef:
			assert(0); // deprecated, should not in data
			encoded->append(value + 4, elem.valuestrsize() + OID::kOIDSize);
			break;
		case mongo::Array:
			assert(colmeta.type == nark::db::ColumnType::CarBin);
			{
				size_t oldsize = encoded->size();
				encoded->resize(oldsize + 4); // reserve for uint32 length
				narkEncodeBsonArray(elem.embeddedObject(), *encoded);
				size_t len = encoded->size() - (oldsize + 4);
				DataView(encoded->data()+oldsize)
						.write(LittleEndian<uint32_t>(uint32_t(len)));
			}
			break;
		case Object:
			assert(colmeta.type == nark::db::ColumnType::CarBin);
			{
				size_t oldsize = encoded->size();
				encoded->resize(oldsize + 4); // reserve for uint32 length
				narkEncodeBsonObject(elem.embeddedObject(), *encoded);
				size_t len = encoded->size() - (oldsize + 4);
				DataView(encoded->data()+oldsize)
						.write(LittleEndian<uint32_t>(uint32_t(len)));
			}
			break;
		case CodeWScope:
			assert(colmeta.type == nark::db::ColumnType::CarBin);
			{
				assert(colmeta.type == nark::db::ColumnType::CarBin);
				size_t oldsize = encoded->size();
				encoded->resize(oldsize + 8); // reserve for uint32 length + uint32 codelen
				DataView(encoded->data()+oldsize + 4)
						.write(LittleEndian<uint32_t>(elem.codeWScopeCodeLen()));
				encoded->append(elem.codeWScopeCode(), elem.codeWScopeCodeLen());
				narkEncodeBsonObject(elem.codeWScopeObject(), *encoded);
				size_t len = encoded->size() - (oldsize + 4);
				DataView(encoded->data()+oldsize)
						.write(LittleEndian<uint32_t>(uint32_t(len)));
			}
			encoded->append(value, elem.objsize());
			break;
		case BinData:
			{
				assert(colmeta.type == nark::db::ColumnType::CarBin);
				uint32_t len = elem.valuestrsize() + 1; // 1 is for subtype byte
				encoded->resize(encoded->size() + 4);
				DataView(encoded->end() - 4)
						.write(LittleEndian<uint32_t>(len));
				encoded->append(value + 4, 1 + elem.valuestrsize());
			}
			break;
		case RegEx:
			{
				const char* p = value;
				size_t len1 = strlen(p); // regex len
				p += len1 + 1;
				size_t len2 = strlen(p);
				encoded->append(p, len1 + 1 + len2 + 1);
			}
			assert(colmeta.type == nark::db::ColumnType::TwoStrZero);
			break;
		default:
			{
				StringBuilder ss;
				ss << BOOST_CURRENT_FUNCTION
				   << ": BSONElement: bad elem.type " << (int)elem.type();
				std::string msg = ss.str();
				massert(10320, msg.c_str(), false);
			}
		}
		m_stored.set1(j);
	}

	if (schemaColumn == schema->columnNum()) {
		// has no schema-less column
		bool isAllStored = m_stored.isall1();
		assert(isAllStored);
		if (!isAllStored) {
			THROW_STD(invalid_argument,
				"schema is forced on all fields, but input data has extra fields");
		}
		return;
	}

	size_t idx = 0;
	for (auto it = obj.begin(), End = obj.end(); it != End; ++it, ++idx) {
		if (m_stored.is1(idx))
			continue;
		BSONElement elem = *it;
		fstring fieldName = elem.fieldName();
		assert(fieldName.ende(0) == 0);
		if (exclude) {
			size_t colid = exclude->m_columnsMeta.find_i(fieldName);
			if (colid >= exclude->columnNum())
				continue;
		}
		encoded->append(fieldName.data(), fieldName.size()+1);
		narkEncodeBsonElemVal(elem, *encoded);
	}
}

nark::valvec<char>
SchemaRecordCoder::encode(const Schema* schema, const Schema* exclude, const BSONObj& obj) {
	nark::valvec<char> encoded;
	encode(schema, exclude, obj, &encoded);
	return encoded;
}

typedef LittleEndianDataOutput<AutoGrownMemIO> MyBsonBuilder;

static void narkDecodeBsonObject(MyBsonBuilder& bb, const char*& pos, const char* end);
static void narkDecodeBsonArray(MyBsonBuilder& bb, const char*& pos, const char* end);

static void narkDecodeBsonElemVal(MyBsonBuilder& bb, const char*& pos, const char* end, int type) {
	switch (type) {
	case EOO:
		invariant(!"narkDecodeBsonElemVal: encountered EOO");
		break;
	case Undefined:
	case jstNULL:
	case MaxKey:
	case MinKey:
		break;
	case mongo::Bool:
		bb << char(*pos ? 1 : 0);
		pos++;
		break;
	case NumberInt:
		bb.ensureWrite(pos, 4);
		pos += 4;
		break;
	case bsonTimestamp:
	case mongo::Date:
	case NumberDouble:
	case NumberLong:
		bb.ensureWrite(pos, 8);
		pos += 8;
		break;
	case jstOID:
		bb.ensureWrite(pos, OID::kOIDSize);
		pos += OID::kOIDSize;
		break;
	case Symbol:
	case Code:
	case mongo::String:
		{
			size_t len = strlen(pos);
			bb << int(len + 1);
			bb.ensureWrite(pos, len + 1);
		//	log() << "decode: strlen=" << len << ", str=" << pos;
			pos += len + 1;
		}
		break;
	case DBRef:
		{
			size_t len = strlen(pos);
			bb << int(len + 1);
			bb.ensureWrite(pos + 4, len + 1 + OID::kOIDSize);
			pos += len + 1 + OID::kOIDSize;
		}
		break;
	case mongo::Array:
		narkDecodeBsonArray(bb, pos, end);
		break;
	case Object:
		narkDecodeBsonObject(bb, pos, end);
		break;
	case CodeWScope:
		{
			int len = ConstDataView(pos).read<LittleEndian<int>>();
			bb.ensureWrite(pos, len);
			pos += len;
		}
		break;
	case BinData:
		{
			int len = ConstDataView(pos).read<LittleEndian<int>>();
			bb << len;
			bb << pos[4]; // binary data subtype
			bb.ensureWrite(pos + 5, len);
			pos += 5 + len;
		}
		break;
	case RegEx:
		{
			size_t len1 = strlen(pos); // regex len
			size_t len2 = strlen(pos + len1 + 1);
			size_t len3 = len1 + len2 + 2;
			bb.ensureWrite(pos, len3);
			pos += len3;
		}
		break;
	default:
		{
			StringBuilder ss;
			ss << "narkDecodeIndexKey(): BSONElement: bad subkey.type " << (int)type;
			std::string msg = ss.str();
			massert(10320, msg.c_str(), false);
		}
	}

}

static void narkDecodeBsonObject(MyBsonBuilder& bb, const char*& pos, const char* end) {
	int byteNumOffset = bb.tell();
	bb << 0; // reserve 4 bytes for object byteNum
	for (;;) {
		if (pos >= end) {
			THROW_STD(invalid_argument, "Invalid encoded bson object");
		}
		const int type = (unsigned char)(*pos++);
		bb << char(type);
		if (type == EOO)
			break;
		StringData fieldname = pos;
		bb.ensureWrite(fieldname.begin(), fieldname.size()+1);
		pos += fieldname.size() + 1;
		narkDecodeBsonElemVal(bb, pos, end, type);
	}
	int objByteNum = int(bb.tell() - byteNumOffset);
//	log() << "byteNumOffset" << byteNumOffset << ", objByteNum=" << objByteNum;
	(int&)bb.buf()[byteNumOffset] = objByteNum;
}

static void narkDecodeBsonArray(MyBsonBuilder& bb, const char*& pos, const char* end) {
	int cnt = nark::load_var_uint32((unsigned char*)pos, (const unsigned char**)&pos);
	if (0 == cnt) {
		bb << int(5); // 5 is empty bson object size
		bb << char(EOO);
		return;
	}
	int arrItemType = (unsigned char)(*pos++);
	int arrByteNumOffset = bb.tell();
	bb << int(0); // reserve for arrByteNum
	for (int arrIndex = 0; arrIndex < cnt; arrIndex++) {
		if (pos >= end) {
			THROW_STD(invalid_argument, "Invalid encoded bson array");
		}
		const int curItemType = arrItemType == 129 ? (unsigned char)(*pos++) : arrItemType;
		bb << char(curItemType);
		std::string idxStr = BSONObjBuilder::numStr(arrIndex);
		bb.ensureWrite(idxStr.c_str(), idxStr.size()+1);
		narkDecodeBsonElemVal(bb, pos, end, curItemType);
	}
	bb << char(EOO);
	int arrByteNum = bb.tell() - arrByteNumOffset;
//	log() << "arrByteNumOffset" << arrByteNumOffset << "arrByteNum=" << arrByteNum;
	(int&)bb.buf()[arrByteNumOffset] = arrByteNum;
}

SharedBuffer
SchemaRecordCoder::decode(const Schema* schema, const char* data, size_t size) {
	assert(nullptr != schema);
	MyBsonBuilder bb;
	const char* pos = data;
	bb.resize(size);
	bb.skip(sizeof(SharedBuffer::Holder));
	bb.skip(4); // object size
	// last is $$ field, the schema-less fields
	size_t schemaColumn
		= schema->m_columnsMeta.end_key(1) == G_schemaLessFieldName
		? schema->m_columnsMeta.end_i() - 1
		: schema->m_columnsMeta.end_i()
		;
	for (size_t i = 0; i < schemaColumn; ++i) {
		fstring     colname = schema->m_columnsMeta.key(i);
		const auto& colmeta = schema->m_columnsMeta.val(i);
		bb.writeByte(colmeta.uType);
		bb.ensureWrite(colname.data(), colname.size()+1); // include '\0'
		switch ((signed char)colmeta.uType) {
		case EOO:
			invariant(!"narkDecodeBsonElemVal: encountered EOO");
			break;
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			assert(0);
			break;
		case mongo::Bool:
			bb << char(*pos ? 1 : 0);
			pos++;
			break;
		case NumberInt:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			{
				int x = ConstDataView(pos).read<BigEndian<int>>();
				x ^= 1 << 31;
				bb << x;
				pos += 4;
			}
		#else
			bb.ensureWrite(pos, 4);
			pos += 4;
		#endif
			break;
		case bsonTimestamp:
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			{
				long long x = ConstDataView(pos).read<BigEndian<long long>>();
				x ^= (long long)1 << 63;
				bb << x;
				pos += 8;
			}
		#else
			bb.ensureWrite(pos, 8);
			pos += 8;
		#endif
			break;
		case jstOID:
			bb.ensureWrite(pos, OID::kOIDSize);
			pos += OID::kOIDSize;
			break;
		case Symbol:
		case Code:
		case mongo::String:
			{
				size_t len = strlen(pos);
				bb << int(len + 1);
				bb.ensureWrite(pos, len + 1);
			//	log() << "decode: strlen=" << len << ", str=" << pos;
				pos += len + 1;
			}
			break;
		case DBRef:
			{
				size_t len = strlen(pos);
				bb << int(len + 1);
				bb.ensureWrite(pos + 4, len + 1 + OID::kOIDSize);
				pos += len + 1 + OID::kOIDSize;
			}
			break;
		case mongo::Array:
			{
				size_t len = ConstDataView(pos).read<LittleEndian<uint32_t> >();
				auto   end = pos + len;
				narkDecodeBsonArray(bb, pos, end);
			}
			break;
		case Object:
			{
				size_t len = ConstDataView(pos).read<LittleEndian<uint32_t> >();
				auto   end = pos + len;
				narkDecodeBsonObject(bb, pos, end);
			}
			break;
		case CodeWScope:
			{
				int binlen = ConstDataView(pos).read<LittleEndian<int>>();
				size_t oldpos = bb.tell();
				bb << uint32_t(0); // reserve for whole len
				int codelen = ConstDataView(pos+4).read<LittleEndian<int>>();
				bb << uint32_t(codelen);
				bb.ensureWrite(pos + 8, codelen);
				auto end = pos + binlen;
				pos += 8 + codelen;
				narkDecodeBsonObject(bb, pos, end);
				uint32_t wholeLen = uint32_t(bb.tell() - oldpos);
				DataView((char*)bb.buf() + oldpos).write<LittleEndian<uint32_t> >(wholeLen);
			}
			break;
		case BinData:
			{
				int len = ConstDataView(pos).read<LittleEndian<int>>();
				bb << len - 1; // pos[4] is binary data subtype
				bb.ensureWrite(pos + 4, len);
				pos += 4 + len;
			}
			break;
		case RegEx:
			{
				size_t len1 = strlen(pos); // regex len
				size_t len2 = strlen(pos + len1 + 1);
				size_t len3 = len1 + len2 + 2;
				bb.ensureWrite(pos, len3);
				pos += len3;
			}
			break;
		default:
			{
				StringBuilder ss;
				ss << "narkDecodeIndexKey(): BSONElement: bad subkey.type " << (int)colmeta.uType;
				std::string msg = ss.str();
				massert(10320, msg.c_str(), false);
			}
		}
	}
	const char* end = data + size;
	if (pos < end) {
		while (pos < end) {
			const int type = (unsigned char)(*pos++);
			bb << char(type);
			if (type == EOO) {
				invariant(pos == end);
				break;
			}
			StringData fieldname = pos;
			bb.ensureWrite(fieldname.begin(), fieldname.size()+1);
			pos += fieldname.size() + 1;
			narkDecodeBsonElemVal(bb, pos, end, type);
		}
		invariant(pos == data + size);
	}
	else {
		invariant(pos == data + size);
		bb << char(EOO); // End of object
	}
	int bsonSize = int(bb.tell() - sizeof(SharedBuffer::Holder));
	DataView((char*)bb.buf() + sizeof(SharedBuffer::Holder))
			.write<LittleEndian<int>>(bsonSize);

	return SharedBuffer::takeOwnership((char*)bb.release());
//	return BSONObj::takeOwnership((char*)bb.release());
}

SharedBuffer
SchemaRecordCoder::decode(const Schema* schema, const nark::valvec<char>& encoded) {
	return decode(schema, encoded.data(), encoded.size());
}

SharedBuffer
SchemaRecordCoder::decode(const Schema* schema, StringData encoded) {
	return decode(schema, encoded.rawData(), encoded.size());
}

SharedBuffer
SchemaRecordCoder::decode(const Schema* schema, nark::fstring encoded) {
	return decode(schema, encoded.data(), encoded.size());
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					nark::valvec<char>* encoded) {
	encoded->erase_all();
	using nark::db::ColumnType;
	BSONObj::iterator iter = bson.begin();
	for(size_t i = 0; i < indexSchema.m_columnsMeta.end_i(); ++i) {
	//	fstring     colname = indexSchema.m_columnsMeta.key(i);
		const auto& colmeta = indexSchema.m_columnsMeta.val(i);
		BSONElement elem(iter.next());
		assert(elem.type() == colmeta.uType);
		const char* value = elem.value();
		switch (elem.type()) {
		case EOO:
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			break;
		case mongo::Bool:
			encoded->push_back(value[0] ? 1 : 0);
			assert(indexSchema.getColumnType(i) == ColumnType::Uint08);
			break;
		case NumberInt:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			// offset binary encoding for byte lex compare
			{
				int x = ConstDataView(value).read<LittleEndian<int>>();
				int y = x ^ (1 << 31);
				encoded->append((char*)&y, 4);
			}
		#else
			encoded->append(value, 4);
		#endif
			assert(indexSchema.getColumnType(i) == ColumnType::Sint32);
			break;
		case bsonTimestamp: // low 32 bit is always positive
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			// offset binary encoding for byte lex compare
			{
				auto x = ConstDataView(value).read<LittleEndian<long long>>();
				auto y = x ^ (1LL << 63);
				encoded->append((char*)&y, 8);
			}
		#else
			encoded->append(value, 8);
		#endif
			break;
		case jstOID:
		//	log() << "encode: OID=" << toHexLower(value, OID::kOIDSize);
			encoded->append(value, OID::kOIDSize);
			assert(colmeta.type == ColumnType::Fixed);
			assert(colmeta.fixedLen == OID::kOIDSize);
			break;
		case Symbol:
		case Code:
		case mongo::String:
		//	log() << "encode: strlen+1=" << elem.valuestrsize() << ", str=" << elem.valuestr();
			assert(colmeta.type == ColumnType::StrZero);
			encoded->append(value + 4, elem.valuestrsize());
			break;
		case DBRef:
			assert(0); // deprecated, should not in data
			encoded->append(value + 4, elem.valuestrsize() + OID::kOIDSize);
			break;
		case mongo::Array:
			break;
		case Object:
			assert(colmeta.type == ColumnType::CarBin);
			break;
		case CodeWScope:
			assert(indexSchema.getColumnType(i) == ColumnType::CarBin);
			break;
		case BinData:
			assert(indexSchema.getColumnType(i) == ColumnType::CarBin);
			break;
		case RegEx:
			{
				const char* p = value;
				size_t len1 = strlen(p); // regex len
				p += len1 + 1;
				size_t len2 = strlen(p);
				encoded->append(p, len1 + 1 + len2 + 1);
			}
			assert(colmeta.type == ColumnType::TwoStrZero);
			break;
		default:
			{
				StringBuilder ss;
				ss << BOOST_CURRENT_FUNCTION
				   << ": BSONElement: bad elem.type " << (int)elem.type();
				std::string msg = ss.str();
				massert(10320, msg.c_str(), false);
			}
		}
	}
}

void encodeIndexKey(const Schema& indexSchema,
					const BSONObj& bson,
					nark::valvec<unsigned char>* encoded) {
	encodeIndexKey(indexSchema, bson,
		reinterpret_cast<nark::valvec<char>*>(encoded));
}

SharedBuffer
decodeIndexKey(const Schema& indexSchema, const char* data, size_t size) {
	MyBsonBuilder bb;
	const char* pos = data;
	bb.resize(size);
	bb.skip(sizeof(SharedBuffer::Holder));
	bb.skip(4); // object size loc
	for (size_t i = 0; i < indexSchema.m_columnsMeta.end_i(); ++i) {
		fstring     colname = indexSchema.m_columnsMeta.key(i);
		const auto& colmeta = indexSchema.m_columnsMeta.val(i);
		bb.writeByte(colmeta.uType);
		bb.ensureWrite(colname.data(), colname.size()+1); // include '\0'
		switch ((signed char)colmeta.uType) {
		case EOO:
			invariant(!"narkDecodeBsonElemVal: encountered EOO");
			break;
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			break;
		case mongo::Bool:
			bb << char(*pos ? 1 : 0);
			pos++;
			break;
		case NumberInt:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			{
				int x = ConstDataView(pos).read<BigEndian<int>>();
				x ^= 1 << 31;
				bb << x;
				pos += 4;
			}
		#else
			bb.ensureWrite(pos, 4);
			pos += 4;
		#endif
			break;
		case bsonTimestamp:
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
		#ifdef MONGO_NARK_ENCODE_BYTE_LEX
			{
				long long x = ConstDataView(pos).read<BigEndian<long long>>();
				x ^= (long long)1 << 63;
				bb << x;
				pos += 8;
			}
		#else
			bb.ensureWrite(pos, 8);
			pos += 8;
		#endif
			break;
		case jstOID:
			bb.ensureWrite(pos, OID::kOIDSize);
			pos += OID::kOIDSize;
			break;
		case Symbol:
		case Code:
		case mongo::String:
			{
				size_t len = strlen(pos);
				bb << int(len + 1);
				bb.ensureWrite(pos, len + 1);
			//	log() << "decode: strlen=" << len << ", str=" << pos;
				pos += len + 1;
			}
			break;
		case DBRef:
			{
				size_t len = strlen(pos);
				bb << int(len + 1);
				bb.ensureWrite(pos + 4, len + 1 + OID::kOIDSize);
				pos += len + 1 + OID::kOIDSize;
			}
			break;
		case mongo::Array:
			THROW_STD(invalid_argument, "mongo::Array must not be a index key field");
			break;
		case Object:
			THROW_STD(invalid_argument, "mongo::Object must not be a index key field");
			break;
		case CodeWScope:
			THROW_STD(invalid_argument, "mongo::CodeWScope must not be a index key field");
			break;
		case BinData:
			THROW_STD(invalid_argument, "mongo::BinData could'nt not be a index key field");
			break;
		case RegEx:
			{
				size_t len1 = strlen(pos); // regex len
				size_t len2 = strlen(pos + len1 + 1);
				size_t len3 = len1 + len2 + 2;
				bb.ensureWrite(pos, len3);
				pos += len3;
			}
			break;
		default:
			{
				StringBuilder ss;
				ss << "narkDecodeIndexKey(): BSONElement: bad subkey.type " << (int)colmeta.uType;
				std::string msg = ss.str();
				massert(10320, msg.c_str(), false);
			}
		}
	}
	invariant(data + size == pos);
	bb << char(EOO); // end of object
	DataView((char*)bb.buf() + sizeof(SharedBuffer::Holder))
			.write<LittleEndian<int>>(int(bb.tell() - sizeof(SharedBuffer::Holder)));

	return SharedBuffer::takeOwnership((char*)bb.release());
}

} } // namespace mongo::narkdb

