#include "index_key.h"
#include <mongo/bson/bsonobjbuilder.h>

namespace mongo {

template<class OutputVec, class T>
void reverseAppend(OutputVec& output, const T* input, size_t ilen) {
	for (size_t k = ilen; k; )
		output.push_back(input[--k]);
}

NarkBsonBlob narkEncodeIndexKey(const BSONObj& key) {
	NarkBsonBlob data;
	narkEncodeIndexKey(key, &data);
	return data;
}

// 1. type: mongo::MaxKey is 127, mongo::MinKey is -1
//    encoded type value = realtype + 1
//    then type can be byte-wise compared
//
// 2. use BigEndian Offset_binary to represent signed integer values
//    then two integer can be compared by byte-wise-lexical-compare
//    such as memcmp(&x, &y, sizeof(x))
//    wiki: https://en.wikipedia.org/wiki/Offset_binary

void narkEncodeIndexKey(const BSONObj& key, NarkBsonBlob* encoded) {
	encoded->resize(0);
	BSONForEach(subkey, key) {
		encoded->push_back(int(subkey.type()) + 1);
		const char* value = subkey.value();
		switch (subkey.type()) {
		case EOO:
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			break;
		case mongo::Bool:
			encoded->push_back(value[0] ? 1 : 0);
			break;
		case NumberInt:
			reverseAppend(*encoded, value, 4);
			encoded->end()[-4] ^= 0x80; // reverse sign bit
			break;
		case bsonTimestamp: // low 32 bit is always positive
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
			reverseAppend(*encoded, value, 8);
			encoded->end()[-8] ^= 0x80; // reverse sign bit
			break;
		case jstOID:
			encoded->append(value, OID::kOIDSize);
			break;
		case Symbol:
		case Code:
		case mongo::String:
			encoded->append(value + 4, subkey.valuestrsize());
			break;
		case DBRef:
			encoded->append(value + 4, subkey.valuestrsize() + OID::kOIDSize);
			break;
		case CodeWScope:
		case Object:
		case mongo::Array:
			invariant(!"narkEncodeIndexKey(): CodeWScope|Object|Array can not be used as key");
			break;
		case BinData:
		//	valueLen = subkey.valuestrsize() + 4 + 1 /*subtype*/;
		//	can not be BinData, because BinDataSize is required and it can't be lex compared
		//  as binary data
			invariant(!"narkEncodeIndexKey(): BinData can not be used as key");
			break;
		case RegEx:
			{
				const char* p = value;
				size_t len1 = strlen(p); // regex len
				p = p + len1 + 1;
				size_t len2;
				len2 = strlen(p);
				encoded->append(p, len1 + 1 + len2 + 1);
			}
			break;
		default:
			{
				StringBuilder ss;
				ss << "narkEncodeIndexKey(): BSONElement: bad subkey.type " << (int)subkey.type();
				std::string msg = ss.str();
				massert(10320, msg.c_str(), false);
			}
		}
	}
}

BSONObj narkDecodeIndexKey(StringData encoded, const BSONObj& fieldnames) {
	BSONObjBuilder bb;
	const char* pos = encoded.begin();
	const char* end = encoded.end();
	bool isNamesEmpty = fieldnames.isEmpty();
	auto iter = fieldnames.begin();
	while (pos < end) {
		// MaxKey is 127, so MaxKey+1 is 128, +128 exceeds (signed char) range
		// (unsigned char)(-128) == (unsigned char)(+128)
		const int type = (unsigned char)(*pos++) - 1;
		bb.bb().appendChar((char)type);
		if (isNamesEmpty) {
			bb.bb().appendStr(StringData());
		} else {
			bb.bb().appendStr((*iter).fieldName());
			++iter;
		}
		switch (type) {
		case EOO:
		case Undefined:
		case jstNULL:
		case MaxKey:
		case MinKey:
			break;
		case mongo::Bool:
			bb.bb().appendNum(char(*pos ? 1 : 0));
			pos++;
			break;
		case NumberInt:
			bb.bb().appendNum(int(ConstDataView(pos).read<BigEndian<int>>() ^ (1<<31)));
			pos += 4;
			break;
		case bsonTimestamp:
		case mongo::Date:
		case NumberDouble:
		case NumberLong:
			bb.bb().appendNum(ConstDataView(pos).read<BigEndian<long long>>() ^ (1LL<<63));
			pos += 8;
			break;
		case jstOID:
			bb.bb().appendBuf(pos, OID::kOIDSize);
			pos += OID::kOIDSize;
			break;
		case Symbol:
		case Code:
		case mongo::String:
			{
				size_t len = strlen(pos);
				bb.bb().appendNum(int(len + 1));
				bb.bb().appendBuf(pos, len + 1);
				pos += 4 + len + 1;
			}
			break;
		case DBRef:
			{
				size_t len = strlen(pos);
				bb.bb().appendNum(int(len + 1));
				bb.bb().appendBuf(pos + 4, len + 1 + OID::kOIDSize);
				pos += 4 + len + 1 + OID::kOIDSize;
			}
			break;
		case CodeWScope:
		case Object:
		case mongo::Array:
			invariant(!"narkDecodeIndexKey(): CodeWScope|Object|Array can not be used as key");
			break;
		case BinData:
			invariant(!"narkDecodeIndexKey(): BinData can not be used as key");
			break;
		case RegEx:
			{
				size_t len1 = strlen(pos); // regex len
				size_t len2 = strlen(pos + len1 + 1);
				size_t len3 = len1 + len2 + 2;
				bb.bb().appendBuf(pos, len3);
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
	return bb.obj();
}

} // namespace
