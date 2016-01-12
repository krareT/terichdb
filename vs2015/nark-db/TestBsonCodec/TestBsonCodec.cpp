// TestBsonCodec.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <mongo_narkdb/record_codec.h>
#include <nark/db/db_table.hpp>

int main(int argc, char* argv[])
{
	if (argc < 3) {
		fprintf(stderr, "usage: %s schema-file bson-file\n", argv[0]);
		return 1;
	}
	using namespace nark;
	using namespace mongo::narkdb;
	nark::LineBuf filebuf;
	const char* schemaFile = argv[1];
	const char* bsonFile   = argv[2];
	nark::Auto_fclose fp(fopen(bsonFile, "rb"));
	if (!fp) {
		fprintf(stderr, "fopen(%s, rb) = %s\n", bsonFile, strerror(errno));
		return 2;
	}
	valvec<char> recBuf;
	nark::db::SchemaConfig schema;
	schema.loadJsonFile(schemaFile);
	mongo::narkdb::SchemaRecordCoder coder;
	mongo::narkdb::SchemaRecordCoder::FieldsMap fields2;
	filebuf.read_all(fp);
	printf("file.len=%zd\n", filebuf.size());
	bool hasFreedomFields = schema.m_rowSchema->m_columnsMeta.end_key(1) == "$$";
	if (!hasFreedomFields) {
		assert(!schema.m_rowSchema->m_columnsMeta.exists("$$"));
	}
	size_t num = 0;
	for (const char* pos = filebuf.begin(); pos < filebuf.end(); ) {
		mongo::BSONObj bson1(pos);
		printf("bson1=%s\n", bson1.toString().c_str());
		coder.encode(&*schema.m_rowSchema, nullptr, bson1, &recBuf);
		printf("encode=%s\n", schema.m_rowSchema->toJsonStr(recBuf).c_str());
		mongo::BSONObj bson2(coder.decode(&*schema.m_rowSchema, recBuf));
		coder.parseToFields(bson2, &fields2);
		printf("bson2=%s\n", bson2.toString().c_str());
		assert(coder.fieldsEqual(coder.m_fields, fields2));
		if (!hasFreedomFields) {
			encodeIndexKey(*schema.m_rowSchema, bson1, &recBuf);
			mongo::BSONObj bson3(decodeIndexKey(*schema.m_rowSchema, recBuf));
			printf("bson3=%s\n", bson3.toString().c_str());
			assert(bson1 == bson3);
		}
		pos += bson1.objsize();
		num++;
	}
	printf("num=%zd\n", num);
    return 0;
}

