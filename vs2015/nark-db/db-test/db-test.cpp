// db-test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <nark/db/db_table.hpp>
#include <nark/db/mock_db_engine.hpp>
#include <nark/io/MemStream.hpp>
#include <nark/io/DataIO.hpp>
#include <nark/io/RangeStream.hpp>
#include <nark/lcast.hpp>

struct TestRow {
	uint64_t id;
	nark::db::Schema::Fixed<9> fix;
	std::string str0;
	std::string str1;
	std::string str2;
	DATA_IO_LOAD_SAVE(TestRow,
		&id
		&fix

		// StrZero would never be serialized as LastColumn/RestAll
		&nark::db::Schema::StrZero(str0)

		&str1
		&nark::RestAll(str2)
		)
};

int main(int argc, char* argv[]) {
	if (argc < 3) {
		fprintf(stderr, "usage: %s indexCol keys ...\n", argv[0]);
		return 1;
	}
	using namespace nark::db;
	using namespace nark;
	CompositeTablePtr tab(new MockCompositeTable());
	tab->load("db1");
	DbContextPtr ctx(tab->createDbContext());

	NativeDataOutput<AutoGrownMemIO> rowBuilder;
	TestRow recRow;

	size_t maxRowNum = 1000;

	for (size_t i = 0; i < maxRowNum; ++i) {
		TestRow recRow;
		recRow.id = rand() % maxRowNum + 1;
		sprintf(recRow.fix.data, "%06llX", recRow.id + 1);
		recRow.str0 = std::string("s0:") + recRow.fix.data;
		recRow.str1 = std::string("s1:") + recRow.fix.data;
		recRow.str2 = std::string("s2:") + recRow.fix.data;
		rowBuilder.rewind();
		rowBuilder << recRow;
		fstring binRow(rowBuilder.begin(), rowBuilder.tell());
		size_t oldsegments = tab->getSegNum();
		ctx->insertRow(binRow, true);
		if (tab->getSegNum() > oldsegments) {
			tab->compact();
		}
	}

//	NativeDataInput<RangeStream<MemIO> > decoder;
//	NativeDataInput<MemIO> decoder;
//	decoder.set(rowBuilder.head());
//	decoder.setRangeLen(decoder.remain());
//	decoder >> recRow;

	{
		valvec<byte> keyHit, val;
		valvec<llong> idvec;
		for (size_t indexId = 0; indexId < tab->getIndexNum(); ++indexId) {
			IndexIteratorPtr indexIter = tab->createIndexIter(indexId);
			const Schema& indexSchema = tab->getIndexSchema(indexId);
			std::string indexName = indexSchema.joinColumnNames();
			std::string keyData;
			for (size_t i = 0; i < maxRowNum/10; ++i) {
				llong keyInt = rand() % (maxRowNum * 11 / 10);
				char keyBuf[64];
				switch (indexId) {
				default:
					assert(0);
					break;
				case 0:
					keyData.assign((char*)&keyInt, 8);
					break;
				case 1: // str0
					sprintf(keyBuf, "s0:%06llX", keyInt);
					keyData = keyBuf;
					break;
				case 2: // str1
					sprintf(keyBuf, "s1:%06llX", keyInt);
					keyData = keyBuf;
					break;
				case 3: // str2
					sprintf(keyBuf, "s2:%06llX", keyInt);
					keyData = keyBuf;
					break;
				case 4: // fix
					assert(indexSchema.getFixedRowLen() > 0);
					keyData.resize(0);
					keyData.resize(indexSchema.getFixedRowLen());
					sprintf(&keyData[0], "%06llX", keyInt);
					break;
				case 5: // str0,str1
					sprintf(keyBuf, "s0:%06llX", keyInt);
					keyData = keyBuf;
					keyData.push_back('\0');
					sprintf(keyBuf, "s0:%06llX", keyInt);
					keyData.append(keyBuf);
					break;
				}
				idvec.resize(0);
				std::string keyJson = indexSchema.toJsonStr(keyData);
				printf("find index key = %s\n", keyJson.c_str());
				llong recId;
				if (indexIter->seekLowerBound(keyData)) {
					while (indexIter->increment(&recId, &keyHit)) {
						assert(recId < tab->numDataRows());
						if (fstring(keyHit) != keyData)
							break;
						idvec.push_back(recId);
					}
				}
				printf("found %zd entries\n", idvec.size());
				for (size_t i = 0; i < idvec.size(); ++i) {
					recId = idvec[i];
					ctx->getValue(recId, &val);
					printf("%8lld  | %s\n", recId, tab->toJsonStr(val).c_str());
				}
			}
		}
	}

	{
		printf("test iterate table ...\n");
		StoreIteratorPtr storeIter = ctx->createTableIter();
		llong recId;
		valvec<byte> val;
		while (storeIter->increment(&recId, &val)) {
			printf("%8lld  | %s\n", recId, tab->toJsonStr(val).c_str());
		}
		printf("test iterate table passed\n");
	}
    return 0;
}

