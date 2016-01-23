// db-test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <nark/db/db_table.hpp>
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

using namespace nark::db;

void doTest(nark::fstring tableClass, PathRef tableDir, size_t maxRowNum) {
	using namespace nark;
	CompositeTablePtr tab = CompositeTable::createTable(tableClass);
	tab->load(tableDir);
	DbContextPtr ctx(tab->createDbContext());

	NativeDataOutput<AutoGrownMemIO> rowBuilder;
	TestRow recRow;

	size_t insertedRows = 0;
	febitvec bits(maxRowNum + 1);
	for (size_t i = 0; i < maxRowNum; ++i) {
		TestRow recRow;
		recRow.id = rand() % maxRowNum + 1;
		sprintf(recRow.fix.data, "%06lld", recRow.id);
		recRow.str0 = std::string("s0:") + recRow.fix.data;
		recRow.str1 = std::string("s1:") + recRow.fix.data;
		recRow.str2 = std::string("s2:") + recRow.fix.data;
		rowBuilder.rewind();
		rowBuilder << recRow;
		fstring binRow(rowBuilder.begin(), rowBuilder.tell());
		if (bits[recRow.id]) {
			printf("dupkey: %s\n", tab->rowSchema().toJsonStr(binRow).c_str());
		}
		if (18 == i || 0x3d == i)
			i = i;
		if (ctx->insertRow(binRow) < 0) {
		//	assert(bits.is1(recRow.id));
			printf("Insert failed: %s\n", ctx->errMsg.c_str());
		} else {
			insertedRows++;
			assert(bits.is0(recRow.id));
		}
		bits.set1(recRow.id);
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
			IndexIteratorPtr indexIter = tab->createIndexIterForward(indexId);
			const Schema& indexSchema = tab->getIndexSchema(indexId);
			std::string keyData;
			for (size_t i = 0; i < maxRowNum/5; ++i) {
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
					sprintf(keyBuf, "s0:%06lld", keyInt);
					keyData = keyBuf;
					break;
				case 2: // str1
					sprintf(keyBuf, "s1:%06lld", keyInt);
					keyData = keyBuf;
					break;
				case 3: // str2
					sprintf(keyBuf, "s2:%06lld", keyInt);
					keyData = keyBuf;
					break;
				case 4: // fix
					assert(indexSchema.getFixedRowLen() > 0);
					keyData.resize(0);
					keyData.resize(indexSchema.getFixedRowLen());
					sprintf(&keyData[0], "%06lld", keyInt);
					break;
				case 5: // str0,str1
					sprintf(keyBuf, "s0:%06lld", keyInt);
					keyData = keyBuf;
					keyData.push_back('\0');
					sprintf(keyBuf, "s0:%06lld", keyInt);
					keyData.append(keyBuf);
					break;
				}
				idvec.resize(0);
				std::string keyJson = indexSchema.toJsonStr(keyData);
				printf("find index key = %s", keyJson.c_str());
				fflush(stdout);
				llong recId;
				if (i == 0x002d && indexId == 1)
					i = i;
				int ret = indexIter->seekLowerBound(keyData, &recId, &keyHit);
				if (ret > 0) {
					printf(", found upper_bound key=%s, recId=%lld\n",
						indexSchema.toJsonStr(keyHit).c_str(), recId);
				//	printf(", found hitkey > key, show first upper_bound:\n");
				//	idvec.push_back(recId);
				}
				else if (ret < 0) { // all keys are less than search key
					printf(", all keys are less than search key\n");
				}
				else if (ret == 0) { // found exact key
					idvec.push_back(recId);
					int hasNext; // int as bool
					while ((hasNext = indexIter->increment(&recId, &keyHit))
							&& fstring(keyHit) == keyData) {
						assert(recId < tab->numDataRows());
						idvec.push_back(recId);
					}
					if (hasNext)
						idvec.push_back(recId);
					printf(", found %zd exact and %d upper_bound\n",
						idvec.size()-hasNext, hasNext);
				}
				for (size_t i = 0; i < idvec.size(); ++i) {
					recId = idvec[i];
					ctx->getValue(recId, &val);
					printf("%8lld  | %s\n", recId, tab->toJsonStr(val).c_str());
				}
			}
		}
	}

	{
		printf("test iterate table, numDataRows=%lld ...\n", tab->numDataRows());
		StoreIteratorPtr storeIter = ctx->createTableIter();
		llong recId;
		valvec<byte> val;
		llong iterRows = 0;
		while (storeIter->increment(&recId, &val)) {
			printf("%8lld  | %s\n", recId, tab->toJsonStr(val).c_str());
			++iterRows;
		}
		printf("test iterate table passed, iterRows=%lld, insertedRows=%lld\n",
			iterRows, insertedRows);
	}

	// last writable segment will put to compressing queue
	tab->syncFinishWriting();
}

int main(int argc, char* argv[]) {
	if (argc < 2) {
		fprintf(stderr, "usage: %s maxRowNum\n", argv[0]);
		return 1;
	}
	size_t maxRowNum = (size_t)strtoull(argv[1], NULL, 10);
//	doTest("MockCompositeTable", "db1", maxRowNum);
	doTest("DfaDbTable", "dfadb", maxRowNum);
	CompositeTable::safeStopAndWaitForCompress();
    return 0;
}

