#include "stdafx.h"
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>
#include <terark/num_to_str.hpp>

struct TestRow {
	int64_t id;
	terark::db::Schema::Fixed<9> fix;
	terark::db::Schema::Fixed<10> fix2;
	std::string str0;
	std::string str1;
	std::string str2;
	std::string str3;
	std::string str4;
	DATA_IO_LOAD_SAVE(TestRow,
		&id
		&fix
		&fix2

		// StrZero would never be serialized as LastColumn/RestAll
		&terark::db::Schema::StrZero(str0)

		&str1
		&str2
		&str3
		&terark::RestAll(str4)
		)
};

using namespace terark::db;

void doTest(const char* tableDir, size_t maxRowNum) {
	using namespace terark;
	CompositeTablePtr tab = CompositeTable::open(tableDir);
	DbContextPtr ctx = tab->createDbContext();

	valvec<llong> recIdvec;
	valvec<byte> recBuf;
	NativeDataOutput<AutoGrownMemIO> rowBuilder;
	TestRow recRow;

	size_t insertedRows = 0;
	febitvec bits(maxRowNum + 1);
	for (size_t i = 0; i < maxRowNum; ++i) {
		TestRow recRow;
		recRow.id = rand() % maxRowNum + 1;
		int len = sprintf(recRow.fix.data, "%06lld", recRow.id);
		recRow.str0 = std::string("s0:") + recRow.fix.data;
		recRow.str1 = std::string("s1:") + recRow.fix.data;
		recRow.str2 = std::string("s2:") + recRow.fix.data;
		recRow.str3 = std::string("s3:") + recRow.fix.data;
		recRow.str4 = std::string("s4:") + recRow.fix.data;
		sprintf(recRow.fix2.data, "F2.%06lld", recRow.id);
		rowBuilder.rewind();
		rowBuilder << recRow;
		fstring binRow(rowBuilder.written());
		if (bits[recRow.id]) {
			if (!tab->indexKeyExists(0, fstring((char*)&recRow.id, 8), &*ctx)) {
				i = i;
			}
			printf("dupkey: %s\n", tab->rowSchema().toJsonStr(binRow).c_str());
			ctx->indexSearchExact(0, fstring((char*)&recRow.id, 8), &recIdvec);
			assert(recIdvec.size() > 0);
		}
		if (600 == i)
			i = i;
		if (3903 == recRow.id)
			i = i;
		llong recId = ctx->insertRow(binRow);
		if (recId < 0) {
			assert(bits.is1(recRow.id));
			printf("Insert failed: %s\n", ctx->errMsg.c_str());
		} else {
			ctx->getValue(recId, &recBuf);
			std::string js1 = tab->toJsonStr(binRow);
			printf("Insert recId = %lld: %s\n", recId, js1.c_str());
			if (binRow != recBuf) {
				std::string js2 = tab->toJsonStr(recBuf);
				printf("Fetch_ recId = %lld: %s\n", recId, js2.c_str());
				assert(0);
			}
			ctx->indexSearchExact(0, fstring((char*)&recRow.id, 8), &recIdvec);
			assert(recIdvec.size() > 0);
			insertedRows++;
			if (bits.is1(recRow.id)) {
				ctx->removeRow(recId);
				llong recId2 = ctx->insertRow(binRow);
				assert(recId2 == recId);
			}
			assert(tab->exists(recId));
			assert(bits.is0(recRow.id));
		}
		bits.set1(recRow.id);

		if (rand() < RAND_MAX*0.3) {
			llong randomRecId = rand() % tab->numDataRows();
			if (22 == randomRecId)
				i = i;
			uint64_t keyId = 0;
			recBuf.erase_all();
			std::string jstr;
			if (tab->exists(randomRecId)) {
				size_t indexId = tab->getIndexId("id");
				assert(indexId < tab->getIndexNum());
				tab->selectOneColumn(randomRecId, indexId, &recBuf, &*ctx);
				keyId = unaligned_load<uint64_t>(recBuf.data());
				if (keyId == 20538)
					keyId = keyId;
				ctx->getValue(randomRecId, &recBuf);
				jstr = tab->toJsonStr(recBuf);
				assert(keyId > 0);
			//	assert(bits.is1(keyId));
			}
			bool isDeleted = false;
			if (rand() < RAND_MAX*0.3) {
				// may remove deleted record
				ctx->removeRow(randomRecId);
				assert(!tab->exists(randomRecId));
				assert(!ctx->indexKeyExists(0, fstring((char*)&keyId, 8)));
				isDeleted = true;
			}
			else if (tab->exists(randomRecId)) {
				ctx->removeRow(randomRecId);
				assert(!tab->exists(randomRecId));
				assert(!ctx->indexKeyExists(0, fstring((char*)&keyId, 8)));
				isDeleted = true;
			}
			if (isDeleted && keyId > 0) {
				printf("delete success: recId = %lld: %s\n"
					, randomRecId, jstr.c_str());
				assert(!tab->exists(randomRecId));
				bits.set0(keyId);
			}
		}

		if (rand() < RAND_MAX*0.3) {
			llong randomRecId = rand() % tab->numDataRows();
			if (tab->exists(randomRecId)) {
				size_t keyId_ColumnId = 0;
				Schema::Fixed<10> fix2;
				tab->selectOneColumn(randomRecId, keyId_ColumnId, &recBuf, &*ctx);
				assert(recBuf.size() == sizeof(llong));
				llong keyId = (llong&)recBuf[0];
				int len = sprintf(fix2.data, "F-%lld", keyId);
				TERARK_RT_assert(len < sizeof(fix2.data), std::out_of_range);
				tab->updateColumn(randomRecId, "fix2", fix2);
			}
		}

		if (double(rand()) / RAND_MAX < 0.001) {
			//tab->compact();
		}
	}

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
		StoreIteratorPtr storeIter = ctx->createTableIterForward();
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
//	doTest("MockDbTable", "db1", maxRowNum);
	doTest("dfadb", maxRowNum);
	CompositeTable::safeStopAndWaitForCompress();
    return 0;
}

