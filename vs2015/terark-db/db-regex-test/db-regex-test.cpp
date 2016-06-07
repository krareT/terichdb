// db-regex-test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <terark/db/db_table.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/RangeStream.hpp>
#include <terark/lcast.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>
#include <terark/fsa/create_regex_dfa.hpp>

using namespace terark;
using namespace terark::db;

struct TestRow {
	uint64_t    seqNo;
	std::string key;
	std::string val;
	DATA_IO_LOAD_SAVE(TestRow,
		&seqNo
		&Schema::StrZero(key)
		&Schema::StrZero(val)
		)
};

void doTest(terark::fstring tableClass, PathRef tableDir, const char* textKeyFile, const char* queryKeyFile) {
	using namespace terark;
	DbTablePtr tab = DbTable::createTable(tableClass);
	tab->load(tableDir);
	DbContextPtr ctx(tab->createDbContext());
	LineBuf		 line;
	valvec<byte> recBuf;
	if (tab->numDataRows() == 0) {
		NativeDataOutput<AutoGrownMemIO> rowBuilder;
		Auto_fclose fp(fopen(textKeyFile, "r"));
		if (!fp) {
			fprintf(stderr, "FATAL: fopen(%s, r) = %s\n", textKeyFile, strerror(errno));
			exit(1);
		}
		valvec<fstring> F;
		size_t lineno = 0;
		while (line.getline(fp) > 0) {
			lineno++;
			line.chomp();
			line.split('\t', &F, 2);
			rowBuilder.rewind();
			TestRow recRow;
			recRow.key = F[0].str();
			if (F.size() == 2) {
				recRow.val = F[1].str();
			}
			else {
				recRow.val = "";
			}
			recRow.seqNo = lineno;
			rowBuilder << recRow;
			fstring binRow(rowBuilder.begin(), rowBuilder.tell());
			tab->insertRow(binRow, &*ctx);
		}
		tab->syncFinishWriting();
		fprintf(stderr, "insertions  = %zd\n", lineno);
		fprintf(stderr, "numDataRows = %lld\n", tab->numDataRows());
	}
	Auto_fclose fp(fopen(queryKeyFile, "r"));
	size_t lineno = 0;
	valvec<llong> recIdvec;
	profiling pf;
	llong tt = 0;
	ctx->regexMatchMemLimit = 16*1024;
	while (line.getline(fp) > 0) {
		lineno++;
		line.chomp();
		if (line.empty()) {
			continue;
		}
		fstring regex(line);
		std::unique_ptr<BaseDFA> regexDFA(create_regex_dfa(regex, "i"));
		llong t0 = pf.now();
		tab->indexMatchRegex(0, &*regexDFA, &recIdvec, &*ctx);
		tt += pf.now() - t0;
		printf("MatchRegex('%s') get %zd matches\n", regex.p, recIdvec.size());
		for (llong recId : recIdvec) {
			tab->getValue(recId, &recBuf, &*ctx);
			std::string js = tab->toJsonStr(recBuf);
			printf("recId=%lld, RecData: %s\n", recId, js.c_str());
		}
	}
	printf("indexMatchRegex: time = %f milliseconds\n", pf.mf(0,tt));
}

int main(int argc, char* argv[]) {
	if (argc < 3) {
		fprintf(stderr, "usage: %s textKeyFile queryKeyFile\n", argv[0]);
		return 1;
	}
	doTest("DfaDbTable", "dfadb", argv[1], argv[2]);
	DbTable::safeStopAndWaitForCompress();
    return 0;
}

