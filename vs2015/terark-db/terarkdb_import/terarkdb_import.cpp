#include "stdafx.h"
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>
#include <terark/db/db_table.hpp>
#ifdef _MSC_VER
	#include <getopt.c>
#endif

void usage(const char* prog) {
	fprintf(stderr, "usage: %s options db-dir input-data-files...\n", prog);
}

int main(int argc, char* argv[]) {
	int inputFormat = 't';
	size_t rowsLimit = 10000000;
	terark::hash_strmap<const char*> colformat;
	for (;;) {
		int opt = getopt(argc, argv, "tjL:");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'j':
		case 't':
			inputFormat = opt;
			break;
		case 'L':
			rowsLimit = strtoull(optarg, NULL, 10);
			break;
		}
	}
GetoptDone:
	if (optind + 2 < argc) {
		usage(argv[0]);
		return 1;
	}
	const char* dbdir = argv[optind + 0];
	terark::db::DbTablePtr tab = terark::db::DbTable::open(dbdir);
	terark::db::DbContextPtr ctx = tab->createDbContext();
	ctx->syncIndex = false;
	terark::LineBuf line;
	terark::valvec<unsigned char> row;
	size_t existedRows = tab->numDataRows();
	size_t skippedRows = 0;
	size_t rows = 0;
	size_t lines = 0;
	size_t bytes = 0;
	size_t colnum = tab->rowSchema().columnNum();
	printf("skip existed %zd rows and import %zd rows\n", existedRows, rowsLimit);
	for (int argIdx = optind + 1; argIdx < argc; ++argIdx) {
		const char* fname = argv[argIdx];
		terark::Auto_fclose fp(fopen(fname, "r"));
		if (!fp) {
			fprintf(stderr, "ERROR: fopen(%s, r) = %s\n", fname, strerror(errno));
			continue;
		}
		while (skippedRows < existedRows && line.getline(fp) > 0) {
			skippedRows++;
			if (skippedRows % TERARK_IF_DEBUG(100000, 1000000) == 0) {
				fprintf(stderr, "skipped %zd rows\n", skippedRows);
			}
		}
		fprintf(stderr, "total skipped %zd rows\n", skippedRows);
		while (rows < rowsLimit && line.getline(fp) > 0) {
			bytes += line.size();
			line.chomp();
			size_t parsed = tab->rowSchema().parseDelimText('\t', line, &row);
			if (parsed == colnum) {
				ctx->insertRow(row);
				rows++;
				if (lines % TERARK_IF_DEBUG(10000, 1000000) == 0) {
					printf("lines=%zd rows=%zd bytes=%zd currRow: %s\n",
						lines, rows, bytes,
						tab->rowSchema().toJsonStr(row).c_str());
					if (tab->getWritableSegNum() > 3) {
						printf("Waiting 10 seconds for compact thread catching up...\n");
						std::this_thread::sleep_for(std::chrono::seconds(10));
					}
					else {
						TERARK_IF_DEBUG(std::this_thread::sleep_for(std::chrono::seconds(2)),;);
					}
				}
			}
			lines++;
		}
	}
	printf("waiting for compact thread complete...\n");
	tab->syncFinishWriting();
	terark::db::DbTable::safeStopAndWaitForCompress();
	printf("done!\n");
    return 0;
}

