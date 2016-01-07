#include "stdafx.h"
#include <nark/util/autoclose.hpp>
#include <nark/util/linebuf.hpp>
#include <nark/util/profiling.hpp>
#include <nark/db/db_table.hpp>
#include <getopt.h>
#include <tbb/task_scheduler_init.h>

void usage(const char* prog) {
	fprintf(stderr, "usage: %s options db-dir input-data-files...\n", prog);
}

static bool g_run = true;
void compactThreadProc(nark::db::CompositeTable* tab) {
	size_t oldsegs = tab->getSegNum();
	nark::profiling pf;
	while (g_run) {
		size_t newsegs = tab->getSegNum();
		if (newsegs != oldsegs) {
			printf("compact: oldsegs=%zd newsegs=%zd\n", oldsegs, newsegs);
			long long t0 = pf.now();
			tab->compact(); // may be take very long time
			long long t1 = pf.now();
			printf("compact: oldsegs=%zd newsegs=%zd time=%f's\n", oldsegs, newsegs, pf.sf(t0,t1));
			oldsegs = newsegs;
		}
		else {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	}
}

int main(int argc, char* argv[]) {
	int inputFormat = 't';
	int compressionThreadsNum = 1;
	size_t rowsLimit = 10000000;
	for (;;) {
		int opt = getopt(argc, argv, "tjL:T:");
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
		case 'T':
			compressionThreadsNum = std::min(atoi(optarg), 8);
			break;
		}
	}
GetoptDone:
	if (optind + 2 < argc) {
		usage(argv[0]);
		return 1;
	}
	const char* dbdir = argv[optind + 0];
	nark::db::CompositeTable::setCompressionThreadsNum(compressionThreadsNum);
	nark::db::CompositeTablePtr tab(nark::db::CompositeTable::createTable("DfaDbTable"));
	nark::db::DbContextPtr ctx = tab->createDbContext();
	tab->load(dbdir);
	ctx->syncIndex = false;
	nark::LineBuf line;
	nark::valvec<unsigned char> row;
	size_t existedRows = tab->numDataRows();
	size_t skippedRows = 0;
	size_t rows = 0;
	size_t lines = 0;
	size_t bytes = 0;
	size_t colnum = tab->rowSchema().columnNum();
//	std::thread thr(std::bind(&compactThreadProc, &tab));
	for (int argIdx = optind + 1; argIdx < argc; ++argIdx) {
		const char* fname = argv[argIdx];
		nark::Auto_fclose fp(fopen(fname, "r"));
		if (!fp) {
			fprintf(stderr, "ERROR: fopen(%s, r) = %s\n", fname, strerror(errno));
			continue;
		}
		while (skippedRows < existedRows && line.getline(fp) > 0) {
			skippedRows++;
			if (skippedRows % FEBIRD_IF_DEBUG(100000, 1000000) == 0) {
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
				if (lines % FEBIRD_IF_DEBUG(100000, 1000000) == 0) {
					printf("lines=%zd rows=%zd bytes=%zd currRow: %s\n",
						lines, rows, bytes,
						tab->rowSchema().toJsonStr(row).c_str());
					if (tab->getWritableSegNum() > 3) {
						printf("Waiting 10 seconds for compact thread catching up...\n");
						std::this_thread::sleep_for(std::chrono::seconds(10));
					}
				}
			}
			lines++;
		}
	}
	printf("waiting for compact thread complete...\n");
	nark::db::CompositeTable::safeStopAndWait();
//	thr.join();
//	tbb::task::wait_for_all();
	printf("done!\n");
    return 0;
}

