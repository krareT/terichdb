#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/db/db_table.hpp>
#include <terark/util/profiling.hpp>
#include <mongo_terarkdb/record_codec.h>
#include <mongo/bson/bsonobj.h>

#ifdef _MSC_VER
	#include <getopt.c>
#endif

void usage(const char* prog) {
	fprintf(stderr, "usage: %s options dbDir startId count...\n", prog);
}

int main(int argc, char* argv[]) {
	for (;;) {
		int opt = getopt(argc, argv, "tj");
		switch (opt) {
		case -1:
			goto GetoptDone;
		case 'j':
			break;
		}
	}
GetoptDone:
	if (optind + 3 < argc) {
		usage(argv[0]);
		return 1;
	}
	using namespace terark;
	mongo::terarkdb::SchemaRecordCoder coder;
	const char* dbdir = argv[optind + 0];
	llong startId = strtoll(argv[optind + 1], NULL, 10);
	llong cnt = strtoll(argv[optind + 2], NULL, 10);
	terark::db::DbTablePtr tab = terark::db::DbTable::open(dbdir);
	terark::db::DbContextPtr ctx = tab->createDbContext();
	terark::valvec<unsigned char> row;
	size_t colnum = tab->rowSchema().columnNum();
//	auto tabIndex = tab->createIndexIterForward("_id");
	for (llong i = 0; i < cnt; ++i) {
		tab->getValue(i, &row, ctx.get());
		printf("rowId=%lld: %s\n", i, tab->rowSchema().toJsonStr(row).c_str());
		mongo::BSONObj bson(coder.decode(&tab->rowSchema(), row));
		mongo::BSONObjIterator iter(bson);
		while (1) {
			auto elem = iter.next(true);
			if (elem.eoo()) {
				break;
			}
			auto fieldname = elem.fieldName();
			if (elem.type() == mongo::String) {
				auto valuelen = elem.valuestrsize();
				auto valuestr = elem.valuestr();
				assert(valuelen > 0);
				printf("fieldname: %s, value: \"%s\"\n", fieldname, valuestr);
			}
			else
				printf("fieldname: %s, type: %d\n", fieldname, elem.type());
		}
		bool isValid = bson.valid();
		printf("bson.valid()=%d, dump=%s\n", isValid, bson.toString().c_str());
	}

	printf("done!\n");
    return 0;
}

