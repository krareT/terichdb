// terark-db-schema-compile.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <terark/db/db_conf.cpp>

using namespace std;
using namespace terark;
using namespace terark::db;

void compileOneSchema(const Schema& schema, const char* className) {
	const size_t colnum = schema.columnNum();
	printf("  struct %s {\n", className);
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = schema.getColumnMeta(i);
		const fstring     colname = schema.getColumnName(i);
		switch (colmeta.type) {
		case ColumnType::Any:
			assert(0); // not supported now
			break;
		case ColumnType::Nested:
			assert(0); // not supported now
			break;
		case ColumnType::Uint08:
			printf("    unsigned char %s;\n", colname.p);
			break;
		case ColumnType::Sint08:
			printf("    signed char %s;\n", colname.p);
			break;
		case ColumnType::Uint16:
			printf("    std::uint16_t %s;\n", colname.p);
			break;
		case ColumnType::Sint16:
			printf("    std::int16_t %s;\n", colname.p);
			break;
		case ColumnType::Uint32:
			printf("    std::uint32_t %s;\n", colname.p);
			break;
		case ColumnType::Sint32:
			printf("    std::int32_t %s;\n", colname.p);
			break;
		case ColumnType::Uint64:
			printf("    std::uint64_t %s;\n", colname.p);
			break;
		case ColumnType::Sint64:
			printf("    std::int64_t %s;\n", colname.p);
			break;
		case ColumnType::Uint128:
			printf("    unsigned __int128 %s;\n", colname.p);
			break;
		case ColumnType::Sint128:
			printf("    signed __int128 %s;\n", colname.p);
			break;
		case ColumnType::Float32:
			printf("    float %s;\n", colname.p);
			break;
		case ColumnType::Float64:
			printf("    double %s;\n", colname.p);
			break;
		case ColumnType::Float128:
			printf("    __float128 %s;\n", colname.p);
			break;
		case ColumnType::Decimal128:
			printf("    __decimal128 %s;\n", colname.p);
			break;
		case ColumnType::Uuid:
			printf("    terark::db::Schema::Fixed<16> %s;\n", colname.p);
			break;
		case ColumnType::Fixed:
			printf("    terark::db::Schema::Fixed<%d> %s;\n", int(colmeta.fixedLen), colname.p);
			break;
		case ColumnType::VarSint:
			printf("    terark::var_int64_t %s;\n", colname.p);
			break;
		case ColumnType::VarUint:
			printf("    terark::var_uint64_t %s;\n", colname.p);
			break;
		case ColumnType::StrZero:
			printf("    std::string %s;\n", colname.p);
			break;
		case ColumnType::TwoStrZero:
			printf("    terark::db::Schema::TwoStrZero %s;\n", colname.p);
			break;
		case ColumnType::Binary:
			printf("    std::string %s;\n", colname.p);
			break;
		case ColumnType::CarBin:
			printf("    terark::db::Schema::CarBin %s;\n", colname.p);
			break;
		}
	}
	printf("\n");
	printf("    DATA_IO_LOAD_SAVE(%s,\n", className);
	for (size_t i = 0; i < colnum; ++i) {
		const ColumnMeta& colmeta = schema.getColumnMeta(i);
		const fstring     colname = schema.getColumnName(i);
		switch (colmeta.type) {
		case ColumnType::Any:
			assert(0); // not supported now
			break;
		case ColumnType::Nested:
			assert(0); // not supported now
			break;
		case ColumnType::Uint08:
		case ColumnType::Sint08:
		case ColumnType::Uint16:
		case ColumnType::Sint16:
		case ColumnType::Uint32:
		case ColumnType::Sint32:
		case ColumnType::Uint64:
		case ColumnType::Sint64:
		case ColumnType::Uint128:
		case ColumnType::Sint128:
		case ColumnType::Float32:
		case ColumnType::Float64:
		case ColumnType::Float128:
		case ColumnType::Decimal128:
		case ColumnType::Uuid:
		case ColumnType::Fixed:
		case ColumnType::VarSint:
		case ColumnType::VarUint:
			printf("      &%s\n", colname.p);
			break;
		case ColumnType::StrZero:
			if (i < colnum-1) {
				printf("      &terark::db::Schema::StrZero(%s)\n", colname.p);
			} else {
				printf("      &terark::RestAll(%s)\n", colname.p);
			}
			break;
		case ColumnType::TwoStrZero:
		case ColumnType::Binary:
		case ColumnType::CarBin:
			if (i < colnum-1) {
				printf("      &%s\n", colname.p);
			} else {
				printf("      &terark::RestAll(%s)\n", colname.p);
			}
			break;
		}
	}
	printf("    )\n");
	printf(
R"EOS(
    %s& decode(terark::fstring row) {
      terark::NativeDataInput<terark::MemIO> dio(row.range());
      dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& dio) const {
      dio.rewind();
      dio << *this;
      return dio.written();
    }
)EOS", className
	);
	printf("  }; // %s\n\n", className);
}

int main(int argc, char* argv[]) {
	if (argc < 4) {
		fprintf(stderr, "usage: %s terark-db-schema-file namespace table-name\n", argv[0]);
		return 1;
	}
	const char* jsonFilePath = argv[1];
	const char* ns = argv[2];
	const char* tabName = argv[3];
	SchemaConfig sconf;
	try {
		sconf.loadJsonFile(jsonFilePath);
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: loadJsonFile(%s) failed: %s\n", jsonFilePath, ex.what());
		return 1;
	}
	printf(R"EOS(#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>

)EOS");
	printf("namespace %s {\n", ns);
	compileOneSchema(*sconf.m_rowSchema, tabName);
	for (size_t i = 0; i < sconf.m_colgroupSchemaSet->indexNum(); ++i) {
		const Schema& schema = *sconf.m_colgroupSchemaSet->getSchema(i);
		if (schema.columnNum() == 1)
			continue;
		std::string cgName = schema.m_name;
		std::transform(cgName.begin(), cgName.end(), cgName.begin(),
			[](unsigned char ch) -> char {
				if (isalnum(ch) || '_' == ch)
					return ch;
				else
					return '_';
			});
		std::string className = tabName;
		className += "_Colgroup_";
		className += cgName;
		compileOneSchema(schema, className.c_str());
		if (i < sconf.m_indexSchemaSet->indexNum()) {
			printf("  typedef %s %s_Index_%s;\n\n", className.c_str(), tabName, cgName.c_str());
		}
	}
	printf("} // namespace %s\n", ns);
    return 0;
}

