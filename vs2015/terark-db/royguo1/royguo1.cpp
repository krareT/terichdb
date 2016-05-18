#include <iostream>
#include <terark/lcast.hpp>

#include "user.hpp"

// g++-4.9 -std=c++1y 3.write_read_delete.cpp -lterark-db-r -lboost_system -lterark-fsa_all-r -lboost_filesystem -Iinclude
// Write data into user_table 
int main(int argc, char* argv[]){
  std::cout<<"Hello Terark, We will write data into db"<<std::endl;

  // open table
  static const char* dbtable = "db";
  terark::db::CompositeTablePtr tab = terark::db::CompositeTable::open(dbtable);

  // write data (1000 records)
  terark::NativeDataOutput<terark::AutoGrownMemIO> rowBuilder;
  terark::db::DbContextPtr ctx = tab->createDbContext();

  test_ns::User u = {};

  int count = 1000;
  for(int i = 1; i <= count; i++) {
	char szBuf[256];
    u.id = i;
    u.name.assign(szBuf, snprintf(szBuf, sizeof(szBuf), "TestName-%d", i));
    u.description.assign(szBuf, snprintf(szBuf, sizeof(szBuf), "Description-%d", i));
    u.age = (i + 10);
    u.update_time = 1463472964753 + i;
    // insert row
    rowBuilder.rewind();
    rowBuilder<<u;
    terark::fstring binRow(rowBuilder.begin(), rowBuilder.tell());
    if (ctx->insertRow(binRow) < 0) {
      printf("Insert failed: %s\n", ctx->errMsg.c_str());
    }
  }

  // read data(1000 records)
  terark::valvec<terark::byte> nameVal;     // column value
  terark::valvec<terark::byte> descVal;     // column value
  terark::valvec<terark::byte> nameDescVal;    // column group value
  terark::valvec<terark::llong> idvec;      // id vector of index
  terark::db::ColumnVec nameDescCV;

  size_t indexId = tab->getIndexId("id");

  // get data by column id is faster than by column name
  size_t nameColumnId = tab->getColumnId("name");
  size_t descColumnId = tab->getColumnId("description");

  // get a full column group is faster than separately get every column
  size_t nameDescColgroupId = tab->getColgroupId("name_and_description");
  auto& nameDescColgroupSchema = tab->getColgroupSchema(nameDescColgroupId);

  std::cout<<"name colId = "<<nameColumnId<<", desc colId ="<<descColumnId<<std::endl;

  if (indexId >= tab->getIndexNum()) {
    fprintf(stderr, "ERROR: index 'id' does not exist\n");
    terark::db::CompositeTable::safeStopAndWaitForCompress();
    return 0;
  }

  // iterate ids, read name and description values
  for(int i = 1; i <= count; i++) {
    terark::fstring key = terark::fstring((char*)&i, 4);
    ctx->indexSearchExact(indexId, key, &idvec);
    // print ids
    // std::cout<<tab->getIndexSchema(indexId).toJsonStr(key).c_str()<<std::endl;
    for (auto recId : idvec) {
      // the simplest way
      ctx->selectOneColumn(recId, nameColumnId, &nameVal);
      ctx->selectOneColumn(recId, descColumnId, &descVal);
      printf("--%.*s  ", (int)nameVal.size(), nameVal.data());
      printf("%.*s\n", (int)descVal.size(), descVal.data());

	  // name and description are defined in one column group
	  // using selectOneColgroup is much faster
	  ctx->selectOneColgroup(recId, nameDescColgroupId, &nameDescVal);
	  nameDescColgroupSchema.parseRow(nameDescVal, &nameDescCV);
	  terark::fstring name = nameDescCV[0];
	  terark::fstring desc = nameDescCV[1];
      printf("++%.*s  ", name.ilen(), name.data());
      printf("%.*s\n", desc.ilen(), desc.data());
    }
  }

  // delete data(1000 records)
  for(int i = 1; i <= count; i++) {
    terark::fstring key = terark::fstring((char*)&i, 4);
    ctx->indexSearchExact(indexId, key, &idvec);
    for (auto recId : idvec) {
      ctx->removeRow(recId);
      // std::cout<<"delete row : "<<recId<<std::endl;
    }
  }

  // close table
  terark::db::CompositeTable::safeStopAndWaitForCompress();
  return 0;
}
