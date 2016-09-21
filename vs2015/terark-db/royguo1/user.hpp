#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>
#include <boost/static_assert.hpp>


namespace test_ns {

  struct User {
    std::uint32_t id;
    std::string name;
    unsigned char age;
    std::string email;
    std::string city;
    std::string street;
    Address addr;

    std::pair<double, double> geopoint;
 // dumpable type does not require sizeof(T)==fixlen, it only requires that
 // dump_size(T)==fixlen, but check for dump_size(T)==fixlen is cumbersome
 // and requires big changes for terark.dataio
 // so we static assert sizeof(T)==fixlen here:
    BOOST_STATIC_ASSERT(sizeof(std::pair<double, double>) == 16);
    BOOST_STATIC_ASSERT((terark::DataIO_is_dump<terark::NativeDataInput<terark::MemIO>, std::pair<double, double> >::value));

    std::string zipcode;
    std::string description;
    std::int32_t update_time;

    DATA_IO_LOAD_SAVE(User,
      &id
      &terark::db::Schema::StrZero(name)
      &age
      &terark::db::Schema::StrZero(email)
      &terark::db::Schema::StrZero(city)
      &terark::db::Schema::StrZero(street)
      &terark::db::Schema::CarBinPack(addr)
      &geopoint
      &terark::db::Schema::StrZero(zipcode)
      &terark::db::Schema::StrZero(description)
      &update_time
    )

    User& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }

    // DbTablePtr use none-const ref is just for ensure application code:
    // var 'tab' must be a 'DbTablePtr', can not be a 'DbTable*'
    static bool checkTableSchema(terark::db::DbTablePtr& tab);

    static terark::db::DbTablePtr
    openTable(const boost::filesystem::path& dbdir) {
      using namespace terark::db;
      DbTablePtr tab = DbTable::open(dbdir);
      if (!checkTableSchema(tab)) {
        THROW_STD(invalid_argument,
          "database schema is inconsistence with compiled c++ code, dbdir: %s",
          dbdir.string().c_str());
      }
      return tab;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 11) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "id") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint32) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "name") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(2);
        const ColumnMeta& colmeta = schema.getColumnMeta(2);
        if (checkColname && colname != "age") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint08) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(3);
        const ColumnMeta& colmeta = schema.getColumnMeta(3);
        if (checkColname && colname != "email") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(4);
        const ColumnMeta& colmeta = schema.getColumnMeta(4);
        if (checkColname && colname != "city") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(5);
        const ColumnMeta& colmeta = schema.getColumnMeta(5);
        if (checkColname && colname != "street") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(6);
        const ColumnMeta& colmeta = schema.getColumnMeta(6);
        if (checkColname && colname != "addr") {
          return false;
        }
        if (colmeta.type != ColumnType::CarBin) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(7);
        const ColumnMeta& colmeta = schema.getColumnMeta(7);
        if (checkColname && colname != "geopoint") {
          return false;
        }
        if (colmeta.type != ColumnType::Fixed) {
          return false;
        }
        if (colmeta.fixedLen != 16) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(8);
        const ColumnMeta& colmeta = schema.getColumnMeta(8);
        if (checkColname && colname != "zipcode") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(9);
        const ColumnMeta& colmeta = schema.getColumnMeta(9);
        if (checkColname && colname != "description") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(10);
        const ColumnMeta& colmeta = schema.getColumnMeta(10);
        if (checkColname && colname != "update_time") {
          return false;
        }
        if (colmeta.type != ColumnType::Sint32) {
          return false;
        }
      }
      return true;
    }
  }; // User

  struct User_Colgroup_id {
    std::uint32_t id;

    DATA_IO_LOAD_SAVE(User_Colgroup_id,
      &id
    )

    User_Colgroup_id& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup_id& select(const User& ___row) {
      id = ___row.id;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.id = id;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 1) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "id") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint32) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup_id
  typedef User_Colgroup_id User_Index_id;


  struct User_Colgroup_update_time {
    std::int32_t update_time;

    DATA_IO_LOAD_SAVE(User_Colgroup_update_time,
      &update_time
    )

    User_Colgroup_update_time& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup_update_time& select(const User& ___row) {
      update_time = ___row.update_time;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.update_time = update_time;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 1) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "update_time") {
          return false;
        }
        if (colmeta.type != ColumnType::Sint32) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup_update_time
  typedef User_Colgroup_update_time User_Index_update_time;


  struct User_Colgroup_city_street {
    std::string city;
    std::string street;

    DATA_IO_LOAD_SAVE(User_Colgroup_city_street,
      &terark::db::Schema::StrZero(city)
      &terark::RestAll(street)
    )

    User_Colgroup_city_street& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup_city_street& select(const User& ___row) {
      city   = ___row.city;
      street = ___row.street;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.city   = city;
      ___row.street = street;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 2) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "city") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "street") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup_city_street
  typedef User_Colgroup_city_street User_Index_city_street;


  struct User_Colgroup_email {
    std::string email;

    DATA_IO_LOAD_SAVE(User_Colgroup_email,
      &terark::RestAll(email)
    )

    User_Colgroup_email& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup_email& select(const User& ___row) {
      email = ___row.email;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.email = email;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 1) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "email") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup_email
  typedef User_Colgroup_email User_Index_email;


  struct User_Colgroup_name_and_description {
    std::string name;
    std::string description;

    DATA_IO_LOAD_SAVE(User_Colgroup_name_and_description,
      &terark::db::Schema::StrZero(name)
      &terark::RestAll(description)
    )

    User_Colgroup_name_and_description& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup_name_and_description& select(const User& ___row) {
      name        = ___row.name;
      description = ___row.description;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.name        = name;
      ___row.description = description;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 2) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "name") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "description") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup_name_and_description

  struct User_Colgroup__RestAll {
    unsigned char age;
    Address addr;

    std::pair<double, double> geopoint;
 // dumpable type does not require sizeof(T)==fixlen, it only requires that
 // dump_size(T)==fixlen, but check for dump_size(T)==fixlen is cumbersome
 // and requires big changes for terark.dataio
 // so we static assert sizeof(T)==fixlen here:
    BOOST_STATIC_ASSERT(sizeof(std::pair<double, double>) == 16);
    BOOST_STATIC_ASSERT((terark::DataIO_is_dump<terark::NativeDataInput<terark::MemIO>, std::pair<double, double> >::value));

    std::string zipcode;

    DATA_IO_LOAD_SAVE(User_Colgroup__RestAll,
      &age
      &terark::db::Schema::CarBinPack(addr)
      &geopoint
      &terark::RestAll(zipcode)
    )

    User_Colgroup__RestAll& decode(terark::fstring ___row) {
      terark::NativeDataInput<terark::MemIO> ___dio(___row.range());
      ___dio >> *this;
      return *this;
    }
    terark::fstring
    encode(terark::NativeDataOutput<terark::AutoGrownMemIO>& ___dio) const {
      ___dio.rewind();
      ___dio << *this;
      return ___dio.written();
    }
    User_Colgroup__RestAll& select(const User& ___row) {
      age      = ___row.age;
      addr     = ___row.addr;
      geopoint = ___row.geopoint;
      zipcode  = ___row.zipcode;
      return *this;
    }
    void assign_to(User& ___row) const {
      ___row.age      = age;
      ___row.addr     = addr;
      ___row.geopoint = geopoint;
      ___row.zipcode  = zipcode;
    }

    static bool
    checkSchema(const terark::db::Schema& schema, bool checkColname = false) {
      using namespace terark;
      using namespace terark::db;
      if (schema.columnNum() != 4) {
        return false;
      }
      {
        const fstring     colname = schema.getColumnName(0);
        const ColumnMeta& colmeta = schema.getColumnMeta(0);
        if (checkColname && colname != "age") {
          return false;
        }
        if (colmeta.type != ColumnType::Uint08) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(1);
        const ColumnMeta& colmeta = schema.getColumnMeta(1);
        if (checkColname && colname != "addr") {
          return false;
        }
        if (colmeta.type != ColumnType::CarBin) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(2);
        const ColumnMeta& colmeta = schema.getColumnMeta(2);
        if (checkColname && colname != "geopoint") {
          return false;
        }
        if (colmeta.type != ColumnType::Fixed) {
          return false;
        }
        if (colmeta.fixedLen != 16) {
          return false;
        }
      }
      {
        const fstring     colname = schema.getColumnName(3);
        const ColumnMeta& colmeta = schema.getColumnMeta(3);
        if (checkColname && colname != "zipcode") {
          return false;
        }
        if (colmeta.type != ColumnType::StrZero) {
          return false;
        }
      }
      return true;
    }
  }; // User_Colgroup__RestAll

  // DbTablePtr use none-const ref is just for ensure application code:
  // var 'tab' must be a 'DbTablePtr', can not be a 'DbTable*'
  bool User::checkTableSchema(terark::db::DbTablePtr& tab) {
    using namespace terark::db;
    assert(tab.get() != nullptr);
    const SchemaConfig& sconf = tab->getSchemaConfig();
    if (!User::checkSchema(*sconf.m_rowSchema)) {
      return false;
    }
    if (!User_Colgroup_id::checkSchema(sconf.getColgroupSchema(0))) {
      return false;
    }
    if (!User_Colgroup_update_time::checkSchema(sconf.getColgroupSchema(1))) {
      return false;
    }
    if (!User_Colgroup_city_street::checkSchema(sconf.getColgroupSchema(2))) {
      return false;
    }
    if (!User_Colgroup_email::checkSchema(sconf.getColgroupSchema(3))) {
      return false;
    }
    if (!User_Colgroup_name_and_description::checkSchema(sconf.getColgroupSchema(4))) {
      return false;
    }
    if (!User_Colgroup__RestAll::checkSchema(sconf.getColgroupSchema(5))) {
      return false;
    }
    return true;
  } // User

} // namespace test_ns
