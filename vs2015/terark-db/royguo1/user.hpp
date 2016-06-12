#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>

#include "db_conf.hpp"
#include "stdio.h"

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

    User& decode(terark::fstring row) {
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
  }; // User

  struct User_Colgroup_city_street {
    std::string city;
    std::string street;

    DATA_IO_LOAD_SAVE(User_Colgroup_city_street,
      &terark::db::Schema::StrZero(city)
      &terark::RestAll(street)
    )

    User_Colgroup_city_street& decode(terark::fstring row) {
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
  }; // User_Colgroup_city_street

  typedef User_Colgroup_city_street User_Index_city_street;

  struct User_Colgroup_name_and_description {
    std::string name;
    std::string description;

    DATA_IO_LOAD_SAVE(User_Colgroup_name_and_description,
      &terark::db::Schema::StrZero(name)
      &terark::RestAll(description)
    )

    User_Colgroup_name_and_description& decode(terark::fstring row) {
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

    User_Colgroup__RestAll& decode(terark::fstring row) {
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
  }; // User_Colgroup__RestAll

} // namespace test_ns
