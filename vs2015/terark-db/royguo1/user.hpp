#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>

namespace test_ns {
  struct User {
    std::uint32_t id;
    std::string name;
    unsigned char age;
    std::string email;
    std::string city;
    std::string street;
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
      &terark::db::Schema::StrZero(zipcode)
      &terark::db::Schema::StrZero(description)
      &update_time
    )
  }; // User

  struct User_Colgroup_city_street {
    std::string city;
    std::string street;

    DATA_IO_LOAD_SAVE(User_Colgroup_city_street,
      &terark::db::Schema::StrZero(city)
      &terark::db::Schema::StrZero(street)
    )
  }; // User_Colgroup_city_street

  typedef User_Colgroup_city_street User_Index_city_street;

  struct User_Colgroup_name_and_description {
    std::string name;
    std::string description;

    DATA_IO_LOAD_SAVE(User_Colgroup_name_and_description,
      &terark::db::Schema::StrZero(name)
      &terark::db::Schema::StrZero(description)
    )
  }; // User_Colgroup_name_and_description

  struct User_Colgroup__RestAll {
    unsigned char age;
    std::string zipcode;

    DATA_IO_LOAD_SAVE(User_Colgroup__RestAll,
      &age
      &terark::db::Schema::StrZero(zipcode)
    )
  }; // User_Colgroup__RestAll

} // namespace test_ns
