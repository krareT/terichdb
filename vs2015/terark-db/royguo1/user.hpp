#pragma once
#include <terark/db/db_table.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/RangeStream.hpp>

namespace test_ns {
  struct User {
    std::uint32_t id;
    std::string name;
    signed char age;
    std::string description;
    std::int32_t update_time;

    DATA_IO_LOAD_SAVE(User,
      &id
      &terark::db::Schema::StrZero(name)
      &age
      &terark::db::Schema::StrZero(description)
      &update_time
    )
  }; // User

  struct User_Colgroup_id_name {
    std::uint32_t id;
    std::string name;

    DATA_IO_LOAD_SAVE(User_Colgroup_id_name,
      &id
      &terark::db::Schema::StrZero(name)
    )
  }; // User_Colgroup_id_name

} // namespace test_ns
