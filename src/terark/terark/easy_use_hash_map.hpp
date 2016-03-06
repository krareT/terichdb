#ifndef __terark_easy_use_hash_map_hpp__
#define __terark_easy_use_hash_map_hpp__

#include "gold_hash_map.hpp"
#include "hash_strmap.hpp"

namespace terark {

template<class Key, class Value>
class easy_use_hash_map : public gold_hash_map<Key, Value> {};

template<class Value>
class easy_use_hash_map<std::string, Value> : public hash_strmap<Value> {};

} // namespace terark


#endif // __terark_easy_use_hash_map_hpp__

