#pragma once
#ifndef __nark_db_json_hpp__
#define __nark_db_json_hpp__

#include <nlohmann/json.hpp>
#include <nark/gold_hash_map.hpp>

namespace nark {
	template<class Key, class Val, class IgnoreLess, class IgnoreAlloc>
	class JsonStrMap : public gold_hash_map<Key, Val
		, std::hash<Key>
		, std::equal_to<Key>
		, node_layout<std::pair<Key, Val>, unsigned, SafeCopy, ValueOut>
		>
	{
	public:
		typedef IgnoreAlloc allocator_type;

		template<class Iter>
		JsonStrMap(Iter first, Iter last) {
			for (Iter iter = first; iter != last; ++iter) {
				this->insert_i(iter->first, iter->second);
			}
		}
		JsonStrMap() {}
	};
	typedef nlohmann::basic_json<JsonStrMap> json;
//	using nlohmann::_json;
}

#endif // __nark_db_json_hpp__
