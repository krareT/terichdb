// TestJson.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <terark/db/json.hpp>
#include <terark/lcast.hpp>
#include <boost/filesystem.hpp>


int main()
{
	boost::filesystem::path path("a/b/c/test.txt");
	printf("path.parent: %s\n", path.parent_path().string().c_str());
	printf("path.stem: %s\n", path.stem().string().c_str());

//	using nlohmann::json;
//	using nlohmann::_json;
//	using namespace nlohmann;
	using terark::json;
	std::string str = "{ ";
	for (int i = 0; i < 20; ++i) {
		char buf[64];
		sprintf(buf, "\"%08d\" : %d,", 20 - i, i);
		str.append(buf);
	}
	str.back() = '}';
	json j = json::parse(str);
	for (auto i = j.begin(); i != j.end(); ++i) {
		int val = i.value();
		printf("%s = %d\n", i.key().c_str(), val);
	}
	j["+inf"] = DBL_MAX + DBL_MAX;
	j["-inf"] = -(DBL_MAX + DBL_MAX);
	j["max"] = DBL_MAX;
	printf("%s\n", j.dump().c_str());
	printf("+inf=%+f\n", DBL_MAX + DBL_MAX);
	printf("-inf=%+f\n", -(DBL_MAX + DBL_MAX));
    return 0;
}

