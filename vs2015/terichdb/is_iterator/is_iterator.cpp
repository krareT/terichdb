// is_iterator.cpp : Defines the entry point for the console application.
//

#include <vector>
#include <terark/valvec.hpp>

struct SSS {
	std::string x, y, z;
};

int main()
{
	SSS s{"x", "y", "z"};
//	printf("%s, %s, %s\n", s.x.c_str(), s.y.c_str(), s.z.c_str());
	terark::valvec<int> a("abc", 3);
	printf("is_iterator<int>::value=%d\n", terark::is_iterator<int>::value);
	printf("is_iterator<int*>::value=%d\n", terark::is_iterator<int*>::value);
	printf("a[0]=%#X\n", a[0]);
	printf("0x%llX\n", static_cast<unsigned long long>(strtod("inf", NULL)));
	printf("0x%llX\n", static_cast<unsigned long long>(strtod("3.5", NULL)));
    return 0;
}

