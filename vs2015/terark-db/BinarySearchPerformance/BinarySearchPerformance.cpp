// BinarySearchPerformance.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.cpp>
#include <boost/current_function.hpp>

int main(int argc, char* argv[]) {
	using namespace terark;
	printf("sizeof(long double) = %zd\n", sizeof(long double));
	valvec<size_t> vec(50);
	valvec<size_t> vec2 = vec;
	valvec<size_t> vec3(std::pair<size_t*, size_t*>(vec.begin(), vec.end()));
	valvec<size_t> vec4(vec.range());
//	valvec<size_t> vec5({ vec.begin(), vec.end() });
	for (size_t i = 0; i < vec.size(); ++i) {
		vec[i] = rand();
	}
	std::sort(vec.begin(), vec.end());
	profiling pf;
	long long t0 = pf.now();
	size_t loop = TERARK_IF_DEBUG(1, 100) * 1000LL * 1000;
	for(size_t i = 0; i < loop; ++i) {
		size_t k = (i << 3 | i >> 29) * 12289 & INT_MAX;
	//	size_t k = rand(); // rand is too slow
		size_t upp = upper_bound_a(vec, k);
		if (upp > vec.size()) {
			abort(); // ensure compiler really do upper_bound_a
		}
	}
	long long t1 = pf.now();
	for(size_t i = 0; i < loop; ++i) {
		size_t k = (i << 3 | i >> 29) & INT_MAX;
		size_t upp = std::upper_bound(vec.begin(), vec.end(), k) - vec.begin();
		if (upp > vec.size()) {
			abort(); // ensure compiler really do upper_bound_a
		}
	}
	long long t2 = pf.now();
	printf("loop = %zd\n", loop);
	printf("terark: %f seconds, avgTime = %f'ns, QPS = %f'M\n", pf.sf(t0,t1), pf.nf(t0,t1)/loop, loop/pf.uf(t0,t1));
	printf("std : %f seconds, avgTime = %f'ns, QPS = %f'M\n", pf.sf(t1,t2), pf.nf(t1,t2)/loop, loop/pf.uf(t1,t2));
    return 0;
}

