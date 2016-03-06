// BinarySearchPerformance.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <terark/util/profiling.cpp>

int main(int argc, char* argv[]) {
	using namespace terark;
	valvec<size_t> vec(50);
	for (size_t i = 0; i < vec.size(); ++i) {
		vec[i] = rand();
	}
	std::sort(vec.begin(), vec.end());
	profiling pf;
	long long t0 = pf.now();
	size_t loop = 10000000;
	for(size_t i = 0; i < loop; ++i) {
		size_t k = (i << 3 | i >> 29) & INT_MAX;
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
	printf("terark: %f seconds, avgTime = %f'ns, QPS = %f'M\n", pf.sf(t0,t1), pf.nf(t0,t1)/loop, loop/pf.uf(t0,t1));
	printf("std : %f seconds, avgTime = %f'ns, QPS = %f'M\n", pf.sf(t1,t2), pf.nf(t1,t2)/loop, loop/pf.uf(t1,t2));
    return 0;
}

