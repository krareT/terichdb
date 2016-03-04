#ifndef __nark_parallel_lib_hpp__
#define __nark_parallel_lib_hpp__

#if defined(NARK_ENABLE_PARALLEL) && defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4007
	#include <parallel/algorithm>
	#define nark_parallel_sort __gnu_parallel::sort
#else
	#include <algorithm>
	#define nark_parallel_sort std::sort
#endif

#endif // __nark_parallel_lib_hpp__

