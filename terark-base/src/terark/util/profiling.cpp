#include "../config.hpp"
#include "profiling.hpp"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#if defined(_MSC_VER)
#  define NOMINMAX
#  define WIN32_LEAN_AND_MEAN
#  include <Windows.h>
#else
#  include <time.h>
#  include <sys/time.h>
#endif

namespace terark {

	profiling::profiling()
	{
#if defined(_MSC_VER)
		LARGE_INTEGER li;
		QueryPerformanceFrequency(&li);
		m_freq = li.QuadPart;
#endif
	}

	long long profiling::now() const
	{
#if defined(_MSC_VER)
		LARGE_INTEGER li;
		QueryPerformanceCounter(&li);
		return li.QuadPart;
#elif defined(CLOCK_MONOTONIC_RAW) || \
	  defined(CLOCK_MONOTONIC) || \
	  defined(CLOCK_THREAD_CPUTIME_ID) || \
	  defined(CLOCK_PROCESS_CPUTIME_ID)  || \
	  defined(CLOCK_REALTIME)
		struct timespec ts;
    #define USE_CLOCK(clock) int ret = clock_gettime(clock, &ts)
	#if 0
	#elif defined(CLOCK_MONOTONIC_RAW)
		USE_CLOCK(CLOCK_MONOTONIC_RAW);
	#elif defined(CLOCK_MONOTONIC)
		USE_CLOCK(CLOCK_MONOTONIC);
	#elif defined(CLOCK_THREAD_CPUTIME_ID)
		USE_CLOCK(CLOCK_THREAD_CPUTIME_ID);
	#elif defined(CLOCK_PROCESS_CPUTIME_ID)
		USE_CLOCK(CLOCK_PROCESS_CPUTIME_ID);
	#else
		USE_CLOCK(CLOCK_REALTIME);
	#endif
		if (ret != 0) {
			perror("profiling::now.clock_gettime");
			abort();
		}
		return (long long)ts.tv_sec * 1000000000 + ts.tv_nsec;
#else
		struct timeval tv;
		int ret = gettimeofday(&tv, NULL);
		if (ret != 0) {
			perror("profiling::now.gettimeofday");
			abort();
		}
		return (long long)tv.tv_sec * 1000000000 + tv.tv_usec * 1000;
#endif
	}

} // namespace terark


