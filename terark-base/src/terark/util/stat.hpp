#pragma once

// for best compatibility, this file should be the last include

#include <sys/types.h>
#include <sys/stat.h>

#ifndef S_ISDIR
#define S_ISDIR(mode)  (((mode) & S_IFMT) == S_IFDIR)
#endif
#ifndef S_ISREG
#define S_ISREG(mode)  (((mode) & S_IFMT) == S_IFREG)
#endif

#ifdef _MSC_VER
	#ifndef _CRT_NONSTDC_NO_DEPRECATE
		#error _CRT_NONSTDC_NO_DEPRECATE must be defined to use posix functions on Visual C++
	#endif
	// VC does not forward stat/fstat to stat64/fstat64
	// VC stat on large file will fail
	#define ll_stat _stat64
	#define ll_fstat _fstat64
	#define ll_lseek _lseeki64
#else
	#define ll_stat  stat
	#define ll_fstat fstat
	#define ll_lseek lseek
#endif

