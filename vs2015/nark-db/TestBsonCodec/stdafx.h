// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#define _CRT_SECURE_NO_WARNINGS

#if defined(_WIN32) || defined(_WIN64)
	#include "targetver.h"
	#include <tchar.h>
#endif

#include <stdio.h>
#include <nark/gold_hash_map.hpp>
#include <nark/hash_strmap.hpp>
#include <nark/util/linebuf.hpp>
#include <nark/util/autoclose.hpp>


// TODO: reference additional headers your program requires here
