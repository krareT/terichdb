/* vim: set tabstop=4 : */
#pragma once

#ifdef TERARK_FUNCTION_USE_BOOST
	#include <boost/function.hpp>
#else
	#include <functional>
#endif

namespace terark {

#ifdef TERARK_FUNCTION_USE_BOOST
	using boost::function;
	using boost::ref;
	using boost::cref;
	using boost::reference_wrapper
#else
	using std::function;
	using std::ref;
	using std::cref;
	using std::reference_wrapper;
#endif

} // namespace terark
