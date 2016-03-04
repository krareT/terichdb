#ifndef __nark_profiling_hpp__
#define __nark_profiling_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4127)
#endif

#include "../config.hpp"

namespace nark {

	class NARK_DLL_EXPORT profiling
	{
#if defined(_MSC_VER)
		long long m_freq;
#endif

	public:
		profiling();

		long long now() const;

#if defined(_MSC_VER)
		long long ns(long long x) const	{ return x * 1000 / (m_freq / 1000000); }
		long long us(long long x) const	{ return x * 1000 / (m_freq / 1000); }
		long long ms(long long x) const	{ return x * 1000 / (m_freq); }

		long long ns(long long x, long long y) const { return (y-x) * 1000 / (m_freq / 1000000); }
		long long us(long long x, long long y) const { return (y-x) * 1000 / (m_freq / 1000); }
		long long ms(long long x, long long y) const { return (y-x) * 1000 / (m_freq); }

		double nf(long long x) const	{ return x*1e9/m_freq; }
		double uf(long long x) const	{ return x*1e6/m_freq; }
		double mf(long long x) const	{ return x*1e3/m_freq; }

		double nf(long long x, long long y) const { return (y-x)*1e9 / m_freq; }
		double uf(long long x, long long y) const { return (y-x)*1e6 / m_freq; }
		double mf(long long x, long long y) const { return (y-x)*1e3 / m_freq; }

		double sf(long long x, long long y) const { return double(y-x) / m_freq; }
#else
		long long ns(long long x) const { return x; }
		long long us(long long x) const	{ return x / 1000; }
		long long ms(long long x) const	{ return x / 1000000; }

		long long ns(long long x, long long y) const { return (y-x); }
		long long us(long long x, long long y) const { return (y-x) / 1000; }
		long long ms(long long x, long long y) const { return (y-x) / 1000000; }

		double nf(long long x) const { return x; }
		double uf(long long x) const { return x / 1e3; }
		double mf(long long x) const { return x / 1e6; }

		double nf(long long x, long long y) const { return (y-x); }
		double uf(long long x, long long y) const { return (y-x) / 1e3; }
		double mf(long long x, long long y) const { return (y-x) / 1e6; }

		double sf(long long x, long long y) const { return (y-x) / 1e9; }
#endif
	};
}

#endif // __nark_profiling_hpp__
