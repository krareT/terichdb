#pragma once

#include <boost/current_function.hpp>
#include <terark/stdtypes.hpp>
#include <terark/valvec.hpp>
#if defined(_MSC_VER)
#else
	#include <sys/mman.h>
#endif

namespace terark {

template<class T>
void use_hugepage_advise(valvec<T>* vec) {
#if defined(_MSC_VER) || !defined(MADV_HUGEPAGE)
#else
	const size_t hugepage_size = size_t(2) << 20;
	size_t nBytes = vec->used_mem_size();
	T* amem = NULL;
	int err = posix_memalign((void**)&amem, hugepage_size, nBytes);
	if (err) {
		fprintf(stderr, "WARN: %s: posix_memalign(%zd, %zd) = %s\n",
			BOOST_CURRENT_FUNCTION, hugepage_size, nBytes, strerror(err));
		return;
	}
	memcpy(amem, vec->data(), vec->used_mem_size());
	size_t size = vec->size();
	vec->clear();
	vec->risk_set_data(amem, size);
	vec->risk_set_capacity(nBytes/sizeof(T));
	err = madvise(amem, nBytes, MADV_HUGEPAGE);
	if (err) {
		fprintf(stderr, "WARN: %s: madvise(MADV_HUGEPAGE, size=%zd[0x%zX]) = %s\n",
			BOOST_CURRENT_FUNCTION, nBytes, nBytes, strerror(errno));
	}
#endif
}

template<class T>
void use_hugepage_resize_no_init(valvec<T>* vec, size_t newsize) {
#if defined(_MSC_VER) || !defined(MADV_HUGEPAGE)
	vec->resize_no_init(newsize);
#else
	const size_t hugepage_size = size_t(2) << 20;
	size_t nBytes = sizeof(T)*newsize;
	T* amem = NULL;
	int err = posix_memalign((void**)&amem, hugepage_size, nBytes);
	if (err) {
		fprintf(stderr, "WARN: %s: posix_memalign(%zd, %zd) = %s\n",
			BOOST_CURRENT_FUNCTION, hugepage_size, nBytes, strerror(err));
		vec->resize_no_init(newsize);
		return;
	}
	size_t copySize = sizeof(T) * std::min(vec->size(), newsize);
	memcpy(amem, vec->data(), copySize);
	vec->clear();
	vec->risk_set_data(amem, newsize);
	vec->risk_set_capacity(nBytes/sizeof(T));
	err = madvise(amem, nBytes, MADV_HUGEPAGE);
	if (err) {
		fprintf(stderr, "WARN: %s: madvise(MADV_HUGEPAGE, size=%zd[0x%zX]) = %s\n",
			BOOST_CURRENT_FUNCTION, nBytes, nBytes, strerror(errno));
	}
	else {
	//	fprintf(stderr, "INFO: %s: madvise(MADV_HUGEPAGE) = success\n",
	//		BOOST_CURRENT_FUNCTION);
	}
#endif
}

} // namespace terark

