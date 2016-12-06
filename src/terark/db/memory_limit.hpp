#ifndef __terark_util_memory_limit_hpp__
#define __terark_util_memory_limit_hpp__

#include <cstdint>
#include <atomic>
#include <mutex>
#include <limits>
#include <condition_variable>
#include <boost/noncopyable.hpp>

namespace terark {

class MemoryLimit : boost::noncopyable {
    size_t used_size = 0;
    size_t check_size =  2ULL * 1024 * 1024 * 1024;
    size_t soft_limit = 12ULL * 1024 * 1024 * 1024;
    size_t hard_limit = 24ULL * 1024 * 1024 * 1024;
    std::mutex mutex;
    std::mutex cvm;
    std::condition_variable cv;

public:
    struct MemoryLimitHandle {
        MemoryLimit* parent;
        size_t size;

        MemoryLimitHandle(MemoryLimit* p, size_t s) : parent(p), size(s) {
        }
        MemoryLimitHandle(MemoryLimitHandle&& o) : parent(o.parent), size(o.size) {
            o.parent = nullptr;
        }
        MemoryLimitHandle& operator = (MemoryLimitHandle&& o) {
            parent = o.parent;
            size = o.size;
            o.parent = nullptr;
        }
        MemoryLimitHandle(MemoryLimitHandle const &) = delete;
        MemoryLimitHandle& operator = (MemoryLimitHandle const&) = delete;

        void release() {
            if (parent) {
                std::unique_lock<std::mutex> lk(parent->mutex);
                parent->used_size -= size;
                parent->cv.notify_all();
                parent = nullptr;
            }
        }

        ~MemoryLimitHandle() {
            release();
        }
    };

    MemoryLimitHandle request(size_t size) {
        auto can_request = [&] {
            return used_size == 0 || used_size + size < (size <= check_size ? hard_limit : soft_limit + check_size);
        };
        while (true) {
            {
                std::unique_lock<std::mutex> lk(mutex);
                if (can_request()) {
                    used_size += size;
                    break;
                }
            }
            {
                std::unique_lock<std::mutex> lk(cvm);
                cv.wait(lk, can_request);
            }
        }
        return MemoryLimitHandle(this, size);
    }

    void init_size(size_t check, size_t soft, size_t hard) {
        check_size = check;
        soft_limit = soft;
        hard_limit = hard;
    }
};

} // namespace terark

#endif // __terark_util_memory_limit_hpp__
