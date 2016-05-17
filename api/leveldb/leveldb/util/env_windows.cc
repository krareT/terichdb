// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <set>
#include <thread>
#include <functional>
#include <memory>
#include <condition_variable>
#include <mutex>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <io.h>
#else
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/param.h>
#include <time.h>
#include <unistd.h>
#endif
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif

#if defined(_WIN32) || defined(_WIN64)
#else
  #include <sys/sysinfo.h>
  #include <linux/unistd.h>
#endif

#include <fstream>

// Boost includes - see WINDOWS file to see which modules to install
#include <boost/filesystem/convenience.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/lexical_cast.hpp>

#include "leveldb/env.h"
#include "leveldb/slice.h"

#if defined(_WIN32) || defined(_WIN64)
#include "util/win_logger.h"
#else
#include "util/posix_logger.h"
#endif
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {
namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

static Status WinIOError(const std::string & context, DWORD err)
{
    std::string err_mess;
    LPTSTR lpErrorText = NULL;
	if (!::FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ALLOCATE_BUFFER, 
		0, 
        err, 
        0, 
        (LPTSTR)&lpErrorText, 
        32000, 
        0))
    {
        err_mess = "unknown error";
    }
    else
    {
    	err_mess = lpErrorText;
	    ::LocalFree(lpErrorText);
    }
    return Status::IOError(context, err_mess);
}

// returns the ID of the current process
static std::uint32_t current_process_id(void) {
#ifdef _WIN32
  return static_cast<std::uint32_t>(::GetCurrentProcessId());
#else
  return static_cast<std::uint32_t>(::getpid());
#endif
}

// returns the ID of the current thread
static std::uint32_t current_thread_id(void) {
#ifdef _WIN32
  return static_cast<std::uint32_t>(::GetCurrentThreadId());
#else
#ifdef __linux
  return static_cast<std::uint32_t>(::syscall(__NR_gettid));
#else
  // just return the pid
  return current_process_id();
#endif
#endif
}

class WindowsSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  HANDLE handle_;

 public:
  WindowsSequentialFile(const std::string& fname, HANDLE h)
    : filename_(fname), handle_(h) { }
  virtual ~WindowsSequentialFile() { CloseHandle(handle_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    size_t r = 0;

    if (!::ReadFile(handle_, scratch, static_cast<DWORD>(n), reinterpret_cast<DWORD *>(&r), NULL))
    {
        return WinIOError(filename_, GetLastError());
    }
   *result = Slice(scratch, r);
   return Status::OK();
  }

  virtual Status Skip(uint64_t n) {
    const LONG lo = static_cast<LONG>(n & 0xffffffff);
    LONG hi = static_cast<LONG>(n >> 32);
    if (SetFilePointer(handle_, lo, &hi, FILE_BEGIN) == INVALID_SET_FILE_POINTER)
    {
        return WinIOError(filename_, GetLastError());
    }
    return Status::OK();
  }
};

class WindowsRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  const void * region_;
  uint64_t length_;
  HANDLE handle_;

 public:
  WindowsRandomAccessFile(const std::string& fname, const void * base, uint64_t l, HANDLE h)
    : filename_(fname), region_(base), length_(l), handle_(h) { }
  virtual ~WindowsRandomAccessFile() 
  { 
      if (region_)
      {
          UnmapViewOfFile(region_);
      }
      CloseHandle(handle_); 
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
            char* scratch) const {
    Status s;
    if (offset + static_cast<uint64_t>(n) > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<const char*>(region_) + offset, n);
    }
    return s;
  }
};

class WindowsWritableFile : public WritableFile {

private:
  boost::filesystem::path filename_;
  HANDLE handle_;

public:
  WindowsWritableFile(const std::string& fname, HANDLE h)
      : filename_(fname), handle_(h) { }

  ~WindowsWritableFile() { CloseHandle(handle_); }

public:
  virtual Status Append(const Slice& data) {

    DWORD written = 0;

    if (!::WriteFile(handle_, data.data(), static_cast<DWORD>(data.size()), &written, NULL))
    {
        return WinIOError(filename_.string(), ::GetLastError());
    }

    if (static_cast<size_t>(written) != data.size())
    {
        return Status::IOError(filename_.string(), "could not write all bytes to disk");
    }

    return Status::OK();
  }

  virtual Status Close() {
    ::CloseHandle(handle_);
    handle_ = NULL;
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    return Status::OK();
  }


};

class BoostFileLock : public FileLock {
 public:
  std::ofstream file_;
  boost::interprocess::file_lock fl_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class BoostLockTable {
 private:
  std::mutex mu_;
  std::set<std::string> locked_files_;
 public:
  bool Insert(const std::string& fname) {
    std::unique_lock<std::mutex> l(mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    std::unique_lock<std::mutex> l(mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() 
  {      
      bool expected = true;
      if (run_bg_thread_.compare_exchange_strong(expected, false))
      {
          bgsignal_.notify_one();
      }

      std::thread * t = nullptr;

      {
          std::unique_lock<std::mutex> lock(mu_);
          t = bgthread_.get();
      }

      if (t)
      {
          t->join();
      }

      std::unique_lock<std::mutex> lock(mu_);
      bgthread_.reset();

      queue_.clear();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                   SequentialFile** result) {
    HANDLE h = ::CreateFile(fname.c_str(), 
        GENERIC_READ, 
        FILE_SHARE_READ, 
        NULL, 
        OPEN_EXISTING, 
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
        NULL);

    if (h == INVALID_HANDLE_VALUE)
    {
        return WinIOError(fname, ::GetLastError());
    }
    *result = new WindowsSequentialFile(fname, h);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                   RandomAccessFile** result) {
    HANDLE h = ::CreateFile(fname.c_str(), 
        GENERIC_READ, 
        FILE_SHARE_READ, 
        NULL, 
        OPEN_EXISTING, 
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (h == INVALID_HANDLE_VALUE)
    {
        return WinIOError(fname, ::GetLastError());
    }

    DWORD hi_size = 0;

    const DWORD lo_size = ::GetFileSize(h, &hi_size);

    Status s;

    if (lo_size == INVALID_FILE_SIZE)
    {
        s = WinIOError(fname, ::GetLastError());
    }

    const uint64_t file_size = (static_cast<uint64_t>(hi_size) << 32uLL) + static_cast<uint64_t>(lo_size);

    HANDLE map = NULL;
    const void * base = nullptr;

    if (s.ok())
    {
        map = ::CreateFileMapping(h, NULL, PAGE_READONLY, 0, 0, NULL);

        if (map == NULL)
        {
            s = WinIOError(fname, ::GetLastError());
        }

        // handle of the file no longer needed whether we knew success or failure
        ::CloseHandle(h);

        if (map)
        {
            base = ::MapViewOfFile(map, FILE_MAP_READ, 0, 0, 0);
            if (!base)
            {
                s = WinIOError(fname, ::GetLastError());
                ::CloseHandle(map);
            }
        }
    }

    if (s.ok())
    {
        *result = new WindowsRandomAccessFile(fname, base, file_size, map);
    }

    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                 WritableFile** result) {

    HANDLE h = ::CreateFile(fname.c_str(), 
        GENERIC_WRITE, 
        0, 
        NULL, 
        CREATE_ALWAYS, 
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (h == INVALID_HANDLE_VALUE)
    {
        return WinIOError(fname, ::GetLastError());
    }

    *result = new WindowsWritableFile(fname, h);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    boost::system::error_code ec;
    return boost::filesystem::exists(fname, ec);
  }

  virtual Status GetChildren(const std::string& dir,
               std::vector<std::string>* result) {
    result->clear();

    boost::system::error_code ec;
    boost::filesystem::directory_iterator current(dir, ec);
    if (ec != 0) {
      return Status::IOError(dir, ec.message());
    }

    boost::filesystem::directory_iterator end;

    for(; current != end; ++current) {
      result->push_back(current->path().filename().generic_string());
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    boost::system::error_code ec;

    boost::filesystem::remove(fname, ec);

    Status result;

    if (ec != 0) {
      result = Status::IOError(fname, ec.message());
    }

    return result;
  }

  virtual Status CreateDir(const std::string& name) {
      Status result;

      boost::system::error_code ec;

      if (boost::filesystem::exists(name, ec) &&
          boost::filesystem::is_directory(name, ec)) {
        return result;
      }
      
      if (!boost::filesystem::create_directories(name, ec)) {
        result = Status::IOError(name, ec.message());
      }

      return result;
    };

    virtual Status DeleteDir(const std::string& name) {
    Status result;

    boost::system::error_code ec;
    if (!boost::filesystem::remove_all(name, ec)) {
      result = Status::IOError(name, ec.message());
    }

    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    boost::system::error_code ec;

    Status result;

    *size = static_cast<uint64_t>(boost::filesystem::file_size(fname, ec));
    if (ec != 0) {
      *size = 0;
       result = Status::IOError(fname, ec.message());
    }

    return result;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    boost::system::error_code ec;

    boost::filesystem::rename(src, target, ec);

    Status result;

    if (ec != 0) {
      result = Status::IOError(src, ec.message());
    }

    return result;
  }


  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = nullptr;
    std::ofstream of(fname.c_str(), std::ios_base::trunc | std::ios_base::out);
    if (of.bad()) {
        return Status::IOError("lock " + fname, "cannot create lock file");
    }
    if (!locks_.Insert(fname)) {
        of.close();
        return Status::IOError("lock " + fname, "already held by process");
    }
    boost::interprocess::file_lock fl(fname.c_str());
    if (!fl.try_lock()) {
        of.close();         
        locks_.Remove(fname);
        return Status::IOError("lock " + fname, "database already in use: could not acquire exclusive lock" );
    }
    BoostFileLock * my_lock = new BoostFileLock();
    my_lock->name_ = fname;
    my_lock->file_ = std::move(of);
    my_lock->fl_ = std::move(fl);
    *lock = my_lock;
    return Status();
  }



  virtual Status UnlockFile(FileLock* lock) {
    BoostFileLock * my_lock = static_cast<BoostFileLock *>(lock);
    Status result;
    try {      
      my_lock->fl_.unlock();      
    } catch (const std::exception & e) {
      result = Status::IOError("unlock " + my_lock->name_, e.what());
    }
    locks_.Remove(my_lock->name_);
    my_lock->file_.close();
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);
  
  virtual Status GetTestDirectory(std::string* result) {
    boost::system::error_code ec;
    boost::filesystem::path temp_dir = 
        boost::filesystem::temp_directory_path(ec);
    if (ec != 0) {
      temp_dir = "tmp";
    }

    temp_dir /= "leveldb_tests";
    temp_dir /= boost::lexical_cast<std::string>(current_process_id());

    // Directory may already exist
    CreateDir(temp_dir.generic_string());

    *result = temp_dir.generic_string();

    return Status::OK();
  }

#ifndef WIN32
  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }
#endif

  virtual Status NewLogger(const std::string& fname, Logger** result) {
  FILE* f = fopen(fname.c_str(), "wt");
  if (f == NULL) {
    *result = NULL;
    return Status::IOError(fname, strerror(errno));
  } else {
#ifdef WIN32
    *result = new WinLogger(f);
#else
    *result = new PosixLogger(f, &PosixEnv::gettid);
#endif
    return Status::OK();
  }
  }

  virtual uint64_t NowMicros() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
  }

  virtual void SleepForMicroseconds(int micros) {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

  public:
  virtual std::thread::native_handle_type GetBackgroundThreadHandle() {
    return bgthread_ ? bgthread_->native_handle() : std::thread::native_handle_type();
  }
#if 0
  struct StartThreadState {
	  void(*user_function)(void*);
	  void* arg;
  };
  static void StartThreadWrapper(void* arg) {
	  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
	  state->user_function(state->arg);
	  delete state;
  }
  void StartThread(void(*function)(void* arg), void* arg) {
	  StartThreadState* state = new StartThreadState;
	  state->user_function = function;
	  state->arg = arg;
	  _beginthread(&StartThreadWrapper, 0, state);
  }
#else
  void StartThread(void(*function)(void*), void* arg) {
	  _beginthread(function, 0, arg);
  }
#endif
  // BGThread() is the body of the background thread
  void BGThread();

  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  std::mutex mu_;
  std::condition_variable bgsignal_;
  std::unique_ptr<std::thread> bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;
  std::atomic<bool> run_bg_thread_;

  BoostLockTable locks_;
};

PosixEnv::PosixEnv() { }

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  std::unique_lock<std::mutex> lock(mu_);

  // Start background thread if necessary
  if (!bgthread_) {
     run_bg_thread_ = true;
     bgthread_.reset(new std::thread(std::bind(&PosixEnv::BGThreadWrapper, this)));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  lock.unlock();

  bgsignal_.notify_one();

}

#ifdef _MSC_VER
// we use SEH, we cannot have a destructor, therefore we pass a pure pointer
static void name_this_thread(const char * thread_name)
{
    if (!::IsDebuggerPresent())
    {
        // no need to do this if no debugger is present, remember threads in Windows don't really have name
        // this is just a secret handshake with Visual Studio to name threads and make debugging more pleasant
        // on UNIXES however the name will appear in utilities such as htop
        return;
    }

    static const DWORD MS_VC_EXCEPTION = 0x406D1388;

#pragma pack(push,8)
    struct THREADNAME_INFO
    {
        DWORD dwType; // Must be 0x1000.
        LPCSTR szName; // Pointer to name (in user addr space).
        DWORD dwThreadID; // Thread ID (-1=caller thread).
        DWORD dwFlags; // Reserved for future use, must be zero.
    };
#pragma pack(pop)

    THREADNAME_INFO info;
    info.dwType = 0x1000;
    info.szName = thread_name; 
    info.dwThreadID = ::GetCurrentThreadId();
    info.dwFlags = 0;

    __try
    {
        RaiseException(MS_VC_EXCEPTION, 0, sizeof(info) / sizeof(ULONG_PTR), (ULONG_PTR*)&info);
    }
    __except (EXCEPTION_EXECUTE_HANDLER)
    {
    }

}
#else

// useful for debugging to name the thread, since we only use env_boost.cc for Windows
// we have this macro
// for env_posix.cc we also do the right thing for FreeBSD, Linux and MacOS

static void name_this_thread(const char *) {}
#endif

void PosixEnv::BGThread() {
    
    name_this_thread("leveldb background");

    try
    {
        while (run_bg_thread_) {
            // Wait until there is an item that is ready to run
            std::unique_lock<std::mutex> lock(mu_);

            while (queue_.empty() && run_bg_thread_) {
                bgsignal_.wait(lock);
            }

            if (!run_bg_thread_)
            {
                break;
            }

            void (*function)(void*) = queue_.front().function;
            void* arg = queue_.front().arg;
            queue_.pop_front();

            lock.unlock();
            (*function)(arg);
        }
    }
    catch (...) {}

}

}

static std::once_flag once;
static Env* default_env = nullptr;
static void InitDefaultEnv() { 
  default_env = new PosixEnv();

  // force background thread creation so that thread affinity can work 
  default_env->Schedule([](void *) {}, nullptr);
}

Env* Env::Default() {
  std::call_once(once, InitDefaultEnv);
  return default_env;
}
/*
void Env::UnsafeDeallocate() {
    // will not be able to call again, but the purpose of this function is to get rid of fake alerts by analyzers
    delete default_env;
    default_env = nullptr;
}
*/


}
