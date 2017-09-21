/* vim: set tabstop=4 : */
#ifndef __terark_pipeline_hpp__
#define __terark_pipeline_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(push)
# pragma warning(disable: 4018)
# pragma warning(disable: 4267)
#endif

#include <stddef.h>
#include <string>
#include <terark/config.hpp>
#include <terark/valvec.hpp>

#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
	#include <boost/function.hpp>
	namespace boost {
		// forward declaration
		// avoid app compile time dependency to <boost/thread.hpp>
		class thread;
		class mutex;
	}
#else
	#include <functional>
	#include <mutex>
	#include <thread>
#endif

namespace terark {

#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
	using boost::thread;
	using boost::mutex;
	using boost::function;
	#define TerarkFuncBind boost::bind
	typedef boost::lock_guard<boost::mutex> PipelineLockGuard;
#else
	using std::thread;
	using std::mutex;
	using std::function;
	#define TerarkFuncBind std::bind
	typedef std::lock_guard<std::mutex> PipelineLockGuard;
#endif

class TERARK_DLL_EXPORT PipelineTask
{
public:
	virtual ~PipelineTask();
};

class TERARK_DLL_EXPORT PipelineQueueItem
{
public:
	uintptr_t plserial;
	PipelineTask* task;

	PipelineQueueItem(uintptr_t plserial, PipelineTask* task)
		: plserial(plserial)
		, task(task)
	{}

	PipelineQueueItem()
		: plserial(0)
		, task(0)
	{}
};

class TERARK_DLL_EXPORT PipelineProcessor;

class TERARK_DLL_EXPORT PipelineStage : boost::noncopyable
{
	friend class PipelineProcessor;

public:
	class queue_t;

protected:
	queue_t* m_out_queue;

	PipelineStage *m_prev, *m_next;
	PipelineProcessor* m_owner;

	struct ThreadData;
	valvec<ThreadData> m_threads;
	enum {
		ple_none,
		ple_generate,
		ple_keep
	} m_pl_enum;
	uintptr_t m_plserial;

	void run_wrapper(int threadno);

	void run_step_first(int threadno);
	void run_step_last(int threadno);
	void run_step_mid(int threadno);

	void run_serial_step_slow(int threadno, void (PipelineStage::*fdo)(PipelineQueueItem&));
	void run_serial_step_fast(int threadno, void (PipelineStage::*fdo)(PipelineQueueItem&));
	void serial_step_do_mid(PipelineQueueItem& item);
	void serial_step_do_last(PipelineQueueItem& item);

	bool isPrevRunning();
	bool isRunning();
	void start(int queue_size);
	void wait();
	void stop();

protected:
	virtual void process(int threadno, PipelineQueueItem* task) = 0;

	virtual void setup(int threadno);
	virtual void clean(int threadno);

	virtual void run(int threadno);
	virtual void onException(int threadno, const std::exception& exp);

public:
	std::string m_step_name;

	//! @param thread_count 0 indicate keepSerial, -1 indicate generate serial
	explicit PipelineStage(int thread_count);

	virtual ~PipelineStage();

	int step_ordinal() const;
	const std::string& err(int threadno) const;

	// helper functions:
	std::string msg_leading(int threadno) const;
	mutex* getMutex() const;
	size_t getInputQueueSize()  const;
	size_t getOutputQueueSize() const;
	void setOutputQueueSize(size_t size);
};

class TERARK_DLL_EXPORT FunPipelineStage : public PipelineStage
{
	function<void(PipelineStage*, int, PipelineQueueItem*)> m_process; // take(this, threadno, task)

protected:
	void process(int threadno, PipelineQueueItem* task);

public:
	FunPipelineStage(int thread_count,
					const function<void(PipelineStage*, int, PipelineQueueItem*)>& fprocess,
					const std::string& step_name = "");
	~FunPipelineStage(); // move destructor into libterark-thread*
};

class TERARK_DLL_EXPORT PipelineProcessor
{
	friend class PipelineStage;

	PipelineStage *m_head;
	int m_queue_size;
	int m_queue_timeout;
	function<void(PipelineTask*)> m_destroyTask;
	mutex* m_mutex;
	mutex  m_mutexForInqueue;
	volatile size_t m_run; // size_t is CPU word, should be bool
	bool m_is_mutex_owner;
	bool m_keepSerial;

protected:
	static void defaultDestroyTask(PipelineTask* task);
	virtual void destroyTask(PipelineTask* task);

	void add_step(PipelineStage* step);
	void clear();

public:
	bool m_silent; // set to true to depress status messages

	static int sysCpuCount();

	PipelineProcessor();

	virtual ~PipelineProcessor();

	int isRunning() const { return m_run; }

	void setQueueSize(int queue_size) { m_queue_size = queue_size; }
	int  getQueueSize() const { return m_queue_size; }
	void setQueueTimeout(int queue_timeout) { m_queue_timeout = queue_timeout; }
	int  getQueueTimeout() const { return m_queue_timeout; }
	void setDestroyTask(const function<void(PipelineTask*)>& fdestory) { m_destroyTask = fdestory; }
	void setMutex(mutex* pmutex);
	mutex* getMutex() { return m_mutex; }

	std::string queueInfo();

	int step_ordinal(const PipelineStage* step) const;
	int total_steps() const;

	PipelineProcessor& operator| (PipelineStage* step) { this->add_step(step); return *this; }
	PipelineProcessor& operator>>(PipelineStage* step) { this->add_step(step); return *this; }

	void start();
	void compile(); // input feed from external, not first step
	void compile(int input_feed_queue_size /* default = m_queue_size */);

	void inqueue(PipelineTask* task);

	void stop() { m_run = false; }
	void wait();
	size_t getInputQueueSize(size_t step_no) const;
};

#define PPL_STEP_0(pObject, Class, MemFun, thread_count) \
	new terark::FunPipelineStage(thread_count\
		, TerarkFuncBind(&Class::MemFun, pObject, _1, _2, _3)\
		, BOOST_STRINGIZE(Class::MemFun)\
		)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define PPL_STEP_1(pObject, Class, MemFun, thread_count, arg1) \
	new terark::FunPipelineStage(thread_count\
		, TerarkFuncBind(&Class::MemFun, pObject, _1, _2, _3, arg1)\
		, BOOST_STRINGIZE(Class::MemFun)\
		)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define PPL_STEP_2(pObject, Class, MemFun, thread_count, arg1, arg2) \
	new terark::FunPipelineStage(thread_count\
		, TerarkFuncBind(&Class::MemFun, pObject, _1, _2, _3, arg1, arg2)\
		, BOOST_STRINGIZE(Class::MemFun)\
		)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#define PPL_STEP_3(pObject, Class, MemFun, thread_count, arg1, arg2, arg3) \
	new terark::FunPipelineStage(thread_count\
		, TerarkFuncBind(&Class::MemFun, pObject, _1, _2, _3, arg1, arg2, arg3)\
		, BOOST_STRINGIZE(Class::MemFun)\
		)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

} // namespace terark

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma warning(pop)
#endif

#endif // __terark_pipeline_hpp__

