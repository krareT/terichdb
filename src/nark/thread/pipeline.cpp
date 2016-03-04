/* vim: set tabstop=4 : */

#include "pipeline.hpp"
#include <nark/circular_queue.hpp>
#include <nark/num_to_str.hpp>
//#include <nark/util/compare.hpp>
#include <vector>
//#include <deque>
//#include <boost/circular_buffer.hpp>
#include <nark/util/concurrent_queue.hpp>
#include <stdio.h>
#include <iostream>

// http://predef.sourceforge.net/

#if defined(NARK_CONCURRENT_QUEUE_USE_BOOST)
	#include <boost/thread/mutex.hpp>
	#include <boost/thread/lock_guard.hpp>
	#include <boost/thread.hpp>
	#include <boost/bind.hpp>
	typedef boost::lock_guard<boost::mutex> PipelineLockGuard;
	#if defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER)
		#include <Windows.h>
		#undef min
		#undef max
	#else
		#include <unistd.h>
	#endif
#else
	typedef std::lock_guard<std::mutex> PipelineLockGuard;
#endif

namespace nark {

using namespace std;

PipelineTask::~PipelineTask()
{
	// do nothing...
}

//typedef concurrent_queue<std::deque<PipelineQueueItem> > base_queue;
typedef util::concurrent_queue<circular_queue<PipelineQueueItem> > base_queue;
class PipelineStage::queue_t : public base_queue
{
public:
	queue_t(int size) : base_queue(size)
	{
		base_queue::queue().init(size);
	}
};

PipelineStage::ThreadData::ThreadData() : m_run(false) {
	m_thread = NULL;
}
PipelineStage::ThreadData::~ThreadData() {}

//////////////////////////////////////////////////////////////////////////

PipelineStage::PipelineStage(int thread_count)
: m_owner(0)
{
	if (0 == thread_count)
	{
		m_pl_enum = ple_keep;
		thread_count = 1;
	}
	else if (-1 == thread_count)
	{
		m_pl_enum = ple_generate;
		thread_count = 1;
	}
	else
	{
		m_pl_enum = ple_none;
	}
	assert(0 < thread_count);
	m_plserial = 0;
	m_prev = m_next = NULL;
	m_out_queue = NULL;
	m_threads.resize(thread_count);
}

PipelineStage::~PipelineStage()
{
	delete m_out_queue;
	for (size_t threadno = 0; threadno != m_threads.size(); ++threadno)
	{
		delete m_threads[threadno].m_thread;
	}
}

int PipelineStage::getInQueueSize() const {
	assert(m_prev->m_out_queue);
	return m_prev->m_out_queue->size();
}

int PipelineStage::getOutQueueSize() const {
	assert(this->m_out_queue);
	return this->m_out_queue->size();
}

void PipelineStage::setOutQueueSize(int size) {
	if (size > 0) {
		assert(NULL == this->m_out_queue);
		this->m_out_queue = new queue_t(size);
	}
}

const std::string& PipelineStage::err(int threadno) const
{
	return m_threads[threadno].m_err_text;
}

int PipelineStage::step_ordinal() const
{
	return m_owner->step_ordinal(this);
}

std::string PipelineStage::msg_leading(int threadno) const
{
	char buf[256];
	int len = sprintf(buf, "step[%d], threadno[%d]", step_ordinal(), threadno);
	return std::string(buf, len);
}

mutex* PipelineStage::getMutex() const
{
	return m_owner->getMutex();
}

void PipelineStage::stop()
{
	m_owner->stop();
}

inline bool PipelineStage::isPrevRunning()
{
	return m_owner->isRunning() || m_prev->isRunning() || !m_prev->m_out_queue->empty();
}

bool PipelineStage::isRunning()
{
	for (int t = 0, n = (int)m_threads.size(); t != n; ++t)
	{
		if (m_threads[t].m_run)
			return true;
	}
	return false;
}

void PipelineStage::wait()
{
	for (size_t t = 0; t != m_threads.size(); ++t)
		m_threads[t].m_thread->join();
}

void PipelineStage::start(int queue_size)
{
	if (this != m_owner->m_head->m_prev) { // is not last step
		if (NULL == m_out_queue)
			m_out_queue = new queue_t(queue_size);
	}
	if (m_threads.size() == 0) {
		throw std::runtime_error("thread count = 0");
	}

	for (size_t threadno = 0; threadno != m_threads.size(); ++threadno)
	{
		// 在 PipelineThread 保存 auto_ptr<thread> 指针
		// 如果直接内嵌一个 thread 实例，那么在 new PipelineThread 时，该线程就开始运行
		// 有可能在 PipelineThread 运行结束时，m_threads[threadno] 还未被赋值，导致程序崩溃
		// 所以分两步，先构造对象，并且对 m_threads[threadno] 赋值，然后再运行线程
		//
		//
		// hold auto_ptr<thread> in PipelineThread
		//
		// if direct embed a thread in PipelineThread, when new PipelineThread,
		// the thread will running, then, maybe after PipelineThread was completed,
		// m_thread[threadno] was not be assigned the PipelineThread* pointer,
		// the app maybe crash in this case.
		//
		// so make 2 stage:
		// first:  construct the PipelineThread and assign it to m_threads[threadno]
		// second: start the thread
		//
		m_threads[threadno].m_thread = new thread(
			NarkFuncBind(&PipelineStage::run_wrapper, this, threadno));
	}
}

void PipelineStage::onException(int threadno, const std::exception& exp)
{
	static_cast<string_appender<>&>(m_threads[threadno].m_err_text = "")
		<< "exception class=" << typeid(exp).name() << ", what=" << exp.what();

//	error message will be printed in this->clean()
//
// 	PipelineLockGuard lock(*m_owner->m_mutex);
// 	std::cerr << "step[" << step_ordinal() << "]: " << m_step_name
// 			  << ", threadno[" << threadno
// 			  << "], caught: " << m_threads[threadno].m_err_text
// 			  << std::endl;
}

void PipelineStage::setup(int threadno)
{
	if (!m_owner->m_silent) {
		PipelineLockGuard lock(*m_owner->getMutex());
		std::cout << "start step[ordinal=" << step_ordinal()
				  << ", threadno=" << threadno
				  << "]: " << m_step_name << std::endl;
	}
}

void PipelineStage::clean(int threadno)
{
	PipelineLockGuard lock(*m_owner->getMutex());
	if (err(threadno).empty()) {
		if (!m_owner->m_silent)
			printf("ended step[ordinal=%d, threadno=%d]: %s\n"
				, step_ordinal(), threadno, m_step_name.c_str());
	} else {
		fprintf(stderr, "ended step[ordinal=%d, threadno=%d]: %s; error: %s\n"
				, step_ordinal(), threadno, m_step_name.c_str()
				, err(threadno).c_str());
	}
}

void PipelineStage::run_wrapper(int threadno)
{
	m_threads[threadno].m_run = true;
	bool setup_successed = false;
	try {
		setup(threadno);
		setup_successed = true;
		run(threadno);
		clean(threadno);
		assert(m_prev == m_owner->m_head || m_prev->m_out_queue->empty());
	}
	catch (const std::exception& exp)
	{
		onException(threadno, exp);
		if (setup_successed)
			clean(threadno);

		m_owner->stop();

		if (m_prev != m_owner->m_head)
		{ // 不是第一个 step, 清空前一个 step 的 out_queue
			while (!m_prev->m_out_queue->empty() || m_prev->isRunning())
			{
				PipelineQueueItem item;
				if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout))
				{
					if (item.task)
						m_owner->destroyTask(item.task);
				}
			}
		}
	}
	m_owner->stop();
	m_threads[threadno].m_run = false;
}

void PipelineStage::run(int threadno)
{
	assert(m_owner->total_steps() >= 1);
	if (this == m_owner->m_head->m_next
		&&
		NULL == m_owner->m_head->m_out_queue // input feed is from 'run_step_first'
	) {
		run_step_first(threadno);
	} else {
		// when NULL != m_owner->m_head->m_out_queue, input feed is from external
		switch (m_pl_enum) {
		default:
			assert(0);
			break;
		case ple_none:
		case ple_generate:
			if (this == m_owner->m_head->m_prev)
				run_step_last(threadno);
			else
				run_step_mid(threadno);
			break;
		case ple_keep:
			assert(m_threads.size() == 1);
			if (this == m_owner->m_head->m_prev)
				run_serial_step_fast(threadno, &PipelineStage::serial_step_do_last);
			else
				run_serial_step_fast(threadno, &PipelineStage::serial_step_do_mid);
			break;
		}
	}
}

void PipelineStage::run_step_first(int threadno)
{
	assert(ple_none == m_pl_enum || ple_generate == m_pl_enum);
	assert(this->m_threads.size() == 1);
	while (m_owner->isRunning())
	{
		PipelineQueueItem item;
		process(threadno, &item);
		if (item.task)
		{
			if (ple_generate == m_pl_enum)
			{
 			//	queue_t::MutexLockSentry lock(*m_out_queue); // not need lock
				item.plserial = ++m_plserial;
			}
			m_out_queue->push_back(item);
		}
	}
}

void PipelineStage::run_step_last(int threadno)
{
	assert(ple_none == m_pl_enum);

	while (isPrevRunning())
	{
		PipelineQueueItem item;
		if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout))
		{
			if (item.task)
				process(threadno, &item);
			if (item.task)
				m_owner->destroyTask(item.task);
		}
	}
}

void PipelineStage::run_step_mid(int threadno)
{
	assert(ple_none == m_pl_enum || (ple_generate == m_pl_enum && m_threads.size() == 1));

	while (isPrevRunning())
	{
		PipelineQueueItem item;
		if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout))
		{
			if (ple_generate == m_pl_enum) {
			// only 1 thread, do not mutex lock
			// use m_out_queue mutex
 			//	queue_t::MutexLockSentry lock(*m_out_queue);
				item.plserial = ++m_plserial;
 			}
			if (item.task)
				process(threadno, &item);
			if (item.task || m_owner->m_keepSerial)
				m_out_queue->push_back(item);
		}
	}
}

namespace {
//SAME_NAME_MEMBER_COMPARATOR_EX(plserial_greater, uintptr_t, uintptr_t, .plserial, std::greater<uintptr_t>)
//SAME_NAME_MEMBER_COMPARATOR_EX(plserial_less   , uintptr_t, uintptr_t, .plserial, std::less   <uintptr_t>)

struct plserial_greater {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.plserial > y.plserial;
	}
};
struct plserial_less {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.plserial < y.plserial;
	}
};
}

void PipelineStage::serial_step_do_mid(PipelineQueueItem& item)
{
	m_out_queue->push_back(item);
}
void PipelineStage::serial_step_do_last(PipelineQueueItem& item)
{
	if (item.task)
		m_owner->destroyTask(item.task);
}

#if defined(_DEBUG) || !defined(NDEBUG)
# define CHECK_SERIAL() assert(item.plserial >= m_plserial);
#else
# define CHECK_SERIAL() \
	if (item.plserial < m_plserial) { \
		string_appender<> oss; \
        oss << "fatal at: " << __FILE__ << ":" << __LINE__ \
            << ", function=" << BOOST_CURRENT_FUNCTION \
            << ", item.plserial=" << item.plserial \
            << ", m_plserial=" << m_plserial; \
        throw std::runtime_error(oss);  \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#endif

void PipelineStage::run_serial_step_slow(int threadno,
								   void (PipelineStage::*fdo)(PipelineQueueItem&)
								  )
{
	assert(ple_keep == m_pl_enum);
	assert(m_threads.size() == 1);
	assert(0 == threadno);
	using namespace std;
	vector<PipelineQueueItem> cache;
	m_plserial = 1;
	while (isPrevRunning()) {
		PipelineQueueItem item;
		if (!m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout))
			continue;
		CHECK_SERIAL()
		if (item.plserial == m_plserial)
		{Loop:
			if (item.task)
				process(threadno, &item);
			(this->*fdo)(item);
			++m_plserial;
			if (!cache.empty() && (item = cache[0]).plserial == m_plserial) {
				pop_heap(cache.begin(), cache.end(), plserial_greater());
				cache.pop_back();
				goto Loop;
			}
		}
		else { // plserial out of order
			cache.push_back(item);
			push_heap(cache.begin(), cache.end(), plserial_greater());
		}
	}
	std::sort(cache.begin(), cache.end(), plserial_less());
	for (vector<PipelineQueueItem>::iterator i = cache.begin(); i != cache.end(); ++i) {
		if (i->task)
			process(threadno, &*i);
		(this->*fdo)(*i);
	}
}

// use a local 'cache' to cache the received tasks, if tasks are out of order,
// hold them in the cache, until next received task is just the expected task.
// if the next received task's serial number is out of the cache's range,
// put it to the overflow vector...
void PipelineStage::run_serial_step_fast(int threadno,
								   void (PipelineStage::*fdo)(PipelineQueueItem&)
								   )
{
	assert(ple_keep == m_pl_enum);
	assert(m_threads.size() == 1);
	assert(0 == threadno);
	using namespace std;
	const ptrdiff_t nlen = NARK_IF_DEBUG(4, 64); // should power of 2
	ptrdiff_t head = 0;
	vector<PipelineQueueItem> cache(nlen), overflow;
	m_plserial = 1; // this is expected_serial
	while (isPrevRunning()) {
		PipelineQueueItem item;
		if (!m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout))
			continue;
		CHECK_SERIAL()
		ptrdiff_t diff = item.plserial - m_plserial; // diff is in [0, nlen)
		//  not all equivalent to cycle queue, so it is not 'diff < nlen-1'
		// if plserial is in [1, nlen) as TCP, it should be 'diff < nlen-1'
		if (nark_likely(diff < nlen)) {
			int index = (head + diff) % nlen;
			cache[index] = item;
		}
		else { // very rare case
			overflow.push_back(item);
			if (overflow.size() >= 2) {
				push_heap(overflow.begin(), overflow.end(), plserial_greater());
				do {
					diff = overflow[0].plserial - m_plserial;
					if (diff < nlen) {
						int index = (head + diff) % nlen;
						cache[index] = overflow[0];
						pop_heap(overflow.begin(), overflow.end(), plserial_greater());
						overflow.pop_back();
					} else
						break;
				} while (!overflow.empty());
			}
		}
		while (cache[head].plserial == m_plserial) {
			if (cache[head].task)
				process(threadno, &cache[head]);
			(this->*fdo)(cache[head]);
			cache[head] = PipelineQueueItem(); // clear
			++m_plserial;
			head = (head + 1) % nlen;
			if (nark_unlikely(!overflow.empty() && overflow[0].plserial == m_plserial)) {
				// very rare case
				cache[head] = overflow[0];
				pop_heap(overflow.begin(), overflow.end(), plserial_greater());
				overflow.pop_back();
			}
		}
	}
	for (ptrdiff_t i = 0; i < nlen; ++i) {
		if (cache[i].plserial)
			overflow.push_back(cache[i]);
	}
	std::sort(overflow.begin(), overflow.end(), plserial_less());
	for (vector<PipelineQueueItem>::iterator i = overflow.begin(); i != overflow.end(); ++i) {
		if (i->task)
			process(threadno, &*i);
		(this->*fdo)(*i);
	}
}

//////////////////////////////////////////////////////////////////////////

FunPipelineStage::FunPipelineStage(int thread_count,
				const function<void(PipelineStage*, int, PipelineQueueItem*)>& fprocess,
				const std::string& step_name)
	: PipelineStage(thread_count)
	, m_process(fprocess)
{
	m_step_name = (step_name);
}

FunPipelineStage::~FunPipelineStage() {}

void FunPipelineStage::process(int threadno, PipelineQueueItem* task)
{
	m_process(this, threadno, task);
}

//////////////////////////////////////////////////////////////////////////

class Null_PipelineStage : public PipelineStage
{
public:
	Null_PipelineStage() : PipelineStage(0) {}
protected:
	virtual void process(int /*threadno*/, PipelineQueueItem* /*task*/)
	{
		assert(0);
	}
};

int PipelineProcessor::sysCpuCount() {
#if defined(NARK_CONCURRENT_QUEUE_USE_BOOST)
  #if defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER)
	SYSTEM_INFO sysInfo;
	GetSystemInfo(&sysInfo);
	return sysInfo.dwNumberOfProcessors;
  #else
	return (int)sysconf(_SC_NPROCESSORS_ONLN);
  #endif
#else
	return (int)std::thread::hardware_concurrency();
#endif
}

PipelineProcessor::PipelineProcessor()
  : m_destroyTask(&PipelineProcessor::defaultDestroyTask)
{
	m_queue_size = 10;
	m_queue_timeout = 20;
	m_head = new Null_PipelineStage;
	m_head->m_prev = m_head->m_next = m_head;
	m_mutex = NULL;
	m_is_mutex_owner = false;
	m_keepSerial = false;
	m_run = false;
	m_silent = false;
}

PipelineProcessor::~PipelineProcessor()
{
	clear();

	delete m_head;
	if (m_is_mutex_owner)
		delete m_mutex;
}

void PipelineProcessor::defaultDestroyTask(PipelineTask* task)
{
	delete task;
}
void PipelineProcessor::destroyTask(PipelineTask* task)
{
	m_destroyTask(task);
}

void PipelineProcessor::setMutex(mutex* pmutex)
{
	if (m_run) {
		throw std::logic_error("can not setMutex after PipelineProcessor::start()");
	}
	m_mutex = pmutex;
	m_is_mutex_owner = false;
}

std::string PipelineProcessor::queueInfo()
{
	string_appender<> oss;
	const PipelineStage* p = m_head->m_next;
	oss << "QueueSize: ";
	while (p != m_head->m_prev) {
		oss << "(" << p->m_step_name << "=" << p->m_out_queue->peekSize() << "), ";
		p = p->m_next;
	}
	oss.resize(oss.size()-2);
	return oss;
}

int PipelineProcessor::step_ordinal(const PipelineStage* step) const
{
	int ordinal = 0;
	const PipelineStage* p = m_head->m_next;
	while (p != step) {
		p = p->m_next;
		++ordinal;
	}
	return ordinal;
}

int PipelineProcessor::total_steps() const
{
	return step_ordinal(m_head);
}

void PipelineProcessor::start()
{
	assert(m_head);
	assert(total_steps() >= 2 || (total_steps() >= 1 && NULL != m_head->m_out_queue));

	m_run = true;

	if (NULL == m_mutex)
	{
		m_mutex = new mutex;
		m_is_mutex_owner = true;
	}
	int plgen = -1, plkeep = -1, nth = 0;
	(void)plgen; // depress warning
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
	{
		if (PipelineStage::ple_generate == s->m_pl_enum)
		{
			assert(-1 == plgen);  // only one generator
			assert(-1 == plkeep); // keep must after generator
			plgen = nth;
		}
		else if (PipelineStage::ple_keep == s->m_pl_enum)
		{
			// must have generator before
			// m_head->m_out_queue is not NULL when data source is external
			assert(-1 != plgen || m_head->m_out_queue);
			plkeep = nth;
		}
		++nth;
	}
	if (-1 != plkeep)
		this->m_keepSerial = true;

	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
		s->start(m_queue_size);
}

//! this is the recommended usage
//!
//! compile means no generator step
//! compile just create the pipeline which is ready to consume input
//! input is from out of pipeline, often driven by main thread
void PipelineProcessor::compile()
{
	compile(m_queue_size);
}
void PipelineProcessor::compile(int input_feed_queue_size)
{
	m_head->m_out_queue = new PipelineStage::queue_t(input_feed_queue_size);
	start();
}

void PipelineProcessor::send(PipelineTask* task)
{
	PipelineQueueItem item(++m_head->m_plserial, task);
	m_head->m_out_queue->push_back(item);
}

void PipelineProcessor::wait()
{
	if (NULL != m_head->m_out_queue) {
		assert(!this->m_run); // user must call stop() before wait
	}
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
		s->wait();
}

void PipelineProcessor::add_step(PipelineStage* step)
{
	step->m_owner = this;
	step->m_prev = m_head->m_prev;
	step->m_next = m_head;
	m_head->m_prev->m_next = step;
	m_head->m_prev = step;
}

void PipelineProcessor::clear()
{
	assert(!m_run);

	PipelineStage* curr = m_head->m_next;
	while (curr != m_head)
	{
		PipelineStage* next = curr->m_next;
		delete curr;
		curr = next;
	}
	m_head->m_prev = m_head->m_next = m_head;
}

int PipelineProcessor::getInQueueSize(int step_no) const {
	PipelineStage* step = m_head;
	for (int i = 0; i < step_no; ++i) {
		step = step->m_next;
		assert(step != m_head); // step_no too large
		if (step == m_head) {
			char msg[128];
			sprintf(msg, "invalid step_no=%d", step_no);
			throw std::invalid_argument(msg);
		}
	}
	return step->m_out_queue->size();
}

} // namespace nark

