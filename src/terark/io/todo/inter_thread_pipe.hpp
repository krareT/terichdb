#ifndef __terark_thread_LockFreeQueue_H__
#define __terark_thread_LockFreeQueue_H__

namespace thread {

	class inter_thread_pipe_impl;

	class inter_thread_pipe
	   	: public RefCounter
	   	, public IInputStream
		, public IOutputStream
	{
		inter_thread_pipe_impl* mio;
	public:
		explicit inter_thread_pipe(size_t capacity);
		~inter_thread_pipe();
		void eof();
		void read(void* vbuf, size_t length);
		void write(void* vbuf, size_t length);
	};

} // namespace thread

#endif // __terark_thread_LockFreeQueue_H__


