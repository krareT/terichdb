#include "inter_thread_pipe.cpp"

namespace terark {

	class inter_thread_pipe_impl
	{
		boost::mutex  m_mutex;
		boost::condition m_cond;
		unsigned char *m_bufp, *m_putp, *m_getp;
		size_t m_size;
		long m_timeout;

	public:
		bool eof()
		{
			boost::mutex::scoped_lock lock(m_mutex);
			return (m_size+(m_get-m_putp)) % m_size == 1;
		}

		size_t read(void* vbuf, size_t length)
		{
			boost::mutex::scoped_lock lock(m_mutex);
		}

		size_t write(void* vbuf, size_t length)
		{

		}

		void flush()
		{
		}
	};

	inter_thread_pipe::inter_thread_pipe(size_t capacity)
		: mio(new capacity)
	{
	}

	inter_thread_pipe::~inter_thread_pipe()
	{
		delete capacity;
	}

	bool inter_thread_pipe::eof()
	{

		return mio->eof();
	}

	size_t inter_thread_pipe::read(void* vbuf, size_t length)
	{
		return mio->read(vbuf, length);
	}

	size_t inter_thread_pipe::write(void* vbuf, size_t length)
	{

	}

	void inter_thread_pipe::flush()
	{
	}

} // namespace thread



