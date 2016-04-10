#include <stdio.h>
#include <terark/num_to_str.hpp>
#include <terark/thread/pipeline.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>

using namespace terark;
using namespace std::placeholders;

int G_bPrint;

class MyTask : public PipelineTask
{
public:
	int val;

	MyTask(int x) : val(x) {}
};

class GeneratorStep : public PipelineStage
{
	int maxNum;
public:
	GeneratorStep(int maxNum) : PipelineStage(-1)
	{
		this->maxNum = maxNum;
	}
	virtual void process(int threadno, PipelineQueueItem* task)
	{
		task->task = new MyTask(this->m_plserial + 2);
		if (G_bPrint) {
			PipelineLockGuard lock(*getMutex());
			printf("generator: m_plserial=%06lu, maxNum=%d\n", m_plserial, maxNum);
		}
		if (0 == --maxNum)
			m_owner->stop();
	}
};

class Main
{
public:
	// prefer use 'vector<int>*', not 'vector<int>&'
	void step1(PipelineStage* step, int threadno, PipelineQueueItem* task, const std::vector<int>* arg1)
	{
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step1: threadno=%d plserial=%06lu\n", threadno, task->plserial);
	}
	void step1_setup(PipelineStage* step, int threadno)
	{
		PipelineLockGuard lock(*step->getMutex());
		printf("step1_setup: threadno=%d\n", threadno);
	}
	void step1_clean(PipelineStage* step, int threadno)
	{
		PipelineLockGuard lock(*step->getMutex());
		printf("step1_clean: threadno=%d\n", threadno);
	}

	void step2(PipelineStage* step, int threadno, PipelineQueueItem* task)
	{
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step2: threadno=%d plserial=%06lu\n", threadno, task->plserial);
	}
	void step3(PipelineStage* step, int threadno, PipelineQueueItem* task)
	{
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step3: threadno=%d plserial=%06lu\n", threadno, task->plserial);
	}
	void step4(PipelineStage* step, int threadno, PipelineQueueItem* task, double arg1)
	{
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step4: threadno=%d plserial=%06lu, arg1=%f\n", threadno, task->plserial, arg1);
	}
	void step5(PipelineStage* step, int threadno, PipelineQueueItem* task, double arg1, const std::string& arg2)
	{
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step5: threadno=%d plserial=%06lu, arg1=%f, arg2=%s\n", threadno, task->plserial, arg1, arg2.c_str());
	}
	unsigned long serial;
	int maxNum;
	void step6(PipelineStage* step, int threadno, PipelineQueueItem* task)
	{
		TERARK_RT_assert(serial == task->plserial, std::runtime_error);
		serial++;
		if (!G_bPrint) return;
		PipelineLockGuard lock(*step->getMutex());
		printf("step6: threadno=%d plserial=%06lu\n", threadno, task->plserial);
	}
	void step6_setup(PipelineStage* step, int threadno)
	{
		serial = 1;
		PipelineLockGuard lock(*step->getMutex());
		printf("step6_setup: threadno=%d\n", threadno);
	}
	void step6_clean(PipelineStage* step, int threadno)
	{
		TERARK_RT_assert(serial == maxNum + 1, std::runtime_error);
		PipelineLockGuard lock(*step->getMutex());
		printf("step6_clean: threadno=%d\n", threadno);
	}
	int main(int argc, char* argv[])
	{
		G_bPrint = argc >= 2 ? atoi(argv[1]) : 0;
		maxNum = argc >= 3 ? atoi(argv[2]) : TERARK_IF_DEBUG(100000, 500000);
		int bcompile = argc >= 4 ? atoi(argv[3]) : 1;

		PipelineProcessor pipeline;
		pipeline.setQueueTimeout(5);
		pipeline.setQueueSize(50);

		std::vector<int> bindArg1;
		// use the UNIX shell pipe denotation
		if (!bcompile)
			pipeline | new GeneratorStep(maxNum);

		pipeline
		| new FunPipelineStage(3,
			TerarkFuncBind(&Main::step1, this, _1, _2, _3, &bindArg1),
			"step1"
			)
		// or PPL_STEP_EX_1(this, Main, step1, 10, &bindArg1)

		| new FunPipelineStage(4, TerarkFuncBind(&Main::step2, this, _1, _2, _3), "step2")
		// or PPL_STEP_0(this, Main, step2, 2)

		| PPL_STEP_0(this, Main, step3, 0)
		| PPL_STEP_1(this, Main, step4, 1, 1.0)
		| PPL_STEP_2(this, Main, step5, 1, 2.0, std::string("abcd"))
	//	| PPL_STEP_EX_0(this, Main, step6, 0)
		;
		terark::profiling pf;
		long long t0 = pf.now();
		if (bcompile) {
			// this is the recommended usage
			//
			// compile means no generator step
			// compile just create the pipeline which is ready to consume input
			// input is from out of pipeline, here is the main thread
			pipeline.compile();

			// corresponding to GeneratorStep
			for (int i = 0; i < maxNum; ++i)
				pipeline.inqueue(new MyTask(i+2)); // send to the pipeline
			pipeline.stop(); // just set stop flag
			pipeline.wait(); // wait for all pending items to be processed
		} else {
			pipeline.start(); // start the pipeline, input is from GeneratorStep

			// wait for the pipeline to auto complete
			// pipeline.stop() is often called by GeneratorStep to indicate EOF
			pipeline.wait();
		}
		long long t1 = pf.now();
		fprintf(stderr, "time=%ld'us, average=%f'us\n", (long)pf.us(t0, t1), (double)pf.ns(t0, t1)/1000/maxNum);
		return 0;
	}
};

int main(int argc, char* argv[])
{
	Main a;
	int ret = a.main(argc, argv);
	return ret;
}

