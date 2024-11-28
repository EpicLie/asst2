#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
    const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // pthread_t threads[num_threads];
    // int thread_ids[num_threads];
    this->num_threads = num_threads;

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

typedef struct {
    IRunnable* runnable;
    int task_id;
    int num_tasks_on_threads;
    int num_total_tasks;
}ThreadData;
void* wrapper(void* arg)
{
    // std::cout << "wrapper" << std::endl;
    ThreadData* data = static_cast<ThreadData*>(arg);

    // 这样的逻辑才对啊，又自然通顺又显然。那之前我是咋错的？？？
    // 噢噢，破案了。因为我之前改bug参照的是math_operations_in_tight_for_loop这个测试的逻辑。
    // 而根据它代码的逻辑，我runTask的第二个参数传进去的应该是num_tasks_on_threads，这样才能高效。
    // 好像是，具体也记不清了。
    for (int i = data->task_id;i < data->task_id + data->num_tasks_on_threads;i++)
        data->runnable->runTask(i, data->num_total_tasks);

    delete data;
    return nullptr;
}

// 注意区分任务数和线程数 比如任务数默认可以是128,但线程数远小于这值
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // 一开始没问题是因为在这里依然是沿用了串行的代码
    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    // std::cout << "Spawn" << " " << num_threads <<"\n"<<"tasks"<<" "<<num_total_tasks<< std::endl;
    const int NUM_THREADS = std::move(this->num_threads);
    pthread_t threads[NUM_THREADS];
    int num_tasks_on_threads;

    num_tasks_on_threads = num_total_tasks / num_threads;
    // std::cout <<"numTotal: "<<num_total_tasks<<"numTasks: " <<num_tasks_on_threads << std::endl;

    for (int i = 0;i < NUM_THREADS;i++)
    {
        ThreadData* data = nullptr;
        int num_tasks_on_threads_ori = num_tasks_on_threads;
        // 函数指针没法随便转，但一般的指针都可以转成void*
        num_tasks_on_threads = (NUM_THREADS * num_tasks_on_threads < num_total_tasks) && (i == NUM_THREADS - 1) ? num_total_tasks - i * num_tasks_on_threads : num_tasks_on_threads;
        // std::cout <<"threads id:"<<i <<" num_tasks: " << num_tasks_on_threads << std::endl;

        data = new ThreadData{ runnable, i * num_tasks_on_threads_ori, num_tasks_on_threads, num_total_tasks };

        if (pthread_create(&threads[i], nullptr, wrapper, data))
        {
            std::cerr << "ERROR in creating thread" << i << std::endl;
            delete data;
            return;
        }
    }
    for (int i = 0;i < NUM_THREADS;i++)
    {
        if (pthread_join(threads[i], nullptr))
        {
            std::cerr << "ERROR in joining thread" << i << std::endl;
        }
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
    const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

// spinning 意思是 工作线程设置为常循环，每次都检查时候有任务执行。没有任务时就空转。
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //



}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
    const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
