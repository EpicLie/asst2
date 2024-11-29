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

// 原来是编译器优化的问题...必须设置volatile
// 现在的问题是，频繁修改共享变量导致其他线程被严重阻塞，降低了并行性

// 共享变量问题解决，但是速度提升不大
// 因为忙等的原因，导致时间片轮转被白白浪费。引入线程阻塞

// 忙等问题解决，速度提升巨大，但还是不够快。
// 猜测是主线程被频繁轮转，浪费了时间。

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

// spinning 意思是 工作线程设置为常循环，每次都检查时候有任务执行。没有任务时就空转。
// 这个任务的目的是：如super_light_test这种每次进入run函数时都要重新创建线程造成开销。因此采用线程池先一次创建完成。
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->complete_thread = new bool[this->num_threads];
    for (int i = 0;i < num_threads;i++)
    {
        thread worker;
        workers.emplace_back(move(worker));
        // cout << "构造第" << i << "个线程" << endl;
        this->complete_thread[i] = 0;// 初始时设定为未完成
        // if (num_total_tasks < this->num_threads)
        // {
        //     cout << "break in construct" << endl;
        //     break;
        // }
            
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    // cout << "开始析构" << endl;
    this->stop_flag = true;
    // cout << "stop_flag" << endl;
    this->init = 1;
    // cout << "init" << endl;
    for (int i = 0;i < num_threads;i++)
    {
        this->complete_thread[i] = 0;
    }
    this->last = 1;
    // cout << "complete" << endl;
    this->condition.notify_all();
    // cout << "notify" << endl;
    for (int i = 0;i < this->num_threads;i++)
    {
        this->workers[i].join();
        if (this->tasks_less_threads)
            break;
        // cout << "线程" << i << "已销毁" << endl;
        // delete this->complete_thread;// 销毁指针
    }
}

// 其实就是让这个不要结束，一直循环，直到析构的时候自动清理
void task(TaskSystemParallelThreadPoolSpinning* cl, int thread_id)
{
    // try_lock 尝试锁定互斥锁，但不会阻塞。如果互斥锁当前未被锁定，则立即锁定并返回 true；
    // 如果互斥锁已经被其他线程锁定，则立即返回 false，不会阻塞当前线程
    // 为了简化情景，完成了自己的那一份就不能再获得任务
    // bool last = 0;
    while (!cl->stop_flag)
    {
        // if (cl->complete_thread[thread_id]||!cl->init)
        // {
        //     // cout << "线程" << thread_id << "由于无任务可做，正在空转" << endl;
        //     continue;
        // }
        unique_lock<mutex> lk(cl->queue_mutex);
        cl->condition.wait(lk, [cl, thread_id] {return !(cl->complete_thread[thread_id] || !cl->init);});

        lk.unlock();
        cl->condition.notify_all();
        if (cl->last)
            continue;
        // int task_id = 0;
        int num_tasks_on_threads = 0;
        // {
        //     while (!cl->queue_mutex.try_lock())
        //     {
        //         // cout << "线程" << thread_id << "由于获取写锁失败，正在空转" << endl;
        //     }
        //     // 此处可以执行临界区代码
        //     // 设置id等
        //     // cout << "线程" << thread_id << "获得锁" << endl;
        //     task_id = cl->task_id;
        //     if (cl->task_id + cl->num_tasks_on_threads_last >= cl->num_total_tasks)
        //     {
        //         cl->task_id = cl->num_total_tasks;
        //         num_tasks_on_threads = cl->num_tasks_on_threads_last;
        //     }
        //     else {
        //         cl->task_id = cl->task_id + cl->num_tasks_on_threads;
        //         num_tasks_on_threads = cl->num_tasks_on_threads;
        //     }
        //     cl->queue_mutex.unlock();
        // }
        int task_id = thread_id * cl->num_tasks_on_threads;
        if (task_id + cl->num_tasks_on_threads_last >= cl->num_total_tasks)
            num_tasks_on_threads = cl->num_tasks_on_threads_last;
        else
            num_tasks_on_threads = cl->num_tasks_on_threads;

        // 执行分配的任务
        for (int i = task_id;i < task_id + num_tasks_on_threads;i++)
            cl->runnable->runTask(i, cl->num_total_tasks);

        cl->complete_thread[thread_id] = 1;
        
        
        cl->condition.notify_all();
        // cout << "线程" << thread_id << "执行完成" << endl;

    }

}

// 我认为想通过修改run函数以实现检测所有任务是否完成，必须要改参数列表传入的参数，从而得知总共的bulk数，
// 否则只能等析构的时候才能知道是不是做完了
// 再者，run函数创建的线程可以独立于run函数的生命周期存在。这样就更简单了。
// 不行啊，不能改test的代码。
// 哦噢，看懂了，只需要确定一个bulk内的tasks完成即可。
// 当读不懂题意的时候还得看看原文。

// 看了其他人的代码，果然要做析构函数......我想了一下午想不出来没有析构要怎么做
void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    // {
    //     this->queue_mutex.lock();
        // cout << "主线程获得锁" << endl;
    // this->queue_mutex.lock();
    this->lk = unique_lock<mutex>(this->queue_mutex);
    // cout << "hhh" << endl;
    // this->condition.wait(this->lk);
    // cout << "hhh" << endl;
    for (int i = 0;i < num_threads;i++)
    {
        this->complete_thread[i] = 0;
    }
    this->stop_flag = false;
    this->runnable = runnable;
    this->num_tasks_on_threads = num_total_tasks / num_threads;
    // cout << this->num_tasks_on_threads << endl;
    this->num_total_tasks = num_total_tasks;
    // this->task_id = 0;
    this->num_tasks_on_threads_last = (num_total_tasks % this->num_threads != 0) ? num_total_tasks % this->num_threads + num_tasks_on_threads : num_tasks_on_threads;
    // cout << this->num_tasks_on_threads_last << endl;
    //     this->queue_mutex.unlock();
    // }
        


    // // 每次进入run函数都加入任务队列
    // for (int i = 0;i < num_total_tasks;i += num_tasks_on_threads)
    // {
    //     if (i + num_tasks_on_threads_last >= num_total_tasks)
    //     {
    //         function<void(TaskSystemParallelThreadPoolSpinning*, int)> f = task;
    //         this->tasks_que.push(f);
    //         break;
    //     }
    //     function<void(TaskSystemParallelThreadPoolSpinning*, int)> f = task;
    //     this->tasks_que.push(f);
    //     // cout << "任务队列已加入第" << i << "个任务" << endl;
    // }

    this->init = 1;
    lk.unlock();
    // cout << "hhhh"<< endl;
    this->condition.notify_all();
    // cout << "hhhh" << endl;
    // 分配任务

    for (int i = 0;i < this->num_threads;i++)
    {
        
        if (this->is_first_run)
        {
            this->workers[i] = thread(task, this, i);
            
            if (i == this->num_threads - 1)
                this->is_first_run = 0;
            if (num_total_tasks < this->num_threads)
            {
                // cout << "break" << endl;
                this->is_first_run = 0;
                this->tasks_less_threads = 1;
                break;
            }
                // cout << "构造第" << i << "个线程" << endl;
        }
            // this->tasks_que.pop();
            // cout << "第" << i << "个线程正式启动" << endl;
        
    }

    // 如何在不把所有线程join（这样线程就无了）的情况下得知此轮run是否把bulk中的所有任务完成？
    for (int i = 0;i < this->num_threads;i++)
    {
        // while (!this->complete_thread[i])
        // {
        //     // bool a = this->complete_thread[i];
        //     // if (a)
        //     //     break;
        //     // cout << "线程" << i << "还未完成" << endl;
        // }
        // cout << "线程" << i << "已完成" << this->complete_thread[i] << endl;
        this->lk = unique_lock<mutex>(this->queue_mutex);
        // cout << "线程" << i << "已完成" << this->complete_thread[i] << endl;
        this->condition.wait(this->lk, [this, i] {return this->complete_thread[i];});
        // cout << "线程" << i << "已完成" << this->complete_thread[i] << endl;
        lk.unlock();

        // this->condition.notify_all();
        
        if (this->tasks_less_threads)
            break;
    }
    this->init = 0;
    // cout << "此时任务队列中还有" << this->tasks_que.size() << "个任务" << endl;

};

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
