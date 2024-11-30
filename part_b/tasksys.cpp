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

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

// 其实数据上没有依赖关系，但是程序上有执行先后的关系
const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}


void task_sleep(TaskSystemParallelThreadPoolSleeping* cl, int thread_id)
{

    while (!cl->stop_flag)
    {
        unique_lock<mutex> lk(cl->queue_mutex);
        cl->condition_threads.wait(lk, [cl, thread_id] {return !(cl->complete_thread[thread_id] || !cl->init);});

        lk.unlock();

        // 避免最后一轮被重复执行
        if (cl->last)
            break;

        int num_tasks_on_threads = 0;
        int task_id = thread_id * cl->num_tasks_on_threads;
        if (task_id + cl->num_tasks_on_threads_last >= cl->num_total_tasks)
            num_tasks_on_threads = cl->num_tasks_on_threads_last;
        else
            num_tasks_on_threads = cl->num_tasks_on_threads;

        // 执行分配的任务
        for (int i = task_id;i < task_id + num_tasks_on_threads;i++)
            cl->runnable->runTask(i, cl->num_total_tasks);

        // 尤其注意：为了避免因为就绪队列中线程的随机性导致的读脏数据的问题，临界区资源一定要加锁
        // 备忘一下：简单来说，设想这样的情形：
        // 因为乱序的原因（先执行了后面的任务和线程），在后面for循环一个个检查有没有完成任务时，因为一个线程没完成（该线程是最后才被完成的），主线程阻塞；
        // 然后，因为别的任务/线程完成了，唤醒了主线程。但此时主线程不会立即执行，而是在就绪队列中排队
        // 碰巧，主线程排队排到了未完成的最后的那个线程的前面。在主线程从内存中恰好读取了最后未完成线程的complete_thread[i]时，时间片到了。
        // 此时轮转到了未完成线程，同时主线程在后面排队。未完成线程完成了任务，将对应的complete_thread[i]置1,然后唤醒了主线程（其实此时主线程是醒的），然后时间片结束
        // 主线程继续，因此返回了脏数据complete_thread[i]，再次阻塞。而此时已经没有线程能唤醒它了。由此死锁。

        // 这里，不同线程之间的complete_thread不竞争，但是它们都和主线程竞争。
        auto lk1 = unique_lock<mutex>(cl->notify_main);
        cl->complete_thread[thread_id] = 1;
        cl->threads_cnt++;

        if (cl->threads_cnt == cl->num_threads)
        {
            lk1.unlock();
            cl->condition_main.notify_one();
            // cout << "返回了主线程" << endl;
        }
        if (lk1.owns_lock())
            lk1.unlock();
        // cout << "线程" << thread_id << "执行完成" << endl;

    }

}


void TaskSystemParallelThreadPoolSleeping::runbulk(IRunnable* runnable, int num_total_tasks) {

    lk = unique_lock<mutex>(runtask_bulk);
    condition_runtask.wait(lk, [this] {return !this->is_construct;});
    lk.unlock();
    if (this->ready_que.empty())
        return;
    auto a = this->ready_que.front();

    runnable = a.task;
    num_total_tasks = a.num_total_tasks;

    this->ready_que.pop();

    this->lk = unique_lock<mutex>(this->queue_mutex);
    for (int i = 0;i < num_threads;i++)
    {
        this->complete_thread[i] = 0;
    }
    this->stop_flag = false;
    this->runnable = runnable;
    this->num_tasks_on_threads = num_total_tasks / num_threads;
    // cout << this->num_tasks_on_threads << endl;
    this->num_total_tasks = num_total_tasks;
    this->num_tasks_on_threads_last = (num_total_tasks % this->num_threads != 0) ? num_total_tasks % this->num_threads + num_tasks_on_threads : num_tasks_on_threads;
    this->init = 1;
    this->threads_cnt = 0;
    this->lk.unlock();
    this->condition_threads.notify_all();

    auto lk1 = unique_lock<mutex>(this->notify_main);

    this->condition_main.wait(lk1, [this] {return this->threads_cnt == this->num_threads;});

    // cout << "主线程已完成" << endl;
    lk1.unlock();

    this->init = 0;
    // this->works_are_completed = 1;
    // if (!this->ready_que.empty())
    // {
    //     auto bulk = ready_que.front();
    runbulk(nullptr, 0);
    //     this->ready_que.pop();
    // }
    this->bulks_are_completed = 1;
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    this->num_threads = num_threads;
    this->complete_thread = new bool[this->num_threads];
    // this->readyInQue = new bool[this->num_threads];
    for (int i = 0;i < num_threads;i++)
    {
        // 通过这种方法，将id和线程和任务进行绑定。想唤醒哪个唤醒哪个

        thread worker = thread(task_sleep, this, i);
        this->workers.emplace_back(move(worker));
        // mutex worker_mutex;
        // this->thread_mutex.emplace_back(worker_mutex);
        // condition_variable worker_condition;
        // this->notify_thread.emplace_back(worker_condition);
        // auto lk = unique_lock<mutex>(this->thread_mutex[i]);
        // this->notify_thread[i].wait(lk)
        // cout << "构造第" << i << "个线程" << endl;
        this->complete_thread[i] = 0;// 初始时设定为未完成
        // this->readyInQue[i] = 0;
    }
    // IRunnable* a = nullptr;
    // int num_total_tasks = 0;
    this->is_construct = 1;
    thread runblk = thread(bind(&TaskSystemParallelThreadPoolSleeping::runbulk, this, nullptr, 0));
    this->workers.emplace_back(move(runblk));
    // runbulk(nullptr, 0);

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    this->stop_flag = true;

    this->init = 1;

    for (int i = 0;i < num_threads;i++)
    {
        this->complete_thread[i] = 0;
    }
    this->last = 1;

    this->condition_threads.notify_all();

    for (int i = 0;i < this->num_threads;i++)
    {
        this->workers[i].join();
        if (this->tasks_less_threads)
            break;
        // cout << "线程" << i << "已销毁" << endl;
        // delete this->complete_thread;// 销毁指针 不能，会造成double free
    }
    

}

// 每个taskbulk有很多个tasks，这些会分给线程来执行。同时返回taskbulk的ID
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //


}



TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    // 首先检查先决任务是否已经加入就绪队列
    // for (int i = 0;i < deps.size();i++)
    // {
    //     // at不存在会抛出异常，find不会
    //     if (this->readyInQue.find(deps[i]) != this->readyInQue.end()&&
    //         this->readyInQue[deps[i]] == 1)
    //         continue;
    //     else
    //     {
    //         this->taskID_cnt++;
    //         this->readyInQue[taskID_cnt] = 0;//阻塞
    //         async_task a;
    //         a.task = runnable;
    //         a.taskID = this->taskID_cnt;
    //         this->ready_que.push(a);
    //         return a.taskID;
    //     }
    // }
    this->taskID_cnt++;
    // this->readyInQue[this->taskID_cnt] = 1;//就绪
    async_task a;
    a.task = runnable;
    a.taskID = this->taskID_cnt;
    a.num_total_tasks = num_total_tasks;
    this->ready_que.push(a);
    this->is_construct = 0;
    this->condition_runtask.notify_all();
    return a.taskID;
        // 检查是否有在阻塞队列中的任务可以被恢复
    // for (int i = 0;i < this->shutdown_que.size();i++)
    // {
    //     if(this->shutdown_que[i].taskID)
    // }

}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    this->workers[this->num_threads].join();

    return;
}
