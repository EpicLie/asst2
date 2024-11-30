#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <pthread.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <functional>
#include <condition_variable>
using namespace std;

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        // friend void* wrapper(void* arg, TaskSystemParallelSpawn& task);
private:
        // pthread_t* threads;
    int num_threads;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();
    friend void task(TaskSystemParallelThreadPoolSpinning*, int);
        // typedef struct {
        //     function<void(int, int, int, TaskSystemParallelThreadPoolSpinning*)> tasks_fun;
        //     int task_id, num_tasks_on_threads, num_total_tasks;
        //     TaskSystemParallelThreadPoolSpinning* cl;
        // }func;
private:
    int num_threads;
    IRunnable* runnable;
    vector<thread> workers;
    // queue<function<void(TaskSystemParallelThreadPoolSpinning*, int)>> tasks_que;
    mutex queue_mutex;
    // int task_id = 0;
    bool stop_flag = false;
    int num_tasks_on_threads = 0;
    int num_total_tasks = 0;
    int num_tasks_on_threads_last = 0;
    bool* complete_thread = nullptr;
    // bool is_first_run = 1;
    bool init = 0;
    condition_variable condition_threads;
    unique_lock<mutex> lk;
    bool last = 0;
    bool tasks_less_threads = 0;
    mutex notify_main;
    condition_variable condition_main;
    
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
            const std::vector<TaskID>& deps);
        friend void task_sleep(TaskSystemParallelThreadPoolSleeping*, int);
        void sync();
private:
    int num_threads;
    IRunnable* runnable;
    vector<thread> workers;
    // queue<function<void(TaskSystemParallelThreadPoolSleeping*, int)>> tasks_que;
    mutex queue_mutex;
    // int task_id = 0;
    bool stop_flag = false;
    int num_tasks_on_threads = 0;
    int num_total_tasks = 0;
    int num_tasks_on_threads_last = 0;
    bool* complete_thread = nullptr;
    // bool is_first_run = 1;
    bool init = 0;
    condition_variable condition_threads;
    unique_lock<mutex> lk;
    bool last = 0;
    bool tasks_less_threads = 0;
    mutex notify_main;
    condition_variable condition_main;
    volatile int threads_cnt = 0;
};

#endif
