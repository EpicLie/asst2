#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <string>
#include <assert.h>

#include "tasksys.h"
#include "tests.h"

#define DEFAULT_NUM_THREADS 8
#define DEFAULT_NUM_TIMING_ITERATIONS 3


void usage(const char* progname, std::string *testnames, int num_tests) {
    printf("Usage: %s [options] testname\n", progname);
    printf("Program Options:\n");
    printf("  -n  --num_threads  <INT>      Number of threads: <INT> (default=%d)\n", DEFAULT_NUM_THREADS);
    printf("  -i  --num_timing_iterations <INT> Number of timing iterations: <INT> (default=%d)\n", DEFAULT_NUM_TIMING_ITERATIONS);
    printf("  -?  --help                    This message\n");
    printf("Valid testnames are:");
    for(int i = 0; i < num_tests; i++) {
        printf(" %s%c", testnames[i].c_str(), (char)((i+1 == num_tests) ? '\n' : ','));
    }
}

enum TaskSystemType {
    SERIAL,
    PARALLEL_SPAWN,
    PARALLEL_THREAD_POOL_SPINNING,
    PARALLEL_THREAD_POOL_SLEEPING,
    N_TASKSYS_IMPLS, // This must be in the last position.
};

// 选取实现方式
ITaskSystem* selectTaskSystemRefImpl(int num_threads, TaskSystemType type) {
    assert(type < N_TASKSYS_IMPLS);

    if (type == SERIAL) {
        return new TaskSystemSerial(num_threads);
    } else if (type == PARALLEL_SPAWN) {
        return new TaskSystemParallelSpawn(num_threads);
    } else if (type == PARALLEL_THREAD_POOL_SPINNING) {
        return new TaskSystemParallelThreadPoolSpinning(num_threads);
    } else if (type == PARALLEL_THREAD_POOL_SLEEPING) {
        return new TaskSystemParallelThreadPoolSleeping(num_threads);
    } else {
        return NULL;
    }
}

int main(int argc, char** argv)
{
    const int n_tests = 31;
    int num_threads = DEFAULT_NUM_THREADS;
    int num_timing_iterations = DEFAULT_NUM_TIMING_ITERATIONS;

    // 函数指针数组  相关函数定义在.h文件中
    TestResults(*test[n_tests])(ITaskSystem*) = {
        simpleTestSync,
        simpleTestAsync,
        pingPongEqualTest,
        pingPongUnequalTest,
        superLightTest,
        superSuperLightTest,
        recursiveFibonacciTest,
        mathOperationsInTightForLoopTest,
        mathOperationsInTightForLoopFewerTasksTest,
        mathOperationsInTightForLoopFanInTest,
        mathOperationsInTightForLoopReductionTreeTest,
        spinBetweenRunCallsTest, //
        mandelbrotChunkedTest,
        pingPongEqualAsyncTest,
        pingPongUnequalAsyncTest,
        superLightAsyncTest,
        superSuperLightAsyncTest,
        recursiveFibonacciAsyncTest,
        mathOperationsInTightForLoopAsyncTest,
        mathOperationsInTightForLoopFewerTasksAsyncTest,
        mathOperationsInTightForLoopFanInAsyncTest,
        mathOperationsInTightForLoopReductionTreeAsyncTest,
        mandelbrotChunkedAsyncTest,
        spinBetweenRunCallsAsyncTest,
        simpleRunDepsTest,
        strictDiamondDepsTest,
        strictGraphDepsSmall,
        strictGraphDepsMedium,
        strictGraphDepsLarge,
    };

    std::string test_names[n_tests] = {
        "simple_test_sync",
        "simple_test_async",
        "ping_pong_equal",
        "ping_pong_unequal",
        "super_light",
        "super_super_light",
        "recursive_fibonacci",
        "math_operations_in_tight_for_loop",
        "math_operations_in_tight_for_loop_fewer_tasks",
        "math_operations_in_tight_for_loop_fan_in",
        "math_operations_in_tight_for_loop_reduction_tree",
        "spin_between_run_calls",
        "mandelbrot_chunked",
        "ping_pong_equal_async",
        "ping_pong_unequal_async",
        "super_light_async",
        "super_super_light_async",
        "recursive_fibonacci_async",
        "math_operations_in_tight_for_loop_async",
        "math_operations_in_tight_for_loop_fewer_tasks_async",
        "math_operations_in_tight_for_loop_fan_in_async",
        "math_operations_in_tight_for_loop_reduction_tree_async",
        "mandelbrot_chunked_async",
        "spin_between_run_calls_async",
        "simple_run_deps_test",
        "strict_diamond_deps_async",
        "strict_graph_deps_small_async",
        "strict_graph_deps_med_async",
        "strict_graph_deps_large_async",
    };
 
    // Parse commandline options
    int opt;
    static struct option long_options[] = {
        {"num_threads",           1, 0,  'n'},
        {"num_timing_iterations", 1, 0,  'i'},
        {"help",                  0, 0,  '?'},
    };

    while ((opt = getopt_long(argc, argv, "n:i:?", long_options, NULL)) != EOF) {

        switch (opt) {
        case 'n':
            num_threads = atoi(optarg);
            break;
        case 'i':
            num_timing_iterations = atoi(optarg);
            break;
        case '?':
        default:
            usage(argv[0], test_names, n_tests);
            return 1;
        }
    }

    if (optind + 1 > argc) {
        fprintf(stderr, "Error: missing test_name!\n");
        usage(argv[0], test_names, n_tests);
        return 1;
    }

    std::string test_name = argv[optind];

    bool found = false;
    for (int test_id = 0; test_id < n_tests; test_id++) {
        if (test_names[test_id].compare(test_name) != 0) {
            continue;
        }

        found = true;
        printf("============================================================="
               "======================\n");
        printf("Test name: %s\n", test_names[test_id].c_str());
        printf("============================================================="
               "======================\n");
        // 每一种实现都遍历一遍 包括有顺序/多线程
        for (int i = 0; i < N_TASKSYS_IMPLS; i++) {
            double minT = 1e30;
            for (int j = 0; j < num_timing_iterations; j++) {

                // Create a new task system
                // 对于顺序执行，虽然接受参数num_threads，但在构造类的时候什么都不会做，只是返回ITaskSystemSerial的类指针
                ITaskSystem* t = selectTaskSystemRefImpl(num_threads, (TaskSystemType)i);
                // printf("%d",SERIAL);  枚举类型是可以直接当成变量使用的。在llvm中的实现其实就是定义了一个变量然后将其赋值而已。默认是从0开始
                // 对于强制转换： int colorInt = 1;
                // Color color = (Color)colorInt;
                // llvm中的实现是：
                // % colorInt = alloca i32
                // store i32 1, i32*% colorInt
                // % colorIntVal = load i32, i32*% colorInt
                // % color = trunc i32 % colorIntVal to i8

                // Run test
                // 选取对应的测试函数，并传入参数执行
                // results包含是否通过和用时信息
                TestResults result = test[test_id](t);

                // Check that the test result was correct
                if (!result.passed) {
                    printf("ERROR: Results did not pass correctness check! (iter=%d, ref_impl=%s)\n",
                        j, t->name());
                    exit(1);
                }

                minT = std::min(minT, result.time);

                // TODO: do this better
                if( j+1 == num_timing_iterations) {
                    printf("[%s]:\t\t[%.3f] ms\n", t->name(), minT * 1000);
                }

                // Shutdown task system so each timing run is from a clean start
                delete t;
            }
        }
        printf("============================================================="
               "======================\n");
    }
    if (!found) {
        fprintf(stderr, "Error: invalid test_name!\n");
        usage(argv[0], test_names, n_tests);
        return 1;
    }

    return 0;
}
