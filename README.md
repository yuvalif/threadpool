# threadpool
## Background
Unlike some existing implementations for thread pool (see benchmark [here](https://github.com/yuvalif/threadpool_benchmark)), where the function to be executed by the thread is submitted together with its arguments, in the implementations here, the functions is passed in the constructor of the pool, and only its arguments are passed when new work needs to be executed by the pool. This is under the assumption that in most systems there is only a handfull of functions that needs to be executed over and over for different arguments. In such a case, multiple pools could be constructed, one for each function.

In addition, even thogh all threads use the same functions, different arguments may be submnitted to speficic workers. This, for example, would allow enforcing FCFS order between the submitter thread and the worker thread. Another case for this is where `F` is not thread safe for certain arguments, if this issue exists, these arguments could be restricted to a specific worker.
## Headers
The implementation is entirely in header files:
* `affinity_thread_pool.h`: has implementation of the pool based on mutex and condition variable, and has no dependencies other that standrad libraries
* `affinity_thread_pool_lockfree.h`: has implementation of the pool based on a lockfree single-producer-single-consumer queue (SPSC) from boost. Hece require the boost library headers to be available (no linking against boost is needed)
* `thread_pool_pinning.h`: this is a utility class that could be used by the user of the thread pool to pin the different threads to specific CPUs. It is an optioonal feature, and, by default, no pinning is done. In some cases it may be useful to pin the threads of the pool to the same NUMA node where the thread that submit the work reside, minimizing L3 cache misses. In more realtime usecases, it may be better to pin the threads to specific cores. The different possible policies that could be reflected by the class are:
  * no pinning
  * global set: use one global set for all workers (e.g. if all workers should share same L3 cache)
  * workers pinned: set of CPUs is spread among workers, with one CPU per worker (note that there could be workers without any CPU and that there could be CPUs not assigned to any worker)
## Thread Safety
As this is a thread pool, it is intended to run in a multi-threaded environment. However, implementation is uder the assumption that there is a single thread that uses the pool (calls `submit()`, `try_submit()` and `stop()`). Note that if `F` or the destructor or constructor of `Arg`are not thread safe, the execution of it from different worker threads will cause race conditions. This could be partially mitigated by using the worker affinity when handling specific Args. The pool is thread safe at class level.
## Interfaces
Although the implementation is different, same interface exist for both classes:
* **Constructor**
  * Template argument `Arg`: the type that the worker function is expecting to perform work on
  * `number_of_workers`: number of worker threads to spawn. In most cases, this should not exceed the number of cores on the machine
  * `queue_size`: number of arguments in the queue between the submitter thread and the worker threads
  * `F`: the function that performs the work. it is expected to get two parameters: `const Arg&` and an unsigned integer holding the ID of the worker thread
  * `pinning`: thread pinning policy. Passing an empty object `thread_pool_pinning()` will cause no pinning of the worker threads
  * `submit_wait_time`: if `submit()` is used, and the queue is full, the submitter thread would sleep the amount og nanoseconds given here before retrying to submit again
  * `wait_time`: internal wai time in nanoseconds in case that there is no work to do by a worker thread
  * `C`: cleanup function called inside the `stop()` call, in case that the destructor of Arg is not sufficient to do resource cleanup (e.g. Arg is a pointer)
  * `name`: name of the pool, appended to the name of the worker threads bot easier debugging (e.g. when using `top -H`)
* **submit()**: blocking submittion of argument to the function. An indication of a specific worker that should handle this work may be added
* **try_submit()**: non-blocking submittion of argument to the function. An indication of a specific worker that should handle this work may be added. Returns whether submission was successfull
* **stop()**: stops all worker threads. May receive an indication if just needs to do a cleanp or actual execution of all pending work is needed
* **queue_size()**: return an estimated size of the queue towards a specific worker


