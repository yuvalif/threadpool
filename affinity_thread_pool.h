#pragma once

#include <queue>
#include <condition_variable>
#include <vector>
#include <thread>
#include <chrono>
#include <functional>
#include <cassert>
#include <atomic>
#include "thread_pool_pinning.h"

// thread pool with multiple queue of inputs to a function
// using a queue synchronized with condition variables
// each queue belongs to a specific worker thread
template<typename Arg>
class affinity_thread_pool
{
public:
    // definitions of the family of function that can be executed by the worker
    typedef std::function<void(const Arg&, std::size_t)> FunctionType;
private:
    // number of worker threads
    const std::size_t m_number_of_workers;
    // thread pool CPU pinning information
    const thread_pool_pinning m_pinning;
    // time for submitter to wait when queue full (nanoseconds)
    // note that the notification from the worker should wake up the submitter 
    // and this timeout is just used a fallback mechanism
    const unsigned long m_submit_wait_time;
    // time to wait when idle (nanoseconds)
    // note that the notification from the submitter should wake up the worker 
    // and this timeout is just used a fallback mechanism
    const unsigned long m_wait_time;
    // internal state indicating that processing of existing work should finish
    // and no new work should be submitted
    volatile bool m_done;
    // queues of work items, one for each worker
    std::vector<std::queue<Arg>> m_queues;
    // mutex used by the empty/full conditions and related variables
    mutable std::vector<std::mutex> m_mutexes;
    // condition used to signal if a queue is empty or not empty
    std::vector<std::condition_variable> m_empty_conds;
    // condition used to signal if a queue is full or not full
    std::vector<std::condition_variable> m_full_conds;
    // the actual worker threads
    std::vector<std::thread> m_workers;
    // running counter incremented every time a submit
    // is called. this is used so that when submitting 
    // in round robin into the queue, we start every time from a different queue
    std::size_t m_running_counter;
    // number of running worker threads
    std::atomic<std::size_t> m_number_of_running_workers;
    // max size of queue
    const std::size_t m_max_queue_size;
    // definitions of the family of cleanup functions on Arg
    typedef std::function<void(const Arg&)> CleanupFunctionType;
    // cleanup function on Arg when FunctionType is not executed
    // assumption is that if Arg dtor is not enough (e.g. Arg is a pointer)
    // then FunctionType would do the cleanup
    const CleanupFunctionType m_cleanup_function;
    // Generic name assigned as a prefix to each worker thread
    const std::string m_name;

    // this is the function executed by the worker threads
    // it pull items ot of the queue until signaled to stop
    // it also passesd worker id to F
    void worker(FunctionType F, std::size_t worker_id)
    {
        assert(worker_id < m_number_of_workers);
        assert(m_number_of_running_workers < m_number_of_workers);
        ++m_number_of_running_workers;
        
        auto& q = m_queues[worker_id];
        std::condition_variable& empty_cond = m_empty_conds[worker_id];
        std::condition_variable& full_cond = m_full_conds[worker_id];
        std::mutex& m = m_mutexes[worker_id];

        while (true) 
        {
            Arg arg;
            {
                std::unique_lock<std::mutex> lock(m);
                // if queue is not empty we continue regardless or "done" indication
                if (q.empty())
                {
                    // queue is empty
                    if (m_done)
                    {
                        // queue is empty and we are done
                        return;
                    }
                    else
                    {
                        // queue is empty but we are not done yet
                        // wait to get notified, either on queue not empty of being done
                        while (!m_done && q.empty())
                        {
                            try
                            {
                                empty_cond.wait_for(lock, std::chrono::nanoseconds(m_wait_time));
                            }
                            catch(...)
                            {
                                // this should not happened 
                                // nothing we can do, just try again
                            }
                        }
                        
                        if (q.empty())
                        {
                            // done and empty
                            return;
                        }
                    }
               }

               // there is work to do
               arg = q.front();
               q.pop();
            }

            // notify that queue is not full
            full_cond.notify_one();
            // execute the work when the mutex is not locked
            F(arg, worker_id);
        }
    }

    // by default we assume the dtor of Arg takes
    // care of cleanup
    static void default_cleanup_function(const Arg&)
    {
        // no-op
    }

public:
    // indicating that any worker thread may execute the work
    static const int NoAffinity = -1;

    // return estimated worker queue size
    std::size_t queue_size(std::size_t worker_id) const
    {
        if (worker_id < m_queues.size())
        {
            std::unique_lock<std::mutex> lock(m_mutexes[worker_id]);
            return m_queues[worker_id].size();
        }
        return 0;
    }

    // return the pinning policy used
    thread_pool_pinning::policy_t pinning_policy() const
    {
        return m_pinning.policy();
    }

    // don't allow copying
    affinity_thread_pool& operator=(const affinity_thread_pool&) = delete;
    affinity_thread_pool(const affinity_thread_pool&) = delete;

    // destructor clean the threads
    ~affinity_thread_pool()
    {
        // dont wait for the threads
        if (!m_done) 
        {
            stop(false);
        }
    }

    //! constructor spawn the threads
    affinity_thread_pool(std::size_t number_of_workers, 
                                  std::size_t queue_size,
                                  FunctionType F,
                                  const thread_pool_pinning& pinning,
                                  unsigned long submit_wait_time, 
                                  unsigned long wait_time, 
                                  CleanupFunctionType C = std::bind(&affinity_thread_pool::default_cleanup_function, std::placeholders::_1),
                                  const std::string& name = "") :
        m_number_of_workers(number_of_workers),
        m_pinning(pinning),
        m_submit_wait_time(submit_wait_time),
        m_wait_time(wait_time),
        m_done(false),
        m_queues(number_of_workers),
        m_mutexes(number_of_workers),
        m_empty_conds(number_of_workers),
        m_full_conds(number_of_workers),
        m_running_counter(0),
        m_number_of_running_workers(0),
        m_max_queue_size(queue_size),
        m_cleanup_function(C),
        m_name(name)
    {
        for (auto i = 0U; i < m_number_of_workers; ++i)
        {
            try
            {
                // start all worker threads
                m_workers.push_back(std::thread(&affinity_thread_pool::worker, this, F, i));

                // Set the name of the thread based on worker ID
                const std::string workerName = m_name + "Worker" + std::to_string(i);
                // only 16 char names are allowed. so, first 15 char are passed plust null terminate
                pthread_setname_np(m_workers[i].native_handle(), workerName.substr(0,15).c_str());
                // get the CPU set for that thread according to the pinning policy
                cpu_set_t cpu_set;
                if (m_pinning.get_cpu_set(i, &cpu_set))
                {
                    pthread_setaffinity_np(m_workers[i].native_handle(), sizeof(cpu_set_t), &cpu_set);
                }
                // if pinning is not possible we just dont set any affinity for the thread
            }
            catch (...)
            {
                // failed to start a thread
                // make sure that we dont wait or all threads to start or we will wait forever
                m_done = true;
                return;
            }
        }
    }

    // submit new argument to be processed by the threads
    // blocking call
    void submit(const Arg& arg, int worker_id = NoAffinity)
    {
        assert(worker_id < static_cast<int>(m_number_of_workers) && worker_id >= NoAffinity);
        std::size_t actual_q_idx = static_cast<std::size_t>(worker_id);
        {
            if (m_done)
            {
                return;
            }
            else if (worker_id == NoAffinity)
            {
                // no affinity, find a free queue
                bool pushed = false;
                // dont always start from firts queue
                std::size_t q_idx = (++m_running_counter)%m_number_of_workers;
                for (auto i = 0U; i < m_number_of_workers; ++i)
                {
                    std::unique_lock<std::mutex> lock(m_mutexes[q_idx]);
                    if (m_queues[q_idx].size() < m_max_queue_size)
                    {
                        m_queues[q_idx].push(arg);
                        pushed = true;
                        actual_q_idx = q_idx;
                        break;
                    }
                    // try the next queue
                    q_idx = (q_idx+1)%m_number_of_workers;
                }
                if (!pushed)
                {
                    std::unique_lock<std::mutex> lock(m_mutexes[q_idx]);
                    // all queues were busy wait on arbitrary queue
                    while (!m_done && m_queues[q_idx].size() >= m_max_queue_size) 
                    {
                        try
                        {
                            m_full_conds[q_idx].wait_for(lock, std::chrono::nanoseconds(m_submit_wait_time));
                        }
                        catch(...)
                        {
                            // this should not happened 
                            // nothing we can do, just try again
                        }
                    }
                    if (m_done)
                    {
                        // marked as done while we were waiting
                        return;
                    }
                    m_queues[q_idx].push(arg);
                    actual_q_idx = q_idx;
                }
            }
            else
            {
                std::unique_lock<std::mutex> lock(m_mutexes[worker_id]);
                // has affinity, try using a specific worker
                while (!m_done && m_queues[worker_id].size() >= m_max_queue_size) 
                {
                    try
                    {
                        m_full_conds[worker_id].wait_for(lock, std::chrono::nanoseconds(m_submit_wait_time));
                    }
                    catch(...)
                    {
                        // this should not happened 
                        // nothing we can do, just try again
                    }
                }
                if (m_done)
                {
                    // marked as done while we were waiting
                    return;
                }
                m_queues[worker_id].push(arg);
            }
        }
        // assertion should fail in the case that NoAffinity was not converted to an actual queue index
        // in this case actual_q_idx will be 0xFFFFFFFF
        assert(actual_q_idx < m_number_of_workers);
        // notify that queue is not empty
        m_empty_conds[actual_q_idx].notify_one();
    }

    // submit new argument to be processed by the threads if queue has space
    // non-blocking call
    bool try_submit(const Arg& arg, int worker_id = NoAffinity)
    {
        std::size_t actual_q_idx = static_cast<std::size_t>(worker_id);
        assert(worker_id < static_cast<int>(m_number_of_workers) && worker_id >= NoAffinity);
        {
            if (m_done)
            {
                return false;
            }
            else if (worker_id == NoAffinity)
            {
                // no affinity, find a free queue
                bool pushed = false;
                // dont always start from firts queue
                std::size_t q_idx = (++m_running_counter)%m_number_of_workers;
                for (auto i = 0U; i < m_number_of_workers; ++i)
                {
                    std::unique_lock<std::mutex> lock(m_mutexes[q_idx]);
                    if (m_queues[q_idx].size() < m_max_queue_size)
                    {
                        m_queues[q_idx].push(arg);
                        pushed = true;
                        actual_q_idx = q_idx;
                        break;
                    }
                    // try the next queue
                    q_idx = (q_idx+1)%m_number_of_workers;
                }
                if (!pushed)
                {
                    // all queues were busy
                    return false;
                }
            }
            else
            {
                std::unique_lock<std::mutex> lock(m_mutexes[worker_id]);
                // has affinity, try using a specific worker
                if (m_queues[worker_id].size() >= m_max_queue_size)
                {
                    return false;
                }
                else
                {
                    m_queues[worker_id].push(arg);
                }
            }
        }
        // assertion should fail in the case that NoAffinity was not converted to an actual queue index
        // in this case actual_q_idx will be 0xFFFFFFFF
        assert(actual_q_idx < m_number_of_workers);
        // notify that queue is not empty
        m_empty_conds[actual_q_idx].notify_one();
        return true;
    }

    // stop all threads, may or may not wait to finish
    void stop(bool wait)
    {
        {
            // take lock on all workers to make sure worker threads are either waiting
            // or processing, and will check on m_done before next iteration
            std::vector<std::unique_lock<std::mutex>> all_locks;
            for (auto& m : m_mutexes)
            {
                all_locks.emplace_back(m);
            }

            if (m_done)
            {
                return;
            }
            // dont allow new submitions
            m_done = true;
            if (!wait)
            {
                // drain the queues without running F on it
                for (auto& q : m_queues)
                {
                    while (!q.empty()) 
                    {
                        m_cleanup_function(q.front());
                        q.pop();
                    }
                }
            }
        }

        // notify all that we are done
        for (auto& cond : m_empty_conds)
        {
            cond.notify_all();
        }
        for (auto& cond : m_full_conds)
        {
            cond.notify_all();
        }

        for (auto& worker : m_workers)
        {
            // join on threads until they actually finish
            try 
            {
                worker.join();
            }
            catch (...)
            {
                // could happen if F is deadlocked or if not all threads actually started
                // not much we can do here
            }
        }
    }
};
 
