#pragma once

#include <thread>
#include <chrono>
#include <vector>
#include <queue>
#include <boost/lockfree/spsc_queue.hpp>
#include "thread_pool_pinning.h"

// thread pool with multiple queues of inputs to a function
// using a boost spsc lockfree queue
// each queue belongs to a specific worker thread
template<typename Arg>
class affinity_thread_pool_lockfree
{
public:
    // definitions of the family of function that can be executed by the worker
    typedef std::function<void(const Arg&, std::size_t)> FunctionType;
private:
    // number of threads
    const std::size_t m_number_of_workers;
    // thread pool CPU pinning information
    const thread_pool_pinning m_pinning;
    // time for submitter to wait when queue full (nanoseconds)
    const unsigned long m_submit_wait_time;
    // time to wait when idle (nanoseconds)
    const unsigned long m_wait_time;
    // indication that the pool should not receive new Args
    volatile bool m_done;
    // single producer single consumer queue is used
    // note that each worker has its own queue, so there is a single consumer
    using queue_t = boost::lockfree::spsc_queue<Arg, boost::lockfree::fixed_sized<true>>;
    // queues of work items, one for each worker
    std::vector<std::unique_ptr<queue_t>> m_queues;
    // following counters are used to estimate the queue size
    // note that due to race conditions size may be off
    // however, error does not grow over time
    // note that this is done to prevent use of atomic counter for size of queue
    std::vector<unsigned long long> m_queues_push;
    std::vector<unsigned long long> m_queues_pop;
    // the actual worker threads
    std::vector<std::thread> m_workers;
    // running counter incremented every time a submit
    // is called. this is used so that when submitting 
    // in round robin into the queue, we start every time from a different queue
    std::size_t m_running_counter;
    // number of running threads
    std::atomic<std::size_t> m_number_of_running_workers;
    // definitions of the family of cleanup functions on Arg
    typedef std::function<void(const Arg&)> CleanupFunctionType;
    // cleanup function on Arg when FunctionType is not executed
    // assumption is that if Arg dtor is not enough (e.g. Arg is a pointer)
    // then FunctionType would do the cleanup
    const CleanupFunctionType m_cleanup_function;
    // Generic name assigned as a prefix to each worker thread
    const std::string m_name;

    // class wrapping around the function allow passing the worker id
    // at the ctor instead of every call
    class InternalFunctionType
    {
    private:
        const FunctionType F;
        const std::size_t m_worker_id;
    public:
        // ctor
        InternalFunctionType(FunctionType f, std::size_t worker_id) : F(f), m_worker_id(worker_id) {}
        // functor
        void operator()(const Arg& arg) const
        {
            F(arg, m_worker_id);
        }
    };

    // this is the function executed by the worker threads
    // it pull items ot of the queue until signaled to stop
    // it also passes the worker_id to F
    void worker(FunctionType F, std::size_t worker_id)
    {
        assert(worker_id < m_number_of_workers);
        assert(m_number_of_running_workers < m_number_of_workers);
        ++m_number_of_running_workers;

        auto& q = m_queues[worker_id];
        auto& pop_count = m_queues_pop[worker_id]; 
        const InternalFunctionType InternalF(F, worker_id);

        while (true) 
        {
#if BOOST_VERSION > 105300
            const unsigned int consumed_args = q->consume_all(InternalF);
            if (consumed_args == 0)
            {
                // if queue is empty and work is done - the thread exits
                if (m_done) 
                {
                    return;
                }
                // wait until there is something in queue
                std::this_thread::sleep_for(std::chrono::nanoseconds(m_wait_time));
            }
            else
            {
                pop_count += consumed_args;
            }
#else
            Arg arg;
            if (!q->pop(arg))
            {
                // if queue is empty and work is done - the thread exits
                if (m_done) 
                {
                    return;
                }
                // wait until there is something in queue
                std::this_thread::sleep_for(std::chrono::nanoseconds(m_wait_time));
            }
            else
            {
                InternalF(arg);
                ++pop_count;
            }
#endif
        }
    }

    // by default we assume the dtor of Arg takes
    // care of cleanup
    static void default_cleanup_function(const Arg&)
    {
        //! no-op
    }

public:
    // indicating that work may be executed on any thread
    static const int NoAffinity = -1;

    // return estimated worker queue size
    size_t queue_size(size_t worker_id) const
    {
        // A valid worker_id should return valid value
        // Retrun 0 in case of worker_id > number of running workers, 
        // This is a valid usecase case for PDB table workerTable(in case of specific thread exclusion).
        assert(worker_id < std::thread::hardware_concurrency());
        if (worker_id < m_queues_push.size())
        {
            // due to race conditions it may be that pop > push
            // in such a case we will return zero
            // note that if: (push - pop) > 9223372036854775807 we will return zero here
            // this is, however, impossible since the max queue size is 65535
            const long long int current_size = m_queues_push[worker_id] - m_queues_pop[worker_id];
            return std::max(current_size, 0LL);
        }
        return 0;
    }

    // return the pinning policy used
    thread_pool_pinning::policy_t pinning_policy() const
    {
        return m_pinning.policy();
    }

    // don't allow copying
    affinity_thread_pool_lockfree& operator=(const affinity_thread_pool_lockfree&) = delete;
    affinity_thread_pool_lockfree(const affinity_thread_pool_lockfree&) = delete;

    // destructor clean the threads but dont wait for them to finish
    ~affinity_thread_pool_lockfree()
    {
        // stop all threads without finishing the work
        if (!m_done)
        {
            stop(false);
        }
    }

    // constructor spawn the threads
    affinity_thread_pool_lockfree(std::size_t number_of_workers, 
                                  std::size_t queue_size,
                                  FunctionType F,
                                  const thread_pool_pinning& pinning,
                                  unsigned long submit_wait_time, 
                                  unsigned long wait_time, 
                                  CleanupFunctionType C = std::bind(&affinity_thread_pool_lockfree::default_cleanup_function, std::placeholders::_1),
                                  const std::string& name = "") :
        m_number_of_workers(number_of_workers),
        m_pinning(pinning),
        m_submit_wait_time(submit_wait_time),
        m_wait_time(wait_time),
        m_done(false),
        m_queues_push(m_number_of_workers),
        m_queues_pop(m_number_of_workers),
        m_running_counter(0),
        m_number_of_running_workers(0),
        m_cleanup_function(C),
        m_name(name)
    {
        // Create all queues first.
        for (auto i = 0; i < m_number_of_workers; ++i)
        {
            m_queues.emplace_back(new queue_t(queue_size));
        }
        // Reset queue size counters
        for (auto i = 0; i < m_number_of_workers; ++i)
        {
            m_queues_push[i] = 0;
            m_queues_pop[i] = 0;
            try
            {
                // start all worker threads
                m_workers.push_back(std::thread(&affinity_thread_pool_lockfree::worker, this, F, i));

                // Set the name of the thread based on worker ID
                const std::string workerName = m_name + "Worker" + std::to_string(i);
                // only 16 char names are allowed. so, first 15 char are passed plust null terminate
                pthread_setname_np(m_workers[i].native_handle(), workerName.substr(0,15).c_str());
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

        // make sure we dont start before all threads are up and running
        // this would prevent a case where we join on thread that didnt start when stopping
        while (m_number_of_running_workers < m_number_of_workers)
        {
            // let them start
            std::this_thread::yield();
        }
    }

    // submit new argument to be processed by the threads
    // blocking call
    void submit(const Arg& arg, int worker_id = NoAffinity)
    {
        assert(worker_id < static_cast<int>(m_number_of_workers) && worker_id >= NoAffinity);

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
            while (!m_done && !pushed)
            {
                for (auto i = 0; i < m_number_of_workers; ++i)
                {
                    if (m_queues[q_idx]->push(arg))
                    {
                        // increment the queue size on successful push
                        ++m_queues_push[q_idx];
                        pushed = true;
                        break;
                    }
                    // try the next queue
                    q_idx = (q_idx+1)%m_number_of_workers;
                }

                // wait until queue has space
                std::this_thread::sleep_for(std::chrono::nanoseconds(m_submit_wait_time));
            }
        }
        else
        {
            // has affinity, try using a specific worker
            while (!m_queues[worker_id]->push(arg))
            {
                // queue is full, wait until queue has space
                std::this_thread::sleep_for(std::chrono::nanoseconds(m_submit_wait_time));
            }
            // increment the queue size on successful push
            ++m_queues_push[worker_id];
        }
    }

    // submit new argument to be processed by the threads if queue has space
    // non-blocking call
    bool try_submit(const Arg& arg, int worker_id = NoAffinity)
    {
        assert(worker_id < static_cast<int>(m_number_of_workers) && worker_id >= NoAffinity);
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
            for (auto i = 0; i < m_number_of_workers; ++i)
            {
                if (m_queues[q_idx]->push(arg))
                {
                    // increment the queue size on successful push
                    ++m_queues_push[q_idx];
                    pushed = true;
                    break;
                }
                // try the next queue
                q_idx = (q_idx+1)%m_number_of_workers;
            }

            return pushed;
        }
        else
        {
            // has affinity, try using a specific worker
            if (m_queues[worker_id]->push(arg))
            {
                // increment the queue size on successful push
                ++m_queues_push[worker_id];
                return true;
            }
            return false;
        }
    }

    // stop all threads, may or may not wait to finish
    void stop(bool wait)
    {
        // no need to call twice
        if (m_done)
        {
            return;
        }

        // dont allow new submitions
        m_done = true;

        if (!wait)
        {
            // drain the queues
            for (auto& q : m_queues)
            {
#if BOOST_VERSION > 105300
                q->consume_all(m_cleanup_function);
#else
                Arg arg;
                while(q->pop(arg))
                {
                    m_cleanup_function(arg);
                }
#endif
            }
            // Reset all queue push/pop counters to zero
            for (auto i = 0; i < m_number_of_workers; ++i)
            {
                m_queues_push[i] = 0;
                m_queues_pop[i] = 0;
            }
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
                // could happen if F is deadlocked
                // not much we can do here
            }
        }
    }
};

