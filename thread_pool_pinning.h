#pragma once

#include <assert.h>
#include <vector>
#include <sched.h>
#include <string.h>

class thread_pool_pinning
{
public:
    // class policies:
    // eNone - no thread pinning
    // eGlobalSet - use one global set for all workers (e.g. if all workers should share same L3 cache)
    // eWorkerPinned - set of CPUs is spread among workers, with one CPU per worker
    //                 note that there could be workers without any CPU
    //                 note that there could be CPUs not assigned to any worker
    // TODO: add eWorkerPinnedForce - if there aren't enough cores, same cores are reused for multiple workers
    enum policy_t {eNone, eGlobalSet, eWorkerPinned};
    
    // default ctor
    thread_pool_pinning() : 
        m_policy(eNone),
        m_cpu_count(0)
    {
        CPU_ZERO(&m_cpu_set);
    }

    // workers are allowed on any CPU from the list with or without overlap
    thread_pool_pinning(policy_t policy, const cpu_set_t* cpu_set) : 
        m_policy(policy), 
        m_cpu_count(CPU_COUNT(cpu_set))
    {
        // CPU_COUNT returns a signed int, but should not be negative
        assert(m_cpu_count >= 0);
        CPU_ZERO(&m_cpu_set);
        if (m_cpu_count > 0)
        {
            // copy the CPU set
            memcpy(&m_cpu_set, cpu_set, sizeof(cpu_set_t));
            // initialize CPU list from set
            for (int i = 0; i < CPU_SETSIZE; ++i)
            {
                if (CPU_ISSET(i, &m_cpu_set))
                {
                    m_cpu_list.push_back(i);
                }
            }
        }
        assert(m_cpu_list.size() == static_cast<std::size_t>(m_cpu_count));
    }

    // copy ctor does deep copy
    thread_pool_pinning(const thread_pool_pinning& other) :
        m_policy(other.m_policy),
        m_cpu_set(other.m_cpu_set),
        m_cpu_count(other.m_cpu_count),
        m_cpu_list(other.m_cpu_list)
    {
    }

    //! dont allow assignment
    const thread_pool_pinning& operator=(const thread_pool_pinning&) = delete;

    // return (by reference) the CPU list of a worker
    // return "false" in the following cases:
    // a. policy type is "none"
    // b. each worker should have its own CPU but there are too many workers
    // c. CPU set is empty
    // return "true" in any other case
    // note that the CPU list is reset in any case
    bool get_cpu_set(std::size_t worker, cpu_set_t* cpu_set) const
    {
        CPU_ZERO(cpu_set);
        if (m_cpu_count == 0)
        {
            return false;
        }

        switch (m_policy)
        {
        case eNone:
            return false;
        case eGlobalSet:
            memcpy(cpu_set, &m_cpu_set, sizeof(cpu_set_t));
            return true;
        case eWorkerPinned:
            if (worker < static_cast<std::size_t>(m_cpu_count))
            {
                CPU_SET(m_cpu_list[worker], cpu_set);
                return true;
            }
            return false;
        }

        assert(0);
        return false;
    }

    // return the policy used for assigning CPUs to threads
    policy_t policy() const
    {
        return m_policy;
    }

    std::string to_string() const
    {
        std::string output;
        switch (m_policy)
        {
        case eNone:
            output = "None";
            break;
        case eGlobalSet:
            output = "GlobalSet: ";
            break;
        case eWorkerPinned:
            output = "WorkerPinned: ";
            break;
        }

        if (m_policy != eNone)
        {
            for (auto cpu : m_cpu_list)
            {
                output += (std::to_string(cpu) + ",");
            }
        }

        return output;
    }

private:
    // policy for assigning CPUs to threads
    const policy_t m_policy;
    // list of CPU the thread pool may use
    cpu_set_t m_cpu_set;
    // number of CPUs in list
    const int m_cpu_count;
    // list of CPUs, vector representation of the set
    std::vector<int> m_cpu_list;
};

