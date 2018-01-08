#include <cmath>    // for std::sqrt
#include <cstdlib>  // for std::rand
#include <thread>   // for std::thread
#include <atomic>   // for std::atomic
#include <string>
#include <algorithm>
#include <vector>	// for std:vector
#include <iostream>
#include <chrono>   // for time calculations
// thread libraries to be tested
#include "affinity_thread_pool.h"
#include "affinity_thread_pool_lockfree.h"

// the work being done is calculating if a number is prime or no and accumulating the result
bool IsPrime(unsigned long n)
{
    // special handle for 0,1,2
    if (n < 3)
    {
	if (n == 2) return true;
    }

    // no need to check above sqrt(n)
    const auto N = std::ceil(std::sqrt(n) + 1);

    for (auto i = 2; i < N; ++i)
    {
        if (n%i == 0)
        {
			return false;
        }
    }

	return true;
}

// check if a number is prime and accumulate into a counter
void CountIfPrime(unsigned long n, unsigned long& count)
{
	if (IsPrime(n)) ++count;
}

struct PrimeArg
{
    PrimeArg() :
        _n(0), _count(nullptr) {}

    PrimeArg(unsigned long n, std::atomic<unsigned long>* count) :
        _n(n), _count(count) {}

    unsigned long _n;
    std::atomic<unsigned long>* _count;
};


// check if a number is prime and accumulate into a counter - thred safe
void CountIfPrimeArg(const PrimeArg& arg, std::size_t worker_id)
{
    // note that worer_id is not used here
    if (arg._count == nullptr) return;
	if (IsPrime(arg._n)) ++*(arg._count);
}

unsigned long count_primes_affinity(const std::vector<unsigned long>& random_inputs, unsigned int NUMBER_OF_PROCS)
{
    std::atomic<unsigned long> number_of_primes(0);
    std::function<void(const PrimeArg&, std::size_t)> func(CountIfPrimeArg);
    const std::size_t QUEUE_SIZE = 1024;
    const unsigned SUBMIT_WAIT_TIME = 10000; // 10 usec
    const unsigned WAIT_TIME = 10; // 10 nanosec
    affinity_thread_pool<PrimeArg> pool(NUMBER_OF_PROCS, QUEUE_SIZE, func, thread_pool_pinning(), SUBMIT_WAIT_TIME, WAIT_TIME);

    // loop over input to accumulate how many primes are there
    std::for_each(random_inputs.begin(), random_inputs.end(), 
           [&](unsigned long n) 
    {
        if(!pool.try_submit(PrimeArg(n, &number_of_primes)))
        {
            // if all workers are busy, just use main thread
            CountIfPrimeArg(PrimeArg(n, &number_of_primes), 0);
        }
    });

    pool.stop(true);
    return number_of_primes;
}

unsigned long count_primes_affinity_lockfree(const std::vector<unsigned long>& random_inputs, unsigned int NUMBER_OF_PROCS) 
{
    std::atomic<unsigned long> number_of_primes(0);
    std::function<void(const PrimeArg&, std::size_t)> func(CountIfPrimeArg);
    const std::size_t QUEUE_SIZE = 1024;
    const unsigned SUBMIT_WAIT_TIME = 10; // 10 nanosec
    const unsigned WAIT_TIME = 10; // 10 nanosec
    affinity_thread_pool_lockfree<PrimeArg> pool(NUMBER_OF_PROCS, QUEUE_SIZE, func, thread_pool_pinning(), SUBMIT_WAIT_TIME, WAIT_TIME);

    // loop over input to accumulate how many primes are there
    std::for_each(random_inputs.begin(), random_inputs.end(), 
           [&](unsigned long n) 
    {
        if (!pool.try_submit(PrimeArg(n, &number_of_primes)))
        {
            // if all workers are busy, just use main thread
            CountIfPrimeArg(PrimeArg(n, &number_of_primes), 0);
        }
    });

    pool.stop(true);
    return number_of_primes;
}

// single threaded calculation of primes
unsigned long count_primes(const std::vector<unsigned long>& random_inputs)
{
    unsigned long number_of_primes = 0;
	// loop over input to accumulate how many primes are there
    std::for_each(random_inputs.begin(), random_inputs.end(), 
            [&](unsigned long n) 
			{ 
				CountIfPrime(n, number_of_primes);
			});

    return number_of_primes;
}

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cout << "Usage: " << argv[0] << " <size of input> <number of procs>" << std::endl;
        return 1;
    }

	const auto MAX_PROCS = std::thread::hardware_concurrency();
    const auto INPUT_SIZE = std::stol(argv[1]);
    const auto NUMBER_OF_PROC = std::stol(argv[2]);

    if (MAX_PROCS < NUMBER_OF_PROC)
    {
        std::cout << "maximum " << MAX_PROCS  << " concurrent threads are supported. use less threads" << std::endl;
        return 1;
    }

    std::vector<unsigned long> random_inputs;

    for (auto i = 0; i < INPUT_SIZE; ++i)
    {
        random_inputs.push_back(std::rand());
    }
    
    std::chrono::time_point<std::chrono::system_clock> start, end;

    start = std::chrono::system_clock::now();
    auto number_of_primes = count_primes(random_inputs);
    end = std::chrono::system_clock::now();
    std::cout << "count_primes:" << number_of_primes << " prime numbers were found. computation took " << 
        std::chrono::duration_cast<std::chrono::nanoseconds> (end - start).count()/INPUT_SIZE  << " nanosec per iteration" << std::endl;

    start = std::chrono::system_clock::now();
    number_of_primes = count_primes_affinity(random_inputs, NUMBER_OF_PROC);
    end = std::chrono::system_clock::now();
    std::cout << "count_primes_affinity:" << number_of_primes << " prime numbers were found. computation took " << 
        std::chrono::duration_cast<std::chrono::nanoseconds> (end - start).count()/INPUT_SIZE  << " nanosec per iteration" << std::endl;

    start = std::chrono::system_clock::now();
    number_of_primes = count_primes_affinity_lockfree(random_inputs, NUMBER_OF_PROC);
    end = std::chrono::system_clock::now();
    std::cout << "count_primes_affinity_lockfree:" << number_of_primes << " prime numbers were found. computation took " << 
        std::chrono::duration_cast<std::chrono::nanoseconds> (end - start).count()/INPUT_SIZE  << " nanosec per iteration" << std::endl;

    return 0;
}

