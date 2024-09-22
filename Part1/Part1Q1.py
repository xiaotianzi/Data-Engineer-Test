import multiprocessing as mp
import time


# Function to check if a number is prime
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True


# Function to process a chunk of data (find prime numbers)
def process_chunk(data_chunk):
    return [n for n in data_chunk if is_prime(n)]


# Multi-threaded approach using multiprocessing
def parallel_find_primes(start, end, num_cores):
    data = list(range(start, end))
    chunk_size = len(data) // num_cores
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    with mp.Pool(num_cores) as pool:
        results = pool.map(process_chunk, chunks)

    primes = [item for sublist in results for item in sublist]
    return primes


# Single-threaded approach to find prime numbers
def single_thread_find_primes(start, end):
    primes = [n for n in range(start, end) if is_prime(n)]
    return primes


# Main program to test the performance of both approaches
if __name__ == "__main__":
    start_num = 2
    end_num = 1000000  # 1 million

    # Measure time for single-threaded approach
    start_time = time.time()
    single_thread_primes = single_thread_find_primes(start_num, end_num)
    single_thread_duration = time.time() - start_time

    # Measure time for multi-threaded approach
    num_cores = mp.cpu_count()
    start_time = time.time()
    multi_thread_primes = parallel_find_primes(start_num, end_num, num_cores)
    multi_thread_duration = time.time() - start_time

    assert set(single_thread_primes) == set(
        multi_thread_primes
    ), "Single-threaded and multi-threaded results do not match"

    print(f"Single-threaded duration: {single_thread_duration:.2f} seconds")
    print(f"Multi-threaded duration: {multi_thread_duration:.2f} seconds")
