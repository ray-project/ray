import ray

import numpy as np
import time

num_cores = 6
num_partition_blocks = num_cores
num_sorting_blocks = num_cores
num_samples_for_pivots = num_partition_blocks * 25
array_len = 100000000


def compute_pivots(values, num_samples, num_partitions):
    """Sampling a subsection of the array and chooses partition pivots"""
    samples = values[np.random.randint(0, len(values), size=num_samples)]
    samples = np.sort(samples)
    pivot_indices = np.arange(1, num_partitions) * (len(samples) //
                                                    num_partitions)
    return samples[pivot_indices]


@ray.remote(num_return_vals=num_sorting_blocks)
def separate(partition, pivots):
    """Sort and partition the array further by the given pivots."""
    sorted = np.sort(partition)
    partition_indices = sorted.searchsorted(pivots)
    return np.split(sorted, partition_indices)


@ray.remote
def merge_and_sort(partition_ids, id):
    """Concatenate the arrays given and sort afterwards"""
    sections = ray.get(partition_ids[id])
    return np.sort(np.concatenate(sections))


if __name__ == "__main__":
    ray.init()

    # Generate a random array.
    values = np.random.randn(array_len)

    # Begin timing the parallel sort example.
    parallel_sort_start = time.time()

    # Generate pivots to use as range partitions.
    pivots = compute_pivots(values, num_samples_for_pivots,
                            num_partition_blocks)

    # Split the array into roughly equal partitions, which we will further
    # partition into ranges by pivots in parallel.
    partitions = np.array_split(values, num_partition_blocks)
    partition_ids = [separate.remote(partition, pivots) for partition in
                     partitions]
    partition_ids = list(map(list, zip(*partition_ids)))

    sorted_ids = [merge_and_sort.remote(partition_ids, id) for id in
                  range(len(partition_ids))]
    sorted = ray.get(sorted_ids)
    parallel_sorted = np.concatenate(sorted)

    parallel_sort_end = time.time()
    print("Parallel sort took {} seconds."
          .format(parallel_sort_end - parallel_sort_start))

    # Run a serial sort as an accuracy check and time comparison.
    serial_sort_start = time.time()
    serial_sorted = np.sort(values)
    serial_sort_end = time.time()
    print("Serial sort took {} seconds."
          .format(serial_sort_end - serial_sort_start))

    # Check that we sorted the array properly.
    assert np.allclose(parallel_sorted, serial_sorted)
    print("Parallel sort successful and correct.")

    ray.shutdown()
