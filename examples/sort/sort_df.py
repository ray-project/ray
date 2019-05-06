import ray

import numpy as np
import pandas as pd
import time

num_cores = 5
array_len = 70000

@ray.remote
def separate(object, pivots):
    """Sort and partition the array further by the given pivots."""
    sorted = np.sort(object)
    partitions = []
    idx = 0
    for pivot in pivots:
        partition = []
        while idx < len(sorted) and sorted[idx] < pivot:
            partition.append(sorted[idx])
            idx += 1
        partitions.append(np.array(partition))
    return partitions

@ray.remote
def merge_and_sort(object):
    """Concatenate the arrays given and sort afterwards"""
    return np.sort(np.concatenate(object))

if __name__ == '__main__':
    ray.init()

    # Generate a random array.
    values = np.random.randn(array_len)

    # Begin timing the parallel sort example.
    parallel_sort_start = time.time()

    # Generate pivots to use as range partitions.
    num_pivots = min(num_cores - 1, len(values))
    pivots = np.random.choice(values, (num_pivots), replace=False)
    pivots = np.append(pivots, np.iinfo(np.int32).max)
    pivots = np.sort(pivots)

    # Split the array into roughly equal partitions, which we will further
    # partition into ranges by pivots in parallel.
    partitions = np.array_split(values, num_cores)
    range_partitioned_ids = [separate.remote(partition, pivots) for partition
        in partitions]
    range_partitioned = ray.get(range_partitioned_ids)

    # Zip together each range, which will be merged and sorted in parallel.
    subsections = list(zip(*range_partitioned))

    sorted_ids = [merge_and_sort.remote(subsection) for subsection
        in subsections]
    sorted = ray.get(sorted_ids)
    sorted = np.concatenate(sorted);

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
    assert np.allclose(sorted, serial_sorted)
    print ("Parallel sort successful and correct.")
