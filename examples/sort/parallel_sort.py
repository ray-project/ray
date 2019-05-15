import numpy as np
import time

import ray

# Can be manually modified to adjust the number of blocks and workers
num_blocks = 8
num_input_blocks = num_blocks
num_output_blocks = num_blocks
num_samples_for_pivots = num_input_blocks * 25
array_len = 10**8


def compute_pivots(values, num_samples, num_partitions):
    """Subsample the array to choose pivots."""
    samples = values[np.random.randint(0, len(values), size=num_samples)]
    samples_sorted = np.sort(samples)
    pivot_indices = np.arange(1, num_partitions) * (len(samples_sorted) //
                                                    num_partitions)
    return samples_sorted[pivot_indices]


@ray.remote(num_return_vals=num_output_blocks)
def partition_block(block, pivots):
    """Sort and partition the array further by the given pivots."""
    block_sorted = np.sort(block)
    partition_indices = block_sorted.searchsorted(pivots)
    return np.split(block_sorted, partition_indices)


@ray.remote
def merge_and_sort(*partition):
    """Merge the sorted input arrays to produce a sorted array."""
    return np.sort(np.concatenate(partition))


if __name__ == "__main__":
    ray.init()

    # Generate a random array.
    values = np.random.randint(0, 256, size=array_len, dtype=np.uint8)

    # Begin timing the parallel sort example.
    parallel_sort_start = time.time()

    # Generate pivots to use as range partitions.
    pivots = compute_pivots(values, num_samples_for_pivots, num_input_blocks)

    # Split the array into roughly equal partitions, which we will further
    # partition into ranges by pivots in parallel.
    blocks = np.array_split(values, num_input_blocks)
    partition_ids = np.array([partition_block.remote(block, pivots) for block
                             in blocks]).T

    sorted_ids = [merge_and_sort.remote(*partition_ids[idx]) for idx in
                  range(len(partition_ids))]
    parallel_sorted = np.concatenate(ray.get(sorted_ids))

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
    assert np.array_equal(parallel_sorted, serial_sorted)
