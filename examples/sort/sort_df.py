import ray

from multiprocessing import Pool
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import subprocess
import time

#import multimerge

# To run this example, you will first need to run "python setup.py install" in
# this directory to build the Cython module.
#
# You will only see speedups if you run this code on more data, this is just a
# small example that can run on a laptop.
#
# The values we used to get a speedup (on a m4.10xlarge instance on EC2) were
#     object_store_size = 84 * 10 ** 9
#     num_cores = 20
#     num_rows = 10 ** 9
#     num_cols = 1

client = None
object_store_size = 2 * 10 ** 9  # 2 GB
num_cores = 8
num_rows = 200000
num_cols = 2
column_names = [str(i) for i in range(num_cols)]
column_to_sort = column_names[0]


@ray.remote
def local_sort(object):
    """Sort a partition of a dataframe."""
    # Get the dataframe from the object store.
    [df] = [object]
    # Sort the dataframe.
    sorted_df = df.sort_values(by=column_to_sort)
    # Get evenly spaced values from the dataframe.
    indices = np.linspace(0, len(df) - 1, num=num_cores, dtype=np.int64)
    # Put the sorted dataframe in the object store and return the corresponding
    # object ID as well as the sampled values.
    return sorted_df, sorted_df.as_matrix().take(indices)


@ray.remote
def local_partitions(object_id_and_pivots):
    """Take a sorted partition of a dataframe and split it into more pieces."""
    object_id, pivots = object_id_and_pivots
    [df] = ray.get([object_id])
    split_at = df[column_to_sort].searchsorted(pivots)
    split_at = [0] + list(split_at) + [len(df)]
    # Partition the sorted dataframe and put each partition into the object
    # store.
    return [ray.put(df[i:j]) for i, j in zip(split_at[:-1], split_at[1:])]


# def merge(object_ids):
#     """Merge a number of sorted dataframes into a single sorted dataframe."""
#     dfs = get_dfs(object_ids)
#
#     # In order to use our multimerge code, we have to convert the arrays from
#     # the Fortran format to the C format.
#     arrays = [np.ascontiguousarray(df.as_matrix()) for df in dfs]
#     for a in arrays:
#         assert a.dtype == np.float64
#         assert not np.isfortran(a)
#
#     # Filter out empty arrays.
#     arrays = [a for a in arrays if a.shape[0] > 0]
#
#     if len(arrays) == 0:
#         return None
#
#     resulting_array = multimerge.multimerge2d(*arrays)
#     merged_df2 = pd.DataFrame(resulting_array, columns=column_names)
#
#     return put_df(merged_df2)


if __name__ == '__main__':
    ray.init()

    # Create a DataFrame from a numpy array.
    df = pd.DataFrame(np.random.randn(num_rows, num_cols),
                      columns=column_names)

    partitions = np.split(df, num_cores)

    # Begin timing the parallel sort example.
    parallel_sort_start = time.time()

    # Sort each partition and subsample them. The subsampled values will be
    # used to create buckets.
    local_sort_result_ids = [local_sort.remote(partition) for partition
        in partitions]
    print(loca_sort_result_ids[0])
    sorted_df_ids, pivot_groups = list(zip(ray.get(local_sort_result_ids)))
    exit()

    # Choose the pivots.
    all_pivots = np.concatenate(pivot_groups)
    indices = np.linspace(0, len(all_pivots) - 1, num=num_cores,
                          dtype=np.int64)
    pivots = np.take(np.sort(all_pivots), indices)

    # Break all of the sorted partitions into even smaller partitions. Group
    # the object IDs from each bucket together.
    result_ids = [local_partitions.remote(sorted_df_id, len(sorted_df_ids) *
        [pivots]) for sorted_df_id in sorted_df_ids]
    results = list(zip(ray.get(result_ids)))

    # Merge each of the buckets and store the results in the object store.
    # object_ids = ray.get([merge.remote(result) for result in results])
    #
    # resulting_ids = [object_id for object_id in object_ids
    #                   if object_id is not None]

    # Stop timing the paralle sort example.
    parallel_sort_end = time.time()

    print('Parallel sort took {} seconds.'
          .format(parallel_sort_end - parallel_sort_start))

    serial_sort_start = time.time()

    original_sorted_df = df.sort_values(by=column_to_sort)

    serial_sort_end = time.time()

    # Check that we sorted the DataFrame properly.

    # original check
    # sorted_dfs = get_dfs(resulting_ids)
    # sorted_df = pd.concat(sorted_dfs)
    #
    # print('Serial sort took {} seconds.'
    #       .format(serial_sort_end - serial_sort_start))
    #
    # assert np.allclose(sorted_df.values, original_sorted_df.values)

    # Kill the object store.
    p.kill()
