from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import numpy as np
import ray

from . import (
    get_npartitions,
    get_nrowpartitions,
    get_ncolpartitions,
    get_nworkers)

from itertools import product


def _get_lengths(df):
    """Gets the length of the dataframe.
    Args:
        df: A remote pd.DataFrame object.
    Returns:
        Returns an integer length of the dataframe object. If the attempt
            fails, returns 0 as the length.
    """
    try:
        return len(df)
    # Because we sometimes have cases where we have summary statistics in our
    # DataFrames
    except TypeError:
        return 0


def _get_widths(df):
    """Gets the width (number of columns) of the dataframe.
    Args:
        df: A remote pd.DataFrame object.
    Returns:
        Returns an integer width of the dataframe object. If the attempt
            fails, returns 0 as the length.
    """
    try:
        return len(df.columns)
    # Because we sometimes have cases where we have summary statistics in our
    # DataFrames
    except TypeError:
        return 0


def _partition_pandas_dataframe(df, num_partitions=None, row_chunksize=None):
    """Partitions a Pandas DataFrame object.
    Args:
        df (pandas.DataFrame): The pandas DataFrame to convert.
        npartitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        row_chunksize (int): The number of rows to put in each partition.
    Returns:
        [ObjectID]: A list of object IDs corresponding to the dataframe
        partitions
    """
    if num_partitions is not None:
        row_chunksize = len(df) // num_partitions \
            if len(df) % num_partitions == 0 \
            else len(df) // num_partitions + 1
    else:
        assert row_chunksize is not None

    temp_df = df

    row_partitions = []
    while len(temp_df) > row_chunksize:
        t_df = temp_df[:row_chunksize]
        # reset_index here because we want a pd.RangeIndex
        # within the partitions. It is smaller and sometimes faster.
        t_df.reset_index(drop=True, inplace=True)
        t_df.columns = pd.RangeIndex(0, len(t_df.columns))
        top = ray.put(t_df)
        row_partitions.append(top)
        temp_df = temp_df[row_chunksize:]
    else:
        if len(df) > row_chunksize:
            temp_df.reset_index(drop=True, inplace=True)
            temp_df.columns = pd.RangeIndex(0, len(temp_df.columns))
        row_partitions.append(ray.put(temp_df))

    return row_partitions


def from_pandas(df, num_partitions=None, chunksize=None):
    """Converts a pandas DataFrame to a Ray DataFrame.
    Args:
        df (pandas.DataFrame): The pandas DataFrame to convert.
        num_partitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        chunksize (int): The number of rows to put in each partition.
    Returns:
        A new Ray DataFrame object.
    """
    from .dataframe import DataFrame

    row_partitions = \
        _partition_pandas_dataframe(df, num_partitions, chunksize)

    return DataFrame(row_partitions=row_partitions,
                     columns=df.columns,
                     index=df.index)


def to_pandas(df):
    """Converts a Ray DataFrame to a pandas DataFrame/Series.
    Args:
        df (ray.DataFrame): The Ray DataFrame to convert.
    Returns:
        A new pandas DataFrame.
    """
    if df._row_partitions is not None:
        pd_df = pd.concat(ray.get(df._row_partitions))
    else:
        pd_df = pd.concat(ray.get(df._col_partitions),
                          axis=1)
    pd_df.index = df.index
    pd_df.columns = df.columns
    return pd_df


@ray.remote
def _deploy_func(func, dataframe, *args):
    """Deploys a function for the _map_partitions call.
    Args:
        dataframe (pandas.DataFrame): The pandas DataFrame for this partition.
    Returns:
        A futures object representing the return value of the function
        provided.
    """
    if len(args) == 0:
        return func(dataframe)
    else:
        return func(dataframe, *args)


def _map_partitions(func, partitions, *argslists):
    """Apply a function across the specified axis

    Args:
        func (callable): The function to apply
        partitions ([ObjectID]): The list of partitions to map func on.

    Returns:
        A new Dataframe containing the result of the function
    """
    if partitions is None:
        return None

    assert(callable(func))
    if len(argslists) == 0:
        return [_deploy_func.remote(func, part) for part in partitions]
    elif len(argslists) == 1:
        return [_deploy_func.remote(func, part, argslists[0])
                for part in partitions]
    else:
        assert(all([len(args) == len(partitions) for args in argslists]))
        return [_deploy_func.remote(func, part, *args)
                for part, args in zip(partitions, *argslists)]


@ray.remote(num_return_vals=2)
def _build_columns(df_col, columns):
    """Build columns and compute lengths for each partition."""
    # Columns and width
    widths = np.array(ray.get([_deploy_func.remote(_get_widths, d)
                      for d in df_col]))
    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(widths))
                    for p_sub_idx in range(widths[p_idx])]

    col_names = ("partition", "index_within_partition")
    column_df = pd.DataFrame(dest_indices, index=columns, columns=col_names)

    return widths, column_df


@ray.remote(num_return_vals=2)
def _build_index(df_row, index):
    """Build index and compute lengths for each partition."""
    # Rows and length
    lengths = np.array(ray.get([_deploy_func.remote(_get_lengths, d)
                       for d in df_row]))

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(lengths))
                    for p_sub_idx in range(lengths[p_idx])]
    col_names = ("partition", "index_within_partition")
    index_df = pd.DataFrame(dest_indices, index=index, columns=col_names)

    return lengths, index_df


def _create_block_partitions(partitions, axis=0, length=None):

    if length is not None and length != 0: # and get_npartitions() > length:
        npartitions = length
    else:
        npartitions = get_ncolpartitions() if axis == 0 else get_nrowpartitions()
        # npartitions = get_npartitions()

    if npartitions == 1:
        x = [[part] for part in partitions]
    else:
        x = [create_blocks._submit(args=(partition, npartitions, axis),
                                   num_return_vals=npartitions)
             for partition in partitions]

    # In the case that axis is 1 we have to transpose because we build the
    # columns into rows. Fortunately numpy is efficient at this.
    return np.array(x) if axis == 0 else np.array(x).T


@ray.remote
def create_blocks(df, npartitions, axis):
    return create_blocks_helper(df, npartitions, axis)


def create_blocks_helper(df, npartitions, axis):
    # Single partition dataframes don't need to be repartitioned
    if npartitions == 1:
        return df
    # In the case that the size is not a multiple of the number of partitions,
    # we need to add one to each partition to avoid losing data off the end
    block_size = df.shape[axis ^ 1] // npartitions \
        if df.shape[axis ^ 1] % npartitions == 0 \
        else df.shape[axis ^ 1] // npartitions + 1

    # if not isinstance(df.columns, pd.RangeIndex):
    #     df.columns = pd.RangeIndex(0, len(df.columns))

    blocks = [df.iloc[:, i * block_size: (i + 1) * block_size]
              if axis == 0
              else df.iloc[i * block_size: (i + 1) * block_size, :]
              for i in range(npartitions)]

    for block in blocks:
        block.columns = pd.RangeIndex(0, len(block.columns))
    return blocks


@ray.remote
def _blocks_to_col(*partition):
    return pd.concat(partition, axis=0, copy=False)\
        .reset_index(drop=True)


@ray.remote
def _blocks_to_row(*partition):
    row_part = pd.concat(partition, axis=1, copy=False)\
        .reset_index(drop=True)
    # Because our block partitions contain different indices (for the
    # columns), this change is needed to ensure correctness.
    row_part.columns = pd.RangeIndex(0, len(row_part.columns))
    return row_part

def _inherit_docstrings(parent):
    """Creates a decorator which overwrites a decorated class' __doc__
    attribute with parent's __doc__ attribute. Also overwrites __doc__ of
    methods and properties defined in the class with the __doc__ of matching
    methods in parent.

    Args:
        parent (object): Class from which the decorated class inherits __doc__.

    Note:
        Currently does not override class' __doc__ or __init__'s __doc__.

    Todo:
        Override the class' __doc__ and __init__'s __doc__  once DataFrame's
            __init__ method matches pandas.DataFrame's __init__ method.

    Returns:
        function: decorator which replaces the decorated class' documentation
            parent's documentation.
    """
    def decorator(cls):
        # cls.__doc__ = parent.__doc__
        for attr, obj in cls.__dict__.items():
            if attr == "__init__":
                continue
            parent_obj = getattr(parent, attr, None)
            if not callable(parent_obj) and \
                    not isinstance(parent_obj, property):
                continue
            if callable(obj):
                obj.__doc__ = parent_obj.__doc__
            elif isinstance(obj, property) and obj.fget is not None:
                p = property(obj.fget, obj.fset, obj.fdel, parent_obj.__doc__)
                setattr(cls, attr, p)

        return cls

    return decorator


@ray.remote
def _reindex_helper(old_index, new_index, axis, npartitions, *df):
    """Reindexes a dataframe to prepare for join/concat.

    Args:
        df: The DataFrame partition
        old_index: The index/column for this partition.
        new_index: The new index/column to assign.
        axis: Which axis to reindex over.

    Returns:
        A new set of blocks made up of DataFrames.
    """
    df = pd.concat(df, axis=axis ^ 1)
    if axis == 1:
        df.index = old_index
        df = df.reindex(new_index, copy=False)
        df.reset_index(inplace=True, drop=True)
    elif axis == 0:
        df.columns = old_index
        df = df.reindex(columns=new_index, copy=False)
        df.columns = pd.RangeIndex(len(df.columns))
    return create_blocks_helper(df, npartitions, axis)


@ray.remote
def _co_op_helper(func, left_columns, right_columns, left_df_len, *zipped):
    """Copartition operation where two DataFrames must have aligned indexes.

    NOTE: This function assumes things are already copartitioned. Requires that
        row partitions are passed in as blocks.

    Args:
        func: The operation to conduct between two DataFrames.
        left_columns: The column names for the left DataFrame.
        right_columns: The column names for the right DataFrame.
        left_df_len: The length of the left. This is used so we can split up
            the zipped partitions.
        zipped: The DataFrame partitions (in blocks).

    Returns:
         A new set of blocks for the partitioned DataFrame.
    """
    left = pd.concat(zipped[:left_df_len], axis=1, copy=False)
    left.columns = left_columns

    right = pd.concat(zipped[left_df_len:], axis=1, copy=False)
    right.columns = right_columns

    new_rows = func(left, right)
    return create_blocks_helper(new_rows, left_df_len, 0)

### EXPERIMENTAL ###

@ray.remote
def explode_block(df, factors):
    row_factor, col_factor = factors

    # In the case that the size is not a multiple of the number of partitions,
    # we need to add one to each partition to avoid losing data off the end
    block_len_row = df.shape[0] // row_factor \
        if df.shape[0] % row_factor == 0 \
        else df.shape[0] // row_factor + 1

    block_len_col = df.shape[1] // col_factor \
        if df.shape[1] % col_factor == 0 \
        else df.shape[1] // col_factor + 1

    # XXX: For some reason, serializing `iloc` on columns is very expensive.
    # Performing a `copy` prior to the serialization negates a bit of this cost
    # Performing a transpose, then a copy, then tranposing again negates more (???)
    iloc = df.iloc
    if col_factor > 1: 
        blocks = [iloc[i * block_len_row: (i + 1) * block_len_row,
                       j * block_len_col: (j + 1) * block_len_col].T.copy().T
                  for i in range(row_factor) for j in range(col_factor)]
    else:
        blocks = [iloc[i * block_len_row: (i + 1) * block_len_row, :]
                  for i in range(row_factor)]

    for block in blocks:
        block.columns = pd.RangeIndex(0, len(block.columns))
    return blocks

def _explode_block_partitions(block_partitions, factors):
    # Sometimes the optimizer might want the same size, thus short circuit
    if factors == (1, 1):
        return block_partitions

    row_factor, col_factor = factors
    nreturns = row_factor * col_factor

    nblocks_row, nblocks_col = block_partitions.shape

    # Ray doesn't support OIDs in NDArrays, so everything must be treated as
    # flat lists and then rebuilt
    explode_lists = [[explode_block._submit(args=(block, factors),
                                num_return_vals=nreturns)
                      for block in row]
                     for row in block_partitions]
    reshaped_lists = [[np.array(explode_list).reshape(row_factor, col_factor)
                       for explode_list in row]
                      for row in explode_lists]
    joined = np.concatenate([np.concatenate(row, axis=1)
                             for row in reshaped_lists],
                            axis=0)

    assert joined.shape == (row_factor * nblocks_row, col_factor * nblocks_col)

    return joined

@ray.remote
def _deploy_func_row(func, *partition):
    row_part = pd.concat(partition, axis=1, copy=False)\
        .reset_index(drop=True)
    # Because our block partitions contain different indices (for the
    # columns), this change is needed to ensure correctness.
    row_part.columns = pd.RangeIndex(0, len(row_part.columns))
    return func(row_part)

@ray.remote
def _deploy_func_col(func, *partition):
    col_part = pd.concat(partition, axis=0, copy=False)\
        .reset_index(drop=True)

    return func(col_part)

def _map_partitions_coalesce(func, block_partitions, axis):
    """Apply a function across the specified axis

    Args:
        func (callable): The function to apply
        axis (0 or 1): The axis to coalesce across before performing the function
        partitions ([ObjectID]): The list of partitions to map func on.

    Returns:
        A new Dataframe containing the result of the function
    """
    if block_partitions is None:
        return None

    assert(callable(func))

    if axis == 0:
        # get col partitions, reduce, perform
        return [_deploy_func_col.remote(func, *block_partitions[:, i])
                for i in range(block_partitions.shape[1])]
    else:
        # get row partitions, reduce, perform
        return [_deploy_func_row.remote(func, *part) for part in block_partitions]

flookup = {
    "isna": {"type": "applymap", "op_rate": 7500 / (2**22 * 2**8 * 2**3), "op_stdev": 0},
    "sum": {"type": "axis-reduce", "op_rate": 0, "op_stdev": 0}
}

MAX_ZSCORE = 2

def _optimize_partitions(in_dims, dsize, fname='isna', **kwargs):
    in_rows, in_cols = in_dims
    in_nparts = in_rows * in_cols
    fprofile = flookup[fname]
    ftype = fprofile["type"]
    op_rate = fprofile["op_rate"]
    op_stdev = fprofile["op_stdev"]

    op_stdev_f = lambda x: op_stdev * x

    def overhead_f(split):
        x_sp, y_sp = split
        return 1.6 * x_sp * y_sp

    def est_time(split):
        in_dsplit = dsize / in_nparts
        
        split_rows, split_cols = split
        split_nparts = split_rows * split_cols

        # Explode time. Add possible task overhead to explode?
        if split == in_dims:
            explode_time = 0
        else:
            # Serializing iloc'd-on-columns DFs is much more expensive than just on rows
            explode_rate = explode_no_cols_rate if split_cols == 1 else explode_cols_rate
            explode_iters = np.ceil((in_nparts) / get_nworkers())
            explode_time = explode_rate * in_dsplit * explode_iters + overhead_f(in_dims)

        if ftype == "applymap":
            dsplit = dsize / split_nparts

        elif ftype == "axis-reduce":
            axis = kwargs["axis"]
            dsplit = dsize / (split_cols if axis == 0 else split_rows)

        task_split_time = op_rate * dsplit
        total_iters = np.ceil((split_nparts) / get_nworkers())

        task_time = total_iters * task_split_time

        # \eps(x, y)
        task_overhead = overhead_f(split)

        # Var(x, y): StdDev for Summed Normal grows by O(sqrt(k))
        task_var = MAX_ZSCORE * op_stdev_f(dsplit) * np.sqrt(total_iters)

        total_time = explode_time + task_time + task_overhead + task_var
        return total_time

    explode_no_cols_rate = 350 / (2**20 * 2**8 * 2**3)
    explode_cols_rate = 2000 / (2**20 * 2**8 * 2**3)
    candidate_splits = list(product(range(in_rows, 17), range(in_cols, 17)))
    times = [(split, est_time(split)) for split in candidate_splits]
    res_df = pd.DataFrame(np.array(tuple(zip(*times))[1]).reshape((17 - in_rows, 17 - in_cols)),
                          index=range(in_rows, 17), columns=range(in_cols, 17))
    print(res_df)
    split, time = min(times, key=lambda x: x[1])
    return split, time

def waitall(df):
    parts = df._block_partitions.flatten().tolist()
    ray.wait(parts, len(parts))
