from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import numpy as np
import ray

from .shuffle import ShuffleActor
from . import get_npartitions


@ray.remote
def assign_partitions(index, num_partitions):
    """Generates a partition assignment based on a dataframe index
    Args:
        index (pandas.DataFrame or pandas.Index):
            The index of the DataFrame to partition. This can either be a pandas DataFrame,
            in which it will represent the RangeIndex of a to-be partitioned ray DataFrame,
            or a pandas Index, in which it will represent the index of a to-be partition 
            pandas DataFrame.
        num_partitions (int):
            The number of partitions to generate assignments for.
    Returns ([pandas.Index]):
        List of indexes that will be sent to each partition
    """
    if isinstance(index, pd.DataFrame):
        uniques = index.index.unique()
    elif isinstance(index, pd.Index):
        uniques = index.unique()
    else:
        raise TypeError("Unexpected value of type {0} assigned to ShuffleActor"
                        .format(type(index).__name__))

    chunksize = len(uniques) // num_partitions \
        if len(uniques) % num_partitions == 0 \
        else len(uniques) // num_partitions + 1

    assignments = []

    while len(uniques) > chunksize:
        temp_idx = uniques[:chunksize]
        assignments.append(temp_idx)
        uniques = uniques[chunksize:]
    else:
        assignments.append(uniques)

    return assignments


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
def _local_groupby(df_rows, axis=0):
    """Apply a groupby on this partition for the blocks sent to it.
    Args:
        df_rows ([pd.DataFrame]): A list of dataframes for this partition. Goes
            through the Ray object store.
    Returns:
        A DataFrameGroupBy object from the resulting groupby.
    """
    concat_df = pd.concat(df_rows, axis=axis)
    return concat_df.groupby(concat_df.index)


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
                for part, *args in zip(partitions, *argslists)]


@ray.remote(num_return_vals=4)
def _build_columns_and_index(df_row, index, df_col, columns):
    """Build columns and index and compute lengths for each partition."""
    # Rows and length
    lengths = ray.get([_deploy_func.remote(_get_lengths, d)
                       for d in df_row])

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(lengths))
                    for p_sub_idx in range(lengths[p_idx])]
    col_names = ("partition", "index_within_partition")
    index_df = pd.DataFrame(dest_indices, index=index, columns=col_names)

    # Columns and width
    widths = ray.get([_deploy_func.remote(lambda df: len(df.columns), d)
                      for d in df_col])
    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(widths))
                    for p_sub_idx in range(widths[p_idx])]

    col_names = ("partition", "index_within_partition")
    column_df = pd.DataFrame(dest_indices, index=columns, columns=col_names)

    return lengths, index_df, widths, column_df


@ray.remote
def _prepend_partitions(last_vals, index, partition, func):
    appended_df = last_vals[:index].append(partition)
    cum_df = func(appended_df)
    return cum_df[index:]


def _create_blk_partitions(partitions, axis=0, length=None):

    if length is not None and get_npartitions() > length:
        npartitions = length
    else:
        npartitions = get_npartitions()

    x = [create_blocks._submit(args=(partition, npartitions, axis),
                               num_return_vals=npartitions)
         for partition in partitions]

    # In the case that axis is 1 we have to transpose because we build the
    # columns into rows. Fortunately numpy is efficent at this.
    return np.array(x) if axis == 0 else np.array(x).T


@ray.remote
def create_blocks(df, npartitions, axis):
    # Single partition dataframes don't need to be repartitioned
    if npartitions == 1:
        return df
    # In the case that the size is not a multiple of the number of partitions,
    # we need to add one to each partition to avoid losing data off the end
    block_size = df.shape[axis ^ 1] // npartitions \
        if df.shape[axis ^ 1] % npartitions == 0 \
        else df.shape[axis ^ 1] // npartitions + 1

    if not isinstance(df.columns, pd.RangeIndex):
        df.columns = pd.RangeIndex(0, len(df.columns))

    return [df.iloc[:, i * block_size: (i + 1) * block_size]
            if axis == 0
            else df.iloc[i * block_size: (i + 1) * block_size, :]
            for i in range(npartitions)]
