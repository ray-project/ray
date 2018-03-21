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


def _partition_pandas_dataframe(df, num_partitions=None, row_chunksize=None,
                                col_chunksize=None):
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

        col_chunksize = len(df.columns) // num_partitions \
            if len(df.columns) % num_partitions == 0 \
            else len(df.columns) // num_partitions + 1
    else:
        assert row_chunksize is not None and col_chunksize is not None

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

    # temp_df = df
    #
    # col_partitions = []
    # while len(temp_df.columns) > col_chunksize:
    #     t_df = temp_df.iloc[:, 0:col_chunksize]
    #     # reset_index here because we want a pd.RangeIndex
    #     # within the partitions. It is smaller and sometimes faster.
    #     t_df.reset_index(drop=True, inplace=True)
    #     t_df.columns = pd.RangeIndex(0, len(t_df.columns))
    #     top = ray.put(t_df)
    #     col_partitions.append(top)
    #     temp_df = temp_df.iloc[:, col_chunksize:]
    # else:
    #     temp_df.reset_index(drop=True, inplace=True)
    #     temp_df.columns = pd.RangeIndex(0, len(temp_df.columns))
    #     col_partitions.append(ray.put(temp_df))

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
def _rebuild_cols(row_partitions, index, columns):
    """Rebuild the column partitions from the row partitions.
    Args:
        row_partitions ([ObjectID]): List of row partitions for the dataframe.
        index (pd.Index): The row index of the entire dataframe.
        columns (pd.Index): The column labels of the entire dataframe.
    Returns:
        [ObjectID]: List of new column partitions.
    """
    # NOTE: Reexamine if this is the correct number of columns solution
    n_cols = min(max(get_npartitions(), len(row_partitions)), len(columns))
    partition_assignments = assign_partitions.remote(columns, n_cols)
    shufflers = [ShuffleActor.remote(
        row_partitions[i] if i < len(row_partitions) else pd.DataFrame(),
        partition_axis=0,
        shuffle_axis=1)
                 for i in range(n_cols)]

    shufflers_done = \
        [shufflers[i].shuffle.remote(
            columns,
            partition_assignments,
            i,
            *shufflers)
         for i in range(len(shufflers))]

    # Block on all shuffles being complete
    ray.get(shufflers_done)

    # TODO: Determine if this is the right place to reset the index
    def fix_indexes(df):
        df.columns = pd.RangeIndex(0, len(df.columns))
        df.reset_index(drop=True, inplace=True)
        return df

    return [shuffler.apply_func.remote(fix_indexes)
            for shuffler in shufflers[:n_cols]]


@ray.remote
def _rebuild_rows(col_partitions, index, columns):
    """Rebuild the row partitions from the column partitions.
    Args:
        col_partitions ([ObjectID]): List of col partitions for the dataframe.
        index (pd.Index): The row index of the entire dataframe.
        columns (pd.Index): The column labels of the entire dataframe.
    Returns:
        [ObjectID]: List of new row Partitions.
    """
    n_rows = min(max(get_npartitions(), len(col_partitions)), len(index))
    partition_assignments = assign_partitions.remote(index, n_rows)
    shufflers = [ShuffleActor.remote(
        col_partitions[i] if i < len(col_partitions) else pd.DataFrame(),
        partition_axis=1,
        shuffle_axis=0)
                 for i in range(n_rows)]

    shufflers_done = \
        [shufflers[i].shuffle.remote(
            index,
            partition_assignments,
            i,
            *shufflers)
         for i in range(len(shufflers))]

    # Block on all shuffles being complete
    ray.get(shufflers_done)

    # TODO: Determine if this is the right place to reset the index
    # TODO: Determine if this needs the same changes as above
    def fix_indexes(df):
        df.columns = pd.RangeIndex(0, len(df.columns))
        df.reset_index(drop=True, inplace=True)
        return df

    return [shuffler.apply_func.remote(fix_indexes) for shuffler in shufflers[:n_rows]]


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
    if argslists is None:
        return [_deploy_func.remote(func, part) for part in partitions]
    elif len(argslists) == 1:
        return [_deploy_func.remote(func, part, argslists[0])
                for part in partitions]
    else:
        assert(all([len(args) == len(partitions) for args in argslists]))
        return [_deploy_func.remote(func, part, *args) for part, *args in zip(partitions, *argslists)]


@ray.remote(num_return_vals=2)
def _compute_length_and_index(dfs, index):
    """Create a default index, which is a RangeIndex
    Returns:
        The pd.RangeIndex object that represents this DataFrame.
    """
    lengths = ray.get([_deploy_func.remote(_get_lengths, d)
                       for d in dfs])

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(lengths))
                    for p_sub_idx in range(lengths[p_idx])]

    idx_df_col_names = ("partition", "index_within_partition")

    return lengths, pd.DataFrame(dest_indices, index=index,
                                 columns=idx_df_col_names)


@ray.remote(num_return_vals=2)
def _compute_width_and_index(dfs, columns):
    """Create a default index, which is a RangeIndex
    Returns:
        The pd.RangeIndex object that represents this DataFrame.
    """
    widths = ray.get([_deploy_func.remote(_get_widths, d)
                      for d in dfs])

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(widths))
                    for p_sub_idx in range(widths[p_idx])]

    idx_df_col_names = ("partition", "index_within_partition")

    return widths, pd.DataFrame(dest_indices, index=columns,
                                columns=idx_df_col_names)


@ray.remote
def _prepend_partitions(last_vals, index, partition, func):
    appended_df = last_vals[:index].append(partition)
    cum_df = func(appended_df)
    return cum_df[index:]
