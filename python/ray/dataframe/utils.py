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

    if len(uniques) % num_partitions == 0:
        chunksize = int(len(uniques) / num_partitions)
    else:
        chunksize = int(len(uniques) / num_partitions) + 1

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


def _partition_pandas_dataframe(df, npartitions=None, chunksize=None):
    """Partitions a Pandas DataFrame object.
    Args:
        df (pandas.DataFrame): The pandas DataFrame to convert.
        npartitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        chunksize (int): The number of rows to put in each partition.
    Returns:
        [ObjectID]: A list of object IDs corresponding to the dataframe
        partitions
    """
    if npartitions is not None:
        chunksize = len(df) // npartitions + 1
    elif chunksize is None:
        raise ValueError("The number of partitions or chunksize must be set.")

    temp_df = df

    dataframes = []
    while len(temp_df) > chunksize:
        t_df = temp_df[:chunksize]
        # reset_index here because we want a pd.RangeIndex
        # within the partitions. It is smaller and sometimes faster.
        t_df = t_df.reset_index(drop=True)
        top = ray.put(t_df)
        dataframes.append(top)
        temp_df = temp_df[chunksize:]
    else:
        temp_df = temp_df.reset_index(drop=True)
        dataframes.append(ray.put(temp_df))

    return dataframes


def from_pandas(df, npartitions=None, chunksize=None):
    """Converts a pandas DataFrame to a Ray DataFrame.
    Args:
        df (pandas.DataFrame): The pandas DataFrame to convert.
        npartitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        chunksize (int): The number of rows to put in each partition.
    Returns:
        A new Ray DataFrame object.
    """
    from .dataframe import DataFrame

    dataframes = _partition_pandas_dataframe(df, npartitions, chunksize)

    return DataFrame(row_partitions=dataframes,
                     columns=df.columns,
                     index=df.index)


def to_pandas(df):
    """Converts a Ray DataFrame to a pandas DataFrame/Series.
    Args:
        df (ray.DataFrame): The Ray DataFrame to convert.
    Returns:
        A new pandas DataFrame.
    """
    pd_df = pd.concat(ray.get(df._row_partitions))
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
    n_cols = min(get_npartitions(), len(row_partitions), len(columns))
    partition_assignments = assign_partitions.remote(columns, n_cols)
    shufflers = [ShuffleActor.remote(x, partition_axis=0, shuffle_axis=1)
                 for x in row_partitions]

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
        df.index = index
        df.columns = np.arange(len(df.columns))
        return df

    return [shuffler.apply_func.remote(fix_indexes) for shuffler in shufflers[:n_cols]]


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
    shufflers = [ShuffleActor.remote(x, partition_axis=1, shuffle_axis=0)
                 for x in col_partitions]

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
        df.columns = columns
        return df.reset_index(drop=True)

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
    assert(callable(func))
    if argslists is None:
        return [_deploy_func.remote(func, part) for part in partitions]
    else:
        assert(all([len(args) == len(partitions) for args in argslists]))
        return [_deploy_func.remote(func, part, *args) for part, *args in zip(partitions, *argslists)]


@ray.remote(num_return_vals=2)
def _compute_length_and_index(dfs):
    """Create a default index, which is a RangeIndex
    Returns:
        The pd.RangeIndex object that represents this DataFrame.
    """
    lengths = ray.get([_deploy_func.remote(_get_lengths, d)
                       for d in dfs])

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(lengths))
                    for p_sub_idx in range(lengths[p_idx])]

    idx_df_col_names = ("partition", "index_within_partition")

    return lengths, pd.DataFrame(dest_indices, columns=idx_df_col_names)


@ray.remote(num_return_vals=2)
def _compute_width_and_index(dfs):
    """Create a default index, which is a RangeIndex
    Returns:
        The pd.RangeIndex object that represents this DataFrame.
    """
    widths = ray.get([_deploy_func.remote(_get_widths, d)
                      for d in dfs])

    dest_indices = [(p_idx, p_sub_idx) for p_idx in range(len(widths))
                    for p_sub_idx in range(widths[p_idx])]

    idx_df_col_names = ("partition", "index_within_partition")

    return widths, pd.DataFrame(dest_indices, columns=idx_df_col_names)


@ray.remote
def _prepend_partitions(last_vals, index, partition, func):
    appended_df = last_vals[:index].append(partition)
    cum_df = func(appended_df)
    return cum_df[index:]
