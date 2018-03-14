from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import ray


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

    if npartitions is not None:
        chunksize = len(df) // npartitions if len(df) % npartitions == 0 \
            else len(df) // npartitions + 1
    elif chunksize is None:
        raise ValueError("The number of partitions or chunksize must be set.")

    temp_df = df

    dataframes = []
    lengths = []
    while len(temp_df) > chunksize:
        t_df = temp_df[:chunksize]
        lengths.append(len(t_df))
        # reset_index here because we want a pd.RangeIndex
        # within the partitions. It is smaller and sometimes faster.
        t_df = t_df.reset_index(drop=True)
        top = ray.put(t_df)
        dataframes.append(top)
        temp_df = temp_df[chunksize:]
    else:
        temp_df = temp_df.reset_index(drop=True)
        dataframes.append(ray.put(temp_df))
        lengths.append(len(temp_df))

    return DataFrame(dataframes, df.columns, index=df.index)


def to_pandas(df):
    """Converts a Ray DataFrame to a pandas DataFrame/Series.

    Args:
        df (ray.DataFrame): The Ray DataFrame to convert.

    Returns:
        A new pandas DataFrame.
    """
    pd_df = pd.concat(ray.get(df._df))
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


@ray.remote(num_return_vals=2)
def _compute_length_and_index(dfs):
    """Create a default index, which is a RangeIndex

    Returns:
        The pd.RangeIndex object that represents this DataFrame.
    """
    lengths = ray.get([_deploy_func.remote(_get_lengths, d)
                       for d in dfs])

    dest_indices = {"partition":
                    [i for i in range(len(lengths))
                     for j in range(lengths[i])],
                    "index_within_partition":
                    [j for i in range(len(lengths))
                     for j in range(lengths[i])]}

    return lengths, pd.DataFrame(dest_indices)


@ray.remote
def _prepend_partitions(last_vals, index, partition, func):
    appended_df = last_vals[:index].append(partition)
    cum_df = func(appended_df)
    return cum_df[index:]


@ray.remote
def assign_partitions(index_df, num_partitions):
    uniques = index_df.index.unique()

    if len(uniques) % num_partitions == 0:
        chunksize = int(len(uniques) / num_partitions)
    else:
        chunksize = int(len(uniques) / num_partitions) + 1

    assignments = []

    while len(uniques) > chunksize:
        temp_df = uniques[:chunksize]
        assignments.append(temp_df)
        uniques = uniques[chunksize:]
    else:
        assignments.append(uniques)

    return assignments
