import pandas as pd
from . import dataframe

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


def from_pandas(df, npartitions=None, chunksize=None, sort=True):
    """Converts a pandas DataFrame to a Ray DataFrame.
    Args:
        df (pandas.DataFrame): The pandas DataFrame to convert.
        npartitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        chunksize (int): The number of rows to put in each partition.
        sort (bool): Whether or not to sort the df as it is being converted.
    Returns:
        A new Ray DataFrame object.
    """
    from .dataframe import DataFrame
    if npartitions is not None:
        chunksize = int(len(df) / npartitions)
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
def _shuffle(df, indices, chunksize):
    """Shuffle data by sending it through the Ray Store.

    Args:
        df (pd.DataFrame): The pandas DataFrame to shuffle.
        indices ([any]): The list of indices for the DataFrame.
        chunksize (int): The number of indices to send.

    Returns:
        The list of pd.DataFrame objects in order of their assignment. This
        order is important because it determines which task will get the data.
    """
    i = 0
    partition = []
    while len(indices) > chunksize:
        oids = df.reindex(indices[:chunksize])
        partition.append(oids)
        indices = indices[chunksize:]
        i += 1
    else:
        oids = df.reindex(indices)
        partition.append(oids)
    return partition


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
