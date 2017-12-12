import pandas as pd
import ray

class DataFrame():
    def __init__(self, df, columns):
        """
        Distributed DataFrame object backed by Pandas dataframes.

        Args:
            df ([ObjectID]): The list of ObjectIDs that contain the dataframe
                partitions.
            columns ([str]): The list of column names for this dataframe.
        """
        self.df = df
        self.columns = columns

    def map_partitions(self, fn, *args):
        """
        Perform a function on each partition.

        Args:
            fn (callable): The function to perform.

        Returns:
            A new DataFrame containing the result of the function.
        """
        assert(callable(fn))

        new_df = []
        for partition in range(len(self.df)):
            # self.df[partition] = _deploy_fn.remote(fn, self.df[partition], *args)

            if len(args) == 0:
                new_df.append(ray.put(fn(ray.get(self.df[partition]))))
            else:
                new_df.append(ray.put(fn(ray.get(self.df[partition]), *args)))
        return DataFrame(new_df, self.columns)

    def copy(self):
        """
        Creates a shallow copy of the DataFrame.

        Returns:
            A new DataFrame pointing to the same partitions as this one.
        """
        return DataFrame(self.df, self.columns)

    def sum(self, axis=None, skipna=True, split_every=False):
        """
        Perform a sum across the DataFrame.

        Args:
            axis ()
            skipna ()
            split_every ()

        Returns:
            The sum of the DataFrame.
        """
        self.map_partitions(lambda df: df.sum(axis=axis, skipna=skipna, split_every=split_every))

    def reduce_by_index(self):
        """

        """
        list_of_dfs = _reduce_by_index.remote(self)
        return DataFrame(list_of_dfs, ray_df.columns)


@ray.remote
def _reduce_by_index(ray_df):
    """

    """
    return []

    indices = list(set([index for index in list(ray.get(ray_df.df).index)]))

    chunksize = int(len(indices) / len(ray_df.df))

    # partitions = [[] for _ in range(len(ray_df))]
    partitions = []
    
    
    for df in ray_df.df:
        i = 0
        while len(indices) > chunksize:
            #oids = _shuffle(df, indices[:chunksize])
            oids = ray.get(df).reindex(indices[:chunksize]).dropna()
            if not partitions[i]:
                partitions[i] = [oids]
            else:
                partitions[i].append(oids)
            indices = indices[chunksize:]
            i += 1
        else:
            # oids = _shuffle(df, indices)
            oids = ray.get(df).reindex(indices).dropna()
            partitions[i].append(oids)


    # return [_local_reduce(partition) for partition in partitions]
    return [pd.concat(ray.get(partition)) for partition in partitions]

    

@ray.remote
def _shuffle(df, indices):
    return df.reindex(indices).dropna()

@ray.remote
def _local_reduce(df_rows):
    """

    """
    concat_df = pd.concat(ray.get(df_rows))
    return concat_df
    # return predicate_fn(concat_df, *args)


@ray.remote
def _deploy_fn(fn, dataframe, *args):
    """
    Deploys a function for the map_partitions call.

    Args:
        dataframe (pandas.DataFrame): The pandas DataFrame for this partition.

    Returns:
        A futures object representing the return value of the function
        provided.
    """
    if len(args) == 0:
        return fn(dataframe)
    else:
        return fn(dataframe, *args)

def from_pandas(data, npartitions=None, chunksize=None, sort=True):
    """
    Converts a pandas DataFrame to a Ray DataFrame. It splits the DataFrame
    across the Ray Object Store.

    Args:
        data (pandas.DataFrame): The pandas DataFrame to convert.
        npartitions (int): The number of partitions to split the DataFrame
            into. Has priority over chunksize.
        chunksize (int): The number of rows to put in each partition.
        sort (bool): Whether or not to sort the data as it is being converted.

    Returns:
        A new Ray DataFrame object.
    """
    if sort and not data.index.is_monotonic_increasing:
        data = data.sort_index(ascending=True)
    
    if npartitions is not None:
        chunksize = int(len(data) / npartitions)
    elif chunksize is None:
        raise ValueError("The number of partitions or chunksize must be set.")
    
    #TODO stop reassigning data
    dataframes = []
    while len(data) > chunksize:
        top = ray.put(data[:chunksize])
        dataframes.append(top)
        data = data[chunksize:]
    else:
        dataframes.append(ray.put(data))

    return DataFrame(dataframes, list(data.columns.values))
