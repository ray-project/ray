from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import numpy as np
import ray
import itertools


class DataFrame(object):

    def __init__(self, df, columns, index=None):
        """Distributed DataFrame object backed by Pandas dataframes.

        Args:
            df ([ObjectID]): The list of ObjectIDs that contain the dataframe
                partitions.
            columns (pandas.Index): The column names for this dataframe, in
                pandas Index object.
            index (pandas.Index or list): The row index for this dataframe.
        """
        assert(len(df) > 0)

        self._df = df
        self._lengths = [_deploy_func.remote(_get_lengths, d)
                         for d in self._df]
        self.columns = columns

        # this _index object is a pd.DataFrame
        # and we use that DataFrame's Index to index the rows.
        self._index = self._default_index()

        if index is not None:
            self.index = index

    def __str__(self):
        return "ray.DataFrame object"

    def __repr__(self):
        return "ray.DataFrame object"

    def _get_index(self):
        """Get the index for this DataFrame.

        Returns:
            The union of all indexes across the partitions.
        """
        return self._index.index

    def _set_index(self, new_index):
        """Set the index for this DataFrame.

        Args:
            new_index: The new index to set this
        """
        self._index.index = new_index

    def _default_index(self):
        """Create a default index, which is a RangeIndex

        Returns:
            The pd.RangeIndex object that represents this DataFrame.
        """
        dest_indices = {"partition":
                        [i for i in range(len(self._lengths))
                         for j in range(self._lengths[i])],
                        "index_within_partition":
                        [j for i in range(len(self._lengths))
                         for j in range(self._lengths[i])]}
        return pd.DataFrame(dest_indices)

    index = property(_get_index, _set_index)

    def _get_lengths(self):
        """Gets the lengths for each partition and caches it if it wasn't.

        Returns:
            A list of integers representing the length of each partition.
        """
        if isinstance(self._length_cache[0], ray.local_scheduler.ObjectID):
            self._length_cache = ray.get(self._length_cache)
        return self._length_cache

    def _set_lengths(self, lengths):
        """Sets the lengths of each partition for this DataFrame.

        We use this because we can compute it when creating the DataFrame.

        Args:
            lengths ([ObjectID or Int]): A list of lengths for each
                partition, in order.
        """
        self._length_cache = lengths

    _lengths = property(_get_lengths, _set_lengths)

    @property
    def size(self):
        """Get the number of elements in the DataFrame.

        Returns:
            The number of elements in the DataFrame.
        """
        return len(self.index) * len(self.columns)

    @property
    def ndim(self):
        """Get the number of dimensions for this DataFrame.

        Returns:
            The number of dimensions for this DataFrame.
        """
        # The number of dimensions is common across all partitions.
        # The first partition will be enough.
        return ray.get(_deploy_func.remote(lambda df: df.ndim, self._df[0]))

    @property
    def ftypes(self):
        """Get the ftypes for this DataFrame.

        Returns:
            The ftypes for this DataFrame.
        """
        # The ftypes are common across all partitions.
        # The first partition will be enough.
        return ray.get(_deploy_func.remote(lambda df: df.ftypes, self._df[0]))

    @property
    def dtypes(self):
        """Get the dtypes for this DataFrame.

        Returns:
            The dtypes for this DataFrame.
        """
        # The dtypes are common across all partitions.
        # The first partition will be enough.
        return ray.get(_deploy_func.remote(lambda df: df.dtypes, self._df[0]))

    @property
    def empty(self):
        """Determines if the DataFrame is empty.

        Returns:
            True if the DataFrame is empty.
            False otherwise.
        """
        all_empty = ray.get(self._map_partitions(lambda df: df.empty)._df)
        return False not in all_empty

    @property
    def values(self):
        """Create a numpy array with the values from this DataFrame.

        Returns:
            The numpy representation of this DataFrame.
        """
        return np.concatenate(
            ray.get(self._map_partitions(lambda df: df.values)._df))

    @property
    def axes(self):
        """Get the axes for the DataFrame.

        Returns:
            The axes for the DataFrame.
        """
        return [self.index, self.columns]

    @property
    def shape(self):
        """Get the size of each of the dimensions in the DataFrame.

        Returns:
            A tuple with the size of each dimension as they appear in axes().
        """
        return (len(self.index), len(self.columns))

    def _map_partitions(self, func, index=None):
        """Apply a function on each partition.

        Args:
            func (callable): The function to Apply.

        Returns:
            A new DataFrame containing the result of the function.
        """
        assert(callable(func))
        new_df = [_deploy_func.remote(func, part) for part in self._df]
        if index is None:
            index = self.index

        return DataFrame(new_df, self.columns, index=index)

    def add_prefix(self, prefix):
        """Add a prefix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(prefix) + str(x))
        return DataFrame(self._df, new_cols, index=self.index)

    def add_suffix(self, suffix):
        """Add a suffix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(x) + str(suffix))
        return DataFrame(self._df, new_cols, index=self.index)

    def applymap(self, func):
        """Apply a function to a DataFrame elementwise.

        Args:
            func (callable): The function to apply.
        """
        assert(callable(func))
        return self._map_partitions(lambda df: df.applymap(lambda x: func(x)))

    def copy(self, deep=True):
        """Creates a shallow copy of the DataFrame.

        Returns:
            A new DataFrame pointing to the same partitions as this one.
        """
        return DataFrame(self._df, self.columns, index=self.index)

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        """Apply a groupby to this DataFrame. See _groupby() remote task.

        Args:
            by: The value to groupby.
            axis: The axis to groupby.
            level: The level of the groupby.
            as_index: Whether or not to store result as index.
            group_keys: Whether or not to group the keys.
            squeeze: Whether or not to squeeze.

        Returns:
            A new DataFrame resulting from the groupby.
        """

        indices = self.index.unique()

        chunksize = int(len(indices) / len(self._df))
        partitions = [_shuffle.remote(df, indices, chunksize)
                      for df in self._df]
        partitions = ray.get(partitions)

        # Transpose the list of dataframes
        # TODO find a better way
        shuffle = []
        for i in range(len(partitions[0])):
            shuffle.append([])
            for j in range(len(partitions)):
                shuffle[i].append(partitions[j][i])
        new_dfs = [_local_groupby.remote(part, axis=axis) for part in shuffle]

        return DataFrame(new_dfs, self.columns, index=indices)

    def reduce_by_index(self, func, axis=0):
        """Perform a reduction based on the row index.

        Args:
            func (callable): The function to call on the partition
                after the groupby.

        Returns:
            A new DataFrame with the result of the reduction.
        """
        return self.groupby(axis=axis)._map_partitions(
            func, index=pd.unique(self.index))

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None):
        """Perform a sum across the DataFrame.

        Args:
            axis (int): The axis to sum on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The sum of the DataFrame.
        """
        intermediate_index = [idx
                              for _ in range(len(self._df))
                              for idx in self.columns]

        sum_of_partitions = self._map_partitions(
            lambda df: df.sum(axis=axis, skipna=skipna, level=level,
                              numeric_only=numeric_only),
            index=intermediate_index)

        return sum_of_partitions.reduce_by_index(
            lambda df: df.sum(axis=axis, skipna=skipna, level=level,
                              numeric_only=numeric_only))

    def abs(self):
        """Apply an absolute value function to all numberic columns.

        Returns:
            A new DataFrame with the applied absolute value.
        """
        for t in self.dtypes:
            if np.dtype('O') == t:
                # TODO Give a more accurate error to Pandas
                raise TypeError("bad operand type for abs():", "str")
        return self._map_partitions(lambda df: df.abs())

    def isin(self, values):
        """Fill a DataFrame with booleans for cells contained in values.

        Args:
            values (iterable, DataFrame, Series, or dict): The values to find.

        Returns:
            A new DataFrame with booleans representing whether or not a cell
            is in values.
            True: cell is contained in values.
            False: otherwise
        """
        return self._map_partitions(lambda df: df.isin(values))

    def isna(self):
        """Fill a DataFrame with booleans for cells containing NA.

        Returns:
            A new DataFrame with booleans representing whether or not a cell
            is NA.
            True: cell contains NA.
            False: otherwise.
        """
        return self._map_partitions(lambda df: df.isna())

    def isnull(self):
        """Fill a DataFrame with booleans for cells containing a null value.

        Returns:
            A new DataFrame with booleans representing whether or not a cell
                is null.
            True: cell contains null.
            False: otherwise.
        """
        return self._map_partitions(lambda df: df.isnull)

    def keys(self):
        """Get the info axis for the DataFrame.

        Returns:
            A pandas Index for this DataFrame.
        """
        # Each partition should have the same index, so we'll use 0's
        return self.columns

    def transpose(self, *args, **kwargs):
        """Transpose columns and rows for the DataFrame.

        Note: Triggers a shuffle.

        Returns:
            A new DataFrame transposed from this DataFrame.
        """
        temp_index = [idx
                      for _ in range(len(self._df))
                      for idx in self.columns]

        temp_columns = self.index
        local_transpose = self._map_partitions(
            lambda df: df.transpose(*args, **kwargs), index=temp_index)
        local_transpose.columns = temp_columns

        # Sum will collapse the NAs from the groupby
        return local_transpose.reduce_by_index(
            lambda df: df.apply(lambda x: x), axis=1)

    T = property(transpose)

    def dropna(self, axis, how, thresh=None, subset=[], inplace=False):
        """Create a new DataFrame from the removed NA values from this one.

        Args:
            axis (int, tuple, or list): The axis to apply the drop.
            how (str): How to drop the NA values.
                'all': drop the label if all values are NA.
                'any': drop the label if any values are NA.
            thresh (int): The minimum number of NAs to require.
            subset ([label]): Labels to consider from other axis.
            inplace (bool): Change this DataFrame or return a new DataFrame.
                True: Modify the data for this DataFrame, return None.
                False: Create a new DataFrame and return it.

        Returns:
            If inplace is set to True, returns None, otherwise returns a new
            DataFrame with the dropna applied.
        """
        raise NotImplementedError("Not yet")
        if how != 'any' and how != 'all':
            raise ValueError("<how> not correctly set.")

    def add(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def agg(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def aggregate(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def align(self, other, join='outer', axis=None, level=None, copy=True,
              fill_value=None, method=None, limit=None, fill_axis=0,
              broadcast_axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def all(self, axis=None, bool_only=None, skipna=None, level=None,
            **kwargs):
        """Return whether all elements are True over requested axis

        Note:
            If axis=None or axis=0, this call applies df.all(axis=1)
            to the transpose of df.
        """
        if axis is None or axis == 0:
            df = self.T
            axis = 1
        else:
            df = self

        mapped = df._map_partitions(lambda df: df.all(axis,
                                                      bool_only,
                                                      skipna,
                                                      level,
                                                      **kwargs))
        return to_pandas(mapped)

    def any(self, axis=None, bool_only=None, skipna=None, level=None,
            **kwargs):
        """Return whether all elements are True over requested axis

        Note:
            If axis=None or axis=0, this call applies df.all(axis=1)
            to the transpose of df.
        """
        if axis is None or axis == 0:
            df = self.T
            axis = 1
        else:
            df = self

        mapped = df._map_partitions(lambda df: df.any(axis,
                                                      bool_only,
                                                      skipna,
                                                      level,
                                                      **kwargs))
        return to_pandas(mapped)

    def append(self, other, ignore_index=False, verify_integrity=False):
        raise NotImplementedError("Not Yet implemented.")

    def apply(self, func, axis=0, broadcast=False, raw=False, reduce=None,
              args=(), **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def as_blocks(self, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def as_matrix(self, columns=None):
        raise NotImplementedError("Not Yet implemented.")

    def asfreq(self, freq, method=None, how=None, normalize=False,
               fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def asof(self, where, subset=None):
        raise NotImplementedError("Not Yet implemented.")

    def assign(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def at_time(self, time, asof=False):
        raise NotImplementedError("Not Yet implemented.")

    def between_time(self, start_time, end_time, include_start=True,
                     include_end=True):
        raise NotImplementedError("Not Yet implemented.")

    def bfill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError("Not Yet implemented.")

    def bool(self):
        """Return the bool of a single element PandasObject.

        This must be a boolean scalar value, either True or False.  Raise a
        ValueError if the PandasObject does not have exactly 1 element, or that
        element is not boolean
        """
        shape = self.shape
        if shape != (1,) and shape != (1, 1):
            raise ValueError("""The PandasObject does not have exactly
                                1 element. Return the bool of a single
                                element PandasObject. The truth value is
                                ambiguous. Use a.empty, a.item(), a.any()
                                or a.all().""")
        else:
            return to_pandas(self).bool()

    def boxplot(self, column=None, by=None, ax=None, fontsize=None, rot=0,
                grid=True, figsize=None, layout=None, return_type=None,
                **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def clip(self, lower=None, upper=None, axis=None, inplace=False, *args,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def clip_lower(self, threshold, axis=None, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def clip_upper(self, threshold, axis=None, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def combine(self, other, func, fill_value=None, overwrite=True):
        raise NotImplementedError("Not Yet implemented.")

    def combine_first(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def compound(self, axis=None, skipna=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def consolidate(self, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def convert_objects(self, convert_dates=True, convert_numeric=False,
                        convert_timedeltas=True, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def corr(self, method='pearson', min_periods=1):
        raise NotImplementedError("Not Yet implemented.")

    def corrwith(self, other, axis=0, drop=False):
        raise NotImplementedError("Not Yet implemented.")

    def count(self, axis=0, level=None, numeric_only=False):
        if axis == 1:
            return self.T.count(axis=0,
                                level=level,
                                numeric_only=numeric_only)
        else:
            temp_index = [idx
                          for _ in range(len(self._df))
                          for idx in self.columns]

            return sum(ray.get(self._map_partitions(lambda df: df.count(
                axis=axis, level=level, numeric_only=numeric_only
            ), index=temp_index)._df))

    def cov(self, min_periods=None):
        raise NotImplementedError("Not Yet implemented.")

    def cummax(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cummin(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cumprod(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def describe(self, percentiles=None, include=None, exclude=None):
        raise NotImplementedError("Not Yet implemented.")

    def diff(self, periods=1, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def div(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def divide(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def dot(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        raise NotImplementedError("Not Yet implemented.")

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def duplicated(self, subset=None, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def eq(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def equals(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def eval(self, expr, inplace=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def ewm(self, com=None, span=None, halflife=None, alpha=None,
            min_periods=0, freq=None, adjust=True, ignore_na=False, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def expanding(self, min_periods=1, freq=None, center=False, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def ffill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError("Not Yet implemented.")

    def fillna(self, value=None, method=None, axis=None, inplace=False,
               limit=None, downcast=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def filter(self, items=None, like=None, regex=None, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def first(self, offset):
        raise NotImplementedError("Not Yet implemented.")

    def first_valid_index(self):
        raise NotImplementedError("Not Yet implemented.")

    def floordiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    @classmethod
    def from_csv(self, path, header=0, sep=', ', index_col=0,
                 parse_dates=True, encoding=None, tupleize_cols=None,
                 infer_datetime_format=False):
        raise NotImplementedError("Not Yet implemented.")

    @classmethod
    def from_dict(self, data, orient='columns', dtype=None):
        raise NotImplementedError("Not Yet implemented.")

    @classmethod
    def from_items(self, items, columns=None, orient='columns'):
        raise NotImplementedError("Not Yet implemented.")

    @classmethod
    def from_records(self, data, index=None, exclude=None, columns=None,
                     coerce_float=False, nrows=None):
        raise NotImplementedError("Not Yet implemented.")

    def ge(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def get(self, key, default=None):
        """Get item from object for given key (DataFrame column, Panel
        slice, etc.). Returns default value if not found.

        Args:
            key (DataFrame column, Panel slice) : the key for which value
            to get

        Returns:
            value (type of items contained in object) : A value that is
            stored at the key
        """
        temp_df = self._map_partitions(lambda df: df.get(key, default=default))
        return to_pandas(temp_df)

    def get_dtype_counts(self):
        """Get the counts of dtypes in this object.

        Returns:
            The counts of dtypes in this object.
        """
        return ray.get(
            _deploy_func.remote(
                lambda df: df.get_dtype_counts(), self._df[0]
            )
        )

    def get_ftype_counts(self):
        """Get the counts of ftypes in this object.

        Returns:
            The counts of ftypes in this object.
        """
        return ray.get(
            _deploy_func.remote(
                lambda df: df.get_ftype_counts(), self._df[0]
            )
        )

    def get_value(self, index, col, takeable=False):
        raise NotImplementedError("Not Yet implemented.")

    def get_values(self):
        raise NotImplementedError("Not Yet implemented.")

    def gt(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def head(self, n=5):
        """Get the first n rows of the dataframe.

        Args:
            n (int): The number of rows to return.

        Returns:
            A new dataframe with the first n rows of the dataframe.
        """
        sizes = self._lengths

        if n >= sum(sizes):
            return self

        cumulative = np.cumsum(np.array(sizes))
        new_dfs = [self._df[i]
                   for i in range(len(cumulative))
                   if cumulative[i] < n]

        last_index = len(new_dfs)

        # this happens when we only need from the first partition
        if last_index == 0:
            num_to_transfer = n
        else:
            num_to_transfer = n - cumulative[last_index - 1]

        new_dfs.append(_deploy_func.remote(lambda df: df.head(num_to_transfer),
                                           self._df[last_index]))

        index = self._index.head(n).index
        return DataFrame(new_dfs, self.columns, index=index)

    def hist(self, data, column=None, by=None, grid=True, xlabelsize=None,
             xrot=None, ylabelsize=None, yrot=None, ax=None, sharex=False,
             sharey=False, figsize=None, layout=None, bins=10, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def idxmax(self, axis=0, skipna=True):
        """Get the index of the first occurrence of the max value of the axis.

        Args:
            axis (int): Identify the max over the rows (1) or columns (0).
            skipna (bool): Whether or not to skip NA values.

        Returns:
            A Series with the index for each maximum value for the axis
                specified.
        """
        for t in self.dtypes:
            if np.dtype('O') == t:
                # TODO Give a more accurate error to Pandas
                raise TypeError("bad operand type for abs():", "str")
        if axis == 1:
            return to_pandas(self._map_partitions(
                lambda df: df.idxmax(axis=axis, skipna=skipna)))
        else:
            return self.T.idxmax(axis=1, skipna=skipna)

    def idxmin(self, axis=0, skipna=True):
        """Get the index of the first occurrence of the min value of the axis.

        Args:
            axis (int): Identify the min over the rows (1) or columns (0).
            skipna (bool): Whether or not to skip NA values.

        Returns:
            A Series with the index for each minimum value for the axis
                specified.
        """
        for t in self.dtypes:
            if np.dtype('O') == t:
                # TODO Give a more accurate error to Pandas
                raise TypeError("bad operand type for abs():", "str")
        if axis == 1:
            return to_pandas(self._map_partitions(
                lambda df: df.idxmin(axis=axis, skipna=skipna)))
        else:
            return self.T.idxmin(axis=1, skipna=skipna)

    def infer_objects(self):
        raise NotImplementedError("Not Yet implemented.")

    def info(self, verbose=None, buf=None, max_cols=None, memory_usage=None,
             null_counts=None):
        raise NotImplementedError("Not Yet implemented.")

    def insert(self, loc, column, value, allow_duplicates=False):
        raise NotImplementedError("Not Yet implemented.")

    def interpolate(self, method='linear', axis=0, limit=None, inplace=False,
                    limit_direction='forward', downcast=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def iterrows(self):
        """Iterate over DataFrame rows as (index, Series) pairs.

        Note:
            Generators can't be pickeled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A generator that iterates over the rows of the frame.
        """
        iters = ray.get([
            _deploy_func.remote(
                lambda df: list(df.iterrows()), part) for part in self._df])
        return itertools.chain.from_iterable(iters)

    def items(self):
        """Iterator over (column name, Series) pairs.

        Note:
            Generators can't be pickeled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A generator that iterates over the columns of the frame.
        """
        iters = ray.get([_deploy_func.remote(
            lambda df: list(df.items()), part) for part in self._df])

        def concat_iters(iterables):
            for partitions in zip(*iterables):
                series = pd.concat([_series for _, _series in partitions])
                yield (series.name, series)

        return concat_iters(iters)

    def iteritems(self):
        """Iterator over (column name, Series) pairs.

        Note:
            Returns the same thing as .items()

        Returns:
            A generator that iterates over the columns of the frame.
        """
        return self.items()

    def itertuples(self, index=True, name='Pandas'):
        """Iterate over DataFrame rows as namedtuples.

        Args:
            index (boolean, default True): If True, return the index as the
                first element of the tuple.
            name (string, default "Pandas"): The name of the returned
            namedtuples or None to return regular tuples.
        Note:
            Generators can't be pickeled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A tuple representing row data. See args for varying tuples.
        """
        iters = ray.get([
            _deploy_func.remote(
                lambda df: list(df.itertuples(index=index, name=name)),
                part) for part in self._df])
        return itertools.chain.from_iterable(iters)

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        raise NotImplementedError("Not Yet implemented.")

    def kurt(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def kurtosis(self, axis=None, skipna=None, level=None, numeric_only=None,
                 **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def last(self, offset):
        raise NotImplementedError("Not Yet implemented.")

    def last_valid_index(self):
        raise NotImplementedError("Not Yet implemented.")

    def le(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def lookup(self, row_labels, col_labels):
        raise NotImplementedError("Not Yet implemented.")

    def lt(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def mad(self, axis=None, skipna=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def mask(self, cond, other=np.nan, inplace=False, axis=None, level=None,
             errors='raise', try_cast=False, raise_on_error=None):
        raise NotImplementedError("Not Yet implemented.")

    def max(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        """Perform max across the DataFrame.

        Args:
            axis (int): The axis to take the max on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The max of the DataFrame.
        """
        if(axis == 1):
            return self._map_partitions(
                lambda df: df.max(axis=axis, skipna=skipna, level=level,
                                  numeric_only=numeric_only, **kwargs))
        else:
            return self.T.max(axis=1, skipna=None, level=None,
                              numeric_only=None, **kwargs)

    def mean(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def median(self, axis=None, skipna=None, level=None, numeric_only=None,
               **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def melt(self, id_vars=None, value_vars=None, var_name=None,
             value_name='value', col_level=None):
        raise NotImplementedError("Not Yet implemented.")

    def memory_usage(self, index=True, deep=False):
        raise NotImplementedError("Not Yet implemented.")

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True, indicator=False,
              validate=None):
        raise NotImplementedError("Not Yet implemented.")

    def min(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        """Perform min across the DataFrame.

        Args:
            axis (int): The axis to take the min on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The min of the DataFrame.
        """
        if(axis == 1):
            return self._map_partitions(
                lambda df: df.min(axis=axis, skipna=skipna, level=level,
                                  numeric_only=numeric_only, **kwargs))
        else:
            return self.T.min(axis=1, skipna=skipna, level=level,
                              numeric_only=numeric_only, **kwargs)

    def mod(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def mode(self, axis=0, numeric_only=False):
        raise NotImplementedError("Not Yet implemented.")

    def mul(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def multiply(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def ne(self, other, axis='columns', level=None):
        raise NotImplementedError("Not Yet implemented.")

    def nlargest(self, n, columns, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def notna(self):
        """Perform notna across the DataFrame.

        Args:
            None

        Returns:
            Boolean DataFrame where value is False if corresponding
            value is NaN, True otherwise
        """
        return self._map_partitions(lambda df: df.notna())

    def notnull(self):
        """Perform notnull across the DataFrame.

        Args:
            None

        Returns:
            Boolean DataFrame where value is False if corresponding
            value is NaN, True otherwise
        """
        return self._map_partitions(lambda df: df.notnull())

    def nsmallest(self, n, columns, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def nunique(self, axis=0, dropna=True):
        raise NotImplementedError("Not Yet implemented.")

    def pct_change(self, periods=1, fill_method='pad', limit=None, freq=None,
                   **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def pipe(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def pivot(self, index=None, columns=None, values=None):
        raise NotImplementedError("Not Yet implemented.")

    def pivot_table(self, values=None, index=None, columns=None,
                    aggfunc='mean', fill_value=None, margins=False,
                    dropna=True, margins_name='All'):
        raise NotImplementedError("Not Yet implemented.")

    def plot(self, x=None, y=None, kind='line', ax=None, subplots=False,
             sharex=None, sharey=False, layout=None, figsize=None,
             use_index=True, title=None, grid=None, legend=True, style=None,
             logx=False, logy=False, loglog=False, xticks=None, yticks=None,
             xlim=None, ylim=None, rot=None, fontsize=None, colormap=None,
             table=False, yerr=None, xerr=None, secondary_y=False,
             sort_columns=False, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def pop(self, item):
        """Pops an item from this DataFrame and returns it.

        Args:
            item (str): Column label to be popped

        Returns:
            A Series containing the popped values. Also modifies this
            DataFrame.
        """

        popped = to_pandas(self._map_partitions(
            lambda df: df.pop(item)))
        self._df = self._map_partitions(lambda df: df.drop([item], axis=1))._df
        return popped

    def pow(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def prod(self, axis=None, skipna=None, level=None, numeric_only=None,
             min_count=0, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def product(self, axis=None, skipna=None, level=None, numeric_only=None,
                min_count=0, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def quantile(self, q=0.5, axis=0, numeric_only=True,
                 interpolation='linear'):
        raise NotImplementedError("Not Yet implemented.")

    def query(self, expr, inplace=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def radd(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rank(self, axis=0, method='average', numeric_only=None,
             na_option='keep', ascending=True, pct=False):
        raise NotImplementedError("Not Yet implemented.")

    def rdiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def reindex(self, labels=None, index=None, columns=None, axis=None,
                method=None, copy=True, level=None, fill_value=np.nan,
                limit=None, tolerance=None):
        raise NotImplementedError("Not Yet implemented.")

    def reindex_axis(self, labels, axis=0, method=None, level=None, copy=True,
                     limit=None, fill_value=np.nan):
        raise NotImplementedError("Not Yet implemented.")

    def reindex_like(self, other, method=None, copy=True, limit=None,
                     tolerance=None):
        raise NotImplementedError("Not Yet implemented.")

    def rename(self, mapper=None, index=None, columns=None, axis=None,
               copy=True, inplace=False, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def rename_axis(self, mapper, axis=0, copy=True, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def reorder_levels(self, order, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def replace(self, to_replace=None, value=None, inplace=False, limit=None,
                regex=False, method='pad', axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def resample(self, rule, how=None, axis=0, fill_method=None, closed=None,
                 label=None, convention='start', kind=None, loffset=None,
                 limit=None, base=0, on=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def reset_index(self, level=None, drop=False, inplace=False, col_level=0,
                    col_fill=''):
        raise NotImplementedError("Not Yet implemented.")

    def rfloordiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rmod(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rmul(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rolling(self, window, min_periods=None, freq=None, center=False,
                win_type=None, on=None, axis=0, closed=None):
        raise NotImplementedError("Not Yet implemented.")

    def round(self, decimals=0, *args, **kwargs):
        return self._map_partitions(lambda df: df.round(decimals=decimals,
                                                        *args,
                                                        **kwargs))

    def rpow(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rsub(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def rtruediv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def select(self, crit, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def select_dtypes(self, include=None, exclude=None):
        raise NotImplementedError("Not Yet implemented.")

    def sem(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def set_axis(self, labels, axis=0, inplace=None):
        raise NotImplementedError("Not Yet implemented.")

    def set_index(self, keys, drop=True, append=False, inplace=False,
                  verify_integrity=False):
        raise NotImplementedError("Not Yet implemented.")

    def set_value(self, index, col, value, takeable=False):
        raise NotImplementedError("Not Yet implemented.")

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def skew(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def slice_shift(self, periods=1, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def sort_index(self, axis=0, level=None, ascending=True, inplace=False,
                   kind='quicksort', na_position='last', sort_remaining=True,
                   by=None):
        raise NotImplementedError("Not Yet implemented.")

    def sort_values(self, by, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        raise NotImplementedError("Not Yet implemented.")

    def sortlevel(self, level=0, axis=0, ascending=True, inplace=False,
                  sort_remaining=True):
        raise NotImplementedError("Not Yet implemented.")

    def squeeze(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def stack(self, level=-1, dropna=True):
        raise NotImplementedError("Not Yet implemented.")

    def std(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def sub(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def subtract(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def swapaxes(self, axis1, axis2, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def swaplevel(self, i=-2, j=-1, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def tail(self, n=5):
        """Get the last n rows of the dataframe.

        Args:
            n (int): The number of rows to return.

        Returns:
            A new dataframe with the last n rows of this dataframe.
        """
        sizes = self._lengths

        if n >= sum(sizes):
            return self

        cumulative = np.cumsum(np.array(sizes.reverse()))

        reverse_dfs = self._df.reverse()
        new_dfs = [reverse_dfs[i]
                   for i in range(len(cumulative))
                   if cumulative[i] < n]

        last_index = len(new_dfs)

        # this happens when we only need from the last partition
        if last_index == 0:
            num_to_transfer = n
        else:
            num_to_transfer = n - cumulative[last_index - 1]

        new_dfs.append(_deploy_func.remote(lambda df: df.tail(num_to_transfer),
                                           reverse_dfs[last_index])).reverse()

        index = self._index.tail(n).index
        return DataFrame(new_dfs, self.columns, index=index)

    def take(self, indices, axis=0, convert=None, is_copy=True, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_clipboard(self, excel=None, sep=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_csv(self, path_or_buf=None, sep=', ', na_rep='', float_format=None,
               columns=None, header=True, index=True, index_label=None,
               mode='w', encoding=None, compression=None, quoting=None,
               quotechar='"', line_terminator='\n', chunksize=None,
               tupleize_cols=None, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        raise NotImplementedError("Not Yet implemented.")

    def to_dense(self):
        raise NotImplementedError("Not Yet implemented.")

    def to_dict(self, orient='dict', into=dict):
        raise NotImplementedError("Not Yet implemented.")

    def to_excel(self, excel_writer, sheet_name='Sheet1', na_rep='',
                 float_format=None, columns=None, header=True, index=True,
                 index_label=None, startrow=0, startcol=0, engine=None,
                 merge_cells=True, encoding=None, inf_rep='inf', verbose=True,
                 freeze_panes=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_feather(self, fname):
        raise NotImplementedError("Not Yet implemented.")

    def to_gbq(self, destination_table, project_id, chunksize=10000,
               verbose=True, reauth=False, if_exists='fail',
               private_key=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_hdf(self, path_or_buf, key, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_html(self, buf=None, columns=None, col_space=None, header=True,
                index=True, na_rep='np.NaN', formatters=None,
                float_format=None, sparsify=None, index_names=True,
                justify=None, bold_rows=True, classes=None, escape=True,
                max_rows=None, max_cols=None, show_dimensions=False,
                notebook=False, decimal='.', border=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_latex(self, buf=None, columns=None, col_space=None, header=True,
                 index=True, na_rep='np.NaN', formatters=None,
                 float_format=None, sparsify=None, index_names=True,
                 bold_rows=False, column_format=None, longtable=None,
                 escape=None, encoding=None, decimal='.', multicolumn=None,
                 multicolumn_format=None, multirow=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_msgpack(self, path_or_buf=None, encoding='utf-8', **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_panel(self):
        raise NotImplementedError("Not Yet implemented.")

    def to_parquet(self, fname, engine='auto', compression='snappy',
                   **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_period(self, freq=None, axis=0, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_pickle(self, path, compression='infer', protocol=4):
        raise NotImplementedError("Not Yet implemented.")

    def to_records(self, index=True, convert_datetime64=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_sparse(self, fill_value=None, kind='block'):
        raise NotImplementedError("Not Yet implemented.")

    def to_sql(self, name, con, flavor=None, schema=None, if_exists='fail',
               index=True, index_label=None, chunksize=None, dtype=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_stata(self, fname, convert_dates=None, write_index=True,
                 encoding='latin-1', byteorder=None, time_stamp=None,
                 data_label=None, variable_labels=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_string(self, buf=None, columns=None, col_space=None, header=True,
                  index=True, na_rep='np.NaN', formatters=None,
                  float_format=None, sparsify=None, index_names=True,
                  justify=None, line_width=None, max_rows=None, max_cols=None,
                  show_dimensions=False):
        raise NotImplementedError("Not Yet implemented.")

    def to_timestamp(self, freq=None, how='start', axis=0, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_xarray(self):
        raise NotImplementedError("Not Yet implemented.")

    def transform(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def truediv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def truncate(self, before=None, after=None, axis=None, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def tshift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def tz_convert(self, tz, axis=0, level=None, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def tz_localize(self, tz, axis=0, level=None, copy=True,
                    ambiguous='raise'):
        raise NotImplementedError("Not Yet implemented.")

    def unstack(self, level=-1, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def update(self, other, join='left', overwrite=True, filter_func=None,
               raise_conflict=False):
        raise NotImplementedError("Not Yet implemented.")

    def var(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def where(self, cond, other=np.nan, inplace=False, axis=None, level=None,
              errors='raise', try_cast=False, raise_on_error=None):
        raise NotImplementedError("Not Yet implemented.")

    def xs(self, key, axis=0, level=None, drop_level=True):
        raise NotImplementedError("Not Yet implemented.")

    def __getitem__(self, key):
        """Get the column specified by key for this DataFrame.

        Args:
            key : The column name.

        Returns:
            A Pandas Series representing the value fo the column.
        """
        result_column_chunks = self._map_partitions(
            lambda df: df.__getitem__(key))
        return to_pandas(result_column_chunks)

    def __setitem__(self, key, value):
        raise NotImplementedError("Not Yet implemented.")

    def __len__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __unicode__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __invert__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __hash__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __iter__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __contains__(self, key):
        raise NotImplementedError("Not Yet implemented.")

    def __nonzero__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __bool__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __abs__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __round__(self, decimals=0):
        raise NotImplementedError("Not Yet implemented.")

    def __array__(self, dtype=None):
        raise NotImplementedError("Not Yet implemented.")

    def __array_wrap__(self, result, context=None):
        raise NotImplementedError("Not Yet implemented.")

    def __getstate__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __setstate__(self, state):
        raise NotImplementedError("Not Yet implemented.")

    def __delitem__(self, key):
        """Delete an item by key. `del a[key]` for example.
           Operation happnes in place.

        Args:
            key: key to delete
        """
        def del_helper(df):
            df.__delitem__(key)
            return df
        self._df = self._map_partitions(del_helper)._df
        self.columns = self.columns.drop(key)

    def __finalize__(self, other, method=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def __copy__(self, deep=True):
        """Make a copy using Ray.DataFrame.copy method

        Args:
            deep: Boolean, deep copy or not.
                  Currently we do not support deep copy.

        Returns:
            A Ray DataFrame object.
        """
        return self.copy(deep=deep)

    def __deepcopy__(self, memo=None):
        """Make a -deep- copy using Ray.DataFrame.copy method
           This is equivalent to copy(deep=True).

        Args:
            memo: No effect. Just to comply with Pandas API.

        Returns:
            A Ray DataFrame object.
        """
        return self.copy(deep=True)

    def __and__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __or__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __xor__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __lt__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __le__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __gt__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __ge__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __eq__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __ne__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __add__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __iadd__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __mul__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __imul__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __pow__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __ipow__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __sub__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __isub__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __neg__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __floordiv__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __truediv__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __mod__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __sizeof__(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def __doc__(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def blocks(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def style(self):
        raise NotImplementedError("Not Yet implemented.")

    def iat(axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __rsub__(other, axis=None, level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def loc(axis=None):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_copy(self):
        raise NotImplementedError("Not Yet implemented.")

    def __itruediv__(other):
        raise NotImplementedError("Not Yet implemented.")

    def __div__(other, axis=None, level=None, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def at(axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def ix(axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def iloc(axis=None):
        raise NotImplementedError("Not Yet implemented.")


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
    if sort and not df.index.is_monotonic_increasing:
        df = df.sort_index(ascending=True)

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
        # reindex here because we want a pd.RangeIndex within the partitions.
        # It is smaller and sometimes faster.
        t_df.reindex()
        top = ray.put(t_df)
        dataframes.append(top)
        temp_df = temp_df[chunksize:]
    else:
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
    return pd.concat(ray.get(df._df))
