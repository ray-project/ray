from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
from pandas.api.types import is_scalar
from pandas.util._validators import validate_bool_kwarg
from pandas.core.index import _ensure_index_from_sequences
from pandas._libs import lib
from pandas.core.dtypes.cast import maybe_upcast_putmask
from pandas.compat import lzip
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_numeric_dtype,
    is_timedelta64_dtype)

import warnings
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
        self.columns = columns

        # this _index object is a pd.DataFrame
        # and we use that DataFrame's Index to index the rows.
        self._lengths, self._index = _compute_length_and_index.remote(self._df)

        if index is not None:
            self.index = index

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if sum(self._lengths) < 40:
            result = repr(to_pandas(self))
            return result

        head = repr(to_pandas(self.head(20)))
        tail = repr(to_pandas(self.tail(20)))

        result = head + "\n...\n" + tail

        return result

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

    index = property(_get_index, _set_index)

    def _get__index(self):
        """Get the _index for this DataFrame.

        Returns:
            The default index.
        """
        if isinstance(self._index_cache, ray.local_scheduler.ObjectID):
            self._index_cache = ray.get(self._index_cache)
        return self._index_cache

    def _set__index(self, new__index):
        """Set the _index for this DataFrame.

        Args:
            new__index: The new default index to set.
        """
        self._index_cache = new__index

    _index = property(_get__index, _set__index)

    def _compute_lengths(self):
        """Updates the stored lengths of DataFrame partions
        """
        self._lengths = [_deploy_func.remote(_get_lengths, d)
                         for d in self._df]

    def _get_lengths(self):
        """Gets the lengths for each partition and caches it if it wasn't.

        Returns:
            A list of integers representing the length of each partition.
        """
        if isinstance(self._length_cache, ray.local_scheduler.ObjectID):
            self._length_cache = ray.get(self._length_cache)
        elif isinstance(self._length_cache, list) and \
                isinstance(self._length_cache[0],
                           ray.local_scheduler.ObjectID):
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

    def _update_inplace(self, df=None, columns=None, index=None):
        """Updates the current DataFrame inplace
        """
        assert(len(df) > 0)

        if df:
            self._df = df
        if columns:
            self.columns = columns
        if index:
            self.index = index

        self._lengths, self._index = _compute_length_and_index.remote(self._df)

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
        df = local_transpose.reduce_by_index(
            lambda df: df.apply(lambda x: x), axis=1)

        # Reassign the columns within partition to self.index.
        # We have to use _depoly_func instead of _map_partition due to
        #    new_labels argument
        def _reassign_columns(df, new_labels):
            df.columns = new_labels
            return df
        df._df = [
            _deploy_func.remote(
                _reassign_columns,
                part,
                self.index) for part in df._df]

        return df

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def agg(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def aggregate(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def align(self, other, join='outer', axis=None, level=None, copy=True,
              fill_value=None, method=None, limit=None, fill_axis=0,
              broadcast_axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def apply(self, func, axis=0, broadcast=False, raw=False, reduce=None,
              args=(), **kwds):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def as_blocks(self, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def as_matrix(self, columns=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def asfreq(self, freq, method=None, how=None, normalize=False,
               fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def asof(self, where, subset=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def assign(self, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def at_time(self, time, asof=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def between_time(self, start_time, end_time, include_start=True,
                     include_end=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def bfill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def clip(self, lower=None, upper=None, axis=None, inplace=False, *args,
             **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def clip_lower(self, threshold, axis=None, inplace=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def clip_upper(self, threshold, axis=None, inplace=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def combine(self, other, func, fill_value=None, overwrite=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def combine_first(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def compound(self, axis=None, skipna=None, level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def consolidate(self, inplace=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def convert_objects(self, convert_dates=True, convert_numeric=False,
                        convert_timedeltas=True, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def corr(self, method='pearson', min_periods=1):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def corrwith(self, other, axis=0, drop=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def count(self, axis=0, level=None, numeric_only=False):
        if axis == 1:
            return self.T.count(axis=0,
                                level=level,
                                numeric_only=numeric_only)
        else:
            temp_index = [idx
                          for _ in range(len(self._df))
                          for idx in self.columns]

            collapsed_df = sum(
                ray.get(
                    self._map_partitions(
                        lambda df: df.count(
                            axis=axis,
                            level=level,
                            numeric_only=numeric_only),
                        index=temp_index)._df))
            return collapsed_df

    def cov(self, min_periods=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cummax(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cummin(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cumprod(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def describe(self, percentiles=None, include=None, exclude=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def diff(self, periods=1, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def div(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def divide(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def dot(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def duplicated(self, subset=None, keep='first'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def eq(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def equals(self, other):
        """
        Checks if other DataFrame is elementwise equal to the current one

        Returns:
            Boolean: True if equal, otherwise False
        """
        def helper(df, index, other_series):
            return df.iloc[index['index_within_partition']] \
                        .equals(other_series)

        results = []
        other_partition = None
        other_df = None
        for i, idx in other._index.iterrows():
            if idx['partition'] != other_partition:
                other_df = ray.get(other._df[idx['partition']])
                other_partition = idx['partition']
            # TODO: group series here into full df partitions to reduce
            # the number of remote calls to helper
            other_series = other_df.iloc[idx['index_within_partition']]
            curr_index = self._index.iloc[i]
            curr_df = self._df[int(curr_index['partition'])]
            results.append(_deploy_func.remote(helper,
                                               curr_df,
                                               curr_index,
                                               other_series))

        for r in results:
            if not ray.get(r):
                return False
        return True

    def eval(self, expr, inplace=False, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ewm(self, com=None, span=None, halflife=None, alpha=None,
            min_periods=0, freq=None, adjust=True, ignore_na=False, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def expanding(self, min_periods=1, freq=None, center=False, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ffill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def fillna(self, value=None, method=None, axis=None, inplace=False,
               limit=None, downcast=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def filter(self, items=None, like=None, regex=None, axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def first(self, offset):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def first_valid_index(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def floordiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @classmethod
    def from_csv(self, path, header=0, sep=', ', index_col=0,
                 parse_dates=True, encoding=None, tupleize_cols=None,
                 infer_datetime_format=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @classmethod
    def from_dict(self, data, orient='columns', dtype=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @classmethod
    def from_items(self, items, columns=None, orient='columns'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @classmethod
    def from_records(self, data, index=None, exclude=None, columns=None,
                     coerce_float=False, nrows=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ge(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def get_values(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def gt(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def info(self, verbose=None, buf=None, max_cols=None, memory_usage=None,
             null_counts=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def insert(self, loc, column, value, allow_duplicates=False):
        """Insert column into DataFrame at specified location.

        Args:
            loc (int): Insertion index. Must verify 0 <= loc <= len(columns).
            column (hashable object): Label of the inserted column.
            value (int, Series, or array-like): The values to insert.
            allow_duplicates (bool): Whether to allow duplicate column names.
        """
        try:
            len(value)
        except TypeError:
            value = [value for _ in range(len(self.index))]

        if len(value) != len(self.index):
            raise ValueError(
                "Column length provided does not match DataFrame length.")
        if loc < 0 or loc > len(self.columns):
            raise ValueError(
                "Location provided must be higher than 0 and lower than the "
                "number of columns.")
        if not allow_duplicates and column in self.columns:
            raise ValueError(
                "Column {} already exists in DataFrame.".format(column))

        cumulative = np.cumsum(self._lengths)
        partitions = [value[cumulative[i-1]:cumulative[i]]
                      for i in range(len(cumulative))
                      if i != 0]

        partitions.insert(0, value[:cumulative[0]])

        # Because insert is always inplace, we have to create this temp fn.
        def _insert(_df, _loc, _column, _part, _allow_duplicates):
            _df.insert(_loc, _column, _part, _allow_duplicates)
            return _df

        self._df = \
            [_deploy_func.remote(_insert,
                                 self._df[i],
                                 loc,
                                 column,
                                 partitions[i],
                                 allow_duplicates)
             for i in range(len(self._df))]

        self.columns = self.columns.insert(loc, column)

    def interpolate(self, method='linear', axis=0, limit=None, inplace=False,
                    limit_direction='forward', downcast=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        iters = itertools.chain.from_iterable(iters)
        series = map(lambda idx_series_tuple: idx_series_tuple[1], iters)
        return zip(self.index, series)

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
                series.index = self.index
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
        iters = itertools.chain.from_iterable(iters)

        def _replace_index(row_tuple, idx):
            # We need to use try-except here because
            # isinstance(row_tuple, namedtuple) won't work.
            try:
                row_tuple = row_tuple._replace(Index=idx)
            except AttributeError:  # Tuple not namedtuple
                row_tuple = (idx,) + row_tuple[1:]
            return row_tuple

        if index:
            iters = itertools.starmap(_replace_index, zip(iters, self.index))
        return iters

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def kurt(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def kurtosis(self, axis=None, skipna=None, level=None, numeric_only=None,
                 **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def last(self, offset):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def last_valid_index(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def le(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def lookup(self, row_labels, col_labels):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def lt(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def mad(self, axis=None, skipna=None, level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def mask(self, cond, other=np.nan, inplace=False, axis=None, level=None,
             errors='raise', try_cast=False, raise_on_error=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def median(self, axis=None, skipna=None, level=None, numeric_only=None,
               **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def melt(self, id_vars=None, value_vars=None, var_name=None,
             value_name='value', col_level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def memory_usage(self, index=True, deep=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True, indicator=False,
              validate=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def mode(self, axis=0, numeric_only=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def mul(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def multiply(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ne(self, other, axis='columns', level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def nlargest(self, n, columns, keep='first'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def nunique(self, axis=0, dropna=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pct_change(self, periods=1, fill_method='pad', limit=None, freq=None,
                   **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pipe(self, func, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pivot(self, index=None, columns=None, values=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pivot_table(self, values=None, index=None, columns=None,
                    aggfunc='mean', fill_value=None, margins=False,
                    dropna=True, margins_name='All'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def plot(self, x=None, y=None, kind='line', ax=None, subplots=False,
             sharex=None, sharey=False, layout=None, figsize=None,
             use_index=True, title=None, grid=None, legend=True, style=None,
             logx=False, logy=False, loglog=False, xticks=None, yticks=None,
             xlim=None, ylim=None, rot=None, fontsize=None, colormap=None,
             table=False, yerr=None, xerr=None, secondary_y=False,
             sort_columns=False, **kwds):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        self.columns = self.columns.drop(item)
        return popped

    def pow(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def prod(self, axis=None, skipna=None, level=None, numeric_only=None,
             min_count=0, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def product(self, axis=None, skipna=None, level=None, numeric_only=None,
                min_count=0, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def quantile(self, q=0.5, axis=0, numeric_only=True,
                 interpolation='linear'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def query(self, expr, inplace=False, **kwargs):
        """Queries the Dataframe with a boolean expression

        Returns:
            A new DataFrame if inplace=False
        """
        new_dfs = [_deploy_func.remote(lambda df: df.query(expr, **kwargs),
                                       part) for part in self._df]

        if inplace:
            self._update_inplace(new_dfs)
        else:
            return DataFrame(new_dfs, self.columns)

    def radd(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rank(self, axis=0, method='average', numeric_only=None,
             na_option='keep', ascending=True, pct=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rdiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def reindex(self, labels=None, index=None, columns=None, axis=None,
                method=None, copy=True, level=None, fill_value=np.nan,
                limit=None, tolerance=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def reindex_axis(self, labels, axis=0, method=None, level=None, copy=True,
                     limit=None, fill_value=np.nan):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def reindex_like(self, other, method=None, copy=True, limit=None,
                     tolerance=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rename(self, mapper=None, index=None, columns=None, axis=None,
               copy=True, inplace=False, level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rename_axis(self, mapper, axis=0, copy=True, inplace=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def reorder_levels(self, order, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def replace(self, to_replace=None, value=None, inplace=False, limit=None,
                regex=False, method='pad', axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def resample(self, rule, how=None, axis=0, fill_method=None, closed=None,
                 label=None, convention='start', kind=None, loffset=None,
                 limit=None, base=0, on=None, level=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def reset_index(self, level=None, drop=False, inplace=False, col_level=0,
                    col_fill=''):
        """Reset this index to default and create column from current index.

        Args:
            level: Only remove the given levels from the index. Removes all
                levels by default
            drop: Do not try to insert index into dataframe columns. This
                resets the index to the default integer index.
            inplace: Modify the DataFrame in place (do not create a new object)
            col_level : If the columns have multiple levels, determines which
                level the labels are inserted into. By default it is inserted
                into the first level.
            col_fill: If the columns have multiple levels, determines how the
                other levels are named. If None then the index name is
                repeated.

        Returns:
            A new DataFrame if inplace is False, None otherwise.
        """
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if inplace:
            new_obj = self
        else:
            new_obj = self.copy()

        def _maybe_casted_values(index, labels=None):
            if isinstance(index, pd.PeriodIndex):
                values = index.asobject.values
            elif isinstance(index, pd.DatetimeIndex) and index.tz is not None:
                values = index
            else:
                values = index.values
                if values.dtype == np.object_:
                    values = lib.maybe_convert_objects(values)

            # if we have the labels, extract the values with a mask
            if labels is not None:
                mask = labels == -1

                # we can have situations where the whole mask is -1,
                # meaning there is nothing found in labels, so make all nan's
                if mask.all():
                    values = np.empty(len(mask))
                    values.fill(np.nan)
                else:
                    values = values.take(labels)
                    if mask.any():
                        values, changed = maybe_upcast_putmask(
                            values, mask, np.nan)
            return values

        _, new_index = _compute_length_and_index.remote(new_obj._df)
        new_index = ray.get(new_index).index
        if level is not None:
            if not isinstance(level, (tuple, list)):
                level = [level]
            level = [self.index._get_level_number(lev) for lev in level]
            if isinstance(self.index, pd.MultiIndex):
                if len(level) < self.index.nlevels:
                    new_index = self.index.droplevel(level)

        if not drop:
            if isinstance(self.index, pd.MultiIndex):
                names = [n if n is not None else ('level_%d' % i)
                         for (i, n) in enumerate(self.index.names)]
                to_insert = lzip(self.index.levels, self.index.labels)
            else:
                default = 'index' if 'index' not in self else 'level_0'
                names = ([default] if self.index.name is None
                         else [self.index.name])
                to_insert = ((self.index, None),)

            multi_col = isinstance(self.columns, pd.MultiIndex)
            for i, (lev, lab) in reversed(list(enumerate(to_insert))):
                if not (level is None or i in level):
                    continue
                name = names[i]
                if multi_col:
                    col_name = (list(name) if isinstance(name, tuple)
                                else [name])
                    if col_fill is None:
                        if len(col_name) not in (1, self.columns.nlevels):
                            raise ValueError("col_fill=None is incompatible "
                                             "with incomplete column name "
                                             "{}".format(name))
                        col_fill = col_name[0]

                    lev_num = self.columns._get_level_number(col_level)
                    name_lst = [col_fill] * lev_num + col_name
                    missing = self.columns.nlevels - len(name_lst)
                    name_lst += [col_fill] * missing
                    name = tuple(name_lst)
                # to ndarray and maybe infer different dtype
                level_values = _maybe_casted_values(lev, lab)
                new_obj.insert(0, name, level_values)

        new_obj.index = new_index
        if not inplace:
            return new_obj

    def rfloordiv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rmod(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rmul(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rolling(self, window, min_periods=None, freq=None, center=False,
                win_type=None, on=None, axis=0, closed=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def round(self, decimals=0, *args, **kwargs):
        return self._map_partitions(lambda df: df.round(decimals=decimals,
                                                        *args,
                                                        **kwargs))

    def rpow(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rsub(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rtruediv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def select(self, crit, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def select_dtypes(self, include=None, exclude=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sem(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def set_axis(self, labels, axis=0, inplace=None):
        """Assign desired index to given axis.

        Args:
            labels (pd.Index or list-like): The Index to assign.
            axis (string or int): The axis to reassign.
            inplace (bool): Whether to make these modifications inplace.

        Returns:
            If inplace is False, returns a new DataFrame, otherwise None.
        """
        if is_scalar(labels):
            warnings.warn(
                'set_axis now takes "labels" as first argument, and '
                '"axis" as named parameter. The old form, with "axis" as '
                'first parameter and \"labels\" as second, is still supported '
                'but will be deprecated in a future version of pandas.',
                FutureWarning, stacklevel=2)
            labels, axis = axis, labels

        if inplace is None:
            warnings.warn(
                'set_axis currently defaults to operating inplace.\nThis '
                'will change in a future version of pandas, use '
                'inplace=True to avoid this warning.',
                FutureWarning, stacklevel=2)
            inplace = True
        if inplace:
            setattr(self, self._index._get_axis_name(axis), labels)
        else:
            obj = self.copy()
            obj.set_axis(labels, axis=axis, inplace=True)
            return obj

    def set_index(self, keys, drop=True, append=False, inplace=False,
                  verify_integrity=False):
        """Set the DataFrame index using one or more existing columns.

        Args:
            keys: column label or list of column labels / arrays.
            drop (boolean): Delete columns to be used as the new index.
            append (boolean): Whether to append columns to existing index.
            inplace (boolean): Modify the DataFrame in place.
            verify_integrity (boolean): Check the new index for duplicates.
                Otherwise defer the check until necessary. Setting to False
                will improve the performance of this method

        Returns:
            If inplace is set to false returns a new DataFrame, otherwise None.
        """
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if not isinstance(keys, list):
            keys = [keys]

        if inplace:
            frame = self
        else:
            frame = self.copy()

        arrays = []
        names = []
        if append:
            names = [x for x in self.index.names]
            if isinstance(self.index, pd.MultiIndex):
                for i in range(self.index.nlevels):
                    arrays.append(self.index._get_level_values(i))
            else:
                arrays.append(self.index)

        to_remove = []
        for col in keys:
            if isinstance(col, pd.MultiIndex):
                # append all but the last column so we don't have to modify
                # the end of this loop
                for n in range(col.nlevels - 1):
                    arrays.append(col._get_level_values(n))

                level = col._get_level_values(col.nlevels - 1)
                names.extend(col.names)
            elif isinstance(col, pd.Series):
                level = col._values
                names.append(col.name)
            elif isinstance(col, pd.Index):
                level = col
                names.append(col.name)
            elif isinstance(col, (list, np.ndarray, pd.Index)):
                level = col
                names.append(None)
            else:
                level = frame[col]._values
                names.append(col)
                if drop:
                    to_remove.append(col)
            arrays.append(level)

        index = _ensure_index_from_sequences(arrays, names)

        if verify_integrity and not index.is_unique:
            duplicates = index.get_duplicates()
            raise ValueError('Index has duplicate keys: %s' % duplicates)

        for c in to_remove:
            del frame[c]

        # clear up memory usage
        index._cleanup()

        frame.index = index

        if not inplace:
            return frame

    def set_value(self, index, col, value, takeable=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def skew(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def slice_shift(self, periods=1, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sort_index(self, axis=0, level=None, ascending=True, inplace=False,
                   kind='quicksort', na_position='last', sort_remaining=True,
                   by=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sort_values(self, by, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sortlevel(self, level=0, axis=0, ascending=True, inplace=False,
                  sort_remaining=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def squeeze(self, axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def stack(self, level=-1, dropna=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def std(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sub(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def subtract(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def swapaxes(self, axis1, axis2, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def swaplevel(self, i=-2, j=-1, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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

        cumulative = np.cumsum(np.array(sizes[::-1]))

        reverse_dfs = self._df[::-1]
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
                                           reverse_dfs[last_index]))

        new_dfs.reverse()

        index = self._index.tail(n).index
        return DataFrame(new_dfs, self.columns, index=index)

    def take(self, indices, axis=0, convert=None, is_copy=True, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_clipboard(self, excel=None, sep=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_csv(self, path_or_buf=None, sep=', ', na_rep='', float_format=None,
               columns=None, header=True, index=True, index_label=None,
               mode='w', encoding=None, compression=None, quoting=None,
               quotechar='"', line_terminator='\n', chunksize=None,
               tupleize_cols=None, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_dense(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_dict(self, orient='dict', into=dict):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_excel(self, excel_writer, sheet_name='Sheet1', na_rep='',
                 float_format=None, columns=None, header=True, index=True,
                 index_label=None, startrow=0, startcol=0, engine=None,
                 merge_cells=True, encoding=None, inf_rep='inf', verbose=True,
                 freeze_panes=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_feather(self, fname):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_gbq(self, destination_table, project_id, chunksize=10000,
               verbose=True, reauth=False, if_exists='fail',
               private_key=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_hdf(self, path_or_buf, key, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_html(self, buf=None, columns=None, col_space=None, header=True,
                index=True, na_rep='np.NaN', formatters=None,
                float_format=None, sparsify=None, index_names=True,
                justify=None, bold_rows=True, classes=None, escape=True,
                max_rows=None, max_cols=None, show_dimensions=False,
                notebook=False, decimal='.', border=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_latex(self, buf=None, columns=None, col_space=None, header=True,
                 index=True, na_rep='np.NaN', formatters=None,
                 float_format=None, sparsify=None, index_names=True,
                 bold_rows=False, column_format=None, longtable=None,
                 escape=None, encoding=None, decimal='.', multicolumn=None,
                 multicolumn_format=None, multirow=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_msgpack(self, path_or_buf=None, encoding='utf-8', **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_panel(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_parquet(self, fname, engine='auto', compression='snappy',
                   **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_period(self, freq=None, axis=0, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_pickle(self, path, compression='infer', protocol=4):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_records(self, index=True, convert_datetime64=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_sparse(self, fill_value=None, kind='block'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_sql(self, name, con, flavor=None, schema=None, if_exists='fail',
               index=True, index_label=None, chunksize=None, dtype=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_stata(self, fname, convert_dates=None, write_index=True,
                 encoding='latin-1', byteorder=None, time_stamp=None,
                 data_label=None, variable_labels=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_string(self, buf=None, columns=None, col_space=None, header=True,
                  index=True, na_rep='np.NaN', formatters=None,
                  float_format=None, sparsify=None, index_names=True,
                  justify=None, line_width=None, max_rows=None, max_cols=None,
                  show_dimensions=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_timestamp(self, freq=None, how='start', axis=0, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_xarray(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def transform(self, func, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def truediv(self, other, axis='columns', level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def truncate(self, before=None, after=None, axis=None, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def tshift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def tz_convert(self, tz, axis=0, level=None, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def tz_localize(self, tz, axis=0, level=None, copy=True,
                    ambiguous='raise'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def unstack(self, level=-1, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def update(self, other, join='left', overwrite=True, filter_func=None,
               raise_conflict=False):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def var(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def where(self, cond, other=np.nan, inplace=False, axis=None, level=None,
              errors='raise', try_cast=False, raise_on_error=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def xs(self, key, axis=0, level=None, drop_level=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __len__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __unicode__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __invert__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __hash__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __iter__(self):
        """Iterate over the columns

        Returns:
            An Iterator over the columns of the dataframe.
        """
        return iter(self.columns)

    def __contains__(self, key):
        return key in self.columns

    def __nonzero__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __bool__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __abs__(self):
        """Creates a modified DataFrame by elementwise taking the absolute value

        Returns:
            A modified DataFrame
        """
        return self.abs()

    def __round__(self, decimals=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __array__(self, dtype=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __array_wrap__(self, result, context=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __getstate__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __setstate__(self, state):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __or__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __xor__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __lt__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __le__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __gt__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __ge__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __eq__(self, other):
        """Computes the equality of this DataFrame with another

        Returns:
            True, if the DataFrames are equal. False otherwise.
        """
        return self.equals(other)

    def __ne__(self, other):
        """Checks that this DataFrame is not equal to another

        Returns:
            True, if the DataFrames are not equal. False otherwise.
        """
        return not self.equals(other)

    def __add__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __iadd__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __mul__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __imul__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __pow__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __ipow__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __sub__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __isub__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __neg__(self):
        """Computes an element wise negative DataFrame

        Returns:
            A modified DataFrame where every element is the negation of before
        """
        for t in self.dtypes:
            if not (is_bool_dtype(t)
                    or is_numeric_dtype(t)
                    or is_timedelta64_dtype(t)):
                raise TypeError("Unary negative expects numeric dtype, not {}"
                                .format(t))

        return self._map_partitions(lambda df: df.__neg__())

    def __floordiv__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __truediv__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __mod__(self, other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __sizeof__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def __doc__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def blocks(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def style(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def iat(axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __rsub__(other, axis=None, level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def loc(self):
        """Purely label-location based indexer for selection by label.

        We currently support: single label, list array, slice object
        We do not support: boolean array, callable
        """
        from .indexing import _Loc_Indexer
        return _Loc_Indexer(self)

    @property
    def is_copy(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __itruediv__(other):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __div__(other, axis=None, level=None, fill_value=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def at(axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ix(axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def iloc(self):
        """Purely integer-location based indexing for selection by position.

        We currently support: single label, list array, slice object
        We do not support: boolean array, callable
        """
        from .indexing import _iLoc_Indexer
        return _iLoc_Indexer(self)


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
