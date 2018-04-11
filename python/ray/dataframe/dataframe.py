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
import pandas.core.common as com
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_list_like,
    is_numeric_dtype,
    is_timedelta64_dtype)
from pandas.core.indexing import (
    check_bool_indexer,
    convert_to_index_sliceable)

import warnings
import numpy as np
import ray
import itertools

from .utils import (
    _deploy_func,
    _map_partitions,
    _partition_pandas_dataframe,
    to_pandas,
    _build_index,
    _blocks_to_col,
    _blocks_to_row,
    _build_columns,
    _create_block_partitions)
from . import get_npartitions


class DataFrame(object):

    def __init__(self, data=None, index=None, columns=None, dtype=None,
                 copy=False, col_partitions=None, row_partitions=None,
                 block_partitions=None):
        """Distributed DataFrame object backed by Pandas dataframes.

        Args:
            data (numpy ndarray (structured or homogeneous) or dict):
                Dict can contain Series, arrays, constants, or list-like
                objects.
            index (pandas.Index or list): The row index for this dataframe.
            columns (pandas.Index): The column names for this dataframe, in
                pandas Index object.
            dtype: Data type to force. Only a single dtype is allowed.
                If None, infer
            copy (boolean): Copy data from inputs.
                Only affects DataFrame / 2d ndarray input
            col_partitions ([ObjectID]): The list of ObjectIDs that contain
                the column dataframe partitions.
            row_partitions ([ObjectID]): The list of ObjectIDs that contain the
                row dataframe partitions.
            block_partitions: A 2D numpy array of block partitions.
        """
        # Check type of data and use appropriate constructor
        if data is not None or (col_partitions is None and
                                row_partitions is None and
                                block_partitions is None):

            pd_df = pd.DataFrame(data=data, index=index, columns=columns,
                                 dtype=dtype, copy=copy)

            # TODO convert _partition_pandas_dataframe to block partitioning.
            row_partitions = \
                _partition_pandas_dataframe(pd_df,
                                            num_partitions=get_npartitions())

            self._block_partitions = \
                _create_block_partitions(row_partitions, axis=0,
                                         length=len(pd_df.columns))

            # Set in case we were only given a single row/column for below.
            axis = 0
            columns = pd_df.columns
            index = pd_df.index
        else:
            # created this invariant to make sure we never have to go into the
            # partitions to get the columns
            assert columns is not None, \
                "Columns not defined, must define columns for internal " \
                "DataFrame creations"

            if block_partitions is not None:
                # put in numpy array here to make accesses easier since it's 2D
                self._block_partitions = np.array(block_partitions)
                assert self._block_partitions.ndim == 2, \
                    "Block Partitions must be 2D."
            else:
                if row_partitions is not None:
                    axis = 0
                    partitions = row_partitions
                elif col_partitions is not None:
                    axis = 1
                    partitions = col_partitions

                self._block_partitions = \
                    _create_block_partitions(partitions, axis=axis,
                                             length=len(columns))

        # Sometimes we only get a single column or row, which is
        # problematic for building blocks from the partitions, so we
        # add whatever dimension we're missing from the input.
        if self._block_partitions.ndim != 2:
            self._block_partitions = np.expand_dims(self._block_partitions,
                                                    axis=axis ^ 1)

        # Create the row and column index objects for using our partitioning.
        self._row_lengths, self._row_index = \
            _build_index.remote(self._block_partitions[:, 0], index)
        self._col_lengths, self._col_index = \
            _build_columns.remote(self._block_partitions[0, :], columns)

    def _get_row_partitions(self):
        return [_blocks_to_row.remote(*part)
                for part in self._block_partitions]

    def _set_row_partitions(self, new_row_partitions):
        self._block_partitions = \
            _create_block_partitions(new_row_partitions, axis=0,
                                     length=len(self.columns))

    _row_partitions = property(_get_row_partitions, _set_row_partitions)

    def _get_col_partitions(self):
        return [_blocks_to_col.remote(*self._block_partitions[:, i])
                for i in range(self._block_partitions.shape[1])]

    def _set_col_partitions(self, new_col_partitions):
        self._block_partitions = \
            _create_block_partitions(new_col_partitions, axis=1,
                                     length=len(self.index))

    _col_partitions = property(_get_col_partitions, _set_col_partitions)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if sum(self._row_lengths) < 60:
            result = repr(to_pandas(self))
            return result

        def head(df, n):
            """Compute the head for this without creating a new DataFrame"""
            new_dfs = _map_partitions(lambda df: df.head(n),
                                      df)

            index = self._row_index.head(n).index
            pd_head = pd.concat(ray.get(new_dfs), axis=1, copy=False)
            pd_head.index = index
            pd_head.columns = self.columns
            return pd_head

        def tail(df, n):
            """Compute the tail for this without creating a new DataFrame"""

            new_dfs = _map_partitions(lambda df: df.tail(n),
                                      df)

            index = self._row_index.tail(n).index
            pd_tail = pd.concat(ray.get(new_dfs), axis=1, copy=False)
            pd_tail.index = index
            pd_tail.columns = self.columns
            return pd_tail

        x = self._col_partitions
        head = head(x, 30)
        tail = tail(x, 30)

        # Make the dots in between the head and tail
        dots = pd.Series(["..."
                          for _ in range(self._block_partitions.shape[1])])
        dots.index = head.columns
        dots.name = "..."

        # We have to do it this way or convert dots to a dataframe and
        # transpose. This seems better.
        result = head.append(dots).append(tail)

        # We use pandas repr so that we match them.
        # The split here is so that we don't repr pandas row lengths.
        return repr(result).split("\n\n")[0] + \
            "\n\n[{0} rows X {1} columns]".format(len(self.index),
                                                  len(self.columns))

    def _get_index(self):
        """Get the index for this DataFrame.

        Returns:
            The union of all indexes across the partitions.
        """
        if isinstance(self._row_index, pd.core.indexes.range.RangeIndex) or \
           isinstance(self._row_index, pd.core.indexes.base.Index):
            return self._row_index
        return self._row_index.index

    def _set_index(self, new_index):
        """Set the index for this DataFrame.

        Args:
            new_index: The new index to set this
        """
        if isinstance(self._row_index, pd.core.indexes.range.RangeIndex) or \
           isinstance(self._row_index, pd.core.indexes.base.Index):
                self._row_index = new_index
        else:
            self._row_index.index = new_index

    index = property(_get_index, _set_index)

    def _get__row_index(self):
        """Get the _row_index for this DataFrame.

        Returns:
            The default index.
        """
        if self._row_index_cache is None:
            return None

        if isinstance(self._row_index_cache, ray.local_scheduler.ObjectID):
            self._row_index_cache = ray.get(self._row_index_cache)
        return self._row_index_cache

    def _set__row_index(self, new__index):
        """Set the _row_index for this DataFrame.

        Args:
            new__index: The new default index to set.
        """
        self._row_index_cache = new__index

    _row_index = property(_get__row_index, _set__row_index)

    def _get_columns(self):
        """Get the columns for this DataFrame.

        Returns:
            The union of all indexes across the partitions.
        """
        if isinstance(self._col_index, pd.core.indexes.range.RangeIndex) or \
           isinstance(self._col_index, pd.core.indexes.base.Index):
            return self._col_index
        return self._col_index.index

    def _set_columns(self, new_index):
        """Set the columns for this DataFrame.

        Args:
            new_index: The new index to set this
        """
        if isinstance(self._col_index, pd.core.indexes.range.RangeIndex) or \
           isinstance(self._col_index, pd.core.indexes.base.Index):
            self._col_index = new_index
            return
        self._col_index.index = new_index

    columns = property(_get_columns, _set_columns)

    def _get__col_index(self):
        """Get the _col_index for this DataFrame.

        Returns:
            The default index.
        """
        if self._col_index_cache is None:
            return None

        if isinstance(self._col_index_cache, ray.local_scheduler.ObjectID):
            self._col_index_cache = ray.get(self._col_index_cache)
        return self._col_index_cache

    def _set__col_index(self, new__index):
        """Set the _col_index for this DataFrame.

        Args:
            new__index: The new default index to set.
        """
        self._col_index_cache = new__index

    _col_index = property(_get__col_index, _set__col_index)

    def _get_row_lengths(self):
        """Gets the lengths for each partition and caches it if it wasn't.

        Returns:
            A list of integers representing the length of each partition.
        """
        if self._row_length_cache is None:
            return None
        if isinstance(self._row_length_cache, ray.local_scheduler.ObjectID):
            self._row_length_cache = ray.get(self._row_length_cache)
        elif isinstance(self._row_length_cache, list) and \
                isinstance(self._row_length_cache[0],
                           ray.local_scheduler.ObjectID):
            self._row_length_cache = ray.get(self._row_length_cache)
        return self._row_length_cache

    def _set_row_lengths(self, lengths):
        """Sets the lengths of each partition for this DataFrame.

        We use this because we can compute it when creating the DataFrame.

        Args:
            lengths ([ObjectID or Int]): A list of lengths for each
                partition, in order.
        """
        self._row_length_cache = lengths

    _row_lengths = property(_get_row_lengths, _set_row_lengths)

    def _get_col_lengths(self):
        """Gets the lengths for each partition and caches it if it wasn't.

        Returns:
            A list of integers representing the length of each partition.
        """
        if self._col_length_cache is None:
            return None
        if isinstance(self._col_length_cache, ray.local_scheduler.ObjectID):
            self._col_length_cache = ray.get(self._col_length_cache)
        elif isinstance(self._col_length_cache, list) and \
                isinstance(self._col_length_cache[0],
                           ray.local_scheduler.ObjectID):
            self._col_length_cache = ray.get(self._col_length_cache)
        return self._col_length_cache

    def _set_col_lengths(self, lengths):
        """Sets the lengths of each partition for this DataFrame.

        We use this because we can compute it when creating the DataFrame.

        Args:
            lengths ([ObjectID or Int]): A list of lengths for each
                partition, in order.
        """
        self._col_length_cache = lengths

    _col_lengths = property(_get_col_lengths, _set_col_lengths)

    def _arithmetic_helper(self, remote_func, axis, level=None):
        # TODO: We don't support `level` right now
        if level is not None:
            raise NotImplementedError("Level not yet supported.")

        axis = self._row_index._get_axis_number(axis) if axis is not None \
            else 0

        oid_series = ray.get(_map_partitions(remote_func,
                             self._col_partitions if axis == 0
                             else self._row_partitions))

        if axis == 0:
            # We use the index to get the internal index.
            oid_series = [(oid_series[i], i) for i in range(len(oid_series))]

            for df, index in oid_series:
                this_partition = \
                    self._col_index[self._col_index['partition'] == index]
                df.index = this_partition[
                    this_partition['index_within_partition'].isin(df.index)
                ].index

            result_series = pd.concat([obj[0] for obj in oid_series],
                                      axis=0, copy=False)
        else:
            result_series = pd.concat(oid_series, axis=0, copy=False)
            result_series.index = self.index
        return result_series

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
        return ray.get(_deploy_func.remote(lambda df: df.ndim,
                                           self._row_partitions[0]))

    @property
    def ftypes(self):
        """Get the ftypes for this DataFrame.

        Returns:
            The ftypes for this DataFrame.
        """
        # The ftypes are common across all partitions.
        # The first partition will be enough.
        result = ray.get(_deploy_func.remote(lambda df: df.ftypes,
                                             self._row_partitions[0]))
        result.index = self.columns
        return result

    @property
    def dtypes(self):
        """Get the dtypes for this DataFrame.

        Returns:
            The dtypes for this DataFrame.
        """
        # The dtypes are common across all partitions.
        # The first partition will be enough.
        result = ray.get(_deploy_func.remote(lambda df: df.dtypes,
                                             self._row_partitions[0]))
        result.index = self.columns
        return result

    @property
    def empty(self):
        """Determines if the DataFrame is empty.

        Returns:
            True if the DataFrame is empty.
            False otherwise.
        """
        all_empty = ray.get(_map_partitions(
            lambda df: df.empty, self._row_partitions))
        return False not in all_empty

    @property
    def values(self):
        """Create a numpy array with the values from this DataFrame.

        Returns:
            The numpy representation of this DataFrame.
        """
        return np.concatenate(ray.get(_map_partitions(
            lambda df: df.values, self._row_partitions)))

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
        return len(self.index), len(self.columns)

    def _update_inplace(self, row_partitions=None, col_partitions=None,
                        columns=None, index=None):
        """Updates the current DataFrame inplace.

        Behavior should be similar to the constructor, given the corresponding
        arguments. Note that len(columns) and len(index) should match the
        corresponding dimensions in the partition(s) passed in, otherwise this
        function will complain.

        Args:
            row_partitions ([ObjectID]):
                The new partitions to replace self._row_partitions directly
            col_partitions ([ObjectID]):
                The new partitions to replace self._col_partitions directly
            columns (pd.Index):
                Index of the column dimension to replace existing columns
            index (pd.Index):
                Index of the row dimension to replace existing index

        Note:
            If `columns` or `index` are not supplied, they will revert to
                default columns or index respectively, as this function does
                not have enough contextual info to rebuild the indexes
                correctly based on the addition/subtraction of rows/columns.
        """
        assert row_partitions is not None or col_partitions is not None, \
            "To update inplace, new column or row partitions must be set."

        if row_partitions is not None:
            self._row_partitions = row_partitions

        elif col_partitions is not None:
            self._col_partitions = col_partitions

        if row_partitions is not None or col_partitions is not None:
            # At least one partition list is being updated, so recompute
            # lengths and indices
            self._row_lengths, self._row_index = \
                _build_index.remote(self._block_partitions[:, 0], index)
            self._col_lengths, self._col_index = \
                _build_columns.remote(self._block_partitions[0, :], columns)

    def add_prefix(self, prefix):
        """Add a prefix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(prefix) + str(x))
        return DataFrame(block_partitions=self._block_partitions,
                         columns=new_cols,
                         index=self.index)

    def add_suffix(self, suffix):
        """Add a suffix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(x) + str(suffix))
        return DataFrame(block_partitions=self._block_partitions,
                         columns=new_cols,
                         index=self.index)

    def applymap(self, func):
        """Apply a function to a DataFrame elementwise.

        Args:
            func (callable): The function to apply.
        """
        if not callable(func):
            raise ValueError(
                "\'{0}\' object is not callable".format(type(func)))

        new_block_partitions = np.array([
            _map_partitions(lambda df: df.applymap(func), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

    def copy(self, deep=True):
        """Creates a shallow copy of the DataFrame.

        Returns:
            A new DataFrame pointing to the same partitions as this one.
        """
        return DataFrame(block_partitions=self._block_partitions,
                         columns=self.columns,
                         index=self.index)

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        """Apply a groupby to this DataFrame. See _groupby() remote task.
        Args:
            by: The value to groupby.
            axis: The axis to groupby.
            level: The level of the groupby.
            as_index: Whether or not to store result as index.
            sort: Whether or not to sort the result by the index.
            group_keys: Whether or not to group the keys.
            squeeze: Whether or not to squeeze.
        Returns:
            A new DataFrame resulting from the groupby.
        """
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None):
        """Perform a sum across the DataFrame.

        Args:
            axis (int): The axis to sum on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The sum of the DataFrame.
        """
        def remote_func(df):
            return df.sum(axis=axis, skipna=skipna, level=level,
                          numeric_only=numeric_only)

        return self._arithmetic_helper(remote_func, axis, level)

    def abs(self):
        """Apply an absolute value function to all numberic columns.

        Returns:
            A new DataFrame with the applied absolute value.
        """
        for t in self.dtypes:
            if np.dtype('O') == t:
                # TODO Give a more accurate error to Pandas
                raise TypeError("bad operand type for abs():", "str")

        new_block_partitions = np.array([_map_partitions(lambda df: df.abs(),
                                                         block)
                                         for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

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
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.isin(values), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

    def isna(self):
        """Fill a DataFrame with booleans for cells containing NA.

        Returns:
            A new DataFrame with booleans representing whether or not a cell
            is NA.
            True: cell contains NA.
            False: otherwise.
        """
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.isna(), block) for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

    def isnull(self):
        """Fill a DataFrame with booleans for cells containing a null value.

        Returns:
            A new DataFrame with booleans representing whether or not a cell
                is null.
            True: cell contains null.
            False: otherwise.
        """
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.isnull(), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

    def keys(self):
        """Get the info axis for the DataFrame.

        Returns:
            A pandas Index for this DataFrame.
        """
        # Each partition should have the same index, so we'll use 0's
        return self.columns

    def transpose(self, *args, **kwargs):
        """Transpose columns and rows for the DataFrame.

        Returns:
            A new DataFrame transposed from this DataFrame.
        """
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.T, block) for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions.T,
                         columns=self.index,
                         index=self.columns)

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
        def remote_func(df):
            return df.all(axis=axis, bool_only=bool_only, skipna=skipna,
                          level=level, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def any(self, axis=None, bool_only=None, skipna=None, level=None,
            **kwargs):
        """Return whether any elements are True over requested axis

        Note:
            If axis=None or axis=0, this call applies on the column partitions,
                otherwise operates on row partitions
        """
        def remote_func(df):
            return df.any(axis=axis, bool_only=bool_only, skipna=skipna,
                          level=level, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

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
        """Synonym for DataFrame.fillna(method='bfill')
        """
        new_df = self.fillna(method='bfill',
                             axis=axis,
                             limit=limit,
                             downcast=downcast,
                             inplace=inplace)
        if not inplace:
            return new_df

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
        """Get the count of non-null objects in the DataFrame.

        Arguments:
            axis: 0 or 'index' for row-wise, 1 or 'columns' for column-wise.
            level: If the axis is a MultiIndex (hierarchical), count along a
                particular level, collapsing into a DataFrame.
            numeric_only: Include only float, int, boolean data

        Returns:
            The count, in a Series (or DataFrame if level is specified).
        """
        def remote_func(df):
            return df.count(axis=axis, level=level, numeric_only=numeric_only)

        return self._arithmetic_helper(remote_func, axis, level)

    def cov(self, min_periods=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def _cumulative_helper(self, func, axis):
        axis = self._row_index._get_axis_number(axis) if axis is not None \
            else 0

        if axis == 0:
            new_cols = _map_partitions(func, self._col_partitions)
            return DataFrame(col_partitions=new_cols,
                             columns=self.columns,
                             index=self.index)
        else:
            new_rows = _map_partitions(func, self._row_partitions)
            return DataFrame(row_partitions=new_rows,
                             columns=self.columns,
                             index=self.index)

    def cummax(self, axis=None, skipna=True, *args, **kwargs):
        """Perform a cumulative maximum across the DataFrame.

        Args:
            axis (int): The axis to take maximum on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The cumulative maximum of the DataFrame.
        """
        def remote_func(df):
            return df.cummax(axis=axis, skipna=skipna, *args, **kwargs)

        return self._cumulative_helper(remote_func, axis)

    def cummin(self, axis=None, skipna=True, *args, **kwargs):
        """Perform a cumulative minimum across the DataFrame.

        Args:
            axis (int): The axis to cummin on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The cumulative minimum of the DataFrame.
        """
        def remote_func(df):
            return df.cummin(axis=axis, skipna=skipna, *args, **kwargs)

        return self._cumulative_helper(remote_func, axis)

    def cumprod(self, axis=None, skipna=True, *args, **kwargs):
        """Perform a cumulative product across the DataFrame.

        Args:
            axis (int): The axis to take product on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The cumulative product of the DataFrame.
        """
        def remote_func(df):
            return df.cumprod(axis=axis, skipna=skipna, *args, **kwargs)

        return self._cumulative_helper(remote_func, axis)

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        """Perform a cumulative sum across the DataFrame.

        Args:
            axis (int): The axis to take sum on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The cumulative sum of the DataFrame.
        """
        def remote_func(df):
            return df.cumsum(axis=axis, skipna=skipna, *args, **kwargs)

        return self._cumulative_helper(remote_func, axis)

    def describe(self, percentiles=None, include=None, exclude=None):
        """
        Generates descriptive statistics that summarize the central tendency,
        dispersion and shape of a dataset's distribution, excluding NaN values.

        Args:
            percentiles (list-like of numbers, optional):
                The percentiles to include in the output.
            include: White-list of data types to include in results
            exclude: Black-list of data types to exclude in results

        Returns: Series/DataFrame of summary statistics
        """
        def describe_helper(df):
            """This to ensure nothing goes on with non-numeric columns"""
            try:
                return df.select_dtypes(exclude='object').describe(
                    percentiles=percentiles,
                    include=include,
                    exclude=exclude)
            # This exception is thrown when there are only non-numeric columns
            # in this partition
            except ValueError:
                return pd.DataFrame()

        # Begin fixing index based on the columns inside.
        parts = ray.get(_map_partitions(describe_helper, self._col_partitions))
        # We use the index to get the internal index.
        parts = [(parts[i], i) for i in range(len(parts))]

        for df, index in parts:
            this_partition = \
                self._col_index[self._col_index['partition'] == index]
            df.columns = this_partition[
                this_partition['index_within_partition'].isin(df.columns)
            ].index

        # Remove index from tuple
        result = pd.concat([obj[0] for obj in parts], axis=1, copy=False)
        return result

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
        """Return new object with labels in requested axis removed.
        Args:
            labels: Index or column labels to drop.

            axis: Whether to drop labels from the index (0 / 'index') or
                columns (1 / 'columns').

            index, columns: Alternative to specifying axis (labels, axis=1 is
                equivalent to columns=labels).

            level: For MultiIndex

            inplace: If True, do operation inplace and return None.

            errors: If 'ignore', suppress error and existing labels are
                dropped.
        Returns:
            dropped : type of caller
        """
        # TODO implement level
        if level is not None:
            raise NotImplementedError("Level not yet supported for drop")

        inplace = validate_bool_kwarg(inplace, "inplace")
        if labels is not None:
            if index is not None or columns is not None:
                raise ValueError("Cannot specify both 'labels' and "
                                 "'index'/'columns'")
            axis = self._row_index._get_axis_name(axis)
            axes = {axis: labels}
        elif index is not None or columns is not None:
            axes, _ = self._row_index._construct_axes_from_arguments((index,
                                                                      columns),
                                                                     {})
        else:
            raise ValueError("Need to specify at least one of 'labels', "
                             "'index' or 'columns'")
        obj = self.copy()

        def drop_helper(obj, axis, label):
            if axis == 'index':
                try:
                    coords = obj._row_index.loc[label]
                    if isinstance(coords, pd.DataFrame):
                        partitions = list(coords['partition'])
                        indexes = list(coords['index_within_partition'])
                    else:
                        partitions, indexes = coords
                        partitions = [partitions]
                        indexes = [indexes]

                    for part, index in zip(partitions, indexes):
                        x = _deploy_func.remote(
                            lambda df: df.drop(labels=index, axis=axis,
                                               errors='ignore'),
                            obj._row_partitions[part])
                        obj._row_partitions = \
                            [obj._row_partitions[i] if i != part
                             else x
                             for i in range(len(obj._row_partitions))]

                        # The decrement here is because we're dropping one at a
                        # time and the index is automatically updated when we
                        # convert back to blocks.
                        obj._row_index = obj._row_index.copy()
                        obj._row_index.loc[
                            (obj._row_index.partition == part) &
                            (obj._row_index.index_within_partition > index),
                            'index_within_partition'] -= 1

                    obj._row_index.drop(labels=label, axis=0, inplace=True)
                except KeyError:
                    return obj
            else:
                try:
                    coords = obj._col_index.loc[label]
                    if isinstance(coords, pd.DataFrame):
                        partitions = list(coords['partition'])
                        indexes = list(coords['index_within_partition'])
                    else:
                        partitions, indexes = coords
                        partitions = [partitions]
                        indexes = [indexes]

                    for part, index in zip(partitions, indexes):
                        x = _deploy_func.remote(
                            lambda df: df.drop(labels=index, axis=axis,
                                               errors='ignore'),
                            obj._col_partitions[part])
                        obj._col_partitions = \
                            [obj._col_partitions[i] if i != part
                             else x
                             for i in range(len(obj._col_partitions))]

                        # The decrement here is because we're dropping one at a
                        # time and the index is automatically updated when we
                        # convert back to blocks.
                        obj._col_index = obj._col_index.copy()
                        obj._col_index.loc[
                            (obj._col_index.partition == part) &
                            (obj._col_index.index_within_partition > index),
                            'index_within_partition'] -= 1

                    obj._col_index.drop(labels=label, axis=0, inplace=True)
                except KeyError:
                    return obj

            return obj

        for axis, labels in axes.items():
            if labels is None:
                continue

            if is_list_like(labels):
                for label in labels:
                    if errors != 'ignore' and label and \
                            label not in getattr(self, axis):
                        raise ValueError("The label [{}] is not in the [{}]",
                                         label, axis)
                    else:
                        obj = drop_helper(obj, axis, label)
            else:
                if errors != 'ignore' and labels and \
                        labels not in getattr(self, axis):
                    raise ValueError("The label [{}] is not in the [{}]",
                                     labels, axis)
                else:
                    obj = drop_helper(obj, axis, labels)

        if not inplace:
            return obj
        else:
            self._row_index = obj._row_index
            self._col_index = obj._col_index
            self._block_partitions = obj._block_partitions

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
        # TODO(kunalgosar): Implement Copartition and use to implement equals
        def helper(df, index, other_series):
            return df.iloc[index['index_within_partition']] \
                        .equals(other_series)

        results = []
        other_partition = None
        other_df = None
        for i, idx in other._row_index.iterrows():
            if idx['partition'] != other_partition:
                other_df = ray.get(other._row_partitions[idx['partition']])
                other_partition = idx['partition']
            # TODO: group series here into full df partitions to reduce
            # the number of remote calls to helper
            other_series = other_df.iloc[idx['index_within_partition']]
            curr_index = self._row_index.iloc[i]
            curr_df = self._row_partitions[int(curr_index['partition'])]
            results.append(_deploy_func.remote(helper,
                                               curr_df,
                                               curr_index,
                                               other_series))

        for r in results:
            if not ray.get(r):
                return False
        return True

    def eval(self, expr, inplace=False, **kwargs):
        """Evaluate a Python expression as a string using various backends.
        Args:
            expr: The expression to evaluate. This string cannot contain any
                Python statements, only Python expressions.

            parser: The parser to use to construct the syntax tree from the
                expression. The default of 'pandas' parses code slightly
                different than standard Python. Alternatively, you can parse
                an expression using the 'python' parser to retain strict
                Python semantics. See the enhancing performance documentation
                for more details.

            engine: The engine used to evaluate the expression.

            truediv: Whether to use true division, like in Python >= 3

            local_dict: A dictionary of local variables, taken from locals()
                by default.

            global_dict: A dictionary of global variables, taken from
                globals() by default.

            resolvers: A list of objects implementing the __getitem__ special
                method that you can use to inject an additional collection
                of namespaces to use for variable lookup. For example, this is
                used in the query() method to inject the index and columns
                variables that refer to their respective DataFrame instance
                attributes.

            level: The number of prior stack frames to traverse and add to
                the current scope. Most users will not need to change this
                parameter.

            target: This is the target object for assignment. It is used when
                there is variable assignment in the expression. If so, then
                target must support item assignment with string keys, and if a
                copy is being returned, it must also support .copy().

            inplace: If target is provided, and the expression mutates target,
                whether to modify target inplace. Otherwise, return a copy of
                target with the mutation.
        Returns:
            ndarray, numeric scalar, DataFrame, Series
        """
        columns = self.columns

        def eval_helper(df):
            df = df.copy()
            df.columns = columns
            df.eval(expr, inplace=True, **kwargs)
            df.columns = pd.RangeIndex(0, len(df.columns))
            return df

        inplace = validate_bool_kwarg(inplace, "inplace")
        new_rows = _map_partitions(eval_helper, self._row_partitions)

        columns_copy = self._col_index.T.copy()
        columns_copy.eval(expr, inplace=True, **kwargs)
        columns = columns_copy.columns

        if inplace:
            self._update_inplace(row_partitions=new_rows, columns=columns)
        else:
            return DataFrame(columns=columns, row_partitions=new_rows)

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
        """Synonym for DataFrame.fillna(method='ffill')
        """
        new_df = self.fillna(method='ffill',
                             axis=axis,
                             limit=limit,
                             downcast=downcast,
                             inplace=inplace)
        if not inplace:
            return new_df

    def fillna(self, value=None, method=None, axis=None, inplace=False,
               limit=None, downcast=None, **kwargs):
        """Fill NA/NaN values using the specified method.

        Args:
            value: Value to use to fill holes. This value cannot be a list.

            method: Method to use for filling holes in reindexed Series pad.
                ffill: propagate last valid observation forward to next valid
                backfill.
                bfill: use NEXT valid observation to fill gap.

            axis: 0 or 'index', 1 or 'columns'.

            inplace: If True, fill in place. Note: this will modify any other
                views on this object.

            limit: If method is specified, this is the maximum number of
                consecutive NaN values to forward/backward fill. In other
                words, if there is a gap with more than this number of
                consecutive NaNs, it will only be partially filled. If method
                is not specified, this is the maximum number of entries along
                the entire axis where NaNs will be filled. Must be greater
                than 0 if not None.

            downcast: A dict of item->dtype of what to downcast if possible,
                or the string 'infer' which will try to downcast to an
                appropriate equal type.

        Returns:
            filled: DataFrame
        """
        # TODO implement value passed as DataFrame
        if isinstance(value, pd.DataFrame):
            raise NotImplementedError("Passing a DataFrame as the value for "
                                      "fillna is not yet supported.")

        inplace = validate_bool_kwarg(inplace, 'inplace')

        axis = self._row_index._get_axis_number(axis) \
            if axis is not None \
            else 0

        if isinstance(value, (list, tuple)):
            raise TypeError('"value" parameter must be a scalar or dict, but '
                            'you passed a "{0}"'.format(type(value).__name__))
        if value is None and method is None:
            raise ValueError('must specify a fill method or value')
        if value is not None and method is not None:
            raise ValueError('cannot specify both a fill method and value')
        if method is not None and method not in ['backfill', 'bfill', 'pad',
                                                 'ffill']:
            expecting = 'pad (ffill) or backfill (bfill)'
            msg = 'Invalid fill method. Expecting {expecting}. Got {method}'\
                  .format(expecting=expecting, method=method)
            raise ValueError(msg)

        if inplace:
            new_obj = self
        else:
            new_obj = self.copy()

        if axis == 0:
            parts = new_obj._col_partitions
            idx_obj = new_obj._col_index
        else:
            parts = new_obj._row_partitions
            idx_obj = new_obj._row_index

        if isinstance(value, (pd.Series, dict)):
            new_vals = {}
            value = dict(value)
            for val in value:
                # Get the local index for the partition
                try:
                    part, index = idx_obj.loc[val]
                # Pandas ignores these errors so we will suppress them too.
                except KeyError:
                    continue

                new_vals[val] = _deploy_func.remote(lambda df: df.fillna(
                    value={index: value[val]},
                    method=method,
                    axis=axis,
                    inplace=False,
                    limit=limit,
                    downcast=downcast,
                    **kwargs), parts[part])

            # Not every partition was changed, so we put everything back that
            # was not changed and update those that were.
            new_parts = [parts[i] if idx_obj.index[i] not in new_vals
                         else new_vals[idx_obj.index[i]]
                         for i in range(len(parts))]
        else:
            new_parts = _map_partitions(lambda df: df.fillna(
                value=value,
                method=method,
                axis=axis,
                inplace=False,
                limit=limit,
                downcast=downcast,
                **kwargs), parts)

        if axis == 0:
            new_obj._update_inplace(col_partitions=new_parts,
                                    columns=self.columns,
                                    index=self.index)
        else:
            new_obj._update_inplace(row_partitions=new_parts,
                                    columns=self.columns,
                                    index=self.index)
        if not inplace:
            return new_obj

    def filter(self, items=None, like=None, regex=None, axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def first(self, offset):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def first_valid_index(self):
        """Return index for first non-NA/null value.

        Returns:
            scalar: type of index
        """
        if self._row_index is not None:
            return self._row_index.first_valid_index()
        return None

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
        try:
            return self[key]
        except (KeyError, ValueError, IndexError):
            return default

    def get_dtype_counts(self):
        """Get the counts of dtypes in this object.

        Returns:
            The counts of dtypes in this object.
        """
        return ray.get(_deploy_func.remote(lambda df: df.get_dtype_counts(),
                                           self._row_partitions[0]))

    def get_ftype_counts(self):
        """Get the counts of ftypes in this object.

        Returns:
            The counts of ftypes in this object.
        """
        return ray.get(_deploy_func.remote(lambda df: df.get_ftype_counts(),
                                           self._row_partitions[0]))

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
        sizes = self._row_lengths

        if n >= sum(sizes):
            return self.copy()

        new_dfs = _map_partitions(lambda df: df.head(n),
                                  self._col_partitions)

        index = self._row_index.head(n).index

        return DataFrame(col_partitions=new_dfs,
                         columns=self.columns,
                         index=index)

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
        if not all([d != np.dtype('O') for d in self.dtypes]):
            raise TypeError(
                "reduction operation 'argmax' not allowed for this dtype")

        def remote_func(df):
            return df.idxmax(axis=axis, skipna=skipna)

        internal_indices = self._arithmetic_helper(remote_func, axis)
        # do this to convert internal indices to correct index
        return internal_indices.apply(lambda x: self.index[x])

    def idxmin(self, axis=0, skipna=True):
        """Get the index of the first occurrence of the min value of the axis.

        Args:
            axis (int): Identify the min over the rows (1) or columns (0).
            skipna (bool): Whether or not to skip NA values.

        Returns:
            A Series with the index for each minimum value for the axis
                specified.
        """
        if not all([d != np.dtype('O') for d in self.dtypes]):
            raise TypeError(
                "reduction operation 'argmax' not allowed for this dtype")

        def remote_func(df):
            return df.idxmin(axis=axis, skipna=skipna)

        internal_indices = self._arithmetic_helper(remote_func, axis)
        # do this to convert internal indices to correct index
        return internal_indices.apply(lambda x: self.index[x])

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
        if not is_list_like(value):
            value = np.full(len(self.index), value)

        if len(value) != len(self.index):
            raise ValueError(
                "Length of values does not match length of index")
        if not allow_duplicates and column in self.columns:
            raise ValueError(
                "cannot insert {0}, already exists".format(column))
        if loc > len(self.columns):
            raise IndexError(
                "index {0} is out of bounds for axis 0 with size {1}".format(
                    loc, len(self.columns)))
        if loc < 0:
            raise ValueError("unbounded slice")

        # Perform insert on a specific column partition
        # Determine which column partition to place it in, and where in that
        # partition
        col_cum_lens = np.cumsum(self._col_lengths)
        col_part_idx = np.digitize(loc, col_cum_lens[:-1])
        col_part_loc = loc - np.asscalar(
            np.concatenate(([0], col_cum_lens))[col_part_idx])

        # Deploy insert function to specific column partition, and replace that
        # column
        def insert_col_part(df):
            df.insert(col_part_loc, column, value, allow_duplicates)
            return df

        new_obj = _deploy_func.remote(insert_col_part,
                                      self._col_partitions[col_part_idx])
        new_cols = [self._col_partitions[i]
                    if i != col_part_idx
                    else new_obj
                    for i in range(len(self._col_partitions))]
        new_col_names = self._col_index.index.insert(loc, column)

        self._update_inplace(col_partitions=new_cols, columns=new_col_names)

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
        def update_iterrow(series, i):
            """Helper function to correct the columns + name of the Series."""
            series.index = self.columns
            series.name = list(self.index)[i]
            return series

        iters = ray.get([_deploy_func.remote(
            lambda df: list(df.iterrows()), part)
            for part in self._row_partitions])
        iters = itertools.chain.from_iterable(iters)
        series = map(lambda s: update_iterrow(s[1][1], s[0]), enumerate(iters))

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
            lambda df: list(df.items()), part)
            for part in self._row_partitions])

        def concat_iters(iterables):
            for partitions in enumerate(zip(*iterables)):
                series = pd.concat([_series for _, _series in partitions[1]])
                series.index = self.index
                series.name = list(self.columns)[partitions[0]]
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
                part) for part in self._row_partitions])
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
        """Return index for last non-NA/null value.

        Returns:
            scalar: type of index
        """
        if self._row_index is not None:
            return self._row_index.last_valid_index()
        return None

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
        def remote_func(df):
            return df.max(axis=axis, skipna=skipna, level=level,
                          numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def mean(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        """Computes mean across the DataFrame.

        Args:
            axis (int): The axis to take the mean on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The mean of the DataFrame. (Pandas series)
        """
        def remote_func(df):
            return df.mean(axis=axis, skipna=skipna, level=level,
                           numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def median(self, axis=None, skipna=None, level=None, numeric_only=None,
               **kwargs):
        """Computes median across the DataFrame.

        Args:
            axis (int): The axis to take the median on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The median of the DataFrame. (Pandas series)
        """
        def remote_func(df):
            return df.median(axis=axis, skipna=skipna, level=level,
                             numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

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
        def remote_func(df):
            return df.min(axis=axis, skipna=skipna, level=level,
                          numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

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
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.notna(), block) for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

    def notnull(self):
        """Perform notnull across the DataFrame.

        Args:
            None

        Returns:
            Boolean DataFrame where value is False if corresponding
            value is NaN, True otherwise
        """
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.notnull(), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

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
        result = self[item]
        del self[item]
        return result

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
        """Return values at the given quantile over requested axis,
            a la numpy.percentile.

        Args:
            q (float): 0 <= q <= 1, the quantile(s) to compute
            axis (int): 0 or 'index' for row-wise,
                        1 or 'columns' for column-wise
            interpolation: {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
                Specifies which interpolation method to use

        Returns:
            quantiles : Series or DataFrame
                    If q is an array, a DataFrame will be returned where the
                    index is q, the columns are the columns of self, and the
                    values are the quantiles.

                    If q is a float, a Series will be returned where the
                    index is the columns of self and the values
                    are the quantiles.
        """

        def quantile_helper(df, q, axis, numeric_only, interpolation):
            try:
                return df.quantile(q=q, axis=axis, numeric_only=numeric_only,
                                   interpolation=interpolation)
            except ValueError:
                return pd.Series()

        if isinstance(q, (pd.Series, np.ndarray, pd.Index, list)):
            # In the case of a list, we build it one at a time.
            # TODO Revisit for performance
            quantiles = []
            for q_i in q:
                def remote_func(df):
                    return quantile_helper(df, q=q_i, axis=axis,
                                           numeric_only=numeric_only,
                                           interpolation=interpolation)

                result = self._arithmetic_helper(remote_func, axis)
                result.name = q_i
                quantiles.append(result)

            return pd.concat(quantiles, axis=1).T
        else:
            def remote_func(df):
                return quantile_helper(df, q=q, axis=axis,
                                       numeric_only=numeric_only,
                                       interpolation=interpolation)

            result = self._arithmetic_helper(remote_func, axis)
            result.name = q
            return result

    def query(self, expr, inplace=False, **kwargs):
        """Queries the Dataframe with a boolean expression

        Returns:
            A new DataFrame if inplace=False
        """
        if '@' in expr:
            raise NotImplementedError("Local variables not yet supported in "
                                      "query.")
        columns = self.columns

        def query_helper(df):
            df = df.copy()
            df.columns = columns
            df.query(expr, inplace=True, **kwargs)
            df.columns = pd.RangeIndex(0, len(df.columns))
            return df

        new_rows = _map_partitions(query_helper,
                                   self._row_partitions)

        if inplace:
            self._update_inplace(row_partitions=new_rows)
        else:
            return DataFrame(row_partitions=new_rows, columns=self.columns)

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
        """Alters axes labels.

        Args:
            mapper, index, columns: Transformations to apply to the axis's
                values.
            axis: Axis to target with mapper.
            copy: Also copy underlying data.
            inplace: Whether to return a new DataFrame.
            level: Only rename a specific level of a MultiIndex.

        Returns:
            If inplace is False, a new DataFrame with the updated axes.
        """
        inplace = validate_bool_kwarg(inplace, 'inplace')

        # We have to do this with the args because of how rename handles
        # kwargs. It doesn't ignore None values passed in, so we have to filter
        # them ourselves.
        args = locals()
        kwargs = {k: v for k, v in args.items()
                  if v is not None and k != "self"}
        # inplace should always be true because this is just a copy, and we
        # will use the results after.
        kwargs['inplace'] = True

        df_to_rename = pd.DataFrame(index=self.index, columns=self.columns)
        df_to_rename.rename(**kwargs)

        if inplace:
            obj = self
        else:
            obj = self.copy()

        obj.index = df_to_rename.index
        obj.columns = df_to_rename.columns

        if not inplace:
            return obj

    def rename_axis(self, mapper, axis=0, copy=True, inplace=False):
        axes_is_columns = axis == 1 or axis == "columns"
        renamed = self if inplace else self.copy()
        if axes_is_columns:
            renamed.columns.name = mapper
        else:
            renamed._row_index.rename_axis(mapper, axis=axis, copy=copy,
                                           inplace=True)
        if not inplace:
            return renamed

    def _set_axis_name(self, name, axis=0, inplace=False):
        """Alter the name or names of the axis.

        Args:
            name: Name for the Index, or list of names for the MultiIndex
            axis: 0 or 'index' for the index; 1 or 'columns' for the columns
            inplace: Whether to modify `self` directly or return a copy

        Returns:
            Type of caller or None if inplace=True.
        """
        axes_is_columns = axis == 1 or axis == "columns"
        renamed = self if inplace else self.copy()
        if axes_is_columns:
            renamed.columns.set_names(name)
        else:
            renamed._row_index.set_names(name)

        if not inplace:
            return renamed

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

        # We're building a new default index dataframe for use later.
        _, new_index = \
            _build_index.remote(new_obj._block_partitions[:, 0], None)

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
                default = 'index'
                i = 0
                while default in self:
                    default = 'level_{}'.format(i)
                    i += 1

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
        new_block_partitions = np.array([_map_partitions(
            lambda df: df.round(decimals=decimals, *args, **kwargs), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

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
            setattr(self, self._row_index._get_axis_name(axis), labels)
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
        """Computes standard deviation across the DataFrame.

        Args:
            axis (int): The axis to take the std on.
            skipna (bool): True to skip NA values, false otherwise.
            ddof (int): degrees of freedom

        Returns:
            The std of the DataFrame (Pandas Series)
        """
        def remote_func(df):
            return df.std(axis=axis, skipna=skipna, level=level, ddof=ddof,
                          numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

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
        sizes = self._row_lengths

        if n >= sum(sizes):
            return self

        new_dfs = _map_partitions(lambda df: df.tail(n),
                                  self._col_partitions)

        index = self._row_index.tail(n).index
        return DataFrame(col_partitions=new_dfs,
                         columns=self.columns,
                         index=index)

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
        """Computes variance across the DataFrame.

        Args:
            axis (int): The axis to take the variance on.
            skipna (bool): True to skip NA values, false otherwise.
            ddof (int): degrees of freedom

        Returns:
            The variance of the DataFrame.
        """
        def remote_func(df):
            return df.var(axis=axis, skipna=skipna, level=level, ddof=ddof,
                          numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

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
            A Pandas Series representing the value for the column.
        """
        key = com._apply_if_callable(key, self)

        # shortcut if we are an actual column
        is_mi_columns = isinstance(self.columns, pd.MultiIndex)
        try:
            if key in self.columns and not is_mi_columns:
                return self._getitem_column(key)
        except (KeyError, ValueError, TypeError):
            pass

        # see if we can slice the rows
        indexer = convert_to_index_sliceable(self._row_index, key)
        if indexer is not None:
            raise NotImplementedError("To contribute to Pandas on Ray, please"
                                      "visit github.com/ray-project/ray.")
            # return self._getitem_slice(indexer)

        if isinstance(key, (pd.Series, np.ndarray, pd.Index, list)):
            return self._getitem_array(key)
        elif isinstance(key, DataFrame):
            raise NotImplementedError("To contribute to Pandas on Ray, please"
                                      "visit github.com/ray-project/ray.")
            # return self._getitem_frame(key)
        elif is_mi_columns:
            raise NotImplementedError("To contribute to Pandas on Ray, please"
                                      "visit github.com/ray-project/ray.")
            # return self._getitem_multilevel(key)
        else:
            return self._getitem_column(key)

    def _getitem_column(self, key):
        partition = self._col_index.loc[key].loc['partition']
        result = ray.get(self._getitem_indiv_col(key, partition))
        result.name = key
        result.index = self.index
        return result

    def _getitem_array(self, key):
        if com.is_bool_indexer(key):
            if isinstance(key, pd.Series) and \
                    not key.index.equals(self.index):
                warnings.warn("Boolean Series key will be reindexed to match "
                              "DataFrame index.", UserWarning, stacklevel=3)
            elif len(key) != len(self.index):
                raise ValueError('Item wrong length {} instead of {}.'.format(
                                 len(key), len(self.index)))
            key = check_bool_indexer(self.index, key)

            new_parts = _map_partitions(lambda df: df[key],
                                        self._col_partitions)
            columns = self.columns
            index = self.index[key]

            return DataFrame(col_partitions=new_parts,
                             columns=columns,
                             index=index)
        else:
            columns = self.columns[key]

            indices_for_rows = [self.columns.index(new_col)
                                for new_col in columns]

            new_parts = [_deploy_func.remote(
                lambda df: df.__getitem__(indices_for_rows),
                part) for part in self._row_partitions]

            index = self.index

            return DataFrame(row_partitions=new_parts,
                             columns=columns,
                             index=index)

    def _getitem_indiv_col(self, key, part):
        loc = self._col_index.loc[key]
        if isinstance(loc, pd.Series):
            index = loc[loc['partition'] == part]
        else:
            index = loc[loc['partition'] == part]['index_within_partition']
        return _deploy_func.remote(
            lambda df: df.__getitem__(index),
            self._col_partitions[part])

    def __setitem__(self, key, value):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __len__(self):
        """Gets the length of the dataframe.

        Returns:
            Returns an integer length of the dataframe object.
        """
        return sum(self._row_lengths)

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
        """Searches columns for specific key

        Args:
            key : The column name

        Returns:
            Returns a boolean if the specified key exists as a column name
        """
        return self.columns.__contains__(key)

    def __nonzero__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __bool__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __abs__(self):
        """Creates a modified DataFrame by taking the absolute value.

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
        """Delete a column by key. `del a[key]` for example.
           Operation happens in place.

           Notes: This operation happen on row and column partition
                  simultaneously. No rebuild.
        Args:
            key: key to delete
        """
        # Create helper method for deleting column(s) in row partition.
        def del_helper(df, to_delete):
            cols = df.columns[to_delete]  # either int or an array of ints

            if not is_list_like(cols):
                cols = [cols]

            for col in cols:
                df.__delitem__(col)

            # Reset the column index to conserve space
            df.columns = pd.RangeIndex(0, len(df.columns))
            return df

        to_delete = self.columns.get_loc(key)
        self._row_partitions = _map_partitions(
            del_helper, self._row_partitions, to_delete)

        # This structure is used to get the correct index inside the partition.
        del_df = self._col_index.loc[key]

        # We need to standardize between multiple and single occurrences in the
        # columns. Putting single occurrences in a pd.DataFrame and transposing
        # results in the same structure as multiple with 'loc'.
        if isinstance(del_df, pd.Series):
            del_df = pd.DataFrame(del_df).T

        # Cast cols as pd.Series as duplicate columns mean result may be
        # np.int64 or pd.Series
        col_parts_to_del = pd.Series(
            self._col_index.loc[key, 'partition']).unique()
        self._col_index.drop(key, inplace=True)
        for i in col_parts_to_del:
            # Compute the correct index inside the partition to delete.
            to_delete_in_partition = \
                del_df[del_df['partition'] == i]['index_within_partition']

            self._col_partitions[i] = _deploy_func.remote(
                del_helper, self._col_partitions[i], to_delete_in_partition)

            partition_mask = (self._col_index['partition'] == i)

            # Since we are replacing columns with RangeIndex inside the
            # partition, we have to make sure that our reference to it is
            # updated as well.
            try:
                self._col_index.loc[partition_mask,
                                    'index_within_partition'] = [
                    p for p in range(sum(partition_mask))]
            except ValueError:
                # Copy the arrow sealed dataframe so we can mutate it.
                # We only do this the first time we try to mutate the sealed.
                self._col_index = self._col_index.copy()
                self._col_index.loc[partition_mask,
                                    'index_within_partition'] = [
                    p for p in range(sum(partition_mask))]

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

        new_block_partitions = np.array([_map_partitions(
            lambda df: df.__neg__(), block)
            for block in self._block_partitions])

        return DataFrame(block_partitions=new_block_partitions,
                         columns=self.columns,
                         index=self.index)

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

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
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")
