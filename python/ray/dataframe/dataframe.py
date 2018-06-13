from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import functools
from pandas.api.types import is_scalar
from pandas.util._validators import validate_bool_kwarg
from pandas.core.index import _ensure_index_from_sequences
from pandas._libs import lib
from pandas.core.dtypes.cast import maybe_upcast_putmask
from pandas import compat
from pandas.compat import lzip, to_str, string_types, cPickle as pkl
import pandas.core.common as com
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_list_like,
    is_numeric_dtype,
    is_timedelta64_dtype,
    _get_dtype_from_object)
from pandas.core.indexing import check_bool_indexer
from pandas.errors import MergeError

import warnings
import numpy as np
from numpy.testing import assert_equal
import ray
import itertools
import io
import sys
import re

from .utils import (
    _deploy_func,
    _map_partitions,
    _partition_pandas_dataframe,
    to_pandas,
    create_blocks_helper,
    _blocks_to_col,
    _blocks_to_row,
    _create_block_partitions,
    _inherit_docstrings,
    _reindex_helper,
    _co_op_helper,
    _match_partitioning,
    _concat_index,
    fix_blocks_dimensions,
    _compile_remote_dtypes)
from . import get_npartitions
from .index_metadata import _IndexMetadata
from .iterator import PartitionIterator


@_inherit_docstrings(pd.DataFrame,
                     excluded=[pd.DataFrame, pd.DataFrame.__init__])
class DataFrame(object):

    def __init__(self, data=None, index=None, columns=None, dtype=None,
                 copy=False, col_partitions=None, row_partitions=None,
                 block_partitions=None, row_metadata=None, col_metadata=None,
                 dtypes_cache=None):
        """Distributed DataFrame object backed by Pandas dataframes.

        Args:
            data (numpy ndarray (structured or homogeneous) or dict):
                Dict can contain Series, arrays, constants, or list-like
                objects.
            index (pandas.Index, list, ObjectID): The row index for this
                dataframe.
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
            row_metadata (_IndexMetadata):
                Metadata for the new dataframe's rows
            col_metadata (_IndexMetadata):
                Metadata for the new dataframe's columns
        """
        if isinstance(data, DataFrame):
            self._frame_data = data._frame_data
            return

        self._dtypes_cache = dtypes_cache

        # Check type of data and use appropriate constructor
        if data is not None or (col_partitions is None and
                                row_partitions is None and
                                block_partitions is None):

            pd_df = pd.DataFrame(data=data, index=index, columns=columns,
                                 dtype=dtype, copy=copy)

            # Cache dtypes
            self._dtypes_cache = pd_df.dtypes

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
            assert columns is not None or col_metadata is not None, \
                "Columns not defined, must define columns or col_metadata " \
                "for internal DataFrame creations"

            if block_partitions is not None:
                axis = 0
                # put in numpy array here to make accesses easier since it's 2D
                self._block_partitions = np.array(block_partitions)
                self._block_partitions = \
                    fix_blocks_dimensions(self._block_partitions, axis)

            else:
                if row_partitions is not None:
                    axis = 0
                    partitions = row_partitions
                    axis_length = len(columns) if columns is not None else \
                        len(col_metadata)
                elif col_partitions is not None:
                    axis = 1
                    partitions = col_partitions
                    axis_length = None
                    # All partitions will already have correct dtypes
                    self._dtypes_cache = [
                            _deploy_func.remote(lambda df: df.dtypes, pd_df)
                            for pd_df in col_partitions
                    ]

                # TODO: write explicit tests for "short and wide"
                # column partitions
                self._block_partitions = \
                    _create_block_partitions(partitions, axis=axis,
                                             length=axis_length)

        assert self._block_partitions.ndim == 2, "Block Partitions must be 2D."

        # Create the row and column index objects for using our partitioning.
        # If the objects haven't been inherited, then generate them
        if row_metadata is not None:
            self._row_metadata = row_metadata.copy()
            if index is not None:
                self.index = index
        else:
            self._row_metadata = _IndexMetadata(self._block_partitions[:, 0],
                                                index=index, axis=0)

        if col_metadata is not None:
            self._col_metadata = col_metadata.copy()
            if columns is not None:
                self.columns = columns
        else:
            self._col_metadata = _IndexMetadata(self._block_partitions[0, :],
                                                index=columns, axis=1)

        if self._dtypes_cache is None:
            self._get_remote_dtypes()

    def _get_frame_data(self):
        data = {}
        data['blocks'] = self._block_partitions
        data['col_metadata'] = self._col_metadata
        data['row_metadata'] = self._row_metadata
        data['columns'] = self.columns
        data['index'] = self.index
        data['dtypes'] = self._dtypes_cache

        return data

    def _set_frame_data(self, data):
        self._block_partitions = data['blocks']
        self._col_metadata = data['col_metadata']
        self._row_metadata = data['row_metadata']
        self.columns = data['columns']
        self.index = data['index']
        self._dtypes_cache = data['dtypes']

    _frame_data = property(_get_frame_data, _set_frame_data)

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

    def _repr_helper_(self):
        if len(self._row_metadata) <= 60 and \
           len(self._col_metadata) <= 20:
            return to_pandas(self)

        def head(df, n, get_local_head=False):
            """Compute the head for this without creating a new DataFrame"""
            if get_local_head:
                return df.head(n)

            new_dfs = _map_partitions(lambda df: df.head(n),
                                      df)

            index = self.index[:n]
            pd_head = pd.concat(ray.get(new_dfs), axis=1, copy=False)
            pd_head.index = index
            pd_head.columns = self.columns
            return pd_head

        def tail(df, n, get_local_tail=False):
            """Compute the tail for this without creating a new DataFrame"""
            if get_local_tail:
                return df.tail(n)

            new_dfs = _map_partitions(lambda df: df.tail(n),
                                      df)

            index = self.index[-n:]
            pd_tail = pd.concat(ray.get(new_dfs), axis=1, copy=False)
            pd_tail.index = index
            pd_tail.columns = self.columns
            return pd_tail

        def front(df, n):
            """Get first n columns without creating a new Dataframe"""

            cum_col_lengths = self._col_metadata._lengths.cumsum()
            index = np.argmax(cum_col_lengths >= 10)
            pd_front = pd.concat(ray.get(x[:index+1]), axis=1, copy=False)
            pd_front = pd_front.iloc[:, :n]
            pd_front.index = self.index
            pd_front.columns = self.columns[:n]
            return pd_front

        def back(df, n):
            """Get last n columns without creating a new Dataframe"""

            cum_col_lengths = np.flip(self._col_metadata._lengths,
                                      axis=0).cumsum()
            index = np.argmax(cum_col_lengths >= 10)
            pd_back = pd.concat(ray.get(x[-(index+1):]), axis=1, copy=False)
            pd_back = pd_back.iloc[:, -n:]
            pd_back.index = self.index
            pd_back.columns = self.columns[-n:]
            return pd_back

        x = self._col_partitions
        get_local_head = False

        # Get first and last 10 columns if there are more than 20 columns
        if len(self._col_metadata) >= 20:
            get_local_head = True
            front = front(x, 10)
            back = back(x, 10)

            col_dots = pd.Series(["..."
                                  for _ in range(len(self.index))])
            col_dots.index = self.index
            col_dots.name = "..."
            x = pd.concat([front, col_dots, back], axis=1, copy=False)

            # If less than 60 rows, x is already in the correct format.
            if len(self._row_metadata) < 60:
                return x

        head = head(x, 30, get_local_head)
        tail = tail(x, 30, get_local_head)

        # Make the dots in between the head and tail
        row_dots = pd.Series(["..."
                              for _ in range(len(head.columns))])
        row_dots.index = head.columns
        row_dots.name = "..."

        # We have to do it this way or convert dots to a dataframe and
        # transpose. This seems better.
        result = head.append(row_dots).append(tail)
        return result

    def __repr__(self):
        # We use pandas repr so that we match them.
        if len(self._row_metadata) <= 60 and \
           len(self._col_metadata) <= 20:
            return repr(self._repr_helper_())
        # The split here is so that we don't repr pandas row lengths.
        result = self._repr_helper_()
        final_result = repr(result).rsplit("\n\n", maxsplit=1)[0] + \
            "\n\n[{0} rows x {1} columns]".format(len(self.index),
                                                  len(self.columns))
        return final_result

    def _repr_html_(self):
        """repr function for rendering in Jupyter Notebooks like Pandas
        Dataframes.

        Returns:
            The HTML representation of a Dataframe.
        """
        # We use pandas _repr_html_ to get a string of the HTML representation
        # of the dataframe.
        if len(self._row_metadata) <= 60 and \
           len(self._col_metadata) <= 20:
            return self._repr_helper_()._repr_html_()
        # We split so that we insert our correct dataframe dimensions.
        result = self._repr_helper_()._repr_html_()
        return result.split("<p>")[0] + \
            "<p>{0} rows x {1} columns</p>\n</div>".format(len(self.index),
                                                           len(self.columns))

    def _get_index(self):
        """Get the index for this DataFrame.

        Returns:
            The union of all indexes across the partitions.
        """
        return self._row_metadata.index

    def _set_index(self, new_index):
        """Set the index for this DataFrame.

        Args:
            new_index: The new index to set this
        """
        self._row_metadata.index = new_index

    index = property(_get_index, _set_index)

    def _get_columns(self):
        """Get the columns for this DataFrame.

        Returns:
            The union of all indexes across the partitions.
        """
        return self._col_metadata.index

    def _set_columns(self, new_index):
        """Set the columns for this DataFrame.

        Args:
            new_index: The new index to set this
        """
        self._col_metadata.index = new_index

    columns = property(_get_columns, _set_columns)

    def _arithmetic_helper(self, remote_func, axis, level=None):
        # TODO: We don't support `level` right now
        if level is not None:
            raise NotImplementedError("Level not yet supported.")

        axis = pd.DataFrame()._get_axis_number(axis) if axis is not None \
            else 0

        oid_series = ray.get(_map_partitions(remote_func,
                             self._col_partitions if axis == 0
                             else self._row_partitions))

        if axis == 0:
            # We use the index to get the internal index.
            oid_series = [(oid_series[i], i) for i in range(len(oid_series))]

            if len(oid_series) > 0:
                for df, partition in oid_series:
                    this_partition = \
                        self._col_metadata.partition_series(partition)
                    df.index = \
                        this_partition[this_partition.isin(df.index)].index

            result_series = pd.concat([obj[0] for obj in oid_series],
                                      axis=0, copy=False)
        else:
            result_series = pd.concat(oid_series, axis=0, copy=False)
            result_series.index = self.index
        return result_series

    def _validate_eval_query(self, expr, **kwargs):
        """Helper function to check the arguments to eval() and query()

        Args:
            expr: The expression to evaluate. This string cannot contain any
                Python statements, only Python expressions.
        """
        if isinstance(expr, str) and expr is '':
            raise ValueError("expr cannot be an empty string")

        if isinstance(expr, str) and '@' in expr:
            raise NotImplementedError("Local variables not yet supported in "
                                      "eval.")

        if isinstance(expr, str) and 'not' in expr:
            if 'parser' in kwargs and kwargs['parser'] == 'python':
                raise NotImplementedError("'Not' nodes are not implemented.")

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

    def _get_remote_dtypes(self):
        """Finds and caches ObjectIDs for the dtypes of each column partition.
        """
        self._dtypes_cache = [_compile_remote_dtypes.remote(*column)
                              for column in self._block_partitions.T]

    @property
    def dtypes(self):
        """Get the dtypes for this DataFrame.

        Returns:
            The dtypes for this DataFrame.
        """
        assert self._dtypes_cache is not None

        if isinstance(self._dtypes_cache, list) and \
                isinstance(self._dtypes_cache[0],
                           ray.ObjectID):
            self._dtypes_cache = pd.concat(ray.get(self._dtypes_cache),
                                           copy=False)
            self._dtypes_cache.index = self.columns

        return self._dtypes_cache

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
                        block_partitions=None, columns=None, index=None,
                        col_metadata=None, row_metadata=None):
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
        assert row_partitions is not None or col_partitions is not None\
            or block_partitions is not None, \
            "To update inplace, new column or row partitions must be set."

        if block_partitions is not None:
            self._block_partitions = block_partitions

        elif row_partitions is not None:
            self._row_partitions = row_partitions

        elif col_partitions is not None:
            self._col_partitions = col_partitions

        if col_metadata is not None:
            self._col_metadata = col_metadata
        else:
            assert columns is not None, \
                "If col_metadata is None, columns must be passed in"
            self._col_metadata = _IndexMetadata(
                self._block_partitions[0, :], index=columns, axis=1)
        if row_metadata is not None:
            self._row_metadata = row_metadata
        else:
            # Index can be None for default index, so we don't check
            self._row_metadata = _IndexMetadata(
                self._block_partitions[:, 0], index=index, axis=0)

        # Update dtypes
        self._get_remote_dtypes()

    def add_prefix(self, prefix):
        """Add a prefix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(prefix) + str(x))
        return DataFrame(block_partitions=self._block_partitions,
                         columns=new_cols,
                         col_metadata=self._col_metadata,
                         row_metadata=self._row_metadata,
                         dtypes_cache=self._dtypes_cache)

    def add_suffix(self, suffix):
        """Add a suffix to each of the column names.

        Returns:
            A new DataFrame containing the new column names.
        """
        new_cols = self.columns.map(lambda x: str(x) + str(suffix))
        return DataFrame(block_partitions=self._block_partitions,
                         columns=new_cols,
                         col_metadata=self._col_metadata,
                         row_metadata=self._row_metadata,
                         dtypes_cache=self._dtypes_cache)

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
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata)

    def copy(self, deep=True):
        """Creates a shallow copy of the DataFrame.

        Returns:
            A new DataFrame pointing to the same partitions as this one.
        """
        return DataFrame(block_partitions=self._block_partitions,
                         columns=self.columns,
                         index=self.index,
                         dtypes_cache=self._dtypes_cache)

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
        axis = pd.DataFrame()._get_axis_number(axis)
        if callable(by):
            by = by(self.index)
        elif isinstance(by, compat.string_types):
            by = self.__getitem__(by).values.tolist()
        elif is_list_like(by):
            if isinstance(by, pd.Series):
                by = by.values.tolist()

            mismatch = len(by) != len(self) if axis == 0 \
                else len(by) != len(self.columns)

            if all(obj in self for obj in by) and mismatch:
                raise NotImplementedError(
                    "Groupby with lists of columns not yet supported.")
            elif mismatch:
                raise KeyError(next(x for x in by if x not in self))

        from .groupby import DataFrameGroupBy
        return DataFrameGroupBy(self, by, axis, level, as_index, sort,
                                group_keys, squeeze, **kwargs)

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None,
            min_count=1, **kwargs):
        """Perform a sum across the DataFrame.

        Args:
            axis (int): The axis to sum on.
            skipna (bool): True to skip NA values, false otherwise.

        Returns:
            The sum of the DataFrame.
        """
        def remote_func(df):
            return df.sum(axis=axis, skipna=skipna, level=level,
                          numeric_only=numeric_only, min_count=min_count,
                          **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def abs(self):
        """Apply an absolute value function to all numeric columns.

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
                         index=self.index,
                         dtypes_cache=self._dtypes_cache)

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

        new_dtypes = pd.Series([np.dtype("bool")] * len(self.columns),
                               index=self.columns)

        return DataFrame(block_partitions=new_block_partitions,
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata,
                         dtypes_cache=new_dtypes)

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

        new_dtypes = pd.Series([np.dtype("bool")] * len(self.columns),
                               index=self.columns)

        return DataFrame(block_partitions=new_block_partitions,
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata,
                         dtypes_cache=new_dtypes)

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

    def dropna(self, axis=0, how='any', thresh=None, subset=None,
               inplace=False):
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
        inplace = validate_bool_kwarg(inplace, "inplace")

        if is_list_like(axis):
            axis = [pd.DataFrame()._get_axis_number(ax) for ax in axis]

            result = self
            # TODO(kunalgosar): this builds an intermediate dataframe,
            # which does unnecessary computation
            for ax in axis:
                result = result.dropna(
                    axis=ax, how=how, thresh=thresh, subset=subset)
            if not inplace:
                return result

            self._update_inplace(block_partitions=result._block_partitions,
                                 columns=result.columns,
                                 index=result.index)

            return None

        axis = pd.DataFrame()._get_axis_number(axis)

        if how is not None and how not in ['any', 'all']:
            raise ValueError('invalid how option: %s' % how)
        if how is None and thresh is None:
            raise TypeError('must specify how or thresh')

        indices = None
        if subset is not None:
            if axis == 1:
                indices = self.index.get_indexer_for(subset)
                check = indices == -1
                if check.any():
                    raise KeyError(list(np.compress(check, subset)))
            else:
                indices = self.columns.get_indexer_for(subset)
                check = indices == -1
                if check.any():
                    raise KeyError(list(np.compress(check, subset)))

        def dropna_helper(df):
            new_df = df.dropna(axis=axis, how=how, thresh=thresh,
                               subset=indices, inplace=False)

            if axis == 1:
                new_index = new_df.columns
                new_df.columns = pd.RangeIndex(0, len(new_df.columns))
            else:
                new_index = new_df.index
                new_df.reset_index(drop=True, inplace=True)

            return new_df, new_index

        parts = self._col_partitions if axis == 1 else self._row_partitions
        result = [_deploy_func._submit(args=(dropna_helper, df),
                                       num_return_vals=2) for df in parts]
        new_parts, new_vals = [list(t) for t in zip(*result)]

        if axis == 1:
            new_vals = [self._col_metadata.get_global_indices(i, vals)
                        for i, vals in enumerate(ray.get(new_vals))]

            # This flattens the 2d array to 1d
            new_vals = [i for j in new_vals for i in j]
            new_cols = self.columns[new_vals]

            if not inplace:
                return DataFrame(col_partitions=new_parts,
                                 columns=new_cols,
                                 index=self.index)

            self._update_inplace(col_partitions=new_parts,
                                 columns=new_cols,
                                 index=self.index)

        else:
            new_vals = [self._row_metadata.get_global_indices(i, vals)
                        for i, vals in enumerate(ray.get(new_vals))]

            # This flattens the 2d array to 1d
            new_vals = [i for j in new_vals for i in j]
            new_rows = self.index[new_vals]

            if not inplace:
                return DataFrame(row_partitions=new_parts,
                                 index=new_rows,
                                 columns=self.columns)

            self._update_inplace(row_partitions=new_parts,
                                 index=new_rows,
                                 columns=self.columns)

            return None

    def add(self, other, axis='columns', level=None, fill_value=None):
        """Add this DataFrame to another or a scalar/list.

        Args:
            other: What to add this this DataFrame.
            axis: The axis to apply addition over. Only applicaable to Series
                or list 'other'.
            level: A level in the multilevel axis to add over.
            fill_value: The value to fill NaN.

        Returns:
            A new DataFrame with the applied addition.
        """
        return self._operator_helper(pd.DataFrame.add, other, axis, level,
                                     fill_value)

    def agg(self, func, axis=0, *args, **kwargs):
        return self.aggregate(func, axis, *args, **kwargs)

    def aggregate(self, func, axis=0, *args, **kwargs):
        axis = pd.DataFrame()._get_axis_number(axis)

        result = None

        if axis == 0:
            try:
                result = self._aggregate(func, axis=axis, *args, **kwargs)
            except TypeError:
                pass

        if result is None:
            kwargs.pop('is_transform', None)
            return self.apply(func, axis=axis, args=args, **kwargs)

        return result

    def _aggregate(self, arg, *args, **kwargs):
        _axis = kwargs.pop('_axis', None)
        if _axis is None:
            _axis = getattr(self, 'axis', 0)
        kwargs.pop('_level', None)

        if isinstance(arg, compat.string_types):
            return self._string_function(arg, *args, **kwargs)

        # Dictionaries have complex behavior because they can be renamed here.
        elif isinstance(arg, dict):
            raise NotImplementedError(
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")
        elif is_list_like(arg):
            return self.apply(arg, axis=_axis, args=args, **kwargs)
        elif callable(arg):
            self._callable_function(arg, _axis, *args, **kwargs)
        else:
            # TODO Make pandas error
            raise ValueError("type {} is not callable".format(type(arg)))

    def _string_function(self, func, *args, **kwargs):
        assert isinstance(func, compat.string_types)

        f = getattr(self, func, None)

        if f is not None:
            if callable(f):
                return f(*args, **kwargs)

            assert len(args) == 0
            assert len([kwarg
                        for kwarg in kwargs
                        if kwarg not in ['axis', '_level']]) == 0
            return f

        f = getattr(np, func, None)
        if f is not None:
            raise NotImplementedError("Numpy aggregates not yet supported.")

        raise ValueError("{} is an unknown string function".format(func))

    def _callable_function(self, func, axis, *args, **kwargs):
        kwargs['axis'] = axis

        def agg_helper(df, arg, index, columns, *args, **kwargs):
            df.index = index
            df.columns = columns
            is_transform = kwargs.pop('is_transform', False)
            new_df = df.agg(arg, *args, **kwargs)

            is_series = False
            index = None
            columns = None

            if isinstance(new_df, pd.Series):
                is_series = True
            else:
                columns = new_df.columns
                index = new_df.index
                new_df.columns = pd.RangeIndex(0, len(new_df.columns))
                new_df.reset_index(drop=True, inplace=True)

            if is_transform:
                if is_scalar(new_df) or len(new_df) != len(df):
                    raise ValueError("transforms cannot produce "
                                     "aggregated results")

            return is_series, new_df, index, columns

        if axis == 0:
            index = self.index
            columns = [self._col_metadata.partition_series(i).index
                       for i in range(len(self._col_partitions))]

            remote_result = \
                [_deploy_func._submit(args=(
                    lambda df: agg_helper(df,
                                          func,
                                          index,
                                          cols,
                                          *args,
                                          **kwargs),
                                      part), num_return_vals=4)
                 for cols, part in zip(columns, self._col_partitions)]

        if axis == 1:
            indexes = [self._row_metadata.partition_series(i).index
                       for i in range(len(self._row_partitions))]
            columns = self.columns

            remote_result = \
                [_deploy_func._submit(args=(
                    lambda df: agg_helper(df,
                                          func,
                                          index,
                                          columns,
                                          *args,
                                          **kwargs),
                                      part), num_return_vals=4)
                 for index, part in zip(indexes, self._row_partitions)]

        # This magic transposes the list comprehension returned from remote
        is_series, new_parts, index, columns = \
            [list(t) for t in zip(*remote_result)]

        # This part is because agg can allow returning a Series or a
        # DataFrame, and we have to determine which here. Shouldn't add
        # too much to latency in either case because the booleans can
        # be returned immediately
        is_series = ray.get(is_series)
        if all(is_series):
            new_series = pd.concat(ray.get(new_parts), copy=False)
            new_series.index = self.columns if axis == 0 else self.index
            return new_series
        # This error is thrown when some of the partitions return Series and
        # others return DataFrames. We do not allow mixed returns.
        elif any(is_series):
            raise ValueError("no results.")
        # The remaining logic executes when we have only DataFrames in the
        # remote objects. We build a Ray DataFrame from the Pandas partitions.
        elif axis == 0:
            new_index = ray.get(index[0])
            # This does not handle the Multi Index case
            new_columns = ray.get(columns)
            new_columns = new_columns[0].append(new_columns[1:])

            return DataFrame(col_partitions=new_parts,
                             columns=new_columns,
                             index=new_index)
        else:
            new_columns = ray.get(columns[0])
            # This does not handle the Multi Index case
            new_index = ray.get(index)
            new_index = new_index[0].append(new_index[1:])

            return DataFrame(row_partitions=new_parts,
                             columns=new_columns,
                             index=new_index)

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
        """Append another DataFrame/list/Series to this one.

        Args:
            other: The object to append to this.
            ignore_index: Ignore the index on appending.
            verify_integrity: Verify the integrity of the index on completion.

        Returns:
            A new DataFrame containing the concatenated values.
        """
        if isinstance(other, (pd.Series, dict)):
            if isinstance(other, dict):
                other = pd.Series(other)
            if other.name is None and not ignore_index:
                raise TypeError('Can only append a Series if ignore_index=True'
                                ' or if the Series has a name')

            if other.name is None:
                index = None
            else:
                # other must have the same index name as self, otherwise
                # index name will be reset
                index = pd.Index([other.name], name=self.index.name)

            combined_columns = self.columns.tolist() + self.columns.union(
                other.index).difference(self.columns).tolist()
            other = other.reindex(combined_columns, copy=False)
            other = pd.DataFrame(other.values.reshape((1, len(other))),
                                 index=index,
                                 columns=combined_columns)
            other = other._convert(datetime=True, timedelta=True)
        elif isinstance(other, list) and not isinstance(other[0], DataFrame):
            other = pd.DataFrame(other)
            if (self.columns.get_indexer(other.columns) >= 0).all():
                other = other.loc[:, self.columns]

        from .concat import concat
        if isinstance(other, (list, tuple)):
            to_concat = [self] + other
        else:
            to_concat = [self, other]

        return concat(to_concat, ignore_index=ignore_index,
                      verify_integrity=verify_integrity)

    def apply(self, func, axis=0, broadcast=False, raw=False, reduce=None,
              args=(), **kwds):
        """Apply a function along input axis of DataFrame.

        Args:
            func: The function to apply
            axis: The axis over which to apply the func.
            broadcast: Whether or not to broadcast.
            raw: Whether or not to convert to a Series.
            reduce: Whether or not to try to apply reduction procedures.

        Returns:
            Series or DataFrame, depending on func.
        """
        axis = pd.DataFrame()._get_axis_number(axis)

        if isinstance(func, compat.string_types):
            if axis == 1:
                kwds['axis'] = axis
            return getattr(self, func)(*args, **kwds)
        elif isinstance(func, dict):
            if axis == 1:
                raise TypeError(
                    "(\"'dict' object is not callable\", "
                    "'occurred at index {0}'".format(self.index[0]))
            if len(self.columns) != len(set(self.columns)):
                warnings.warn(
                    'duplicate column names not supported with apply().',
                    FutureWarning, stacklevel=2)
            has_list = list in map(type, func.values())
            part_ind_tuples = [(self._col_metadata[key], key) for key in func]

            if has_list:
                # if input dict has a list, the function to apply must wrap
                # single functions in lists as well to get the desired output
                # format
                result = [_deploy_func.remote(
                    lambda df: df.iloc[:, ind].apply(
                        func[key] if is_list_like(func[key])
                        else [func[key]]),
                    self._col_partitions[part])
                    for (part, ind), key in part_ind_tuples]
                return pd.concat(ray.get(result), axis=1, copy=False)
            else:
                result = [_deploy_func.remote(
                    lambda df: df.iloc[:, ind].apply(func[key]),
                    self._col_partitions[part])
                    for (part, ind), key in part_ind_tuples]
                return pd.Series(ray.get(result), index=func.keys())

        elif is_list_like(func):
            if axis == 1:
                raise TypeError(
                    "(\"'list' object is not callable\", "
                    "'occurred at index {0}'".format(self.index[0]))
            # TODO: some checking on functions that return Series or Dataframe
            new_cols = _map_partitions(lambda df: df.apply(func),
                                       self._col_partitions)

            # resolve function names for the DataFrame index
            new_index = [f_name if isinstance(f_name, compat.string_types)
                         else f_name.__name__ for f_name in func]
            return DataFrame(col_partitions=new_cols,
                             columns=self.columns,
                             index=new_index,
                             col_metadata=self._col_metadata)
        elif callable(func):
            return self._callable_function(func, axis=axis, *args, **kwds)

    def as_blocks(self, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def as_matrix(self, columns=None):
        """Convert the frame to its Numpy-array representation.

        Args:
            columns: If None, return all columns, otherwise,
                returns specified columns.

        Returns:
            values: ndarray
        """
        # TODO this is very inefficient, also see __array__
        return to_pandas(self).as_matrix(columns)

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
        if isinstance(dtype, dict):
            if (not set(dtype.keys()).issubset(set(self.columns)) and
                    errors == 'raise'):
                raise KeyError(
                    "Only a column name can be used for the key in"
                    "a dtype mappings argument.")
            columns = list(dtype.keys())
            col_idx = [(self.columns.get_loc(columns[i]), columns[i])
                       if columns[i] in self.columns
                       else (columns[i], columns[i])
                       for i in range(len(columns))]
            new_dict = {}
            for idx, key in col_idx:
                new_dict[idx] = dtype[key]
            new_rows = _map_partitions(lambda df, dt: df.astype(dtype=dt,
                                                                copy=True,
                                                                errors=errors,
                                                                **kwargs),
                                       self._row_partitions, new_dict)
            if copy:
                return DataFrame(row_partitions=new_rows,
                                 columns=self.columns,
                                 index=self.index)
            self._row_partitions = new_rows
        else:
            new_blocks = [_map_partitions(lambda d: d.astype(dtype=dtype,
                                                             copy=True,
                                                             errors=errors,
                                                             **kwargs),
                                          block)
                          for block in self._block_partitions]
            if copy:
                return DataFrame(block_partitions=new_blocks,
                                 columns=self.columns,
                                 index=self.index)
            self._block_partitions = new_blocks

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
        axis = pd.DataFrame()._get_axis_number(axis) if axis is not None \
            else 0

        if axis == 0:
            new_cols = _map_partitions(func, self._col_partitions)
            return DataFrame(col_partitions=new_cols,
                             row_metadata=self._row_metadata,
                             col_metadata=self._col_metadata)
        else:
            new_rows = _map_partitions(func, self._row_partitions)
            return DataFrame(row_partitions=new_rows,
                             row_metadata=self._row_metadata,
                             col_metadata=self._col_metadata)

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

        for df, partition in parts:
            this_partition = self._col_metadata.partition_series(partition)
            df.columns = this_partition[this_partition.isin(df.columns)].index

        # Remove index from tuple
        result = pd.concat([obj[0] for obj in parts], axis=1, copy=False)
        return result

    def diff(self, periods=1, axis=0):
        """Finds the difference between elements on the axis requested

        Args:
            periods: Periods to shift for forming difference
            axis: Take difference over rows or columns

        Returns:
            DataFrame with the diff applied
        """
        axis = pd.DataFrame()._get_axis_number(axis)
        partitions = (self._col_partitions if
                      axis == 0 else self._row_partitions)

        result = _map_partitions(lambda df:
                                 df.diff(axis=axis, periods=periods),
                                 partitions)

        if (axis == 1):
            return DataFrame(row_partitions=result,
                             columns=self.columns,
                             index=self.index)
        if (axis == 0):
            return DataFrame(col_partitions=result,
                             columns=self.columns,
                             index=self.index)

    def div(self, other, axis='columns', level=None, fill_value=None):
        """Divides this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the divide against this.
            axis: The axis to divide over.
            level: The Multilevel index level to apply divide over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Divide applied.
        """
        return self._operator_helper(pd.DataFrame.div, other, axis, level,
                                     fill_value)

    def divide(self, other, axis='columns', level=None, fill_value=None):
        """Synonym for div.

        Args:
            other: The object to use to apply the divide against this.
            axis: The axis to divide over.
            level: The Multilevel index level to apply divide over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Divide applied.
        """
        return self.div(other, axis, level, fill_value)

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
            axis = pd.DataFrame()._get_axis_name(axis)
            axes = {axis: labels}
        elif index is not None or columns is not None:
            axes, _ = pd.DataFrame()._construct_axes_from_arguments((index,
                                                                     columns),
                                                                    {})
        else:
            raise ValueError("Need to specify at least one of 'labels', "
                             "'index' or 'columns'")
        obj = self.copy()

        def drop_helper(obj, axis, label):
            # TODO(patyang): If you drop from the index first, you can do it
            # in batch by returning the dropped items. Likewise coords.drop
            # leaves the coords df in an inconsistent state.
            if axis == 'index':
                try:
                    coords = obj._row_metadata[label]
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
                        obj._row_metadata.squeeze(part, index)

                    obj._row_metadata.drop(labels=label)
                except KeyError:
                    return obj
            else:
                try:
                    coords = obj._col_metadata[label]
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
                        obj._col_metadata.squeeze(part, index)

                    obj._col_metadata.drop(labels=label)
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
            self._row_metadata = obj._row_metadata
            self._col_metadata = obj._col_metadata
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
        """Checks element-wise that this is equal to other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the eq over.
            level: The Multilevel index level to apply eq over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.eq, other, axis, level)

    def equals(self, other):
        """
        Checks if other DataFrame is elementwise equal to the current one

        Returns:
            Boolean: True if equal, otherwise False
        """

        if not self.index.equals(other.index) or not \
                self.columns.equals(other.columns):
            return False

        # We copartition because we don't know what the DataFrames look like
        # before this. Empty partitions can give problems with
        # _match_partitioning (See _match_partitioning)
        new_zipped_parts = self._copartition(other, self.index)

        equals_partitions = [_equals_helper.remote(left, right)
                             for left, right in new_zipped_parts]

        # To avoid getting all we use next notation.
        return next((False for eq in equals_partitions if not ray.get(eq)),
                    True)

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
        self._validate_eval_query(expr, **kwargs)

        columns = self.columns

        def eval_helper(df):
            df.columns = columns
            result = df.eval(expr, inplace=False, **kwargs)
            # If result is a series, expr was not an assignment expression.
            if not isinstance(result, pd.Series):
                result.columns = pd.RangeIndex(0, len(result.columns))
            return result

        inplace = validate_bool_kwarg(inplace, "inplace")
        new_rows = _map_partitions(eval_helper, self._row_partitions)

        result_type = ray.get(_deploy_func.remote(lambda df: type(df),
                                                  new_rows[0]))
        if result_type is pd.Series:
            new_series = pd.concat(ray.get(new_rows), axis=0, copy=False)
            new_series.index = self.index
            return new_series

        columns_copy = self._col_metadata._coord_df.copy().T
        columns_copy.eval(expr, inplace=True, **kwargs)
        columns = columns_copy.columns

        if inplace:
            self._update_inplace(row_partitions=new_rows, columns=columns,
                                 index=self.index)
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

        axis = pd.DataFrame()._get_axis_number(axis) \
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

        parts, coords_obj = (new_obj._col_partitions,
                             new_obj._col_metadata) if axis == 0 else \
                            (new_obj._row_partitions,
                             new_obj._row_metadata)

        if isinstance(value, (pd.Series, dict)):
            new_vals = {}
            value = dict(value)
            for val in value:
                # Get the local index for the partition
                try:
                    part, index = coords_obj[val]
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
            new_parts = [parts[i] if coords_obj.index[i] not in new_vals
                         else new_vals[coords_obj.index[i]]
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
        """Subset rows or columns based on their labels

        Args:
            items (list): list of labels to subset
            like (string): retain labels where `arg in label == True`
            regex (string): retain labels matching regex input
            axis: axis to filter on

        Returns:
            A new dataframe with the filter applied.
        """
        nkw = com._count_not_none(items, like, regex)
        if nkw > 1:
            raise TypeError('Keyword arguments `items`, `like`, or `regex` '
                            'are mutually exclusive')
        if nkw == 0:
            raise TypeError('Must pass either `items`, `like`, or `regex`')

        if axis is None:
            axis = 'columns'  # This is the default info axis for dataframes

        axis = pd.DataFrame()._get_axis_number(axis)
        labels = self.columns if axis else self.index

        if items is not None:
            bool_arr = labels.isin(items)
        elif like is not None:
            def f(x):
                return like in to_str(x)
            bool_arr = labels.map(f).tolist()
        else:
            def f(x):
                return matcher.search(to_str(x)) is not None
            matcher = re.compile(regex)
            bool_arr = labels.map(f).tolist()

        if not axis:
            return self[bool_arr]
        return self[self.columns[bool_arr]]

    def first(self, offset):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def first_valid_index(self):
        """Return index for first non-NA/null value.

        Returns:
            scalar: type of index
        """
        return self._row_metadata.first_valid_index()

    def floordiv(self, other, axis='columns', level=None, fill_value=None):
        """Divides this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the divide against this.
            axis: The axis to divide over.
            level: The Multilevel index level to apply divide over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Divide applied.
        """
        return self._operator_helper(pd.DataFrame.floordiv, other, axis, level,
                                     fill_value)

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
        """Checks element-wise that this is greater than or equal to other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the gt over.
            level: The Multilevel index level to apply gt over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.ge, other, axis, level)

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
        """Checks element-wise that this is greater than other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the gt over.
            level: The Multilevel index level to apply gt over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.gt, other, axis, level)

    def head(self, n=5):
        """Get the first n rows of the dataframe.

        Args:
            n (int): The number of rows to return.

        Returns:
            A new dataframe with the first n rows of the dataframe.
        """
        if n >= len(self._row_metadata):
            return self.copy()

        new_dfs = _map_partitions(lambda df: df.head(n),
                                  self._col_partitions)

        index = self._row_metadata.index[:n]

        return DataFrame(col_partitions=new_dfs,
                         col_metadata=self._col_metadata,
                         index=index,
                         dtypes_cache=self._dtypes_cache)

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
        if not all(d != np.dtype('O') for d in self.dtypes):
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
        if not all(d != np.dtype('O') for d in self.dtypes):
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

        def info_helper(df):
            output_buffer = io.StringIO()
            df.info(verbose=verbose,
                    buf=output_buffer,
                    max_cols=max_cols,
                    memory_usage=memory_usage,
                    null_counts=null_counts)
            return output_buffer.getvalue()

        # Combine the per-partition info and split into lines
        result = ''.join(ray.get(_map_partitions(info_helper,
                                                 self._col_partitions)))
        lines = result.split('\n')

        # Class denoted in info() output
        class_string = '<class \'ray.dataframe.dataframe.DataFrame\'>\n'

        # Create the Index info() string by parsing self.index
        index_string = self.index.summary() + '\n'

        # A column header is needed in the inf() output
        col_header = 'Data columns (total {0} columns):\n'.format(
                len(self.columns))

        # Parse the per-partition values to get the per-column details
        # Find all the lines in the output that start with integers
        prog = re.compile('^[0-9]+.+')
        col_lines = [prog.match(line) for line in lines]
        cols = [c.group(0) for c in col_lines if c is not None]
        # replace the partition columns names with real column names
        columns = ["{0}\t{1}\n".format(self.columns[i],
                                       cols[i].split(" ", 1)[1])
                   for i in range(len(cols))]
        col_string = ''.join(columns) + '\n'

        # A summary of the dtypes in the dataframe
        dtypes_string = "dtypes: "
        for dtype, count in self.dtypes.value_counts().iteritems():
            dtypes_string += "{0}({1}),".format(dtype, count)
        dtypes_string = dtypes_string[:-1] + '\n'

        # Compute the memory usage by summing per-partitions return values
        # Parse lines for memory usage number
        prog = re.compile('^memory+.+')
        mems = [prog.match(line) for line in lines]
        mem_vals = [float(re.search(r'\d+', m.group(0)).group())
                    for m in mems if m is not None]

        memory_string = ""

        if len(mem_vals) != 0:
            # Sum memory usage from each partition
            if memory_usage != 'deep':
                memory_string = 'memory usage: {0}+ bytes'.format(
                        sum(mem_vals))
            else:
                memory_string = 'memory usage: {0} bytes'.format(sum(mem_vals))

        # Combine all the components of the info() output
        result = ''.join([class_string, index_string, col_header,
                          col_string, dtypes_string, memory_string])

        # Write to specified output buffer
        if buf:
            buf.write(result)
        else:
            sys.stdout.write(result)

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

        partition, index_within_partition = \
            self._col_metadata.insert(column, loc)

        # Deploy insert function to specific column partition, and replace that
        # column
        def insert_col_part(df):
            if isinstance(value, pd.Series) and \
                    isinstance(value.dtype,
                               pd.core.dtypes.dtypes.DatetimeTZDtype):
                # Need to set index to index of this dtype or inserted values
                # become NaT
                df.index = value
                df.insert(index_within_partition, column,
                          value, allow_duplicates)
                df.index = pd.RangeIndex(0, len(df))
            else:
                df.insert(index_within_partition, column,
                          value, allow_duplicates)
            return df

        new_obj = _deploy_func.remote(insert_col_part,
                                      self._col_partitions[partition])

        new_cols = [self._col_partitions[i]
                    if i != partition
                    else new_obj
                    for i in range(len(self._col_partitions))]
        new_col_names = self.columns.insert(loc, column)

        self._update_inplace(col_partitions=new_cols, columns=new_col_names,
                             index=self.index)

    def interpolate(self, method='linear', axis=0, limit=None, inplace=False,
                    limit_direction='forward', downcast=None, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def iterrows(self):
        """Iterate over DataFrame rows as (index, Series) pairs.

        Note:
            Generators can't be pickled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A generator that iterates over the rows of the frame.
        """
        index_iter = (self._row_metadata.partition_series(i).index
                      for i in range(len(self._row_partitions)))

        def iterrow_helper(part):
            df = ray.get(part)
            df.columns = self.columns
            df.index = next(index_iter)
            return df.iterrows()

        partition_iterator = PartitionIterator(self._row_partitions,
                                               iterrow_helper)

        for v in partition_iterator:
            yield v

    def items(self):
        """Iterator over (column name, Series) pairs.

        Note:
            Generators can't be pickled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A generator that iterates over the columns of the frame.
        """
        col_iter = (self._col_metadata.partition_series(i).index
                    for i in range(len(self._col_partitions)))

        def items_helper(part):
            df = ray.get(part)
            df.columns = next(col_iter)
            df.index = self.index
            return df.items()

        partition_iterator = PartitionIterator(self._col_partitions,
                                               items_helper)

        for v in partition_iterator:
            yield v

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
            Generators can't be pickled so from the remote function
            we expand the generator into a list before getting it.
            This is not that ideal.

        Returns:
            A tuple representing row data. See args for varying tuples.
        """
        index_iter = (self._row_metadata.partition_series(i).index
                      for i in range(len(self._row_partitions)))

        def itertuples_helper(part):
            df = ray.get(part)
            df.columns = self.columns
            df.index = next(index_iter)
            return df.itertuples(index=index, name=name)

        partition_iterator = PartitionIterator(self._row_partitions,
                                               itertuples_helper)

        for v in partition_iterator:
            yield v

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        """Join two or more DataFrames, or a DataFrame with a collection.

        Args:
            other: What to join this DataFrame with.
            on: A column name to use from the left for the join.
            how: What type of join to conduct.
            lsuffix: The suffix to add to column names that match on left.
            rsuffix: The suffix to add to column names that match on right.
            sort: Whether or not to sort.

        Returns:
            The joined DataFrame.
        """

        if on is not None:
            raise NotImplementedError("Not yet.")

        if isinstance(other, pd.Series):
            if other.name is None:
                raise ValueError("Other Series must have a name")
            other = DataFrame({other.name: other})

        if isinstance(other, DataFrame):
            if on is not None:
                index = self[on]
            else:
                index = self.index

            new_index = index.join(other.index, how=how, sort=sort)

            # Joining two empty DataFrames is fast, and error checks for us.
            new_column_labels = pd.DataFrame(columns=self.columns) \
                .join(pd.DataFrame(columns=other.columns),
                      lsuffix=lsuffix, rsuffix=rsuffix).columns

            new_partition_num = max(len(self._block_partitions.T),
                                    len(other._block_partitions.T))

            # Join is a concat once we have shuffled the data internally.
            # We shuffle the data by computing the correct order.
            # Another important thing to note: We set the current self index
            # to the index variable which may be 'on'.
            new_self = np.array([
                _reindex_helper._submit(args=tuple([index, new_index, 1,
                                                    new_partition_num] +
                                                   block.tolist()),
                                        num_return_vals=new_partition_num)
                for block in self._block_partitions.T])
            new_other = np.array([
                _reindex_helper._submit(args=tuple([other.index, new_index, 1,
                                                    new_partition_num] +
                                                   block.tolist()),
                                        num_return_vals=new_partition_num)
                for block in other._block_partitions.T])

            # Append the blocks together (i.e. concat)
            new_block_parts = np.concatenate((new_self, new_other)).T

            # Default index in the case that on is set.
            if on is not None:
                new_index = None

            # TODO join the two metadata tables for performance.
            return DataFrame(block_partitions=new_block_parts,
                             index=new_index,
                             columns=new_column_labels)
        else:
            # This constraint carried over from Pandas.
            if on is not None:
                raise ValueError("Joining multiple DataFrames only supported"
                                 " for joining on index")

            # Joining the empty DataFrames with either index or columns is
            # fast. It gives us proper error checking for the edge cases that
            # would otherwise require a lot more logic.
            new_index = pd.DataFrame(index=self.index).join(
                [pd.DataFrame(index=obj.index) for obj in other],
                how=how, sort=sort).index

            new_column_labels = pd.DataFrame(columns=self.columns).join(
                [pd.DataFrame(columns=obj.columns) for obj in other],
                lsuffix=lsuffix, rsuffix=rsuffix).columns

            new_partition_num = max([len(self._block_partitions.T)] +
                                    [len(obj._block_partitions.T)
                                     for obj in other])

            new_self = np.array([
                _reindex_helper._submit(args=tuple([self.index, new_index, 1,
                                                    new_partition_num] +
                                                   block.tolist()),
                                        num_return_vals=new_partition_num)
                for block in self._block_partitions.T])

            new_others = np.array([_reindex_helper._submit(
                args=tuple([obj.index, new_index, 1, new_partition_num] +
                           block.tolist()),
                num_return_vals=new_partition_num
            ) for obj in other for block in obj._block_partitions.T])

            # Append the columns together (i.e. concat)
            new_block_parts = np.concatenate((new_self, new_others)).T

            # TODO join the two metadata tables for performance.
            return DataFrame(block_partitions=new_block_parts,
                             index=new_index,
                             columns=new_column_labels)

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
        return self._row_metadata.last_valid_index()

    def le(self, other, axis='columns', level=None):
        """Checks element-wise that this is less than or equal to other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the le over.
            level: The Multilevel index level to apply le over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.le, other, axis, level)

    def lookup(self, row_labels, col_labels):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def lt(self, other, axis='columns', level=None):
        """Checks element-wise that this is less than other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the lt over.
            level: The Multilevel index level to apply lt over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.lt, other, axis, level)

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

        def remote_func(df):
            return df.memory_usage(index=False, deep=deep)

        result = self._arithmetic_helper(remote_func, axis=0)

        result.index = self.columns
        if index:
            index_value = self._row_metadata.index.memory_usage(deep=deep)
            return pd.Series(index_value, index=['Index']).append(result)

        return result

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True, indicator=False,
              validate=None):
        """Database style join, where common columns in "on" are merged.

        Args:
            right: The DataFrame to merge against.
            how: What type of join to use.
            on: The common column name(s) to join on. If None, and left_on and
                right_on  are also None, will default to all commonly named
                columns.
            left_on: The column(s) on the left to use for the join.
            right_on: The column(s) on the right to use for the join.
            left_index: Use the index from the left as the join keys.
            right_index: Use the index from the right as the join keys.
            sort: Sort the join keys lexicographically in the result.
            suffixes: Add this suffix to the common names not in the "on".
            copy: Does nothing in our implementation
            indicator: Adds a column named _merge to the DataFrame with
                metadata from the merge about each row.
            validate: Checks if merge is a specific type.

        Returns:
             A merged Dataframe
        """

        if not isinstance(right, DataFrame):
            raise ValueError("can not merge DataFrame with instance of type "
                             "{}".format(type(right)))

        args = (how, on, left_on, right_on, left_index, right_index, sort,
                suffixes, False, indicator, validate)

        left_cols = ray.put(self.columns)
        right_cols = ray.put(right.columns)

        # This can be put in a remote function because we don't need it until
        # the end, and the columns can be built asynchronously. This takes the
        # columns defining off the critical path and speeds up the overall
        # merge.
        new_columns = _merge_columns.remote(left_cols, right_cols, *args)

        if on is not None:
            if left_on is not None or right_on is not None:
                raise MergeError("Can only pass argument \"on\" OR \"left_on\""
                                 " and \"right_on\", not a combination of "
                                 "both.")
            if not is_list_like(on):
                on = [on]

            if next((True for key in on if key not in self), False) or \
                    next((True for key in on if key not in right), False):

                missing_key = \
                    next((str(key) for key in on if key not in self), "") + \
                    next((str(key) for key in on if key not in right), "")
                raise KeyError(missing_key)

        elif right_on is not None or right_index is True:
            if left_on is None and left_index is False:
                # Note: This is not the same error as pandas, but pandas throws
                # a ValueError NoneType has no len(), and I don't think that
                # helps enough.
                raise TypeError("left_on must be specified or left_index must "
                                "be true if right_on is specified.")

        elif left_on is not None or left_index is True:
            if right_on is None and right_index is False:
                # Note: See note above about TypeError.
                raise TypeError("right_on must be specified or right_index "
                                "must be true if right_on is specified.")

        if left_on is not None:
            if not is_list_like(left_on):
                left_on = [left_on]

            if next((True for key in left_on if key not in self), False):
                raise KeyError(next(key for key in left_on
                                    if key not in self))

        if right_on is not None:
            if not is_list_like(right_on):
                right_on = [right_on]

            if next((True for key in right_on if key not in right), False):
                raise KeyError(next(key for key in right_on
                                    if key not in right))

        # There's a small chance that our partitions are already perfect, but
        # if it's not, we need to adjust them. We adjust the right against the
        # left because the defaults of merge rely on the order of the left. We
        # have to push the index down here, so if we're joining on the right's
        # index we go ahead and push it down here too.
        if not np.array_equal(self._row_metadata._lengths,
                              right._row_metadata._lengths) or right_index:

            repartitioned_right = np.array([_match_partitioning._submit(
                args=(df, self._row_metadata._lengths, right.index),
                num_return_vals=len(self._row_metadata._lengths))
                for df in right._col_partitions]).T
        else:
            repartitioned_right = right._block_partitions

        if not left_index and not right_index:
            # Passing None to each call specifies that we don't care about the
            # left's index for the join.
            left_idx = itertools.repeat(None)

            # We only return the index if we need to update it, and that only
            # happens when either left_index or right_index is True. We will
            # use this value to add the return vals if we are getting an index
            # back.
            return_index = False
        else:
            # We build this to push the index down so that we can use it for
            # the join.
            left_idx = \
                (v.index for k, v in
                 self._row_metadata._coord_df.copy().groupby('partition'))
            return_index = True

        new_blocks = \
            np.array([_co_op_helper._submit(
                args=tuple([lambda x, y: x.merge(y, *args),
                            left_cols, right_cols,
                            len(self._block_partitions.T), next(left_idx)] +
                           np.concatenate(obj).tolist()),
                num_return_vals=len(self._block_partitions.T) + return_index)
                for obj in zip(self._block_partitions,
                               repartitioned_right)])

        if not return_index:
            # Default to RangeIndex if left_index and right_index both false.
            new_index = None
        else:
            new_index_parts = new_blocks[:, -1]
            new_index = _concat_index.remote(*new_index_parts)
            new_blocks = new_blocks[:, :-1]

        return DataFrame(block_partitions=new_blocks,
                         columns=new_columns,
                         index=new_index)

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
        """Mods this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the mod against this.
            axis: The axis to mod over.
            level: The Multilevel index level to apply mod over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Mod applied.
        """
        return self._operator_helper(pd.DataFrame.mod, other, axis, level,
                                     fill_value)

    def mode(self, axis=0, numeric_only=False):
        """Perform mode across the DataFrame.

        Args:
            axis (int): The axis to take the mode on.
            numeric_only (bool): if True, only apply to numeric columns.

        Returns:
            DataFrame: The mode of the DataFrame.
        """
        axis = pd.DataFrame()._get_axis_number(axis)

        def mode_helper(df):
            mode_df = df.mode(axis=axis, numeric_only=numeric_only)
            return mode_df, mode_df.shape[axis]

        def fix_length(df, *lengths):
            max_len = max(lengths[0])
            df = df.reindex(pd.RangeIndex(max_len), axis=axis)
            return df

        parts = self._col_partitions if axis == 0 else self._row_partitions

        result = [_deploy_func._submit(args=(lambda df: mode_helper(df),
                                             part), num_return_vals=2)
                  for part in parts]

        parts, lengths = [list(t) for t in zip(*result)]

        parts = [_deploy_func.remote(
            lambda df, *l: fix_length(df, l), part, *lengths)
            for part in parts]

        if axis == 0:
            return DataFrame(col_partitions=parts,
                             columns=self.columns)
        else:
            return DataFrame(row_partitions=parts,
                             index=self.index)

    def mul(self, other, axis='columns', level=None, fill_value=None):
        """Multiplies this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the multiply against this.
            axis: The axis to multiply over.
            level: The Multilevel index level to apply multiply over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Multiply applied.
        """
        return self._operator_helper(pd.DataFrame.mul, other, axis, level,
                                     fill_value)

    def multiply(self, other, axis='columns', level=None, fill_value=None):
        """Synonym for mul.

        Args:
            other: The object to use to apply the multiply against this.
            axis: The axis to multiply over.
            level: The Multilevel index level to apply multiply over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Multiply applied.
        """
        return self.mul(other, axis, level, fill_value)

    def ne(self, other, axis='columns', level=None):
        """Checks element-wise that this is not equal to other.

        Args:
            other: A DataFrame or Series or scalar to compare to.
            axis: The axis to perform the ne over.
            level: The Multilevel index level to apply ne over.

        Returns:
            A new DataFrame filled with Booleans.
        """
        return self._operator_helper(pd.DataFrame.ne, other, axis, level)

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

        new_dtypes = pd.Series([np.dtype("bool")] * len(self.columns),
                               index=self.columns)

        return DataFrame(block_partitions=new_block_partitions,
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata,
                         dtypes_cache=new_dtypes)

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

        new_dtypes = pd.Series([np.dtype("bool")] * len(self.columns),
                               index=self.columns)

        return DataFrame(block_partitions=new_block_partitions,
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata,
                         dtypes_cache=new_dtypes)

    def nsmallest(self, n, columns, keep='first'):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def nunique(self, axis=0, dropna=True):
        """Return Series with number of distinct
           observations over requested axis.

        Args:
            axis : {0 or 'index', 1 or 'columns'}, default 0
            dropna : boolean, default True

        Returns:
            nunique : Series
        """
        def remote_func(df):
            return df.nunique(axis=axis, dropna=dropna)

        return self._arithmetic_helper(remote_func, axis)

    def pct_change(self, periods=1, fill_method='pad', limit=None, freq=None,
                   **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pipe(self, func, *args, **kwargs):
        """Apply func(self, *args, **kwargs)

        Args:
            func: function to apply to the df.
            args: positional arguments passed into ``func``.
            kwargs: a dictionary of keyword arguments passed into ``func``.

        Returns:
            object: the return type of ``func``.
        """
        return com._pipe(self, func, *args, **kwargs)

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
        """Pow this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the pow against this.
            axis: The axis to pow over.
            level: The Multilevel index level to apply pow over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Pow applied.
        """
        return self._operator_helper(pd.DataFrame.pow, other, axis, level,
                                     fill_value)

    def prod(self, axis=None, skipna=None, level=None, numeric_only=None,
             min_count=1, **kwargs):
        """Return the product of the values for the requested axis

        Args:
            axis : {index (0), columns (1)}
            skipna : boolean, default True
            level : int or level name, default None
            numeric_only : boolean, default None
            min_count : int, default 1

        Returns:
            prod : Series or DataFrame (if level specified)
        """
        def remote_func(df):
            return df.prod(axis=axis, skipna=skipna, level=level,
                           numeric_only=numeric_only, min_count=min_count,
                           **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def product(self, axis=None, skipna=None, level=None, numeric_only=None,
                min_count=1, **kwargs):
        """Return the product of the values for the requested axis

        Args:
            axis : {index (0), columns (1)}
            skipna : boolean, default True
            level : int or level name, default None
            numeric_only : boolean, default None
            min_count : int, default 1

        Returns:
            product : Series or DataFrame (if level specified)
        """
        return self.prod(axis=axis, skipna=skipna, level=level,
                         numeric_only=numeric_only, min_count=min_count,
                         **kwargs)

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

        def check_bad_dtype(t):
            return t == np.dtype('O') or is_timedelta64_dtype(t)

        if not numeric_only:
            # check if there are any object columns
            if all(check_bad_dtype(t) for t in self.dtypes):
                raise TypeError("can't multiply sequence by non-int of type "
                                "'float'")
            else:
                if next((True for t in self.dtypes if check_bad_dtype(t)),
                        False):
                    dtype = next(t for t in self.dtypes if check_bad_dtype(t))
                    raise ValueError("Cannot compare type '{}' with type '{}'"
                                     .format(type(dtype), float))
        else:
            # Normally pandas returns this near the end of the quantile, but we
            # can't afford the overhead of running the entire operation before
            # we error.
            if all(check_bad_dtype(t) for t in self.dtypes):
                raise ValueError("need at least one array to concatenate")

        # check that all qs are between 0 and 1
        pd.DataFrame()._check_percentile(q)

        def quantile_helper(df, base_object):
            """Quantile to be run inside each partitoin.

            Args:
                df: The DataFrame composing the partition.
                base_object: An empty pd.Series or pd.DataFrame depending on q.

            Returns:
                 A new Series or DataFrame depending on q.
            """
            # This if call prevents ValueErrors with object only partitions
            if (numeric_only and
                    all(dtype == np.dtype('O') or
                        is_timedelta64_dtype(dtype)
                        for dtype in df.dtypes)):
                return base_object
            else:
                return df.quantile(q=q, axis=axis, numeric_only=numeric_only,
                                   interpolation=interpolation)

        axis = pd.DataFrame()._get_axis_number(axis)

        if isinstance(q, (pd.Series, np.ndarray, pd.Index, list)):

            q_index = pd.Float64Index(q)

            if axis == 0:
                new_partitions = _map_partitions(
                    lambda df: quantile_helper(df, pd.DataFrame()),
                    self._col_partitions)

                # select only correct dtype columns
                new_columns = self.dtypes[self.dtypes.apply(
                                          lambda x: is_numeric_dtype(x))].index

            else:
                new_partitions = _map_partitions(
                    lambda df: quantile_helper(df, pd.DataFrame()),
                    self._row_partitions)
                new_columns = self.index

            return DataFrame(col_partitions=new_partitions,
                             index=q_index,
                             columns=new_columns)

        else:
            # When q is a single float, we return a Series, so using
            # arithmetic_helper works well here.
            result = self._arithmetic_helper(
                lambda df: quantile_helper(df, pd.Series()), axis)
            result.name = q
            return result

    def query(self, expr, inplace=False, **kwargs):
        """Queries the Dataframe with a boolean expression

        Returns:
            A new DataFrame if inplace=False
        """
        self._validate_eval_query(expr, **kwargs)

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
            self._update_inplace(row_partitions=new_rows, index=self.index)
        else:
            return DataFrame(row_partitions=new_rows,
                             col_metadata=self._col_metadata)

    def radd(self, other, axis='columns', level=None, fill_value=None):
        return self.add(other, axis, level, fill_value)

    def rank(self, axis=0, method='average', numeric_only=None,
             na_option='keep', ascending=True, pct=False):

        """
        Compute numerical data ranks (1 through n) along axis.
        Equal values are assigned a rank that is the [method] of
        the ranks of those values.

        Args:
            axis (int): 0 or 'index' for row-wise,
                        1 or 'columns' for column-wise
            interpolation: {'average', 'min', 'max', 'first', 'dense'}
                Specifies which method to use for equal vals
            numeric_only (boolean)
                Include only float, int, boolean data.
            na_option: {'keep', 'top', 'bottom'}
                Specifies how to handle NA options
            ascending (boolean):
                Decedes ranking order
            pct (boolean):
                Computes percentage ranking of data
        Returns:
            A new DataFrame
        """

        def rank_helper(df):
            return df.rank(axis=axis, method=method,
                           numeric_only=numeric_only,
                           na_option=na_option,
                           ascending=ascending, pct=pct)

        axis = pd.DataFrame()._get_axis_number(axis)

        if (axis == 1):
            new_cols = self.dtypes[self.dtypes.apply(
                                   lambda x: is_numeric_dtype(x))].index
            result = _map_partitions(rank_helper,
                                     self._row_partitions)
            return DataFrame(row_partitions=result,
                             columns=new_cols,
                             index=self.index)

        if (axis == 0):
            result = _map_partitions(rank_helper,
                                     self._col_partitions)
            return DataFrame(col_partitions=result,
                             columns=self.columns,
                             index=self.index)

    def rdiv(self, other, axis='columns', level=None, fill_value=None):
        return self._single_df_op_helper(
            lambda df: df.rdiv(other, axis, level, fill_value),
            other, axis, level)

    def reindex(self, labels=None, index=None, columns=None, axis=None,
                method=None, copy=True, level=None, fill_value=np.nan,
                limit=None, tolerance=None):
        if level is not None:
            raise NotImplementedError(
                "Multilevel Index not Implemented. "
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")

        axis = pd.DataFrame()._get_axis_number(axis) if axis is not None \
            else 0
        if axis == 0 and labels is not None:
            index = labels
        elif labels is not None:
            columns = labels

        new_blocks = self._block_partitions
        if index is not None:
            old_index = self.index
            new_blocks = np.array([reindex_helper._submit(
                args=(old_index, index, 1, len(new_blocks), method,
                      fill_value, limit, tolerance) + tuple(block.tolist()),
                num_return_vals=len(new_blocks))
                for block in new_blocks.T]).T
        else:
            index = self.index

        if columns is not None:
            old_columns = self.columns
            new_blocks = np.array([reindex_helper._submit(
                args=(old_columns, columns, 0, new_blocks.shape[1], method,
                      fill_value, limit, tolerance) + tuple(block.tolist()),
                num_return_vals=new_blocks.shape[1])
                for block in new_blocks])
        else:
            columns = self.columns

        if copy:
            return DataFrame(block_partitions=new_blocks,
                             index=index,
                             columns=columns)

        self._update_inplace(block_partitions=new_blocks,
                             index=index,
                             columns=columns)

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
            renamed.index.name = mapper
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
            renamed.index.set_names(name)

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
        new_index = pd.RangeIndex(len(self))
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
        return self._single_df_op_helper(
            lambda df: df.rfloordiv(other, axis, level, fill_value),
            other, axis, level)

    def rmod(self, other, axis='columns', level=None, fill_value=None):
        return self._single_df_op_helper(
            lambda df: df.rmod(other, axis, level, fill_value),
            other, axis, level)

    def rmul(self, other, axis='columns', level=None, fill_value=None):
        return self.mul(other, axis, level, fill_value)

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
                         row_metadata=self._row_metadata,
                         col_metadata=self._col_metadata)

    def rpow(self, other, axis='columns', level=None, fill_value=None):
        return self._single_df_op_helper(
            lambda df: df.rpow(other, axis, level, fill_value),
            other, axis, level)

    def rsub(self, other, axis='columns', level=None, fill_value=None):
        return self._single_df_op_helper(
            lambda df: df.rsub(other, axis, level, fill_value),
            other, axis, level)

    def rtruediv(self, other, axis='columns', level=None, fill_value=None):
        return self._single_df_op_helper(
            lambda df: df.rtruediv(other, axis, level, fill_value),
            other, axis, level)

    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None):
        """Returns a random sample of items from an axis of object.

        Args:
            n: Number of items from axis to return. Cannot be used with frac.
                Default = 1 if frac = None.
            frac: Fraction of axis items to return. Cannot be used with n.
            replace: Sample with or without replacement. Default = False.
            weights: Default 'None' results in equal probability weighting.
                If passed a Series, will align with target object on index.
                Index values in weights not found in sampled object will be
                ignored and index values in sampled object not in weights will
                be assigned weights of zero. If called on a DataFrame, will
                accept the name of a column when axis = 0. Unless weights are
                a Series, weights must be same length as axis being sampled.
                If weights do not sum to 1, they will be normalized to sum
                to 1. Missing values in the weights column will be treated as
                zero. inf and -inf values not allowed.
            random_state: Seed for the random number generator (if int), or
                numpy RandomState object.
            axis: Axis to sample. Accepts axis number or name.

        Returns:
            A new Dataframe
        """

        axis = pd.DataFrame()._get_axis_number(axis) if axis is not None \
            else 0

        if axis == 0:
            axis_length = len(self._row_metadata)
        else:
            axis_length = len(self._col_metadata)

        if weights is not None:

            # Index of the weights Series should correspond to the index of the
            # Dataframe in order to sample
            if isinstance(weights, pd.Series):
                weights = weights.reindex(self.axes[axis])

            # If weights arg is a string, the weights used for sampling will
            # the be values in the column corresponding to that string
            if isinstance(weights, string_types):
                if axis == 0:
                    try:
                        weights = self[weights]
                    except KeyError:
                        raise KeyError("String passed to weights not a "
                                       "valid column")
                else:
                    raise ValueError("Strings can only be passed to "
                                     "weights when sampling from rows on "
                                     "a DataFrame")

            weights = pd.Series(weights, dtype='float64')

            if len(weights) != axis_length:
                raise ValueError("Weights and axis to be sampled must be of "
                                 "same length")

            if (weights == np.inf).any() or (weights == -np.inf).any():
                raise ValueError("weight vector may not include `inf` values")

            if (weights < 0).any():
                raise ValueError("weight vector many not include negative "
                                 "values")

            # weights cannot be NaN when sampling, so we must set all nan
            # values to 0
            weights = weights.fillna(0)

            # If passed in weights are not equal to 1, renormalize them
            # otherwise numpy sampling function will error
            weights_sum = weights.sum()
            if weights_sum != 1:
                if weights_sum != 0:
                    weights = weights / weights_sum
                else:
                    raise ValueError("Invalid weights: weights sum to zero")

            weights = weights.values

        if n is None and frac is None:
            # default to n = 1 if n and frac are both None (in accordance with
            # Pandas specification)
            n = 1
        elif n is not None and frac is None and n % 1 != 0:
            # n must be an integer
            raise ValueError("Only integers accepted as `n` values")
        elif n is None and frac is not None:
            # compute the number of samples based on frac
            n = int(round(frac * axis_length))
        elif n is not None and frac is not None:
            # Pandas specification does not allow both n and frac to be passed
            # in
            raise ValueError('Please enter a value for `frac` OR `n`, not '
                             'both')
        if n < 0:
            raise ValueError("A negative number of rows requested. Please "
                             "provide positive value.")

        if n == 0:
            # An Empty DataFrame is returned if the number of samples is 0.
            # The Empty Dataframe should have either columns or index specified
            # depending on which axis is passed in.
            return DataFrame(columns=[] if axis == 1 else self.columns,
                             index=self.index if axis == 1 else [])

        if axis == 1:
            axis_labels = self.columns
            partition_metadata = self._col_metadata
            partitions = self._col_partitions
        else:
            axis_labels = self.index
            partition_metadata = self._row_metadata
            partitions = self._row_partitions

        if random_state is not None:
            # Get a random number generator depending on the type of
            # random_state that is passed in
            if isinstance(random_state, int):
                random_num_gen = np.random.RandomState(random_state)
            elif isinstance(random_state, np.random.randomState):
                random_num_gen = random_state
            else:
                # random_state must be an int or a numpy RandomState object
                raise ValueError("Please enter an `int` OR a "
                                 "np.random.RandomState for random_state")

            # choose random numbers and then get corresponding labels from
            # chosen axis
            sample_indices = random_num_gen.randint(
                                low=0,
                                high=len(partition_metadata),
                                size=n)
            samples = axis_labels[sample_indices]
        else:
            # randomly select labels from chosen axis
            samples = np.random.choice(a=axis_labels, size=n,
                                       replace=replace, p=weights)

        # create an array of (partition, index_within_partition) tuples for
        # each sample
        part_ind_tuples = [partition_metadata[sample]
                           for sample in samples]

        if axis == 1:
            # tup[0] refers to the partition number and tup[1] is the index
            # within that partition
            new_cols = [_deploy_func.remote(lambda df: df.iloc[:, [tup[1]]],
                        partitions[tup[0]]) for tup in part_ind_tuples]
            return DataFrame(col_partitions=new_cols,
                             columns=samples,
                             index=self.index)
        else:
            new_rows = [_deploy_func.remote(lambda df: df.loc[[tup[1]]],
                        partitions[tup[0]]) for tup in part_ind_tuples]
            return DataFrame(row_partitions=new_rows,
                             columns=self.columns,
                             index=samples)

    def select(self, crit, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def select_dtypes(self, include=None, exclude=None):
        # Validates arguments for whether both include and exclude are None or
        # if they are disjoint. Also invalidates string dtypes.
        pd.DataFrame().select_dtypes(include, exclude)

        if include and not is_list_like(include):
            include = [include]
        elif not include:
            include = []

        if exclude and not is_list_like(exclude):
            exclude = [exclude]
        elif not exclude:
            exclude = []

        sel = tuple(map(set, (include, exclude)))

        include, exclude = map(
            lambda x: set(map(_get_dtype_from_object, x)), sel)

        include_these = pd.Series(not bool(include), index=self.columns)
        exclude_these = pd.Series(not bool(exclude), index=self.columns)

        def is_dtype_instance_mapper(column, dtype):
            return column, functools.partial(issubclass, dtype.type)

        for column, f in itertools.starmap(is_dtype_instance_mapper,
                                           self.dtypes.iteritems()):
            if include:  # checks for the case of empty include or exclude
                include_these[column] = any(map(f, include))
            if exclude:
                exclude_these[column] = not any(map(f, exclude))

        dtype_indexer = include_these & exclude_these
        indicate = [i for i in range(len(dtype_indexer.values))
                    if not dtype_indexer.values[i]]
        return self.drop(columns=self.columns[indicate], inplace=False)

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
            setattr(self, pd.DataFrame()._get_axis_name(axis), labels)
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
        """Return unbiased skew over requested axis Normalized by N-1

        Args:
            axis : {index (0), columns (1)}
            skipna : boolean, default True
            Exclude NA/null values when computing the result.
            level : int or level name, default None
            numeric_only : boolean, default None

        Returns:
            skew : Series or DataFrame (if level specified)
        """
        def remote_func(df):
            return df.skew(axis=axis, skipna=skipna, level=level,
                           numeric_only=numeric_only, **kwargs)

        return self._arithmetic_helper(remote_func, axis, level)

    def slice_shift(self, periods=1, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def sort_index(self, axis=0, level=None, ascending=True, inplace=False,
                   kind='quicksort', na_position='last', sort_remaining=True,
                   by=None):
        """Sort a DataFrame by one of the indices (columns or index).

        Args:
            axis: The axis to sort over.
            level: The MultiIndex level to sort over.
            ascending: Ascending or descending
            inplace: Whether or not to update this DataFrame inplace.
            kind: How to perform the sort.
            na_position: Where to position NA on the sort.
            sort_remaining: On Multilevel Index sort based on all levels.
            by: (Deprecated) argument to pass to sort_values.

        Returns:
            A sorted DataFrame
        """
        if level is not None:
            raise NotImplementedError("Multilevel index not yet implemented.")

        if by is not None:
            warnings.warn("by argument to sort_index is deprecated, "
                          "please use .sort_values(by=...)",
                          FutureWarning, stacklevel=2)
            if level is not None:
                raise ValueError("unable to simultaneously sort by and level")
            return self.sort_values(by, axis=axis, ascending=ascending,
                                    inplace=inplace)

        axis = pd.DataFrame()._get_axis_number(axis)

        args = (axis, level, ascending, False, kind, na_position,
                sort_remaining)

        def _sort_helper(df, index, axis, *args):
            if axis == 0:
                df.index = index
            else:
                df.columns = index

            result = df.sort_index(*args)
            df.reset_index(drop=True, inplace=True)
            df.columns = pd.RangeIndex(len(df.columns))
            return result

        if axis == 0:
            index = self.index
            new_column_parts = _map_partitions(
                lambda df: _sort_helper(df, index, axis, *args),
                self._col_partitions)

            new_columns = self.columns
            new_index = self.index.sort_values(ascending=ascending)
            new_row_parts = None
        else:
            columns = self.columns
            new_row_parts = _map_partitions(
                lambda df: _sort_helper(df, columns, axis, *args),
                self._row_partitions)

            new_columns = self.columns.sort_values(ascending=ascending)
            new_index = self.index
            new_column_parts = None

        if not inplace:
            return DataFrame(col_partitions=new_column_parts,
                             row_partitions=new_row_parts,
                             index=new_index,
                             columns=new_columns)
        else:
            self._update_inplace(row_partitions=new_row_parts,
                                 col_partitions=new_column_parts,
                                 columns=new_columns,
                                 index=new_index)

    def sort_values(self, by, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        """Sorts by a column/row or list of columns/rows.

        Args:
            by: A list of labels for the axis to sort over.
            axis: The axis to sort.
            ascending: Sort in ascending or descending order.
            inplace: If true, do the operation inplace.
            kind: How to sort.
            na_position: Where to put np.nan values.

        Returns:
             A sorted DataFrame.
        """

        axis = pd.DataFrame()._get_axis_number(axis)

        if not is_list_like(by):
            by = [by]

        if axis == 0:
            broadcast_value_dict = {str(col): self[col] for col in by}
            broadcast_values = pd.DataFrame(broadcast_value_dict)
        else:
            broadcast_value_list = [to_pandas(self[row::len(self.index)])
                                    for row in by]

            index_builder = list(zip(broadcast_value_list, by))

            for row, idx in index_builder:
                row.index = [str(idx)]

            broadcast_values = pd.concat([row for row, idx in index_builder],
                                         copy=False)

        # We are converting the by to string here so that we don't have a
        # collision with the RangeIndex on the inner frame. It is cheap and
        # gaurantees that we sort by the correct column.
        by = [str(col) for col in by]

        args = (by, axis, ascending, False, kind, na_position)

        def _sort_helper(df, broadcast_values, axis, *args):
            """Sorts the data on a partition.

            Args:
                df: The DataFrame to sort.
                broadcast_values: The by DataFrame to use for the sort.
                axis: The axis to sort over.
                args: The args for the sort.

            Returns:
                 A new sorted DataFrame.
            """
            if axis == 0:
                broadcast_values.index = df.index
                names = broadcast_values.columns
            else:
                broadcast_values.columns = df.columns
                names = broadcast_values.index

            return pd.concat([df, broadcast_values], axis=axis ^ 1,
                             copy=False).sort_values(*args)\
                .drop(names, axis=axis ^ 1)

        if axis == 0:
            new_column_partitions = _map_partitions(
                lambda df: _sort_helper(df, broadcast_values, axis, *args),
                self._col_partitions)

            new_row_partitions = None
            new_columns = self.columns

            # This is important because it allows us to get the axis that we
            # aren't sorting over. We need the order of the columns/rows and
            # this will provide that in the return value.
            new_index = broadcast_values.sort_values(*args).index
        else:
            new_row_partitions = _map_partitions(
                lambda df: _sort_helper(df, broadcast_values, axis, *args),
                self._row_partitions)

            new_column_partitions = None
            new_columns = broadcast_values.sort_values(*args).columns
            new_index = self.index

        if inplace:
            self._update_inplace(row_partitions=new_row_partitions,
                                 col_partitions=new_column_partitions,
                                 columns=new_columns,
                                 index=new_index)
        else:
            return DataFrame(row_partitions=new_row_partitions,
                             col_partitions=new_column_partitions,
                             columns=new_columns,
                             index=new_index,
                             dtypes_cache=self._dtypes_cache)

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
        """Subtract a DataFrame/Series/scalar from this DataFrame.

        Args:
            other: The object to use to apply the subtraction to this.
            axis: THe axis to apply the subtraction over.
            level: Mutlilevel index level to subtract over.
            fill_value: The value to fill NaNs with.

        Returns:
             A new DataFrame with the subtraciont applied.
        """
        return self._operator_helper(pd.DataFrame.sub, other, axis, level,
                                     fill_value)

    def subtract(self, other, axis='columns', level=None, fill_value=None):
        """Alias for sub.

        Args:
            other: The object to use to apply the subtraction to this.
            axis: THe axis to apply the subtraction over.
            level: Mutlilevel index level to subtract over.
            fill_value: The value to fill NaNs with.

        Returns:
             A new DataFrame with the subtraciont applied.
        """
        return self.sub(other, axis, level, fill_value)

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
        if n >= len(self._row_metadata):
            return self

        new_dfs = _map_partitions(lambda df: df.tail(n),
                                  self._col_partitions)

        index = self._row_metadata.index[-n:]
        return DataFrame(col_partitions=new_dfs,
                         col_metadata=self._col_metadata,
                         index=index,
                         dtypes_cache=self._dtypes_cache)

    def take(self, indices, axis=0, convert=None, is_copy=True, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_clipboard(self, excel=None, sep=None, **kwargs):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_clipboard(excel, sep, **kwargs)

    def to_csv(self, path_or_buf=None, sep=",", na_rep="", float_format=None,
               columns=None, header=True, index=True, index_label=None,
               mode="w", encoding=None, compression=None, quoting=None,
               quotechar='"', line_terminator="\n", chunksize=None,
               tupleize_cols=None, date_format=None, doublequote=True,
               escapechar=None, decimal="."):

        kwargs = {
            'path_or_buf': path_or_buf,
            'sep': sep,
            'na_rep': na_rep,
            'float_format': float_format,
            'columns': columns,
            'header': header,
            'index': index,
            'index_label': index_label,
            'mode': mode,
            'encoding': encoding,
            'compression': compression,
            'quoting': quoting,
            'quotechar': quotechar,
            'line_terminator': line_terminator,
            'chunksize': chunksize,
            'tupleize_cols': tupleize_cols,
            'date_format': date_format,
            'doublequote': doublequote,
            'escapechar': escapechar,
            'decimal': decimal
        }

        if compression is not None:
            warnings.warn("Defaulting to Pandas implementation",
                          PendingDeprecationWarning)
            return to_pandas(self).to_csv(**kwargs)

        if tupleize_cols is not None:
            warnings.warn("The 'tupleize_cols' parameter is deprecated and "
                          "will be removed in a future version",
                          FutureWarning, stacklevel=2)
        else:
            tupleize_cols = False

        remote_kwargs_id = ray.put(dict(kwargs, path_or_buf=None))
        columns_id = ray.put(self.columns)

        def get_csv_str(df, index, columns, header, kwargs):
            df.index = index
            df.columns = columns
            kwargs["header"] = header
            return df.to_csv(**kwargs)

        idxs = [0] + np.cumsum(self._row_metadata._lengths).tolist()
        idx_args = [self.index[idxs[i]:idxs[i+1]]
                    for i in range(len(self._row_partitions))]
        csv_str_ids = _map_partitions(
                get_csv_str, self._row_partitions, idx_args,
                [columns_id] * len(self._row_partitions),
                [header] + [False] * (len(self._row_partitions) - 1),
                [remote_kwargs_id] * len(self._row_partitions))

        if path_or_buf is None:
            buf = io.StringIO()
        elif isinstance(path_or_buf, str):
            buf = open(path_or_buf, mode)
        else:
            buf = path_or_buf

        for csv_str_id in csv_str_ids:
            buf.write(ray.get(csv_str_id))
            buf.flush()

        result = None
        if path_or_buf is None:
            result = buf.getvalue()
            buf.close()
        elif isinstance(path_or_buf, str):
            buf.close()
        return result

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

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_excel(excel_writer, sheet_name, na_rep,
                            float_format, columns, header, index,
                            index_label, startrow, startcol, engine,
                            merge_cells, encoding, inf_rep, verbose,
                            freeze_panes)

    def to_feather(self, fname):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_feather(fname)

    def to_gbq(self, destination_table, project_id, chunksize=10000,
               verbose=True, reauth=False, if_exists='fail',
               private_key=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_hdf(self, path_or_buf, key, **kwargs):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_hdf(path_or_buf, key, **kwargs)

    def to_html(self, buf=None, columns=None, col_space=None, header=True,
                index=True, na_rep='np.NaN', formatters=None,
                float_format=None, sparsify=None, index_names=True,
                justify=None, bold_rows=True, classes=None, escape=True,
                max_rows=None, max_cols=None, show_dimensions=False,
                notebook=False, decimal='.', border=None):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_html(buf, columns, col_space, header,
                           index, na_rep, formatters,
                           float_format, sparsify, index_names,
                           justify, bold_rows, classes, escape,
                           max_rows, max_cols, show_dimensions,
                           notebook, decimal, border)

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_json(path_or_buf, orient, date_format,
                           double_precision, force_ascii, date_unit,
                           default_handler, lines, compression)

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

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_msgpack(path_or_buf, encoding, **kwargs)

    def to_panel(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_parquet(self, fname, engine='auto', compression='snappy',
                   **kwargs):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_parquet(fname, engine, compression, **kwargs)

    def to_period(self, freq=None, axis=0, copy=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def to_pickle(self, path, compression='infer',
                  protocol=pkl.HIGHEST_PROTOCOL):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_pickle(path, compression, protocol)

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

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_sql(name, con, flavor, schema, if_exists,
                          index, index_label, chunksize, dtype)

    def to_stata(self, fname, convert_dates=None, write_index=True,
                 encoding='latin-1', byteorder=None, time_stamp=None,
                 data_label=None, variable_labels=None):

        warnings.warn("Defaulting to Pandas implementation",
                      PendingDeprecationWarning)

        port_frame = to_pandas(self)
        port_frame.to_stata(fname, convert_dates, write_index,
                            encoding, byteorder, time_stamp,
                            data_label, variable_labels)

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
        kwargs["is_transform"] = True
        result = self.agg(func, *args, **kwargs)
        try:
            result.columns = self.columns
            result.index = self.index
        except ValueError:
            raise ValueError("transforms cannot produce aggregated results")
        return result

    def truediv(self, other, axis='columns', level=None, fill_value=None):
        """Divides this DataFrame against another DataFrame/Series/scalar.

        Args:
            other: The object to use to apply the divide against this.
            axis: The axis to divide over.
            level: The Multilevel index level to apply divide over.
            fill_value: The value to fill NaNs with.

        Returns:
            A new DataFrame with the Divide applied.
        """
        return self._operator_helper(pd.DataFrame.truediv, other, axis, level,
                                     fill_value)

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
        """Modify DataFrame in place using non-NA values from other.

        Args:
            other: DataFrame, or object coercible into a DataFrame
            join: {'left'}, default 'left'
            overwrite: If True then overwrite values for common keys in frame
            filter_func: Can choose to replace values other than NA.
            raise_conflict: If True, will raise an error if the DataFrame and
                other both contain data in the same place.

        Returns:
            None
        """
        if raise_conflict:
            raise NotImplementedError(
                "raise_conflict parameter not yet supported. "
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")

        if not isinstance(other, DataFrame):
            other = DataFrame(other)

        def update_helper(x, y):
            x.update(y, join, overwrite, filter_func, False)
            return x

        self._inter_df_op_helper(update_helper, other, join, 0, None,
                                 inplace=True)

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
        """Replaces values not meeting condition with values in other.

        Args:
            cond: A condition to be met, can be callable, array-like or a
                DataFrame.
            other: A value or DataFrame of values to use for setting this.
            inplace: Whether or not to operate inplace.
            axis: The axis to apply over. Only valid when a Series is passed
                as other.
            level: The MultiLevel index level to apply over.
            errors: Whether or not to raise errors. Does nothing in Pandas.
            try_cast: Try to cast the result back to the input type.
            raise_on_error: Whether to raise invalid datatypes (deprecated).

        Returns:
            A new DataFrame with the replaced values.
        """

        inplace = validate_bool_kwarg(inplace, 'inplace')

        if isinstance(other, pd.Series) and axis is None:
            raise ValueError("Must specify axis=0 or 1")

        if level is not None:
            raise NotImplementedError("Multilevel Index not yet supported on "
                                      "Pandas on Ray.")

        axis = pd.DataFrame()._get_axis_number(axis) if axis is not None else 0

        cond = cond(self) if callable(cond) else cond

        if not isinstance(cond, DataFrame):
            if not hasattr(cond, 'shape'):
                cond = np.asanyarray(cond)
            if cond.shape != self.shape:
                raise ValueError("Array conditional must be same shape as "
                                 "self")
            cond = DataFrame(cond, index=self.index, columns=self.columns)

        zipped_partitions = self._copartition(cond, self.index)
        args = (False, axis, level, errors, try_cast, raise_on_error)

        if isinstance(other, DataFrame):
            other_zipped = (v for k, v in self._copartition(other,
                                                            self.index))

            new_partitions = [_where_helper.remote(k, v, next(other_zipped),
                                                   self.columns, cond.columns,
                                                   other.columns, *args)
                              for k, v in zipped_partitions]

        # Series has to be treated specially because we're operating on row
        # partitions from here on.
        elif isinstance(other, pd.Series):
            if axis == 0:
                # Pandas determines which index to use based on axis.
                other = other.reindex(self.index)
                other.index = pd.RangeIndex(len(other))

                # Since we're working on row partitions, we have to partition
                # the Series based on the partitioning of self (since both
                # self and cond are co-partitioned by self.
                other_builder = []
                for length in self._row_metadata._lengths:
                    other_builder.append(other[:length])
                    other = other[length:]
                    # Resetting the index here ensures that we apply each part
                    # to the correct row within the partitions.
                    other.index = pd.RangeIndex(len(other))

                other = (obj for obj in other_builder)

                new_partitions = [_where_helper.remote(k, v, next(other,
                                                                  pd.Series()),
                                                       self.columns,
                                                       cond.columns,
                                                       None, *args)
                                  for k, v in zipped_partitions]
            else:
                other = other.reindex(self.columns)
                new_partitions = [_where_helper.remote(k, v, other,
                                                       self.columns,
                                                       cond.columns,
                                                       None, *args)
                                  for k, v in zipped_partitions]

        else:
            new_partitions = [_where_helper.remote(k, v, other, self.columns,
                                                   cond.columns, None, *args)
                              for k, v in zipped_partitions]

        if inplace:
            self._update_inplace(row_partitions=new_partitions,
                                 row_metadata=self._row_metadata,
                                 col_metadata=self._col_metadata)
        else:
            return DataFrame(row_partitions=new_partitions,
                             row_metadata=self._row_metadata,
                             col_metadata=self._col_metadata)

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
        indexer = self._row_metadata.convert_to_index_sliceable(key)
        if indexer is not None:
            return self._getitem_slice(indexer)

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
        # may result in multiple columns?
        partition = self._col_metadata[key, 'partition']
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
            columns = self._col_metadata[key].index
            column_indices = {item: i for i, item in enumerate(self.columns)}
            indices_for_rows = [column_indices[column] for column in columns]

            def get_columns_partition(df):
                result = df.__getitem__(indices_for_rows),
                result.columns = pd.RangeIndex(0, len(result.columns))
                return result

            new_parts = [_deploy_func.remote(
                lambda df: df.__getitem__(indices_for_rows),
                part) for part in self._row_partitions]

            index = self.index

            return DataFrame(row_partitions=new_parts,
                             columns=columns,
                             index=index)

    def _getitem_indiv_col(self, key, part):
        loc = self._col_metadata[key]
        if isinstance(loc, pd.Series):
            index = loc[loc['partition'] == part]
        else:
            index = loc[loc['partition'] == part]['index_within_partition']
        return _deploy_func.remote(
            lambda df: df.__getitem__(index),
            self._col_partitions[part])

    def _getitem_slice(self, key):
        new_cols = _map_partitions(lambda df: df[key],
                                   self._col_partitions)

        index = self.index[key]
        return DataFrame(col_partitions=new_cols,
                         col_metadata=self._col_metadata,
                         index=index)

    def __getattr__(self, key):
        """After regular attribute access, looks up the name in the columns

        Args:
            key (str): Attribute name.

        Returns:
            The value of the attribute.
        """
        try:
            return object.__getattribute__(self, key)
        except AttributeError as e:
            if key in self.columns:
                return self[key]
            raise e

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise NotImplementedError(
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")
        if key not in self.columns:
            self.insert(loc=len(self.columns), column=key, value=value)
        else:
            loc = self.columns.get_loc(key)
            self.__delitem__(key)
            self.insert(loc=loc, column=key, value=value)

    def __len__(self):
        """Gets the length of the dataframe.

        Returns:
            Returns an integer length of the dataframe object.
        """
        return len(self._row_metadata)

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
        # TODO: This is very inefficient and needs fix, also see as_matrix
        return to_pandas(self).__array__(dtype=dtype)

    def __array_wrap__(self, result, context=None):
        # TODO: This is very inefficient, see also __array__ and as_matrix
        return to_pandas(self).__array_wrap__(result, context=context)

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

        # This structure is used to get the correct index inside the partition.
        del_df = self._col_metadata[key]

        # We need to standardize between multiple and single occurrences in the
        # columns. Putting single occurrences in a pd.DataFrame and transposing
        # results in the same structure as multiple with 'loc'.
        if isinstance(del_df, pd.Series):
            del_df = pd.DataFrame(del_df).T

        # Cast cols as pd.Series as duplicate columns mean result may be
        # np.int64 or pd.Series
        col_parts_to_del = \
            pd.Series(del_df['partition'].copy()).unique()
        self._col_metadata.drop(key)

        for i in col_parts_to_del:
            # Compute the correct index inside the partition to delete.
            to_delete_in_partition = \
                del_df[del_df['partition'] == i]['index_within_partition']

            for j in range(self._block_partitions.shape[0]):
                self._block_partitions[j, i] = _deploy_func.remote(
                    del_helper, self._block_partitions[j, i],
                    to_delete_in_partition)

        self._col_metadata.reset_partition_coords(col_parts_to_del)

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
        return self.lt(other)

    def __le__(self, other):
        return self.le(other)

    def __gt__(self, other):
        return self.gt(other)

    def __ge__(self, other):
        return self.ge(other)

    def __eq__(self, other):
        return self.eq(other)

    def __ne__(self, other):
        return self.ne(other)

    def __add__(self, other):
        return self.add(other)

    def __iadd__(self, other):
        return self.add(other)

    def __radd__(self, other, axis="columns", level=None, fill_value=None):
        return self.radd(other, axis, level, fill_value)

    def __mul__(self, other):
        return self.mul(other)

    def __imul__(self, other):
        return self.mul(other)

    def __rmul__(self, other, axis="columns", level=None, fill_value=None):
        return self.rmul(other, axis, level, fill_value)

    def __pow__(self, other):
        return self.pow(other)

    def __ipow__(self, other):
        return self.pow(other)

    def __rpow__(self, other, axis="columns", level=None, fill_value=None):
        return self.rpow(other, axis, level, fill_value)

    def __sub__(self, other):
        return self.sub(other)

    def __isub__(self, other):
        return self.sub(other)

    def __rsub__(self, other, axis="columns", level=None, fill_value=None):
        return self.rsub(other, axis, level, fill_value)

    def __floordiv__(self, other):
        return self.floordiv(other)

    def __ifloordiv__(self, other):
        return self.floordiv(other)

    def __rfloordiv__(self, other, axis="columns", level=None,
                      fill_value=None):
        return self.rfloordiv(other, axis, level, fill_value)

    def __truediv__(self, other):
        return self.truediv(other)

    def __itruediv__(self, other):
        return self.truediv(other)

    def __rtruediv__(self, other, axis="columns", level=None, fill_value=None):
        return self.rtruediv(other, axis, level, fill_value)

    def __mod__(self, other):
        return self.mod(other)

    def __imod__(self, other):
        return self.mod(other)

    def __rmod__(self, other, axis="columns", level=None, fill_value=None):
        return self.rmod(other, axis, level, fill_value)

    def __div__(self, other, axis="columns", level=None, fill_value=None):
        return self.div(other, axis, level, fill_value)

    def __rdiv__(self, other, axis="columns", level=None, fill_value=None):
        return self.rdiv(other, axis, level, fill_value)

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
                         col_metadata=self._col_metadata,
                         row_metadata=self._row_metadata)

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

    def iat(self, axis=None):
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

    def at(self, axis=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ix(self, axis=None):
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

    def _copartition(self, other, new_index):
        """Colocates the values of other with this for certain operations.

        NOTE: This method uses the indexes of each DataFrame to order them the
            same. This operation does an implicit shuffling of data and zips
            the two DataFrames together to be operated on.

        Args:
            other: The other DataFrame to copartition with.

        Returns:
            Two new sets of partitions, copartitioned and zipped.
        """
        # Put in the object store so they aren't serialized each iteration.
        old_self_index = ray.put(self.index)
        new_index = ray.put(new_index)
        old_other_index = ray.put(other.index)

        new_num_partitions = max(len(self._block_partitions.T),
                                 len(other._block_partitions.T))

        new_partitions_self = \
            np.array([_reindex_helper._submit(
                args=tuple([old_self_index, new_index, 1,
                            new_num_partitions] + block.tolist()),
                num_return_vals=new_num_partitions)
                for block in self._block_partitions.T]).T

        new_partitions_other = \
            np.array([_reindex_helper._submit(
                args=tuple([old_other_index, new_index, 1,
                            new_num_partitions] + block.tolist()),
                num_return_vals=new_num_partitions)
                for block in other._block_partitions.T]).T

        return zip(new_partitions_self, new_partitions_other)

    def _operator_helper(self, func, other, axis, level, *args):
        """Helper method for inter-dataframe and scalar operations"""
        if isinstance(other, DataFrame):
            return self._inter_df_op_helper(
                lambda x, y: func(x, y, axis, level, *args),
                other, "outer", axis, level)
        else:
            return self._single_df_op_helper(
                lambda df: func(df, other, axis, level, *args),
                other, axis, level)

    def _inter_df_op_helper(self, func, other, how, axis, level,
                            inplace=False):
        if level is not None:
            raise NotImplementedError("Mutlilevel index not yet supported "
                                      "in Pandas on Ray")
        axis = pd.DataFrame()._get_axis_number(axis)

        new_column_index = self.columns.join(other.columns, how=how)
        new_index = self.index.join(other.index, how=how)
        copartitions = self._copartition(other, new_index)

        new_blocks = \
            np.array([_co_op_helper._submit(
                args=tuple([func, self.columns, other.columns,
                            len(part[0]), None] +
                      np.concatenate(part).tolist()),
                num_return_vals=len(part[0]))
                for part in copartitions])

        if not inplace:
            # TODO join the Index Metadata objects together for performance.
            return DataFrame(block_partitions=new_blocks,
                             columns=new_column_index,
                             index=new_index)
        else:
            self._update_inplace(block_partitions=new_blocks,
                                 columns=new_column_index,
                                 index=new_index)

    def _single_df_op_helper(self, func, other, axis, level):
        if level is not None:
            raise NotImplementedError("Multilevel index not yet supported "
                                      "in Pandas on Ray")
        axis = pd.DataFrame()._get_axis_number(axis)

        if is_list_like(other):
            new_index = self.index
            new_column_index = self.columns
            new_col_metadata = self._col_metadata
            new_row_metadata = self._row_metadata
            new_blocks = None

            if axis == 0:
                if len(other) != len(self.index):
                    raise ValueError(
                        "Unable to coerce to Series, length must be {0}: "
                        "given {1}".format(len(self.index), len(other)))
                new_columns = _map_partitions(func, self._col_partitions)
                new_rows = None
            else:
                if len(other) != len(self.columns):
                    raise ValueError(
                        "Unable to coerce to Series, length must be {0}: "
                        "given {1}".format(len(self.columns), len(other)))
                new_rows = _map_partitions(func, self._row_partitions)
                new_columns = None

        else:
            new_blocks = np.array([_map_partitions(func, block)
                                   for block in self._block_partitions])
            new_columns = None
            new_rows = None
            new_index = self.index
            new_column_index = self.columns
            new_col_metadata = self._col_metadata
            new_row_metadata = self._row_metadata

        return DataFrame(col_partitions=new_columns,
                         row_partitions=new_rows,
                         block_partitions=new_blocks,
                         index=new_index,
                         columns=new_column_index,
                         col_metadata=new_col_metadata,
                         row_metadata=new_row_metadata)


@ray.remote
def _merge_columns(left_columns, right_columns, *args):
    """Merge two columns to get the correct column names and order.

    Args:
        left_columns: The columns on the left side of the merge.
        right_columns: The columns on the right side of the merge.
        args: The arguments for the merge.

    Returns:
         The columns for the merge operation.
    """
    return pd.DataFrame(columns=left_columns, index=[0], dtype='uint8').merge(
        pd.DataFrame(columns=right_columns, index=[0], dtype='uint8'),
        *args).columns


@ray.remote
def _where_helper(left, cond, other, left_columns, cond_columns,
                  other_columns, *args):

    left = pd.concat(ray.get(left.tolist()), axis=1, copy=False)
    # We have to reset the index and columns here because we are coming
    # from blocks and the axes are set according to the blocks. We have
    # already correctly copartitioned everything, so there's no
    # correctness problems with doing this.
    left.reset_index(inplace=True, drop=True)
    left.columns = left_columns

    cond = pd.concat(ray.get(cond.tolist()), axis=1, copy=False)
    cond.reset_index(inplace=True, drop=True)
    cond.columns = cond_columns

    if isinstance(other, np.ndarray):
        other = pd.concat(ray.get(other.tolist()), axis=1, copy=False)
        other.reset_index(inplace=True, drop=True)
        other.columns = other_columns

    return left.where(cond, other, *args)


@ray.remote
def reindex_helper(old_index, new_index, axis, npartitions, method, fill_value,
                   limit, tolerance, *df):
    df = pd.concat(df, axis=axis ^ 1, copy=False)
    if axis == 1:
        df.index = old_index
    else:
        df.columns = old_index

    df = df.reindex(new_index, copy=False, axis=axis ^ 1,
                    method=method, fill_value=fill_value,
                    limit=limit, tolerance=tolerance)
    return create_blocks_helper(df, npartitions, axis)


@ray.remote
def _equals_helper(left, right):
    right = pd.concat(ray.get(right.tolist()), axis=1, copy=False)
    left = pd.concat(ray.get(left.tolist()), axis=1, copy=False)
    # Since we know that the index and columns match, we can just check the
    # values. We can't use np.array_equal here because it doesn't recognize
    # np.nan as equal to another np.nan
    try:
        assert_equal(left.values, right.values)
    except AssertionError:
        return False
    return True
