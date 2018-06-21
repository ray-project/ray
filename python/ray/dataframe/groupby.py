from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas
import numpy as np
import pandas.core.groupby
from pandas.core.dtypes.common import is_list_like
import pandas.core.common as com

import ray

from .utils import _inherit_docstrings, _reindex_helper
from .concat import concat
from .index_metadata import _IndexMetadata


@_inherit_docstrings(pandas.core.groupby.DataFrameGroupBy,
                     excluded=[pandas.core.groupby.DataFrameGroupBy,
                               pandas.core.groupby.DataFrameGroupBy.__init__])
class DataFrameGroupBy(object):

    def __init__(self, df, by, axis, level, as_index, sort, group_keys,
                 squeeze, **kwargs):

        self._columns = df.columns
        self._index = df.index
        self._axis = axis

        self._row_metadata = df._row_metadata
        self._col_metadata = df._col_metadata

        if axis == 0:
            partitions = [column for column in df._block_partitions.T]
            self._index_grouped = \
                pandas.Series(self._index, index=self._index) \
                .groupby(by=by, sort=sort)
        else:
            partitions = [row for row in df._block_partitions]
            self._index_grouped = \
                pandas.Series(self._columns, index=self._columns) \
                .groupby(by=by, sort=sort)

        self._keys_and_values = [(k, v)
                                 for k, v in self._index_grouped]

        if len(self) > 1:
            self._grouped_partitions = \
                list(zip(*(groupby._submit(args=(by,
                                                 axis,
                                                 level,
                                                 as_index,
                                                 sort,
                                                 group_keys,
                                                 squeeze)
                                           + tuple(part.tolist()),
                                           num_return_vals=len(self))
                           for part in partitions)))
        else:
            if axis == 0:
                self._grouped_partitions = [df._col_partitions]
            else:
                self._grouped_partitions = [df._row_partitions]

    def __getattr__(self, key):
        """Afer regular attribute access, looks up the name in the columns

        Args:
            key (str): Attribute name.

        Returns:
            The value of the attribute.
        """
        try:
            return object.__getattribute__(self, key)
        except AttributeError as e:
            if key in self._columns:
                raise NotImplementedError(
                    "SeriesGroupBy is not implemented."
                    "To contribute to Pandas on Ray, please visit "
                    "github.com/ray-project/ray.")
            raise e

    @property
    def _iter(self):
        from .dataframe import DataFrame

        if self._axis == 0:
            return [(self._keys_and_values[i][0],
                     DataFrame(col_partitions=part,
                               columns=self._columns,
                               index=self._keys_and_values[i][1].index,
                               col_metadata=self._col_metadata))
                    for i, part in enumerate(self._grouped_partitions)]
        else:
            return [(self._keys_and_values[i][0],
                     DataFrame(row_partitions=part,
                               columns=self._keys_and_values[i][1].index,
                               index=self._index,
                               row_metadata=self._row_metadata))
                    for i, part in enumerate(self._grouped_partitions)]

    @property
    def ngroups(self):
        return len(self)

    def skew(self, **kwargs):
        return self._apply_agg_function(lambda df: df.skew(axis=self._axis,
                                                           **kwargs))

    def ffill(self, limit=None):
        return self._apply_df_function(lambda df: df.ffill(axis=self._axis,
                                                           limit=limit))

    def sem(self, ddof=1):
        return self._apply_agg_function(lambda df: df.sem(axis=self._axis,
                                                          ddof=ddof))

    def mean(self, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.mean(axis=self._axis,
                                                           *args,
                                                           **kwargs))

    def any(self):
        return self._apply_agg_function(lambda df: df.any(axis=self._axis))

    @property
    def plot(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ohlc(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __bytes__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def tshift(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    @property
    def groups(self):
        return {k: pandas.Index(v) for k, v in self._keys_and_values}

    def min(self, **kwargs):
        return self._apply_agg_function(lambda df: df.min(axis=self._axis,
                                                          **kwargs))

    def idxmax(self):
        def idxmax_helper(df, index):
            result = df.idxmax(axis=self._axis)
            result = result.apply(lambda v: index[v])
            return result

        results = [idxmax_helper(g[1], i[1])
                   for g, i in zip(self._iter, self._index_grouped)]

        new_df = concat(results, axis=1)
        if self._axis == 0:
            new_df = new_df.T
            new_df.columns = self._columns
            new_df.index = [k for k, v in self._iter]
        else:
            new_df.columns = [k for k, v in self._iter]
            new_df.index = self._index
        return new_df

    @property
    def ndim(self):
        return 2  # ndim is always 2 for DataFrames

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def nth(self, n, dropna=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cumsum(self, axis=0, *args, **kwargs):
        return self._apply_df_function(lambda df: df.cumsum(axis,
                                                            *args,
                                                            **kwargs))

    @property
    def indices(self):
        return dict(self._keys_and_values)

    def pct_change(self):
        return self._apply_agg_function(
            lambda df: df.pct_change(axis=self._axis))

    def filter(self, func, dropna=True, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cummax(self, axis=0, **kwargs):
        return self._apply_df_function(lambda df: df.cummax(axis,
                                                            **kwargs))

    def apply(self, func, *args, **kwargs):
        def apply_helper(df):
            return df.apply(func, axis=self._axis, *args, **kwargs)

        result = [func(v) for k, v in self._iter]
        if self._axis == 0:
            if isinstance(result[0], pandas.Series):
                # Applied an aggregation function
                new_df = concat(result, axis=1).T
                new_df.columns = self._columns
                new_df.index = [k for k, v in self._iter]
            else:
                new_df = concat(result, axis=self._axis)
                new_df._block_partitions = np.array([_reindex_helper._submit(
                    args=tuple([new_df.index, self._index, self._axis ^ 1,
                                len(new_df._block_partitions)]
                               + block.tolist()),
                    num_return_vals=len(new_df._block_partitions))
                    for block in new_df._block_partitions.T]).T
                new_df.index = self._index
                new_df._row_metadata = \
                    _IndexMetadata(new_df._block_partitions[:, 0],
                                   index=new_df.index, axis=0)
        else:
            if isinstance(result[0], pandas.Series):
                # Applied an aggregation function
                new_df = concat(result, axis=1)
                new_df.columns = [k for k, v in self._iter]
                new_df.index = self._index
            else:
                new_df = concat(result, axis=self._axis)
                new_df._block_partitions = np.array([_reindex_helper._submit(
                    args=tuple([new_df.columns, self._columns, self._axis ^ 1,
                                new_df._block_partitions.shape[1]]
                               + block.tolist()),
                    num_return_vals=new_df._block_partitions.shape[1])
                    for block in new_df._block_partitions])
                new_df.columns = self._columns
                new_df._col_metadata = \
                    _IndexMetadata(new_df._block_partitions[0, :],
                                   index=new_df.columns, axis=1)
        return new_df

    @property
    def dtypes(self):
        if self._axis == 1:
            raise ValueError("Cannot call dtypes on groupby with axis=1")
        return self._apply_agg_function(lambda df: df.dtypes)

    def first(self, **kwargs):
        return self._apply_agg_function(lambda df: df.first(offset=0,
                                                            **kwargs))

    def backfill(self, limit=None):
        return self.bfill(limit)

    def __getitem__(self, key):
        # This operation requires a SeriesGroupBy Object
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def cummin(self, axis=0, **kwargs):
        return self._apply_df_function(lambda df: df.cummin(axis=axis,
                                                            **kwargs))

    def bfill(self, limit=None):
        return self._apply_df_function(lambda df: df.bfill(axis=self._axis,
                                                           limit=limit))

    def idxmin(self):
        def idxmin_helper(df, index):
            result = df.idxmin(axis=self._axis)
            result = result.apply(lambda v: index[v])
            return result

        results = [idxmin_helper(g[1], i[1])
                   for g, i in zip(self._iter, self._index_grouped)]

        new_df = concat(results, axis=1)
        if self._axis == 0:
            new_df = new_df.T
            new_df.columns = self._columns
            new_df.index = [k for k, v in self._iter]
        else:
            new_df.columns = [k for k, v in self._iter]
            new_df.index = self._index
        return new_df

    def prod(self, **kwargs):
        return self._apply_agg_function(lambda df: df.prod(axis=self._axis,
                                                           **kwargs))

    def std(self, ddof=1, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.std(axis=self._axis,
                                                          ddof=ddof,
                                                          *args,
                                                          **kwargs))

    def aggregate(self, arg, *args, **kwargs):
        if self._axis != 0:
            # This is not implemented in pandas,
            # so we throw a different message
            raise NotImplementedError("axis other than 0 is not supported")

        if is_list_like(arg):
            raise NotImplementedError(
                "This requires Multi-level index to be implemented. "
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")
        return self._apply_agg_function(lambda df: df.agg(arg,
                                                          axis=self._axis,
                                                          *args,
                                                          **kwargs))

    def last(self, **kwargs):
        return self._apply_df_function(lambda df: df.last(offset=0,
                                                          **kwargs))

    def mad(self):
        return self._apply_agg_function(lambda df: df.mad())

    def rank(self):
        return self._apply_df_function(lambda df: df.rank(axis=self._axis))

    @property
    def corrwith(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def pad(self, limit=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def max(self, **kwargs):
        return self._apply_agg_function(lambda df: df.max(axis=self._axis,
                                                          **kwargs))

    def var(self, ddof=1, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.var(ddof=ddof,
                                                          axis=self._axis,
                                                          *args,
                                                          **kwargs))

    def get_group(self, name, obj=None):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def __len__(self):
        return len(self._keys_and_values)

    def all(self):
        return self._apply_agg_function(lambda df: df.all())

    def size(self):
        return self._apply_agg_function(lambda df: df.size)

    def sum(self, **kwargs):
        return self._apply_agg_function(lambda df:
                                        df.sum(axis=self._axis, **kwargs))

    def __unicode__(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def describe(self, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def boxplot(self, grouped, subplots=True, column=None, fontsize=None,
                rot=0, grid=True, ax=None, figsize=None, layout=None, **kwds):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def ngroup(self, ascending=True):
        return self._index_grouped.ngroup(ascending)

    def nunique(self, dropna=True):
        return self._apply_agg_function(lambda df: df.nunique(dropna=dropna,
                                                              axis=self._axis))

    def resample(self, rule, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def median(self, **kwargs):
        return self._apply_agg_function(lambda df: df.median(axis=self._axis,
                                                             **kwargs))

    def head(self, n=5):
        result = [v.head(n) for k, v in self._iter]
        new_df = concat(result, axis=self._axis)

        if self._axis == 0:
            index_head = [v[:n] for k, v in self._keys_and_values]
            flattened_index = {i for j in index_head for i in j}
            sorted_index = [i for i in self._index if i in flattened_index]
            new_df._block_partitions = np.array([_reindex_helper._submit(
                args=tuple([new_df.index, sorted_index, 1,
                            len(new_df._block_partitions)] + block.tolist()),
                num_return_vals=len(new_df._block_partitions))
                for block in new_df._block_partitions.T]).T
            new_df.index = sorted_index
            new_df._row_metadata = \
                _IndexMetadata(new_df._block_partitions[:, 0],
                               index=new_df.index, axis=0)

        return new_df

    def cumprod(self, axis=0, *args, **kwargs):
        return self._apply_df_function(lambda df: df.cumprod(axis,
                                                             *args,
                                                             **kwargs))

    def __iter__(self):
        return self._iter.__iter__()

    def agg(self, arg, *args, **kwargs):
        return self.aggregate(arg, *args, **kwargs)

    def cov(self):
        return self._apply_agg_function(lambda df: df.cov())

    def transform(self, func, *args, **kwargs):
        return self._apply_df_function(lambda df: df.transform(func,
                                                               *args,
                                                               **kwargs))

    def corr(self, **kwargs):
        return self._apply_agg_function(lambda df: df.corr(**kwargs))

    def fillna(self, **kwargs):
        return self._apply_df_function(lambda df: df.fillna(axis=self._axis,
                                                            **kwargs))

    def count(self, **kwargs):
        return self._apply_agg_function(lambda df: df.count(self._axis,
                                                            **kwargs))

    def pipe(self, func, *args, **kwargs):
        return com._pipe(self, func, *args, **kwargs)

    def cumcount(self, ascending=True):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def tail(self, n=5):
        result = [v.tail(n) for k, v in self._iter]
        new_df = concat(result, axis=self._axis)

        if self._axis == 0:
            index_tail = [v[-n:] for k, v in self._keys_and_values]
            flattened_index = {i for j in index_tail for i in j}
            sorted_index = [i for i in self._index if i in flattened_index]
            new_df._block_partitions = np.array([_reindex_helper._submit(
                args=tuple([new_df.index, sorted_index, 1,
                            len(new_df._block_partitions)] + block.tolist()),
                num_return_vals=len(new_df._block_partitions))
                for block in new_df._block_partitions.T]).T
            new_df.index = sorted_index
            new_df._row_metadata = \
                _IndexMetadata(new_df._block_partitions[:, 0],
                               index=new_df.index, axis=0)

        return new_df

    # expanding and rolling are unique cases and need to likely be handled
    # separately. They do not appear to be commonly used.
    def expanding(self, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def rolling(self, *args, **kwargs):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def hist(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def quantile(self, q=0.5, **kwargs):
        if is_list_like(q):
            raise NotImplementedError(
                "This requires Multi-level index to be implemented. "
                "To contribute to Pandas on Ray, please visit "
                "github.com/ray-project/ray.")

        return self._apply_agg_function(lambda df: df.quantile(q=q,
                                                               axis=self._axis,
                                                               **kwargs))

    def diff(self):
        raise NotImplementedError(
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    def take(self, **kwargs):
        return self._apply_df_function(lambda df: df.take(**kwargs))

    def _apply_agg_function(self, f):
        assert callable(f), "\'{0}\' object is not callable".format(type(f))

        result = [f(v) for k, v in self._iter]
        new_df = concat(result, axis=1)

        if self._axis == 0:
            new_df = new_df.T
            new_df.columns = self._columns
            new_df.index = [k for k, v in self._iter]
        else:
            new_df.columns = [k for k, v in self._iter]
            new_df.index = self._index
        return new_df

    def _apply_df_function(self, f, concat_axis=None):
        assert callable(f), "\'{0}\' object is not callable".format(type(f))

        result = [f(v) for k, v in self._iter]
        concat_axis = self._axis if concat_axis is None else concat_axis

        new_df = concat(result, axis=concat_axis)

        if self._axis == 0:
            new_df._block_partitions = np.array([_reindex_helper._submit(
                args=tuple([new_df.index, self._index, 1,
                            len(new_df._block_partitions)] + block.tolist()),
                num_return_vals=len(new_df._block_partitions))
                for block in new_df._block_partitions.T]).T
            new_df.index = self._index
            new_df._row_metadata = \
                _IndexMetadata(new_df._block_partitions[:, 0],
                               index=new_df.index, axis=0)
        else:
            new_df._block_partitions = np.array([_reindex_helper._submit(
                args=tuple([new_df.columns, self._columns, 0,
                            new_df._block_partitions.shape[1]]
                           + block.tolist()),
                num_return_vals=new_df._block_partitions.shape[1])
                for block in new_df._block_partitions])
            new_df.columns = self._columns
            new_df._col_metadata = \
                _IndexMetadata(new_df._block_partitions[0, :],
                               index=new_df.columns, axis=1)

        return new_df


@ray.remote
def groupby(by, axis, level, as_index, sort, group_keys, squeeze, *df):

    df = pandas.concat(df, axis=axis)

    return [v for k, v in df.groupby(by=by,
                                     axis=axis,
                                     level=level,
                                     as_index=as_index,
                                     sort=sort,
                                     group_keys=group_keys,
                                     squeeze=squeeze)]
