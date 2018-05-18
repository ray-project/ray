from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas.core.groupby
import pandas as pd
from pandas.core.dtypes.common import is_list_like
import ray

from .utils import _map_partitions
from .utils import _inherit_docstrings
from .dataframe import DataFrame
from .concat import concat


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
            self._index_grouped = pd.Series(self._index, index=self._index)\
                .groupby(by=by, sort=sort)
        else:
            partitions = [row for row in df._block_partitions]
            self._index_grouped = \
                pd.Series(self._columns, index=self._columns) \
                .groupby(by=by, sort=sort)

        self._keys_and_values = [(k, v)
                                 for k, v in self._index_grouped]

        self._grouped_partitions = \
            list(zip(*(groupby._submit(args=(by,
                                             axis,
                                             level,
                                             as_index,
                                             sort,
                                             group_keys,
                                             squeeze) + tuple(part.tolist()),
                                       num_return_vals=len(self))
                       for part in partitions)))

    @property
    def _iter(self):
        from .dataframe import DataFrame

        if self._axis == 0:
            return [(self._keys_and_values[i][0],
                     DataFrame(col_partitions=part,
                               columns=self._columns,
                               index=self._keys_and_values[i][1].index,
                               row_metadata=self._row_metadata[
                                   self._keys_and_values[i][1].index],
                               col_metadata=self._col_metadata))
                    for i, part in enumerate(self._grouped_partitions)]
        else:
            return [(self._keys_and_values[i][0],
                     DataFrame(row_partitions=part,
                               columns=self._keys_and_values[i][1].index,
                               index=self._index,
                               row_metadata=self._row_metadata,
                               col_metadata=self._col_metadata[
                                   self._keys_and_values[i][1].index]))
                    for i, part in enumerate(self._grouped_partitions)]

    @property
    def ngroups(self):
        return len(self)

    def skew(self, **kwargs):
        return self._apply_agg_function(lambda df: df.skew(axis=self._axis,
                                                           **kwargs).T,
                                        concat_axis=0)

    def ffill(self, limit=None):
        return self._apply_df_function(lambda df: df.ffill(axis=self._axis,
                                                           limit=limit))

    def sem(self, ddof=1):
        return self._apply_agg_function(lambda df: df.sem(ddof=ddof))

    def mean(self, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.mean(*args, **kwargs))

    def any(self):
        return self._apply_agg_function(lambda df: df.any())

    @property
    def plot(self):
        raise NotImplementedError("Not Yet implemented.")

    def ohlc(self):
        raise NotImplementedError("Not Yet implemented.")

    def __bytes__(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def tshift(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def groups(self):
        return {k: pd.Index(v) for k, v in self._keys_and_values}

    def min(self, **kwargs):
        return self._apply_agg_function(lambda df: df.min(**kwargs))

    def idxmax(self):
        return self._apply_agg_function(lambda df: df.idxmax())

    @property
    def ndim(self):
        return self._index_grouped.ndim

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def nth(self, n, dropna=None):
        raise NotImplementedError("Not Yet implemented.")

    def cumsum(self, axis=0, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.cumsum(axis,
                                                             *args,
                                                             **kwargs))

    @property
    def indices(self):
        return dict(self._keys_and_values)

    def pct_change(self):
        return self._apply_agg_function(lambda df: df.pct_change())

    def filter(self, func, dropna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cummax(self, axis=0, **kwargs):
        return self._apply_agg_function(lambda df: df.cummax(axis=axis,
                                                             **kwargs))

    def apply(self, func, *args, **kwargs):
        return self._apply_df_function(lambda df: df.apply(func,
                                                           *args,
                                                           **kwargs)) \
            if is_list_like(func) \
            else self._apply_agg_function(lambda df: df.apply(func,
                                                              *args,
                                                              **kwargs))

    @property
    def dtypes(self):
        return self._apply_agg_function(lambda df: df.dtypes)

    def first(self, **kwargs):
        return self._apply_agg_function(lambda df: df.first(offset=0,
                                                            **kwargs))

    def backfill(self, limit=None):
        return self.bfill(limit)

    def __getitem__(self, key):
        # This operation requires a SeriesGroupBy Object
        raise NotImplementedError("Not Yet implemented.")

    def cummin(self, axis=0, **kwargs):
        return self._apply_agg_function(lambda df: df.cummin(axis=axis,
                                                             **kwargs))

    def bfill(self, limit=None):
        return self._apply_agg_function(lambda df: df.bfill(limit=limit))

    def idxmin(self):
        return self._apply_agg_function(lambda df: df.idxmin())

    def prod(self, **kwargs):
        return self._apply_agg_function(lambda df: df.prod(**kwargs))

    def std(self, ddof=1, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.std(ddof=ddof,
                                                          *args, **kwargs))

    def aggregate(self, arg, *args, **kwargs):
        return self._apply_df_function(lambda df: df.agg(arg,
                                                         *args,
                                                         **kwargs)) \
            if is_list_like(arg) \
            else self._apply_agg_function(lambda df: df.agg(arg,
                                                            *args,
                                                            **kwargs))

    def last(self, **kwargs):
        return self._apply_df_function(lambda df: df.last(**kwargs))

    def mad(self):
        return self._apply_agg_function(lambda df: df.mad())

    def rank(self):
        return self._apply_df_function(lambda df: df.rank())

    @property
    def corrwith(self):
        raise NotImplementedError("Not Yet implemented.")

    def pad(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def max(self, **kwargs):
        return self._apply_agg_function(lambda df: df.max(**kwargs))

    def var(self, ddof=1, *args, **kwargs):
        return self._apply_agg_function(lambda df: df.var(ddof,
                                                          *args,
                                                          **kwargs))

    def get_group(self, name, obj=None):
        raise NotImplementedError("Not Yet implemented.")

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
        raise NotImplementedError("Not Yet implemented.")

    def describe(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def boxplot(self, grouped, subplots=True, column=None, fontsize=None,
                rot=0, grid=True, ax=None, figsize=None, layout=None, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def ngroup(self, ascending=True):
        return self._index_grouped.ngroup(ascending)

    def nunique(self, dropna=True):
        return self._apply_agg_function(lambda df: df.nunique(dropna))

    def resample(self, rule, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def median(self, **kwargs):
        return self._apply_agg_function(lambda df: df.median(**kwargs))

    def head(self, n=5):
        return self._apply_df_function(lambda df: df.head(n))

    def cumprod(self, axis=0, *args, **kwargs):
        return self._apply_df_function(lambda df: df.cumprod(axis,
                                                             *args,
                                                             **kwargs))

    def __iter__(self):
        return self._iter.__iter__()

    def agg(self, arg, *args, **kwargs):
        def agg_help(df):
            if isinstance(df, pd.Series):
                return pd.DataFrame(df).T
            else:
                return df
        x = [v.agg(arg, axis=self._axis, *args, **kwargs)
             for k, v in self._iter]

        new_parts = _map_partitions(lambda df: agg_help(df), x)

        from .concat import concat
        result = concat(new_parts)

        return result

    def cov(self):
        return self._apply_agg_function(lambda df: df.cov())

    def transform(self, func, *args, **kwargs):
        from .concat import concat

        new_parts = concat([v.transform(func, *args, **kwargs)
                            for k, v in self._iter])
        return new_parts

    def corr(self, **kwargs):
        return self._apply_agg_function(lambda df: df.corr(**kwargs))

    def fillna(self, **kwargs):
        return self._apply_df_function(lambda df: df.fillna(**kwargs))

    def count(self, **kwargs):
        return self._apply_agg_function(lambda df: df.count(**kwargs))

    def pipe(self, func, *args, **kwargs):
        return self._apply_df_function(lambda df: df.pipe(func,
                                                          *args,
                                                          **kwargs))

    def cumcount(self, ascending=True):
        raise NotImplementedError("Not Yet implemented.")

    def tail(self, n=5):
        return self._apply_df_function(lambda df: df.tail(n))

    # expanding and rolling are unique cases and need to likely be handled
    # separately. They do not appear to be commonly used.
    def expanding(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def rolling(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def hist(self):
        raise NotImplementedError("Not Yet implemented.")

    def quantile(self, q=0.5, **kwargs):
        return self._apply_df_function(lambda df: df.quantile(q, **kwargs)) \
            if is_list_like(q) \
            else self._apply_agg_function(lambda df: df.quantile(q, **kwargs))

    def diff(self):
        raise NotImplementedError("Not Yet implemented.")

    def take(self, **kwargs):
        return self._apply_df_function(lambda df: df.take(**kwargs))

    def _apply_agg_function(self, f, concat_axis=None):
        assert callable(f), "\'{0}\' object is not callable".format(type(f))

        result = [DataFrame(f(v)).T for k, v in self._iter]
        concat_axis = self._axis if concat_axis is None else concat_axis

        new_df = concat(result, axis=concat_axis)
        if self._axis == 0:
            new_df.columns = self._columns
            new_df.index = [k for k, v in self._iter]
        else:
            new_df = new_df.T
            new_df.columns = [k for k, v in self._iter]
            new_df.index = self._index
        return new_df

    def _apply_df_function(self, f, concat_axis=None):
        assert callable(f), "\'{0}\' object is not callable".format(type(f))

        result = [f(v) for k, v in self._iter]
        concat_axis = self._axis if concat_axis is None else concat_axis

        new_df = concat(result, axis=concat_axis)

        if self._axis == 0:
            new_df.reindex(self._index, axis=0, copy=False)
        else:
            new_df.reindex(self._columns, axis=1, copy=False)

        return new_df


@ray.remote
def groupby(by, axis, level, as_index, sort, group_keys, squeeze, *df):

    df = pd.concat(df, axis=axis)

    return [v for k, v in df.groupby(by=by,
                                     axis=axis,
                                     level=level,
                                     as_index=as_index,
                                     sort=sort,
                                     group_keys=group_keys,
                                     squeeze=squeeze)]
