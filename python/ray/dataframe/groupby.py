from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas.core.groupby
import numpy as np
import pandas as pd
import ray

from .utils import _map_partitions
from .utils import _inherit_docstrings


@_inherit_docstrings(pandas.core.groupby.DataFrameGroupBy)
class DataFrameGroupBy(object):

    def __init__(self, df, by, axis, level, as_index, sort, group_keys,
                 squeeze, **kwargs):

        self._columns = df.columns
        self._index = df.index
        self._axis = axis

        self._row_metadata = df._row_metadata
        self._col_metadata = df._col_metadata

        if axis == 0:
            partitions = df._col_partitions
            index_grouped = pd.Series(self._index).groupby(by=by, sort=sort)
        else:
            partitions = df._row_partitions
            index_grouped = pd.Series(self._columns).groupby(by=by, sort=sort)

        self._keys_and_values = [(k, v) for k, v in index_grouped]

        self._grouped_partitions = \
            zip(*(groupby._submit(args=(part,
                                        by,
                                        axis,
                                        level,
                                        as_index,
                                        sort,
                                        group_keys,
                                        squeeze),
                                  num_return_vals=len(self._keys_and_values))
                  for part in partitions))

    @property
    def _iter(self):
        from .dataframe import DataFrame

        if self._axis == 0:
            return ((self._keys_and_values[i][0],
                     DataFrame(col_partitions=part,
                               columns=self._columns,
                               index=self._keys_and_values[i][1].index,
                               row_metadata=self._row_metadata.loc[
                                   self._keys_and_values[i][1].index],
                               col_metadata=self._col_metadata))
                    for i, part in enumerate(self._grouped_partitions))
        else:
            return ((self._keys_and_values[i][0],
                     DataFrame(row_partitions=part,
                               columns=self._keys_and_values[i][1].index,
                               index=self._index))
                    for i, part in enumerate(self._grouped_partitions))

    def _map_partitions(self, func):
        """Apply a function on each partition.

        Args:
            func (callable): The function to Apply.

        Returns:
            A new DataFrame containing the result of the function.
        """
        from .dataframe import DataFrame

        new_parts = np.array([_map_partitions(func, obj._col_partitions)
                              for k, obj in self._iter])

        return DataFrame(block_partitions=new_parts,
                         columns=self._columns)

    @property
    def ngroups(self):
        return len(self._keys_and_values)

    @property
    def skew(self):
        raise NotImplementedError("Not Yet implemented.")

    def ffill(self, limit=None):
        return self._apply_function(lambda df: df.ffill(limit=limit))

    def sem(self, ddof=1):
        return self._apply_function(lambda df: df.sem(ddof=ddof))

    def mean(self, *args, **kwargs):
        return self._apply_function(lambda df: df.mean(*args, **kwargs))

    def any(self):
        return self._apply_function(lambda df: df.any())

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
        raise NotImplementedError("Not Yet implemented.")

    def min(self, **kwargs):
        return self._apply_function(lambda df: df.min())

    def idxmax(self):
        return self._apply_function(lambda df: df.idxmax())

    @property
    def ndim(self):
        raise NotImplementedError("Not Yet implemented.")

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def nth(self, n, dropna=None):
        raise NotImplementedError("Not Yet implemented.")

    def cumsum(self, axis=0, *args, **kwargs):
        return self._apply_function(lambda df: df.cumsum())

    @property
    def indices(self):
        raise NotImplementedError("Not Yet implemented.")

    def pct_change(self):
        return self._apply_function(lambda df: df.pct_change())

    def filter(self, func, dropna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cummax(self, axis=0, **kwargs):
        return self._apply_function(lambda df: df.cummax(axis=axis, **kwargs))

    def apply(self, func, *args, **kwargs):
        return self._map_partitions(func)

    def rolling(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def dtypes(self):
        return self._apply_function(lambda df: df.dtypes)

    def first(self, **kwargs):
        return self._apply_function(lambda df: df.first(offset=0, **kwargs))

    def backfill(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def __getitem__(self, key):
        # This operation requires a SeriesGroupBy Object
        raise NotImplementedError("Not Yet implemented.")

    def cummin(self, axis=0, **kwargs):
        return self._apply_function(lambda df: df.cummin(axis=axis, **kwargs))

    def bfill(self, limit=None):
        return self._apply_function(lambda df: df.bfill(limit=limit))

    def idxmin(self):
        return self._apply_function(lambda df: df.idxmin())

    def prod(self, **kwargs):
        return self._apply_function(lambda df: df.prod(**kwargs))

    def std(self, ddof=1, *args, **kwargs):
        return self._apply_function(lambda df: df.std(ddof=ddof,
                                                      *args, **kwargs))

    def aggregate(self, arg, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def last(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def mad(self):
        return self._apply_function(lambda df: df.mad())

    def rank(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def corrwith(self):
        raise NotImplementedError("Not Yet implemented.")

    def pad(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def max(self, **kwargs):
        return self._apply_function(lambda df: df.max())

    def var(self, ddof=1, *args, **kwargs):
        return self._apply_function(lambda df: df.var())

    def get_group(self, name, obj=None):
        raise NotImplementedError("Not Yet implemented.")

    def __len__(self):
        raise NotImplementedError("Not Yet implemented.")

    def all(self):
        return self._apply_function(lambda df: df.all())

    def size(self):
        return self._apply_function(lambda df: df.size)

    def sum(self, **kwargs):
        return self._apply_function(lambda df:
                                    df.sum(axis=self._axis, **kwargs))

    def __unicode__(self):
        raise NotImplementedError("Not Yet implemented.")

    def describe(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def boxplot(grouped, subplots=True, column=None, fontsize=None, rot=0,
                grid=True, ax=None, figsize=None, layout=None, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def ngroup(self, ascending=True):
        raise NotImplementedError("Not Yet implemented.")

    def nunique(self, dropna=True):
        return self._apply_function(lambda df: df.nunique())

    def resample(self, rule, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def median(self, **kwargs):
        return self._apply_function(lambda df: df.median().astype(int))

    def head(self, n=5):
        raise NotImplementedError("Not Yet implemented.")

    def cumprod(self, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def __iter__(self):
        return self._iter.__iter__()

    def agg(self, arg, *args, **kwargs):
        def agg_help(df):
            if isinstance(df, pd.Series):
                return pd.DataFrame(df).T
            else:
                return df
        x = [v.agg(arg, axis=self._axis, *args, **kwargs) for k, v in self._iter]

        new_parts = _map_partitions(lambda df: agg_help(df), x)

        from .dataframe import DataFrame
        if self._axis == 0:
            return DataFrame(row_partitions=new_parts,
                             columns=self._columns,
                             index=[k for k, v in self._iter])
        else:
            return DataFrame(col_partitions=new_parts,
                             columns=[k for k, v in self._iter],
                             index=self._index)

    def cov(self):
        return self._apply_function(lambda df: df.cov())

    def transform(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def corr(self):
        return self._apply_function(lambda df: df.corr())

    def fillna(self):
        raise NotImplementedError("Not Yet implemented.")

    def count(self):
        return self._apply_function(lambda df: df.count())

    def pipe(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cumcount(self, ascending=True):
        raise NotImplementedError("Not Yet implemented.")

    def tail(self, n=5):
        raise NotImplementedError("Not Yet implemented.")

    def expanding(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def hist(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def quantile(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def diff(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def take(self):
        raise NotImplementedError("Not Yet implemented.")

    def _apply_function(self, f):
        if not callable(f):
            raise ValueError(
                "\'{0}\' object is not callable".format(type(f)))

        result = [pd.DataFrame(f(v)).T
                  for k, v in self._iter]

        new_df = pd.concat(result)
        if self._axis == 0:
            new_df.columns = self._columns
            new_df.index = [k for k, v in self._iter]
        else:
            new_df = new_df.T
            new_df.columns = [k for k, v in self._iter]
            new_df.index = self._index
        return new_df


@ray.remote
def groupby(df, by=None, axis=0, level=None, as_index=True, sort=True,
            group_keys=True, squeeze=False):

    return [v for k, v in df.groupby(by=by,
                                     axis=axis,
                                     level=level,
                                     as_index=as_index,
                                     sort=sort,
                                     group_keys=group_keys,
                                     squeeze=squeeze)]
