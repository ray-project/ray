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

    def __init__(self, df,
                 by=None, axis=0, level=None, as_index=True, sort=True,
                 group_keys=True, squeeze=False, **kwargs):

        index = df.index
        index_grouped = pd.Series(index).groupby(by=by, sort=sort)
        keys_and_values = [(k, v) for k, v in index_grouped]

        grouped_partitions = np.array(
            [groupby._submit(args=(part,
                                   by,
                                   axis,
                                   level,
                                   as_index,
                                   sort,
                                   group_keys,
                                   squeeze),
                             num_return_vals=len(keys_and_values))
             for part in df._col_partitions]).T

        from .dataframe import DataFrame

        self._iter = [(keys_and_values[i][0],
                       DataFrame(col_partitions=z[i].tolist(),
                                 columns=df.columns,
                                 index=keys_and_values[i][1].index))
                      for i in range(len(grouped_partitions))]

    def _map_partitions(self, func, index=None):
        """Apply a function on each partition.

        Args:
            func (callable): The function to Apply.

        Returns:
            A new DataFrame containing the result of the function.
        """
        from .dataframe import DataFrame

        # for k, obj in self.p:
        #     z = _map_partitions(func, obj._col_partitions)
        #

        # return DataFrame(col_partitions=new_df, index=index,
        #                  columns=columns)

    @property
    def ngroups(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def skew(self):
        raise NotImplementedError("Not Yet implemented.")

    def ffill(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def sem(self, ddof=1):
        raise NotImplementedError("Not Yet implemented.")

    def mean(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def any(self):
        raise NotImplementedError("Not Yet implemented.")

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
        raise NotImplementedError("Not Yet implemented.")

    @property
    def idxmax(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def ndim(self):
        raise NotImplementedError("Not Yet implemented.")

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def nth(self, n, dropna=None):
        raise NotImplementedError("Not Yet implemented.")

    def cumsum(self, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def indices(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def pct_change(self):
        raise NotImplementedError("Not Yet implemented.")

    def filter(self, func, dropna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cummax(self, axis=0, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def apply(self, func, *args, **kwargs):
        return self._map_partitions(func)

    def rolling(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def dtypes(self):
        raise NotImplementedError("Not Yet implemented.")

    def first(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def backfill(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def __getitem__(self, key):
        raise NotImplementedError("Not Yet implemented.")

    def cummin(self, axis=0, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def bfill(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def idxmin(self):
        raise NotImplementedError("Not Yet implemented.")

    def prod(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def std(self, ddof=1, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def aggregate(self, arg, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def last(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def mad(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def rank(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def corrwith(self):
        raise NotImplementedError("Not Yet implemented.")

    def pad(self, limit=None):
        raise NotImplementedError("Not Yet implemented.")

    def max(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def var(self, ddof=1, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def get_group(self, name, obj=None):
        raise NotImplementedError("Not Yet implemented.")

    def __len__(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def all(self):
        raise NotImplementedError("Not Yet implemented.")

    def size(self):
        raise NotImplementedError("Not Yet implemented.")

    def sum(self, **kwargs):
        self._map_partitions(lambda df: df.sum())

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
        raise NotImplementedError("Not Yet implemented.")

    def resample(self, rule, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def median(self, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def head(self, n=5):
        raise NotImplementedError("Not Yet implemented.")

    def cumprod(self, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def __iter__(self):
        return self._iter.__iter__()

    def agg(self, arg, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def cov(self):
        raise NotImplementedError("Not Yet implemented.")

    def transform(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def corr(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def fillna(self):
        raise NotImplementedError("Not Yet implemented.")

    def count(self):
        raise NotImplementedError("Not Yet implemented.")

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
