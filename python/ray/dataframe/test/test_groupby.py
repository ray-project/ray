from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import sys
import pandas
import numpy as np
import ray.dataframe as pd
from ray.dataframe.utils import (
    from_pandas,
    to_pandas)

PY2 = False
if sys.version_info.major < 3:
    PY2 = True


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    assert isinstance(ray_df, pd.DataFrame)
    assert to_pandas(ray_df).equals(pandas_df)


@pytest.fixture
def ray_df_almost_equals_pandas(ray_df, pandas_df):
    assert isinstance(ray_df, pd.DataFrame)
    difference = to_pandas(ray_df) - pandas_df
    diff_max = difference.max().max()
    assert to_pandas(ray_df).equals(pandas_df) or diff_max < 0.0001


@pytest.fixture
def ray_series_equals_pandas(ray_df, pandas_df):
    assert ray_df.equals(pandas_df)


@pytest.fixture
def ray_df_equals(ray_df1, ray_df2):
    assert to_pandas(ray_df1).equals(to_pandas(ray_df2))


@pytest.fixture
def ray_groupby_equals_pandas(ray_groupby, pandas_groupby):
    for g1, g2 in zip(ray_groupby, pandas_groupby):
        assert g1[0] == g2[0]
        ray_df_equals_pandas(g1[1], g2[1])


def test_simple_row_groupby():
    pandas_df = pandas.DataFrame({'col1': [0, 1, 2, 3],
                                  'col2': [4, 5, 6, 7],
                                  'col3': [3, 8, 12, 10],
                                  'col4': [17, 13, 16, 15],
                                  'col5': [-4, -5, -6, -7]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 2, 1, 2]
    n = 1

    ray_groupby = ray_df.groupby(by=by)
    pandas_groupby = pandas_df.groupby(by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)
    test_sem(ray_groupby, pandas_groupby)
    test_mean(ray_groupby, pandas_groupby)
    test_any(ray_groupby, pandas_groupby)
    test_min(ray_groupby, pandas_groupby)
    test_idxmax(ray_groupby, pandas_groupby)
    test_ndim(ray_groupby, pandas_groupby)
    test_cumsum(ray_groupby, pandas_groupby)
    test_pct_change(ray_groupby, pandas_groupby)
    test_cummax(ray_groupby, pandas_groupby)

    apply_functions = [lambda df: df.sum(), lambda df: -df]
    for func in apply_functions:
        test_apply(ray_groupby, pandas_groupby, func)

    test_dtypes(ray_groupby, pandas_groupby)
    test_first(ray_groupby, pandas_groupby)
    test_backfill(ray_groupby, pandas_groupby)
    test_cummin(ray_groupby, pandas_groupby)
    test_bfill(ray_groupby, pandas_groupby)
    test_idxmin(ray_groupby, pandas_groupby)
    test_prod(ray_groupby, pandas_groupby)
    test_std(ray_groupby, pandas_groupby)

    agg_functions = ['min', 'max']
    for func in agg_functions:
        test_agg(ray_groupby, pandas_groupby, func)
        test_aggregate(ray_groupby, pandas_groupby, func)

    test_last(ray_groupby, pandas_groupby)
    test_mad(ray_groupby, pandas_groupby)
    test_rank(ray_groupby, pandas_groupby)
    test_max(ray_groupby, pandas_groupby)
    test_var(ray_groupby, pandas_groupby)
    test_len(ray_groupby, pandas_groupby)
    test_sum(ray_groupby, pandas_groupby)
    test_ngroup(ray_groupby, pandas_groupby)
    test_nunique(ray_groupby, pandas_groupby)
    test_median(ray_groupby, pandas_groupby)
    test_head(ray_groupby, pandas_groupby, n)
    test_cumprod(ray_groupby, pandas_groupby)
    test_cov(ray_groupby, pandas_groupby)

    transform_functions = [lambda df: df + 4, lambda df: -df - 10]
    for func in transform_functions:
        test_transform(ray_groupby, pandas_groupby, func)

    pipe_functions = [lambda dfgb: dfgb.sum()]
    for func in pipe_functions:
        test_pipe(ray_groupby, pandas_groupby, func)

    test_corr(ray_groupby, pandas_groupby)
    test_fillna(ray_groupby, pandas_groupby)
    test_count(ray_groupby, pandas_groupby)
    test_tail(ray_groupby, pandas_groupby, n)
    test_quantile(ray_groupby, pandas_groupby)
    test_take(ray_groupby, pandas_groupby)


def test_single_group_row_groupby():
    pandas_df = pandas.DataFrame({'col1': [0, 1, 2, 3],
                                  'col2': [4, 5, 36, 7],
                                  'col3': [3, 8, 12, 10],
                                  'col4': [17, 3, 16, 15],
                                  'col5': [-4, 5, -6, -7]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 1, 1, 1]
    n = 6

    ray_groupby = ray_df.groupby(by=by)
    pandas_groupby = pandas_df.groupby(by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)
    test_sem(ray_groupby, pandas_groupby)
    test_mean(ray_groupby, pandas_groupby)
    test_any(ray_groupby, pandas_groupby)
    test_min(ray_groupby, pandas_groupby)
    test_idxmax(ray_groupby, pandas_groupby)
    test_ndim(ray_groupby, pandas_groupby)
    test_cumsum(ray_groupby, pandas_groupby)
    test_pct_change(ray_groupby, pandas_groupby)
    test_cummax(ray_groupby, pandas_groupby)

    apply_functions = [lambda df: df.sum(), lambda df: -df]
    for func in apply_functions:
        test_apply(ray_groupby, pandas_groupby, func)

    test_dtypes(ray_groupby, pandas_groupby)
    test_first(ray_groupby, pandas_groupby)
    test_backfill(ray_groupby, pandas_groupby)
    test_cummin(ray_groupby, pandas_groupby)
    test_bfill(ray_groupby, pandas_groupby)
    test_idxmin(ray_groupby, pandas_groupby)
    test_prod(ray_groupby, pandas_groupby)
    test_std(ray_groupby, pandas_groupby)

    agg_functions = ['min', 'max']
    for func in agg_functions:
        test_agg(ray_groupby, pandas_groupby, func)
        test_aggregate(ray_groupby, pandas_groupby, func)

    test_last(ray_groupby, pandas_groupby)
    test_mad(ray_groupby, pandas_groupby)
    test_rank(ray_groupby, pandas_groupby)
    test_max(ray_groupby, pandas_groupby)
    test_var(ray_groupby, pandas_groupby)
    test_len(ray_groupby, pandas_groupby)
    test_sum(ray_groupby, pandas_groupby)
    test_ngroup(ray_groupby, pandas_groupby)
    test_nunique(ray_groupby, pandas_groupby)
    test_median(ray_groupby, pandas_groupby)
    test_head(ray_groupby, pandas_groupby, n)
    test_cumprod(ray_groupby, pandas_groupby)
    test_cov(ray_groupby, pandas_groupby)

    transform_functions = [lambda df: df + 4, lambda df: -df - 10]
    for func in transform_functions:
        test_transform(ray_groupby, pandas_groupby, func)

    pipe_functions = [lambda dfgb: dfgb.sum()]
    for func in pipe_functions:
        test_pipe(ray_groupby, pandas_groupby, func)

    test_corr(ray_groupby, pandas_groupby)
    test_fillna(ray_groupby, pandas_groupby)
    test_count(ray_groupby, pandas_groupby)
    test_tail(ray_groupby, pandas_groupby, n)
    test_quantile(ray_groupby, pandas_groupby)
    test_take(ray_groupby, pandas_groupby)


def test_large_row_groupby():
    pandas_df = pandas.DataFrame(np.random.randint(0, 8, size=(100, 4)),
                                 columns=list('ABCD'))

    ray_df = from_pandas(pandas_df, 2)

    by = pandas_df['A'].tolist()
    n = 4

    ray_groupby = ray_df.groupby(by=by)
    pandas_groupby = pandas_df.groupby(by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)
    test_sem(ray_groupby, pandas_groupby)
    test_mean(ray_groupby, pandas_groupby)
    test_any(ray_groupby, pandas_groupby)
    test_min(ray_groupby, pandas_groupby)
    test_idxmax(ray_groupby, pandas_groupby)
    test_ndim(ray_groupby, pandas_groupby)
    test_cumsum(ray_groupby, pandas_groupby)
    test_pct_change(ray_groupby, pandas_groupby)
    test_cummax(ray_groupby, pandas_groupby)

    apply_functions = [lambda df: df.sum(), lambda df: -df]
    for func in apply_functions:
        test_apply(ray_groupby, pandas_groupby, func)

    test_dtypes(ray_groupby, pandas_groupby)
    test_first(ray_groupby, pandas_groupby)
    test_backfill(ray_groupby, pandas_groupby)
    test_cummin(ray_groupby, pandas_groupby)
    test_bfill(ray_groupby, pandas_groupby)
    test_idxmin(ray_groupby, pandas_groupby)
    # test_prod(ray_groupby, pandas_groupby) causes overflows
    test_std(ray_groupby, pandas_groupby)

    agg_functions = ['min', 'max']
    for func in agg_functions:
        test_agg(ray_groupby, pandas_groupby, func)
        test_aggregate(ray_groupby, pandas_groupby, func)

    test_last(ray_groupby, pandas_groupby)
    test_mad(ray_groupby, pandas_groupby)
    test_rank(ray_groupby, pandas_groupby)
    test_max(ray_groupby, pandas_groupby)
    test_var(ray_groupby, pandas_groupby)
    test_len(ray_groupby, pandas_groupby)
    test_sum(ray_groupby, pandas_groupby)
    test_ngroup(ray_groupby, pandas_groupby)
    test_nunique(ray_groupby, pandas_groupby)
    test_median(ray_groupby, pandas_groupby)
    test_head(ray_groupby, pandas_groupby, n)
    # test_cumprod(ray_groupby, pandas_groupby) causes overflows
    test_cov(ray_groupby, pandas_groupby)

    transform_functions = [lambda df: df + 4, lambda df: -df - 10]
    for func in transform_functions:
        test_transform(ray_groupby, pandas_groupby, func)

    pipe_functions = [lambda dfgb: dfgb.sum()]
    for func in pipe_functions:
        test_pipe(ray_groupby, pandas_groupby, func)

    test_corr(ray_groupby, pandas_groupby)
    test_fillna(ray_groupby, pandas_groupby)
    test_count(ray_groupby, pandas_groupby)
    test_tail(ray_groupby, pandas_groupby, n)
    test_quantile(ray_groupby, pandas_groupby)
    test_take(ray_groupby, pandas_groupby)


def test_simple_col_groupby():
    pandas_df = pandas.DataFrame({'col1': [0, 3, 2, 3],
                                  'col2': [4, 1, 6, 7],
                                  'col3': [3, 8, 2, 10],
                                  'col4': [1, 13, 6, 15],
                                  'col5': [-4, 5, 6, -7]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 2, 3, 2, 1]

    ray_groupby = ray_df.groupby(axis=1, by=by)
    pandas_groupby = pandas_df.groupby(axis=1, by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)
    test_sem(ray_groupby, pandas_groupby)
    test_mean(ray_groupby, pandas_groupby)
    test_any(ray_groupby, pandas_groupby)
    test_min(ray_groupby, pandas_groupby)
    test_ndim(ray_groupby, pandas_groupby)

    if not PY2:
        # idxmax and idxmin fail on column groupby in pandas with python2
        test_idxmax(ray_groupby, pandas_groupby)
        test_idxmin(ray_groupby, pandas_groupby)
        test_rank(ray_groupby, pandas_groupby)
        test_quantile(ray_groupby, pandas_groupby)

    # https://github.com/pandas-dev/pandas/issues/21127
    # test_cumsum(ray_groupby, pandas_groupby)
    # test_cummax(ray_groupby, pandas_groupby)
    # test_cummin(ray_groupby, pandas_groupby)
    # test_cumprod(ray_groupby, pandas_groupby)

    test_pct_change(ray_groupby, pandas_groupby)
    apply_functions = [lambda df: -df, lambda df: df.sum(axis=1)]
    for func in apply_functions:
        test_apply(ray_groupby, pandas_groupby, func)

    test_first(ray_groupby, pandas_groupby)
    test_backfill(ray_groupby, pandas_groupby)
    test_bfill(ray_groupby, pandas_groupby)
    test_prod(ray_groupby, pandas_groupby)
    test_std(ray_groupby, pandas_groupby)
    test_last(ray_groupby, pandas_groupby)
    test_mad(ray_groupby, pandas_groupby)
    test_max(ray_groupby, pandas_groupby)
    test_var(ray_groupby, pandas_groupby)
    test_len(ray_groupby, pandas_groupby)
    test_sum(ray_groupby, pandas_groupby)

    # Pandas fails on this case with ValueError
    # test_ngroup(ray_groupby, pandas_groupby)
    # test_nunique(ray_groupby, pandas_groupby)
    test_median(ray_groupby, pandas_groupby)
    test_cov(ray_groupby, pandas_groupby)

    transform_functions = [lambda df: df + 4, lambda df: -df - 10]
    for func in transform_functions:
        test_transform(ray_groupby, pandas_groupby, func)

    pipe_functions = [lambda dfgb: dfgb.sum()]
    for func in pipe_functions:
        test_pipe(ray_groupby, pandas_groupby, func)

    test_corr(ray_groupby, pandas_groupby)
    test_fillna(ray_groupby, pandas_groupby)
    test_count(ray_groupby, pandas_groupby)
    test_take(ray_groupby, pandas_groupby)


@pytest.fixture
def test_ngroups(ray_groupby, pandas_groupby):
    assert ray_groupby.ngroups == pandas_groupby.ngroups


@pytest.fixture
def test_skew(ray_groupby, pandas_groupby):
    ray_df_almost_equals_pandas(ray_groupby.skew(), pandas_groupby.skew())


@pytest.fixture
def test_ffill(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.ffill(), pandas_groupby.ffill())


@pytest.fixture
def test_sem(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.sem()


@pytest.fixture
def test_mean(ray_groupby, pandas_groupby):
    ray_df_almost_equals_pandas(ray_groupby.mean(), pandas_groupby.mean())


@pytest.fixture
def test_any(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.any(), pandas_groupby.any())


@pytest.fixture
def test_min(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.min(), pandas_groupby.min())


@pytest.fixture
def test_idxmax(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.idxmax(), pandas_groupby.idxmax())


@pytest.fixture
def test_ndim(ray_groupby, pandas_groupby):
    assert ray_groupby.ndim == pandas_groupby.ndim


@pytest.fixture
def test_cumsum(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.cumsum(), pandas_groupby.cumsum())
    ray_df_equals_pandas(ray_groupby.cumsum(axis=1),
                         pandas_groupby.cumsum(axis=1))


@pytest.fixture
def test_pct_change(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.pct_change()


@pytest.fixture
def test_cummax(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.cummax(), pandas_groupby.cummax())
    ray_df_equals_pandas(ray_groupby.cummax(axis=1),
                         pandas_groupby.cummax(axis=1))


@pytest.fixture
def test_apply(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.apply(func), pandas_groupby.apply(func))


@pytest.fixture
def test_dtypes(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.dtypes, pandas_groupby.dtypes)


@pytest.fixture
def test_first(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.first()


@pytest.fixture
def test_backfill(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.backfill(), pandas_groupby.backfill())


@pytest.fixture
def test_cummin(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.cummin(), pandas_groupby.cummin())
    ray_df_equals_pandas(ray_groupby.cummin(axis=1),
                         pandas_groupby.cummin(axis=1))


@pytest.fixture
def test_bfill(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.bfill(), pandas_groupby.bfill())


@pytest.fixture
def test_idxmin(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.idxmin(), pandas_groupby.idxmin())


@pytest.fixture
def test_prod(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.prod(), pandas_groupby.prod())


@pytest.fixture
def test_std(ray_groupby, pandas_groupby):
    ray_df_almost_equals_pandas(ray_groupby.std(), pandas_groupby.std())


@pytest.fixture
def test_aggregate(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.aggregate(func),
                         pandas_groupby.aggregate(func))


@pytest.fixture
def test_agg(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.agg(func), pandas_groupby.agg(func))


@pytest.fixture
def test_last(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.last()


@pytest.fixture
def test_mad(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.mad()


@pytest.fixture
def test_rank(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.rank(), pandas_groupby.rank())


@pytest.fixture
def test_max(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.max(), pandas_groupby.max())


@pytest.fixture
def test_var(ray_groupby, pandas_groupby):
    ray_df_almost_equals_pandas(ray_groupby.var(), pandas_groupby.var())


@pytest.fixture
def test_len(ray_groupby, pandas_groupby):
    assert len(ray_groupby) == len(pandas_groupby)


@pytest.fixture
def test_sum(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.sum(), pandas_groupby.sum())


@pytest.fixture
def test_ngroup(ray_groupby, pandas_groupby):
    ray_series_equals_pandas(ray_groupby.ngroup(), pandas_groupby.ngroup())


@pytest.fixture
def test_nunique(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.nunique(), pandas_groupby.nunique())


@pytest.fixture
def test_median(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.median(), pandas_groupby.median())


@pytest.fixture
def test_head(ray_groupby, pandas_groupby, n):
    ray_df_equals_pandas(ray_groupby.head(n=n), pandas_groupby.head(n=n))


@pytest.fixture
def test_cumprod(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.cumprod(), pandas_groupby.cumprod())
    ray_df_equals_pandas(ray_groupby.cumprod(axis=1),
                         pandas_groupby.cumprod(axis=1))


@pytest.fixture
def test_cov(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.cov()


@pytest.fixture
def test_transform(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.transform(func),
                         pandas_groupby.transform(func))


@pytest.fixture
def test_corr(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.corr()


@pytest.fixture
def test_fillna(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.fillna(method="ffill"),
                         pandas_groupby.fillna(method="ffill"))


@pytest.fixture
def test_count(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.count(), pandas_groupby.count())


@pytest.fixture
def test_pipe(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.pipe(func), pandas_groupby.pipe(func))


@pytest.fixture
def test_tail(ray_groupby, pandas_groupby, n):
    ray_df_equals_pandas(ray_groupby.tail(n=n), pandas_groupby.tail(n=n))


@pytest.fixture
def test_quantile(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.quantile(q=0.4),
                         pandas_groupby.quantile(q=0.4))


@pytest.fixture
def test_take(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.take(indices=[1])
