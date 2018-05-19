from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import pandas
import ray.dataframe as pd
from ray.dataframe.utils import (
    from_pandas,
    to_pandas)


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    assert isinstance(ray_df, pd.DataFrame)
    assert to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def ray_series_equals_pandas(ray_df, pandas_df):
    assert ray_df.sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def ray_df_equals(ray_df1, ray_df2):
    assert to_pandas(ray_df1).sort_index().equals(
        to_pandas(ray_df2).sort_index()
    )


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


def test_simple_col_groupby():
    pandas_df = pandas.DataFrame({'col1': [0, 1, 2, 3],
                                  'col2': [4, 5, 6, 7],
                                  'col3': [3, 8, 12, 10],
                                  'col4': [17, 13, 16, 15],
                                  'col5': [-4, -5, -6, -7]})

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
    test_idxmax(ray_groupby, pandas_groupby)
    test_ndim(ray_groupby, pandas_groupby)

    # https://github.com/pandas-dev/pandas/issues/21127
    # test_cumsum(ray_groupby, pandas_groupby)
    # test_cummax(ray_groupby, pandas_groupby)
    # test_cummin(ray_groupby, pandas_groupby)

    test_pct_change(ray_groupby, pandas_groupby)
    apply_functions = [lambda df: -df, lambda df: df.sum(axis=1)]
    for func in apply_functions:
        test_apply(ray_groupby, pandas_groupby, func)

    test_first(ray_groupby, pandas_groupby)
    test_backfill(ray_groupby, pandas_groupby)
    test_bfill(ray_groupby, pandas_groupby)
    test_idxmin(ray_groupby, pandas_groupby)
    test_prod(ray_groupby, pandas_groupby)
    test_std(ray_groupby, pandas_groupby)


@pytest.fixture
def test_ngroups(ray_groupby, pandas_groupby):
    assert ray_groupby.ngroups == pandas_groupby.ngroups


@pytest.fixture
def test_skew(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.skew(), pandas_groupby.skew())


@pytest.fixture
def test_ffill(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.ffill(), pandas_groupby.ffill())


@pytest.fixture
def test_sem(ray_groupby, pandas_groupby):
    with pytest.raises(NotImplementedError):
        ray_groupby.sem()


@pytest.fixture
def test_mean(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.mean(), pandas_groupby.mean())


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
    ray_df_equals_pandas(ray_groupby.std(), pandas_groupby.std())


@pytest.fixture
def test_aggregate(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.aggregate(func),
                         pandas_groupby.aggregate(func))


@pytest.fixture
def test_agg(ray_groupby, pandas_groupby, func):
    ray_df_equals_pandas(ray_groupby.agg(func), pandas_groupby.agg(func))
