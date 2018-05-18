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
                                  'col3': [8, 9, 10, 11],
                                  'col4': [12, 13, 14, 15],
                                  'col5': [0, 0, 0, 0]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 2, 1, 2]

    ray_groupby = ray_df.groupby(by=by)
    pandas_groupby = pandas_df.groupby(by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)


def test_simple_col_groupby():
    pandas_df = pandas.DataFrame({'col1': [0, 1, 2, 3],
                                  'col2': [4, 5, 6, 7],
                                  'col3': [8, 9, 10, 11],
                                  'col4': [12, 13, 14, 15],
                                  'col5': [0, 0, 0, 0]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 2, 3, 2, 1]

    ray_groupby = ray_df.groupby(axis=1, by=by)
    pandas_groupby = pandas_df.groupby(axis=1, by=by)

    ray_groupby_equals_pandas(ray_groupby, pandas_groupby)
    test_ngroups(ray_groupby, pandas_groupby)
    test_skew(ray_groupby, pandas_groupby)
    test_ffill(ray_groupby, pandas_groupby)


@pytest.fixture
def test_ngroups(ray_groupby, pandas_groupby):
    assert ray_groupby.ngroups == pandas_groupby.ngroups


@pytest.fixture
def test_skew(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.skew(), pandas_groupby.skew())


@pytest.fixture
def test_ffill(ray_groupby, pandas_groupby):
    ray_df_equals_pandas(ray_groupby.ffill(), pandas_groupby.ffill())
