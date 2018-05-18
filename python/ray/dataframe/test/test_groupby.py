from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import pandas as pd
from ray.dataframe.utils import (
    from_pandas,
    to_pandas)


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def ray_series_equals_pandas(ray_df, pandas_df):
    return ray_df.sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def ray_df_equals(ray_df1, ray_df2):
    return to_pandas(ray_df1).sort_index().equals(
        to_pandas(ray_df2).sort_index()
    )


@pytest.fixture
def ray_groupby_equals_pandas(ray_groupby, pandas_groupby):
    for g1, g2 in zip(ray_groupby, pandas_groupby):
        assert g1[0] == g2[0]
        assert ray_df_equals_pandas(g1[1], g2[1])


def test_simple_row_groupby():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15],
                              'col5': [0, 0, 0, 0]})

    ray_df = from_pandas(pandas_df, 2)

    by = [1, 2, 1, 2]

    ray_groupby = ray_df.groupby(by=by)
    pandas_groupby = pandas_df.groupby(by=by)

    assert ray_groupby_equals_pandas(ray_groupby, pandas_groupby)





    by_col_axis = [[1, 2, 3, 2, 1],
                   [0, 0, 0, 0, 0],
                   [1, 2, 3, 4, 5]]
