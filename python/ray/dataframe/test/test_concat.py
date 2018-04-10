from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import pandas as pd
import ray.dataframe as rdf
from ray.dataframe.utils import (
    to_pandas,
    from_pandas
)


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def ray_df_equals(ray_df1, ray_df2):
    return to_pandas(ray_df1).sort_index().equals(
        to_pandas(ray_df2).sort_index()
    )


@pytest.fixture
def generate_dfs():
    df = pd.DataFrame({'col1': [0, 1, 2, 3],
                       'col2': [4, 5, 6, 7],
                       'col3': [8, 9, 10, 11],
                       'col4': [12, 13, 14, 15],
                       'col5': [0, 0, 0, 0]})

    df2 = pd.DataFrame({'col1': [0, 1, 2, 3],
                        'col2': [4, 5, 6, 7],
                        'col3': [8, 9, 10, 11],
                        'col6': [12, 13, 14, 15],
                        'col7': [0, 0, 0, 0]})
    return df, df2


@pytest.fixture
def test_df_concat():
    df, df2 = generate_dfs()

    assert(ray_df_equals_pandas(rdf.concat([df, df2]), pd.concat([df, df2])))


def test_ray_concat():
    df, df2 = generate_dfs()
    ray_df, ray_df2 = from_pandas(df, 2), from_pandas(df2, 2)

    assert(ray_df_equals_pandas(rdf.concat([ray_df, ray_df2]),
                                pd.concat([df, df2])))


def test_ray_concat_on_index():
    df, df2 = generate_dfs()
    ray_df, ray_df2 = from_pandas(df, 2), from_pandas(df2, 2)

    assert(ray_df_equals_pandas(rdf.concat([ray_df, ray_df2], axis='index'),
                                pd.concat([df, df2], axis='index')))

    assert(ray_df_equals_pandas(rdf.concat([ray_df, ray_df2], axis='rows'),
                                pd.concat([df, df2], axis='rows')))

    assert(ray_df_equals_pandas(rdf.concat([ray_df, ray_df2], axis=0),
                                pd.concat([df, df2], axis=0)))


def test_ray_concat_on_column():
    df, df2 = generate_dfs()
    ray_df, ray_df2 = from_pandas(df, 2), from_pandas(df2, 2)

    with pytest.raises(NotImplementedError):
        rdf.concat([ray_df, ray_df2], axis=1)

    with pytest.raises(NotImplementedError):
        rdf.concat([ray_df, ray_df2], axis="columns")


def test_invalid_axis_errors():
    df, df2 = generate_dfs()
    ray_df, ray_df2 = from_pandas(df, 2), from_pandas(df2, 2)

    with pytest.raises(ValueError):
        rdf.concat([ray_df, ray_df2], axis=2)


def test_mixed_concat():
    df, df2 = generate_dfs()
    df3 = df.copy()

    mixed_dfs = [from_pandas(df, 2), from_pandas(df2, 2), df3]

    assert(ray_df_equals_pandas(rdf.concat(mixed_dfs),
                                pd.concat([df, df2, df3])))


def test_mixed_inner_concat():
    df, df2 = generate_dfs()
    df3 = df.copy()

    mixed_dfs = [from_pandas(df, 2), from_pandas(df2, 2), df3]

    with pytest.raises(NotImplementedError):
        rdf.concat(mixed_dfs, join="inner")
