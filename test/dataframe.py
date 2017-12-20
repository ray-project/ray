from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import ray.dataframe as rdf
import numpy as np
import pandas as pd
import ray


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return rdf.to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def test_roundtrip(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df, pandas_df))


@pytest.fixture
def test_index(ray_df, pandas_df):
    assert(ray_df.index.equals(pandas_df.index))


@pytest.fixture
def test_size(ray_df, pandas_df):
    assert(ray_df.size == pandas_df.size)


@pytest.fixture
def test_ndim(ray_df, pandas_df):
    assert(ray_df.ndim == pandas_df.ndim)


@pytest.fixture
def test_ftypes(ray_df, pandas_df):
    assert(ray_df.ftypes.equals(pandas_df.ftypes))


@pytest.fixture
def test_values(ray_df, pandas_df):
    assert(np.array_equal(ray_df.values, pandas_df.values))


@pytest.fixture
def test_axes(ray_df, pandas_df):
    assert(np.array_equal(ray_df.axes, pandas_df.axes))


@pytest.fixture
def test_shape(ray_df, pandas_df):
    assert(ray_df.shape == pandas_df.shape)


@pytest.fixture
def test_add_prefix(ray_df, pandas_df):
    test_prefix = "TEST"
    new_ray_df = ray_df.add_prefix(test_prefix)
    new_pandas_df = pandas_df.add_prefix(test_prefix)
    assert(new_ray_df.columns.equals(new_pandas_df.columns))


@pytest.fixture
def test_add_suffix(ray_df, pandas_df):
    test_suffix = "TEST"
    new_ray_df = ray_df.add_suffix(test_suffix)
    new_pandas_df = pandas_df.add_suffix(test_suffix)

    assert(new_ray_df.columns.equals(new_pandas_df.columns))


@pytest.fixture
def test_applymap(ray_df, pandas_df, testfunc):
    new_ray_df = ray_df.applymap(testfunc)
    new_pandas_df = pandas_df.applymap(testfunc)

    assert(ray_df_equals_pandas(new_ray_df, new_pandas_df))


@pytest.fixture
def test_copy(ray_df):
    new_ray_df = ray_df.copy()

    assert(new_ray_df is not ray_df)
    assert(new_ray_df.df == ray_df.df)


@pytest.fixture
def test_sum(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.sum(), pandas_df.sum()))


@pytest.fixture
def test_abs(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.abs(), pandas_df.abs()))


@pytest.fixture
def test_keys(ray_df, pandas_df):
    assert(ray_df.keys().equals(pandas_df.keys()))


@pytest.fixture
def test_transpose(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.T, pandas_df.T))
    assert(ray_df_equals_pandas(ray_df.transpose(), pandas_df.transpose()))


def test_int_dataframe():
    ray.init()

    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    testfuncs = [lambda x: x + 1,
                 lambda x: str(x),
                 lambda x: x * x,
                 lambda x: x,
                 lambda x: False]

    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_values(ray_df, pandas_df)
    test_axes(ray_df, pandas_df)
    test_shape(ray_df, pandas_df)
    test_add_prefix(ray_df, pandas_df)
    test_add_suffix(ray_df, pandas_df)

    for testfunc in testfuncs:
        test_applymap(ray_df, pandas_df, testfunc)

    test_copy(ray_df)
    test_sum(ray_df, pandas_df)
    test_abs(ray_df, pandas_df)
    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)


def test_float_dataframe():

    pandas_df = pd.DataFrame({'col1': [0.0, 1.0, 2.0, 3.0],
                              'col2': [4.0, 5.0, 6.0, 7.0],
                              'col3': [8.0, 9.0, 10.0, 11.0],
                              'col4': [12.0, 13.0, 14.0, 15.0]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    testfuncs = [lambda x: x + 1,
                 lambda x: str(x),
                 lambda x: x * x,
                 lambda x: x,
                 lambda x: False]

    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_values(ray_df, pandas_df)
    test_axes(ray_df, pandas_df)
    test_shape(ray_df, pandas_df)
    test_add_prefix(ray_df, pandas_df)
    test_add_suffix(ray_df, pandas_df)

    for testfunc in testfuncs:
        test_applymap(ray_df, pandas_df, testfunc)

    test_copy(ray_df)
    test_sum(ray_df, pandas_df)
    test_abs(ray_df, pandas_df)
    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)
