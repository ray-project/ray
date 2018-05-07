from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import ray.dataframe as rdf
from ray.dataframe.utils import (
    from_pandas,
    to_pandas)

from pandas.tests.frame.common import TestData


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
def test_roundtrip(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df, pandas_df))


@pytest.fixture
def test_index(ray_df, pandas_df):
    assert(ray_df.index.equals(pandas_df.index))
    ray_df_cp = ray_df.copy()
    pandas_df_cp = pandas_df.copy()

    ray_df_cp.index = [str(i) for i in ray_df_cp.index]
    pandas_df_cp.index = [str(i) for i in pandas_df_cp.index]
    assert(ray_df_cp.index.sort_values().equals(pandas_df_cp.index))


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
def test_dtypes(ray_df, pandas_df):
    assert(ray_df.dtypes.equals(pandas_df.dtypes))


@pytest.fixture
def test_values(ray_df, pandas_df):
    np.testing.assert_equal(ray_df.values, pandas_df.values)


@pytest.fixture
def test_axes(ray_df, pandas_df):
    for ray_axis, pd_axis in zip(ray_df.axes, pandas_df.axes):
        assert (np.array_equal(ray_axis, pd_axis))


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

    assert new_ray_df is not ray_df
    assert np.array_equal(new_ray_df._block_partitions,
                          ray_df._block_partitions)


@pytest.fixture
def test_sum(ray_df, pandas_df):
    assert(ray_df.sum().sort_index().equals(pandas_df.sum().sort_index()))


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


@pytest.fixture
def test_get(ray_df, pandas_df, key):
    assert(ray_df.get(key).equals(pandas_df.get(key)))
    assert ray_df.get(
        key, default='default').equals(
            pandas_df.get(key, default='default'))


@pytest.fixture
def test_get_dtype_counts(ray_df, pandas_df):
    assert(ray_df.get_dtype_counts().equals(pandas_df.get_dtype_counts()))


@pytest.fixture
def test_get_ftype_counts(ray_df, pandas_df):
    assert(ray_df.get_ftype_counts().equals(pandas_df.get_ftype_counts()))


@pytest.fixture
def create_test_dataframe():
    df = pd.DataFrame({'col1': [0, 1, 2, 3],
                       'col2': [4, 5, 6, 7],
                       'col3': [8, 9, 10, 11],
                       'col4': [12, 13, 14, 15],
                       'col5': [0, 0, 0, 0]})

    return from_pandas(df, 2)


def test_int_dataframe():

    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15],
                              'col5': [0, 0, 0, 0]})
    ray_df = from_pandas(pandas_df, 2)

    testfuncs = [lambda x: x + 1,
                 lambda x: str(x),
                 lambda x: x * x,
                 lambda x: x,
                 lambda x: False]

    query_funcs = ['col1 < col2', 'col3 > col4', 'col1 == col2',
                   '(col2 > col1) and (col1 < col3)']

    keys = ['col1',
            'col2',
            'col3',
            'col4']

    filter_by = {'items': ['col1', 'col5'],
                 'regex': '4$|3$',
                 'like': 'col'}

    test_filter(ray_df, pandas_df, filter_by)
    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_dtypes(ray_df, pandas_df)
    test_values(ray_df, pandas_df)
    test_axes(ray_df, pandas_df)
    test_shape(ray_df, pandas_df)
    test_add_prefix(ray_df, pandas_df)
    test_add_suffix(ray_df, pandas_df)

    for testfunc in testfuncs:
        test_applymap(ray_df, pandas_df, testfunc)

    test_copy(ray_df)
    test_sum(ray_df, pandas_df)
    test_prod(ray_df, pandas_df)
    test_product(ray_df, pandas_df)
    test_abs(ray_df, pandas_df)
    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

    test_mean(ray_df, pandas_df)
    test_var(ray_df, pandas_df)
    test_std(ray_df, pandas_df)
    test_median(ray_df, pandas_df)
    test_quantile(ray_df, pandas_df, .25)
    test_quantile(ray_df, pandas_df, .5)
    test_quantile(ray_df, pandas_df, .75)
    test_describe(ray_df, pandas_df)
    test_diff(ray_df, pandas_df)
    test_rank(ray_df, pandas_df)

    test_all(ray_df, pandas_df)
    test_any(ray_df, pandas_df)
    test___getitem__(ray_df, pandas_df)
    test___neg__(ray_df, pandas_df)
    test___iter__(ray_df, pandas_df)
    test___abs__(ray_df, pandas_df)
    test___delitem__(ray_df, pandas_df)
    test___copy__(ray_df, pandas_df)
    test___deepcopy__(ray_df, pandas_df)
    test_bool(ray_df, pandas_df)
    test_count(ray_df, pandas_df)
    test_head(ray_df, pandas_df, 2)
    test_head(ray_df, pandas_df)
    test_tail(ray_df, pandas_df)
    test_idxmax(ray_df, pandas_df)
    test_idxmin(ray_df, pandas_df)
    test_pop(ray_df, pandas_df)

    test___len__(ray_df, pandas_df)
    test_first_valid_index(ray_df, pandas_df)
    test_last_valid_index(ray_df, pandas_df)

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    test_max(ray_df, pandas_df)
    test_min(ray_df, pandas_df)
    test_notna(ray_df, pandas_df)
    test_notnull(ray_df, pandas_df)
    test_cummax(ray_df, pandas_df)
    test_cummin(ray_df, pandas_df)
    test_cumprod(ray_df, pandas_df)
    test_cumsum(ray_df, pandas_df)
    test_pipe(ray_df, pandas_df)

    # test_loc(ray_df, pandas_df)
    # test_iloc(ray_df, pandas_df)

    labels = ['a', 'b', 'c', 'd']
    test_set_axis(ray_df, pandas_df, labels, 0)
    test_set_axis(ray_df, pandas_df, labels, 'rows')
    labels.append('e')
    test_set_axis(ray_df, pandas_df, labels, 1)
    test_set_axis(ray_df, pandas_df, labels, 'columns')

    for key in keys:
        test_set_index(ray_df, pandas_df, key)

    test_reset_index(ray_df, pandas_df)
    test_reset_index(ray_df, pandas_df, inplace=True)

    for key in keys:
        test___contains__(ray_df, key, True)
    test___contains__(ray_df, "Not Exists", False)

    for key in keys:
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])

    test___array__(ray_df, pandas_df)

    apply_agg_functions = ['sum', lambda df: df.sum(), ['sum', 'mean'],
                           ['sum', 'sum']]
    for func in apply_agg_functions:
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        if not isinstance(func, list):
            test_agg(ray_df, pandas_df, func, 1)
            test_apply(ray_df, pandas_df, func, 1)
            test_aggregate(ray_df, pandas_df, func, 1)
        else:
            with pytest.raises(TypeError):
                test_agg(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_apply(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_aggregate(ray_df, pandas_df, func, 1)

        func = ['sum', lambda df: df.sum()]
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        with pytest.raises(TypeError):
            test_apply(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_aggregate(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_agg(ray_df, pandas_df, func, 1)

        test_transform(ray_df, pandas_df)


def test_float_dataframe():

    pandas_df = pd.DataFrame({'col1': [0.0, 1.0, 2.0, 3.0],
                              'col2': [4.0, 5.0, 6.0, 7.0],
                              'col3': [8.0, 9.0, 10.0, 11.0],
                              'col4': [12.0, 13.0, 14.0, 15.0],
                              'col5': [0.0, 0.0, 0.0, 0.0]})

    ray_df = from_pandas(pandas_df, 3)

    testfuncs = [lambda x: x + 1,
                 lambda x: str(x),
                 lambda x: x * x,
                 lambda x: x,
                 lambda x: False]

    query_funcs = ['col1 < col2', 'col3 > col4', 'col1 == col2',
                   '(col2 > col1) and (col1 < col3)']

    keys = ['col1',
            'col2',
            'col3',
            'col4']

    filter_by = {'items': ['col1', 'col5'],
                 'regex': '4$|3$',
                 'like': 'col'}

    test_filter(ray_df, pandas_df, filter_by)
    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_dtypes(ray_df, pandas_df)
    test_values(ray_df, pandas_df)
    test_axes(ray_df, pandas_df)
    test_shape(ray_df, pandas_df)
    test_add_prefix(ray_df, pandas_df)
    test_add_suffix(ray_df, pandas_df)

    for testfunc in testfuncs:
        test_applymap(ray_df, pandas_df, testfunc)

    test_copy(ray_df)
    test_sum(ray_df, pandas_df)
    test_prod(ray_df, pandas_df)
    test_product(ray_df, pandas_df)
    test_abs(ray_df, pandas_df)
    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

    test_mean(ray_df, pandas_df)
    # TODO Clear floating point error.
    # test_var(ray_df, pandas_df)
    test_std(ray_df, pandas_df)
    test_median(ray_df, pandas_df)
    test_quantile(ray_df, pandas_df, .25)
    test_quantile(ray_df, pandas_df, .5)
    test_quantile(ray_df, pandas_df, .75)
    test_describe(ray_df, pandas_df)
    test_diff(ray_df, pandas_df)
    test_rank(ray_df, pandas_df)

    test_all(ray_df, pandas_df)
    test_any(ray_df, pandas_df)
    test___getitem__(ray_df, pandas_df)
    test___neg__(ray_df, pandas_df)
    test___iter__(ray_df, pandas_df)
    test___abs__(ray_df, pandas_df)
    test___delitem__(ray_df, pandas_df)
    test___copy__(ray_df, pandas_df)
    test___deepcopy__(ray_df, pandas_df)
    test_bool(ray_df, pandas_df)
    test_count(ray_df, pandas_df)
    test_head(ray_df, pandas_df, 3)
    test_head(ray_df, pandas_df)
    test_tail(ray_df, pandas_df)
    test_idxmax(ray_df, pandas_df)
    test_idxmin(ray_df, pandas_df)
    test_pop(ray_df, pandas_df)
    test_max(ray_df, pandas_df)
    test_min(ray_df, pandas_df)
    test_notna(ray_df, pandas_df)
    test_notnull(ray_df, pandas_df)
    test_cummax(ray_df, pandas_df)
    test_cummin(ray_df, pandas_df)
    test_cumprod(ray_df, pandas_df)
    test_cumsum(ray_df, pandas_df)
    test_pipe(ray_df, pandas_df)

    test___len__(ray_df, pandas_df)
    test_first_valid_index(ray_df, pandas_df)
    test_last_valid_index(ray_df, pandas_df)

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    # test_loc(ray_df, pandas_df)
    # test_iloc(ray_df, pandas_df)

    labels = ['a', 'b', 'c', 'd']
    test_set_axis(ray_df, pandas_df, labels, 0)
    test_set_axis(ray_df, pandas_df, labels, 'rows')
    labels.append('e')
    test_set_axis(ray_df, pandas_df, labels, 1)
    test_set_axis(ray_df, pandas_df, labels, 'columns')

    for key in keys:
        test_set_index(ray_df, pandas_df, key)
        test_set_index(ray_df, pandas_df, key, inplace=True)

    test_reset_index(ray_df, pandas_df)
    test_reset_index(ray_df, pandas_df, inplace=True)

    for key in keys:
        test___contains__(ray_df, key, True)
    test___contains__(ray_df, "Not Exists", False)

    for key in keys:
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])

    # TODO Nans are always not equal to each other, fix it
    # test___array__(ray_df, pandas_df)

    apply_agg_functions = ['sum', lambda df: df.sum(), ['sum', 'mean'],
                           ['sum', 'sum']]
    for func in apply_agg_functions:
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        if not isinstance(func, list):
            test_agg(ray_df, pandas_df, func, 1)
            test_apply(ray_df, pandas_df, func, 1)
            test_aggregate(ray_df, pandas_df, func, 1)
        else:
            with pytest.raises(TypeError):
                test_agg(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_apply(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_aggregate(ray_df, pandas_df, func, 1)

        func = ['sum', lambda df: df.sum()]
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        with pytest.raises(TypeError):
            test_apply(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_aggregate(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_agg(ray_df, pandas_df, func, 1)

        test_transform(ray_df, pandas_df)


def test_mixed_dtype_dataframe():
    pandas_df = pd.DataFrame({
        'col1': [1, 2, 3, 4],
        'col2': [4, 5, 6, 7],
        'col3': [8.0, 9.4, 10.1, 11.3],
        'col4': ['a', 'b', 'c', 'd']})

    ray_df = from_pandas(pandas_df, 2)

    testfuncs = [lambda x: x + x,
                 lambda x: str(x),
                 lambda x: x,
                 lambda x: False]

    query_funcs = ['col1 < col2', 'col1 == col2',
                   '(col2 > col1) and (col1 < col3)']

    keys = ['col1',
            'col2',
            'col3',
            'col4']

    filter_by = {'items': ['col1', 'col5'],
                 'regex': '4$|3$',
                 'like': 'col'}

    test_filter(ray_df, pandas_df, filter_by)
    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_dtypes(ray_df, pandas_df)
    test_values(ray_df, pandas_df)
    test_axes(ray_df, pandas_df)
    test_shape(ray_df, pandas_df)
    test_add_prefix(ray_df, pandas_df)
    test_add_suffix(ray_df, pandas_df)

    for testfunc in testfuncs:
        test_applymap(ray_df, pandas_df, testfunc)

    test_copy(ray_df)
    test_sum(ray_df, pandas_df)

    with pytest.raises(TypeError):
        test_abs(ray_df, pandas_df)
        test___abs__(ray_df, pandas_df)

    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

    test_mean(ray_df, pandas_df)
    # TODO Clear floating point error.
    # test_var(ray_df, pandas_df)
    test_std(ray_df, pandas_df)
    test_median(ray_df, pandas_df)
    test_quantile(ray_df, pandas_df, .25)
    test_quantile(ray_df, pandas_df, .5)
    test_quantile(ray_df, pandas_df, .75)
    test_describe(ray_df, pandas_df)

    # TODO Reolve once Pandas-20962 is resolved.
    # test_rank(ray_df, pandas_df)

    test_all(ray_df, pandas_df)
    test_any(ray_df, pandas_df)
    test___getitem__(ray_df, pandas_df)

    with pytest.raises(TypeError):
        test___neg__(ray_df, pandas_df)

    test___iter__(ray_df, pandas_df)
    test___delitem__(ray_df, pandas_df)
    test___copy__(ray_df, pandas_df)
    test___deepcopy__(ray_df, pandas_df)
    test_bool(ray_df, pandas_df)
    test_count(ray_df, pandas_df)
    test_head(ray_df, pandas_df, 2)
    test_head(ray_df, pandas_df)
    test_tail(ray_df, pandas_df)

    with pytest.raises(TypeError):
        test_idxmax(ray_df, pandas_df)
    with pytest.raises(TypeError):
        test_idxmin(ray_df, pandas_df)

    test_pop(ray_df, pandas_df)
    test_max(ray_df, pandas_df)
    test_min(ray_df, pandas_df)
    test_notna(ray_df, pandas_df)
    test_notnull(ray_df, pandas_df)
    test_pipe(ray_df, pandas_df)

    # TODO Fix pandas so that the behavior is correct
    # We discovered a bug where argmax does not always give the same result
    # depending on what your other dtypes are.
    # test_cummax(ray_df, pandas_df)
    # test_cummin(ray_df, pandas_df)
    # test_cumprod(ray_df, pandas_df)
    # test_cumsum(ray_df, pandas_df)

    test___len__(ray_df, pandas_df)
    test_first_valid_index(ray_df, pandas_df)
    test_last_valid_index(ray_df, pandas_df)

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    # test_loc(ray_df, pandas_df)
    # test_iloc(ray_df, pandas_df)

    labels = ['a', 'b', 'c', 'd']
    test_set_axis(ray_df, pandas_df, labels, 0)
    test_set_axis(ray_df, pandas_df, labels, 'rows')
    test_set_axis(ray_df, pandas_df, labels, 1)
    test_set_axis(ray_df, pandas_df, labels, 'columns')

    for key in keys:
        test_set_index(ray_df, pandas_df, key)
        test_set_index(ray_df, pandas_df, key, inplace=True)

    test_reset_index(ray_df, pandas_df)
    test_reset_index(ray_df, pandas_df, inplace=True)

    for key in keys:
        test___contains__(ray_df, key, True)
    test___contains__(ray_df, "Not Exists", False)

    for key in keys:
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])

    test___array__(ray_df, pandas_df)

    apply_agg_functions = ['sum', lambda df: df.sum()]
    for func in apply_agg_functions:
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)

        func = ['sum', lambda df: df.sum()]
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        with pytest.raises(TypeError):
            test_apply(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_aggregate(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_agg(ray_df, pandas_df, func, 1)

        test_transform(ray_df, pandas_df)


def test_nan_dataframe():
    pandas_df = pd.DataFrame({
        'col1': [1, 2, 3, np.nan],
        'col2': [4, 5, np.nan, 7],
        'col3': [8, np.nan, 10, 11],
        'col4': [np.nan, 13, 14, 15]})

    ray_df = from_pandas(pandas_df, 2)

    testfuncs = [lambda x: x + x,
                 lambda x: str(x),
                 lambda x: x,
                 lambda x: False]

    query_funcs = ['col1 < col2', 'col3 > col4', 'col1 == col2',
                   '(col2 > col1) and (col1 < col3)']

    keys = ['col1',
            'col2',
            'col3',
            'col4']

    filter_by = {'items': ['col1', 'col5'],
                 'regex': '4$|3$',
                 'like': 'col'}

    test_filter(ray_df, pandas_df, filter_by)
    test_roundtrip(ray_df, pandas_df)
    test_index(ray_df, pandas_df)
    test_size(ray_df, pandas_df)
    test_ndim(ray_df, pandas_df)
    test_ftypes(ray_df, pandas_df)
    test_dtypes(ray_df, pandas_df)
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
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

    test_mean(ray_df, pandas_df)
    test_var(ray_df, pandas_df)
    test_std(ray_df, pandas_df)
    test_median(ray_df, pandas_df)
    test_quantile(ray_df, pandas_df, .25)
    test_quantile(ray_df, pandas_df, .5)
    test_quantile(ray_df, pandas_df, .75)
    test_describe(ray_df, pandas_df)
    test_diff(ray_df, pandas_df)
    test_rank(ray_df, pandas_df)

    test_all(ray_df, pandas_df)
    test_any(ray_df, pandas_df)
    test___getitem__(ray_df, pandas_df)
    test___neg__(ray_df, pandas_df)
    test___iter__(ray_df, pandas_df)
    test___abs__(ray_df, pandas_df)
    test___delitem__(ray_df, pandas_df)
    test___copy__(ray_df, pandas_df)
    test___deepcopy__(ray_df, pandas_df)
    test_bool(ray_df, pandas_df)
    test_count(ray_df, pandas_df)
    test_head(ray_df, pandas_df, 2)
    test_head(ray_df, pandas_df)
    test_tail(ray_df, pandas_df)
    test_idxmax(ray_df, pandas_df)
    test_idxmin(ray_df, pandas_df)
    test_pop(ray_df, pandas_df)
    test_max(ray_df, pandas_df)
    test_min(ray_df, pandas_df)
    test_notna(ray_df, pandas_df)
    test_notnull(ray_df, pandas_df)
    test_cummax(ray_df, pandas_df)
    test_cummin(ray_df, pandas_df)
    test_cumprod(ray_df, pandas_df)
    test_cumsum(ray_df, pandas_df)
    test_pipe(ray_df, pandas_df)

    test___len__(ray_df, pandas_df)
    test_first_valid_index(ray_df, pandas_df)
    test_last_valid_index(ray_df, pandas_df)

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    # test_loc(ray_df, pandas_df)
    # test_iloc(ray_df, pandas_df)

    labels = ['a', 'b', 'c', 'd']
    test_set_axis(ray_df, pandas_df, labels, 0)
    test_set_axis(ray_df, pandas_df, labels, 'rows')
    test_set_axis(ray_df, pandas_df, labels, 1)
    test_set_axis(ray_df, pandas_df, labels, 'columns')

    for key in keys:
        test_set_index(ray_df, pandas_df, key)
        test_set_index(ray_df, pandas_df, key, inplace=True)

    test_reset_index(ray_df, pandas_df)
    test_reset_index(ray_df, pandas_df, inplace=True)

    for key in keys:
        test___contains__(ray_df, key, True)
    test___contains__(ray_df, "Not Exists", False)

    for key in keys:
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])

    # TODO Nans are always not equal to each other, fix it
    # test___array__(ray_df, pandas_df)

    apply_agg_functions = ['sum', lambda df: df.sum(), ['sum', 'mean'],
                           ['sum', 'sum']]
    for func in apply_agg_functions:
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        if not isinstance(func, list):
            test_agg(ray_df, pandas_df, func, 1)
            test_apply(ray_df, pandas_df, func, 1)
            test_aggregate(ray_df, pandas_df, func, 1)
        else:
            with pytest.raises(TypeError):
                test_agg(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_apply(ray_df, pandas_df, func, 1)
            with pytest.raises(TypeError):
                test_aggregate(ray_df, pandas_df, func, 1)

        func = ['sum', lambda df: df.sum()]
        test_apply(ray_df, pandas_df, func, 0)
        test_aggregate(ray_df, pandas_df, func, 0)
        test_agg(ray_df, pandas_df, func, 0)
        with pytest.raises(TypeError):
            test_apply(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_aggregate(ray_df, pandas_df, func, 1)
        with pytest.raises(TypeError):
            test_agg(ray_df, pandas_df, func, 1)

        test_transform(ray_df, pandas_df)


def test_dense_nan_df():
    ray_df = rdf.DataFrame([[np.nan, 2, np.nan, 0],
                            [3, 4, np.nan, 1],
                            [np.nan, np.nan, np.nan, 5]],
                           columns=list('ABCD'))

    pd_df = pd.DataFrame([[np.nan, 2, np.nan, 0],
                          [3, 4, np.nan, 1],
                          [np.nan, np.nan, np.nan, 5]],
                         columns=list('ABCD'))

    test_dropna(ray_df, pd_df)
    test_dropna_inplace(ray_df, pd_df)
    test_dropna_multiple_axes(ray_df, pd_df)
    test_dropna_multiple_axes_inplace(ray_df, pd_df)


@pytest.fixture
def test_inter_df_math(op, simple=False):
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                            "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df_equals_pandas(getattr(ray_df, op)(ray_df),
                         getattr(pandas_df, op)(pandas_df))
    ray_df_equals_pandas(getattr(ray_df, op)(4),
                         getattr(pandas_df, op)(4))
    ray_df_equals_pandas(getattr(ray_df, op)(4.0),
                         getattr(pandas_df, op)(4.0))

    ray_df2 = rdf.DataFrame({"A": [0, 2], "col1": [0, 19], "col2": [1, 1]})
    pandas_df2 = pd.DataFrame({"A": [0, 2], "col1": [0, 19], "col2": [1, 1]})

    ray_df_equals_pandas(getattr(ray_df, op)(ray_df2),
                         getattr(pandas_df, op)(pandas_df2))

    list_test = [0, 1, 2, 4]

    if not simple:
        ray_df_equals_pandas(getattr(ray_df, op)(list_test, axis=1),
                             getattr(pandas_df, op)(list_test, axis=1))

        ray_df_equals_pandas(getattr(ray_df, op)(list_test, axis=0),
                             getattr(pandas_df, op)(list_test, axis=0))


@pytest.fixture
def test_comparison_inter_ops(op):
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                            "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df_equals_pandas(getattr(ray_df, op)(ray_df),
                         getattr(pandas_df, op)(pandas_df))
    ray_df_equals_pandas(getattr(ray_df, op)(4),
                         getattr(pandas_df, op)(4))
    ray_df_equals_pandas(getattr(ray_df, op)(4.0),
                         getattr(pandas_df, op)(4.0))

    ray_df2 = rdf.DataFrame({"A": [0, 2], "col1": [0, 19], "col2": [1, 1]})
    pandas_df2 = pd.DataFrame({"A": [0, 2], "col1": [0, 19], "col2": [1, 1]})

    ray_df_equals_pandas(getattr(ray_df2, op)(ray_df2),
                         getattr(pandas_df2, op)(pandas_df2))


@pytest.fixture
def test_inter_df_math_right_ops(op):
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                            "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df_equals_pandas(getattr(ray_df, op)(4),
                         getattr(pandas_df, op)(4))
    ray_df_equals_pandas(getattr(ray_df, op)(4.0),
                         getattr(pandas_df, op)(4.0))


def test_add():
    test_inter_df_math("add", simple=False)


@pytest.fixture
def test_agg(ray_df, pandas_df, func, axis):
    ray_result = ray_df.agg(func, axis)
    pandas_result = pandas_df.agg(func, axis)
    if isinstance(ray_result, rdf.DataFrame):
        assert ray_df_equals_pandas(ray_result, pandas_result)
    else:
        assert ray_result.equals(pandas_result)


@pytest.fixture
def test_aggregate(ray_df, pandas_df, func, axis):
    ray_result = ray_df.aggregate(func, axis)
    pandas_result = pandas_df.aggregate(func, axis)
    if isinstance(ray_result, rdf.DataFrame):
        assert ray_df_equals_pandas(ray_result, pandas_result)
    else:
        assert ray_result.equals(pandas_result)


def test_align():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.align(None)


@pytest.fixture
def test_all(ray_df, pd_df):
    assert pd_df.all().equals(ray_df.all())
    assert pd_df.all(axis=1).equals(ray_df.all(axis=1))


@pytest.fixture
def test_any(ray_df, pd_df):
    assert pd_df.any().equals(ray_df.any())
    assert pd_df.any(axis=1).equals(ray_df.any(axis=1))


def test_append():
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                            "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df2 = rdf.DataFrame({"col5": [0], "col6": [1]})

    pandas_df2 = pd.DataFrame({"col5": [0], "col6": [1]})

    assert ray_df_equals_pandas(ray_df.append(ray_df2),
                                pandas_df.append(pandas_df2))

    with pytest.raises(ValueError):
        ray_df.append(ray_df2, verify_integrity=True)


@pytest.fixture
def test_apply(ray_df, pandas_df, func, axis):
    ray_result = ray_df.apply(func, axis)
    pandas_result = pandas_df.apply(func, axis)
    if isinstance(ray_result, rdf.DataFrame):
        assert ray_df_equals_pandas(ray_result, pandas_result)
    else:
        assert ray_result.equals(pandas_result)


def test_as_blocks():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.as_blocks()


def test_as_matrix():
    test_data = TestData()
    frame = rdf.DataFrame(test_data.frame)
    mat = frame.as_matrix()

    frame_columns = frame.columns
    for i, row in enumerate(mat):
        for j, value in enumerate(row):
            col = frame_columns[j]
            if np.isnan(value):
                assert np.isnan(frame[col][i])
            else:
                assert value == frame[col][i]

    # mixed type
    mat = rdf.DataFrame(test_data.mixed_frame).as_matrix(['foo', 'A'])
    assert mat[0, 0] == 'bar'

    df = rdf.DataFrame({'real': [1, 2, 3], 'complex': [1j, 2j, 3j]})
    mat = df.as_matrix()
    assert mat[0, 0] == 1j

    # single block corner case
    mat = rdf.DataFrame(test_data.frame).as_matrix(['A', 'B'])
    expected = test_data.frame.reindex(columns=['A', 'B']).values
    tm.assert_almost_equal(mat, expected)


def test_asfreq():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.asfreq(None)


def test_asof():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.asof(None)


def test_assign():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.assign()


def test_astype():
    td = TestData()
    ray_df_frame = from_pandas(td.frame, 2)
    our_df_casted = ray_df_frame.astype(np.int32)
    expected_df_casted = pd.DataFrame(td.frame.values.astype(np.int32),
                                      index=td.frame.index,
                                      columns=td.frame.columns)

    assert(ray_df_equals_pandas(our_df_casted, expected_df_casted))

    our_df_casted = ray_df_frame.astype(np.float64)
    expected_df_casted = pd.DataFrame(td.frame.values.astype(np.float64),
                                      index=td.frame.index,
                                      columns=td.frame.columns)

    assert(ray_df_equals_pandas(our_df_casted, expected_df_casted))

    our_df_casted = ray_df_frame.astype(str)
    expected_df_casted = pd.DataFrame(td.frame.values.astype(str),
                                      index=td.frame.index,
                                      columns=td.frame.columns)

    assert(ray_df_equals_pandas(our_df_casted, expected_df_casted))


def test_at_time():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.at_time(None)


def test_between_time():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.between_time(None, None)


@pytest.fixture
def test_bfill(num_partitions=2):
    test_data = TestData()
    test_data.tsframe['A'][:5] = np.nan
    test_data.tsframe['A'][-5:] = np.nan
    ray_df = from_pandas(test_data.tsframe, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.bfill(),
        test_data.tsframe.bfill()
    )


@pytest.fixture
def test_bool(ray_df, pd_df):
    with pytest.raises(ValueError):
        ray_df.bool()
        pd_df.bool()

    single_bool_pd_df = pd.DataFrame([True])
    single_bool_ray_df = from_pandas(single_bool_pd_df, 1)

    assert single_bool_pd_df.bool() == single_bool_ray_df.bool()


def test_boxplot():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.boxplot()


def test_clip():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.clip()


def test_clip_lower():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.clip_lower(None)


def test_clip_upper():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.clip_upper(None)


def test_combine():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.combine(None, None)


def test_combine_first():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.combine_first(None)


def test_compound():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.compound()


def test_consolidate():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.consolidate()


def test_convert_objects():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.convert_objects()


def test_corr():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.corr()


def test_corrwith():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.corrwith(None)


@pytest.fixture
def test_count(ray_df, pd_df):
    assert ray_df.count().equals(pd_df.count())
    assert ray_df.count(axis=1).equals(pd_df.count(axis=1))


def test_cov():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.cov()


@pytest.fixture
def test_cummax(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.cummax(), pandas_df.cummax()))


@pytest.fixture
def test_cummin(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.cummin(), pandas_df.cummin()))


@pytest.fixture
def test_cumprod(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.cumprod(), pandas_df.cumprod()))


@pytest.fixture
def test_cumsum(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.cumsum(), pandas_df.cumsum()))


@pytest.fixture
def test_describe(ray_df, pandas_df):
    assert(ray_df.describe().equals(pandas_df.describe()))


@pytest.fixture
def test_diff(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.diff(), pandas_df.diff()))
    assert(ray_df_equals_pandas(ray_df.diff(axis=1), pandas_df.diff(axis=1)))
    assert(ray_df_equals_pandas(ray_df.diff(periods=1),
                                pandas_df.diff(periods=1)))


def test_div():
    test_inter_df_math("div", simple=False)


def test_divide():
    test_inter_df_math("divide", simple=False)


def test_dot():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.dot(None)


def test_drop():
    ray_df = create_test_dataframe()
    simple = pd.DataFrame({"A": [1, 2, 3, 4], "B": [0, 1, 2, 3]})
    ray_simple = from_pandas(simple, 2)
    assert ray_df_equals_pandas(ray_simple.drop("A", axis=1), simple[['B']])
    assert ray_df_equals_pandas(ray_simple.drop(["A", "B"], axis='columns'),
                                simple[[]])
    assert ray_df_equals_pandas(ray_simple.drop([0, 1, 3], axis=0),
                                simple.loc[[2], :])
    assert ray_df_equals_pandas(ray_simple.drop([0, 3], axis='index'),
                                simple.loc[[1, 2], :])

    pytest.raises(ValueError, ray_simple.drop, 5)
    pytest.raises(ValueError, ray_simple.drop, 'C', 1)
    pytest.raises(ValueError, ray_simple.drop, [1, 5])
    pytest.raises(ValueError, ray_simple.drop, ['A', 'C'], 1)

    # errors = 'ignore'
    assert ray_df_equals_pandas(ray_simple.drop(5, errors='ignore'), simple)
    assert ray_df_equals_pandas(ray_simple.drop([0, 5], errors='ignore'),
                                simple.loc[[1, 2, 3], :])
    assert ray_df_equals_pandas(ray_simple.drop('C', axis=1, errors='ignore'),
                                simple)
    assert ray_df_equals_pandas(ray_simple.drop(['A', 'C'], axis=1,
                                errors='ignore'),
                                simple[['B']])

    # non-unique - wheee!
    nu_df = pd.DataFrame(pd.compat.lzip(range(3), range(-3, 1), list('abc')),
                         columns=['a', 'a', 'b'])
    ray_nu_df = from_pandas(nu_df, 3)
    assert ray_df_equals_pandas(ray_nu_df.drop('a', axis=1), nu_df[['b']])
    assert ray_df_equals_pandas(ray_nu_df.drop('b', axis='columns'),
                                nu_df['a'])
    assert ray_df_equals_pandas(ray_nu_df.drop([]), nu_df)  # GH 16398

    nu_df = nu_df.set_index(pd.Index(['X', 'Y', 'X']))
    nu_df.columns = list('abc')
    ray_nu_df = from_pandas(nu_df, 3)
    assert ray_df_equals_pandas(ray_nu_df.drop('X', axis='rows'),
                                nu_df.loc[["Y"], :])
    assert ray_df_equals_pandas(ray_nu_df.drop(['X', 'Y'], axis=0),
                                nu_df.loc[[], :])

    # inplace cache issue
    # GH 5628
    df = pd.DataFrame(np.random.randn(10, 3), columns=list('abc'))
    ray_df = from_pandas(df, 2)
    expected = df[~(df.b > 0)]
    ray_df.drop(labels=df[df.b > 0].index, inplace=True)
    assert ray_df_equals_pandas(ray_df, expected)


def test_drop_api_equivalence():
    # equivalence of the labels/axis and index/columns API's (GH12392)
    df = pd.DataFrame([[1, 2, 3], [3, 4, 5], [5, 6, 7]],
                      index=['a', 'b', 'c'],
                      columns=['d', 'e', 'f'])
    ray_df = from_pandas(df, 3)

    res1 = ray_df.drop('a')
    res2 = ray_df.drop(index='a')
    assert ray_df_equals(res1, res2)

    res1 = ray_df.drop('d', 1)
    res2 = ray_df.drop(columns='d')
    assert ray_df_equals(res1, res2)

    res1 = ray_df.drop(labels='e', axis=1)
    res2 = ray_df.drop(columns='e')
    assert ray_df_equals(res1, res2)

    res1 = ray_df.drop(['a'], axis=0)
    res2 = ray_df.drop(index=['a'])
    assert ray_df_equals(res1, res2)

    res1 = ray_df.drop(['a'], axis=0).drop(['d'], axis=1)
    res2 = ray_df.drop(index=['a'], columns=['d'])
    assert ray_df_equals(res1, res2)

    with pytest.raises(ValueError):
        ray_df.drop(labels='a', index='b')

    with pytest.raises(ValueError):
        ray_df.drop(labels='a', columns='b')

    with pytest.raises(ValueError):
        ray_df.drop(axis=1)


def test_drop_duplicates():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.drop_duplicates()


@pytest.fixture
def test_dropna(ray_df, pd_df):
    assert ray_df_equals_pandas(ray_df.dropna(axis=1, how='all'),
                                pd_df.dropna(axis=1, how='all'))

    assert ray_df_equals_pandas(ray_df.dropna(axis=1, how='any'),
                                pd_df.dropna(axis=1, how='any'))

    assert ray_df_equals_pandas(ray_df.dropna(axis=0, how='all'),
                                pd_df.dropna(axis=0, how='all'))

    assert ray_df_equals_pandas(ray_df.dropna(thresh=2),
                                pd_df.dropna(thresh=2))


@pytest.fixture
def test_dropna_inplace(ray_df, pd_df):
    ray_df = ray_df.copy()
    pd_df = pd_df.copy()

    ray_df.dropna(thresh=2, inplace=True)
    pd_df.dropna(thresh=2, inplace=True)

    assert ray_df_equals_pandas(ray_df, pd_df)

    ray_df.dropna(axis=1, how='any', inplace=True)
    pd_df.dropna(axis=1, how='any', inplace=True)

    assert ray_df_equals_pandas(ray_df, pd_df)


@pytest.fixture
def test_dropna_multiple_axes(ray_df, pd_df):
    assert ray_df_equals_pandas(
        ray_df.dropna(how='all', axis=[0, 1]),
        pd_df.dropna(how='all', axis=[0, 1])
    )
    assert ray_df_equals_pandas(
        ray_df.dropna(how='all', axis=(0, 1)),
        pd_df.dropna(how='all', axis=(0, 1))
    )


@pytest.fixture
def test_dropna_multiple_axes_inplace(ray_df, pd_df):
    ray_df_copy = ray_df.copy()
    pd_df_copy = pd_df.copy()

    ray_df_copy.dropna(how='all', axis=[0, 1], inplace=True)
    pd_df_copy.dropna(how='all', axis=[0, 1], inplace=True)

    assert ray_df_equals_pandas(ray_df_copy, pd_df_copy)

    ray_df_copy = ray_df.copy()
    pd_df_copy = pd_df.copy()

    ray_df_copy.dropna(how='all', axis=(0, 1), inplace=True)
    pd_df_copy.dropna(how='all', axis=(0, 1), inplace=True)

    assert ray_df_equals_pandas(ray_df_copy, pd_df_copy)


def test_duplicated():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.duplicated()


def test_eq():
    test_comparison_inter_ops("eq")


def test_equals():
    pandas_df1 = pd.DataFrame({'col1': [2.9, 3, 3, 3],
                               'col2': [2, 3, 4, 1]})
    ray_df1 = from_pandas(pandas_df1, 2)
    ray_df2 = from_pandas(pandas_df1, 3)

    assert ray_df1.equals(ray_df2)

    pandas_df2 = pd.DataFrame({'col1': [2.9, 3, 3, 3],
                               'col2': [2, 3, 5, 1]})
    ray_df3 = from_pandas(pandas_df2, 4)

    assert not ray_df3.equals(ray_df1)
    assert not ray_df3.equals(ray_df2)


def test_eval_df_use_case():
    df = pd.DataFrame({'a': np.random.randn(10),
                       'b': np.random.randn(10)})
    ray_df = from_pandas(df, 2)
    df.eval("e = arctan2(sin(a), b)",
            engine='python',
            parser='pandas', inplace=True)
    ray_df.eval("e = arctan2(sin(a), b)",
                engine='python',
                parser='pandas', inplace=True)
    # TODO: Use a series equality validator.
    assert ray_df_equals_pandas(ray_df, df)


def test_eval_df_arithmetic_subexpression():
    df = pd.DataFrame({'a': np.random.randn(10),
                       'b': np.random.randn(10)})
    ray_df = from_pandas(df, 2)
    df.eval("not_e = sin(a + b)",
            engine='python',
            parser='pandas', inplace=True)
    ray_df.eval("not_e = sin(a + b)",
                engine='python',
                parser='pandas', inplace=True)
    # TODO: Use a series equality validator.
    assert ray_df_equals_pandas(ray_df, df)


def test_ewm():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ewm()


def test_expanding():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.expanding()


@pytest.fixture
def test_ffill(num_partitions=2):
    test_data = TestData()
    test_data.tsframe['A'][:5] = np.nan
    test_data.tsframe['A'][-5:] = np.nan
    ray_df = from_pandas(test_data.tsframe, num_partitions)

    assert ray_df_equals_pandas(
        ray_df.ffill(),
        test_data.tsframe.ffill()
    )


def test_fillna():
    test_fillna_sanity()
    test_fillna_downcast()
    test_ffill()
    test_ffill2()
    test_bfill()
    test_bfill2()
    test_fillna_inplace()
    # test_frame_fillna_limit()
    # test_frame_pad_backfill_limit()
    test_fillna_dtype_conversion()
    test_fillna_skip_certain_blocks()
    test_fillna_dict_series()

    with pytest.raises(NotImplementedError):
        test_fillna_dataframe()

    test_fillna_columns()
    test_fillna_invalid_method()
    test_fillna_invalid_value()
    test_fillna_col_reordering()


@pytest.fixture
def test_fillna_sanity(num_partitions=2):
    test_data = TestData()
    tf = test_data.tsframe
    tf.loc[tf.index[:5], 'A'] = np.nan
    tf.loc[tf.index[-5:], 'A'] = np.nan

    zero_filled = test_data.tsframe.fillna(0)
    ray_df = from_pandas(test_data.tsframe, num_partitions).fillna(0)
    assert ray_df_equals_pandas(ray_df, zero_filled)

    padded = test_data.tsframe.fillna(method='pad')
    ray_df = from_pandas(test_data.tsframe,
                         num_partitions).fillna(method='pad')
    assert ray_df_equals_pandas(ray_df, padded)

    # mixed type
    mf = test_data.mixed_frame
    mf.loc[mf.index[5:20], 'foo'] = np.nan
    mf.loc[mf.index[-10:], 'A'] = np.nan

    result = test_data.mixed_frame.fillna(value=0)
    ray_df = from_pandas(test_data.mixed_frame,
                         num_partitions).fillna(value=0)
    assert ray_df_equals_pandas(ray_df, result)

    result = test_data.mixed_frame.fillna(method='pad')
    ray_df = from_pandas(test_data.mixed_frame,
                         num_partitions).fillna(method='pad')
    assert ray_df_equals_pandas(ray_df, result)

    pytest.raises(ValueError, test_data.tsframe.fillna)
    pytest.raises(ValueError, from_pandas(test_data.tsframe,
                                          num_partitions).fillna)
    with pytest.raises(ValueError):
        from_pandas(test_data.tsframe, num_partitions).fillna(
            5, method='ffill'
        )

    # mixed numeric (but no float16)
    mf = test_data.mixed_float.reindex(columns=['A', 'B', 'D'])
    mf.loc[mf.index[-10:], 'A'] = np.nan
    result = mf.fillna(value=0)
    ray_df = from_pandas(mf, num_partitions).fillna(value=0)
    assert ray_df_equals_pandas(ray_df, result)

    result = mf.fillna(method='pad')
    ray_df = from_pandas(mf, num_partitions).fillna(method='pad')
    assert ray_df_equals_pandas(ray_df, result)

    # TODO: Use this when Arrow issue resolves:
    # (https://issues.apache.org/jira/browse/ARROW-2122)
    # empty frame (GH #2778)
    # df = DataFrame(columns=['x'])
    # for m in ['pad', 'backfill']:
    #     df.x.fillna(method=m, inplace=True)
    #     df.x.fillna(method=m)

    # with different dtype (GH3386)
    df = pd.DataFrame([['a', 'a', np.nan, 'a'], [
                        'b', 'b', np.nan, 'b'], ['c', 'c', np.nan, 'c']])

    result = df.fillna({2: 'foo'})
    ray_df = from_pandas(df, num_partitions).fillna({2: 'foo'})

    assert ray_df_equals_pandas(ray_df, result)

    ray_df = from_pandas(df, num_partitions)
    df.fillna({2: 'foo'}, inplace=True)
    ray_df.fillna({2: 'foo'}, inplace=True)
    assert ray_df_equals_pandas(ray_df, result)

    # limit and value
    df = pd.DataFrame(np.random.randn(10, 3))
    df.iloc[2:7, 0] = np.nan
    df.iloc[3:5, 2] = np.nan

    # result = df.fillna(999, limit=1)
    # ray_df = from_pandas(df, num_partitions).fillna(999, limit=1)

    # assert ray_df_equals_pandas(ray_df, result)

    # with datelike
    # GH 6344
    df = pd.DataFrame({
        'Date': [pd.NaT, pd.Timestamp("2014-1-1")],
        'Date2': [pd.Timestamp("2013-1-1"), pd.NaT]
    })
    result = df.fillna(value={'Date': df['Date2']})
    ray_df = from_pandas(df, num_partitions).fillna(
        value={'Date': df['Date2']}
    )
    assert ray_df_equals_pandas(ray_df, result)

    # TODO: Use this when Arrow issue resolves:
    # (https://issues.apache.org/jira/browse/ARROW-2122)
    # with timezone
    # GH 15855
    """
    df = pd.DataFrame({'A': [pd.Timestamp('2012-11-11 00:00:00+01:00'),
                             pd.NaT]})
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(ray_df.fillna(method='pad'),
                                df.fillna(method='pad'))

    df = pd.DataFrame({'A': [pd.NaT,
                             pd.Timestamp('2012-11-11 00:00:00+01:00')]})
    ray_df = from_pandas(df, num_partitions).fillna(method='bfill')
    assert ray_df_equals_pandas(ray_df, df.fillna(method='bfill'))
    """


@pytest.fixture
def test_fillna_downcast(num_partitions=2):
    # GH 15277
    # infer int64 from float64
    df = pd.DataFrame({'a': [1., np.nan]})
    result = df.fillna(0, downcast='infer')
    ray_df = from_pandas(df, num_partitions).fillna(0, downcast='infer')
    assert ray_df_equals_pandas(ray_df, result)

    # infer int64 from float64 when fillna value is a dict
    df = pd.DataFrame({'a': [1., np.nan]})
    result = df.fillna({'a': 0}, downcast='infer')
    ray_df = from_pandas(df, num_partitions).fillna(
        {'a': 0}, downcast='infer'
    )
    assert ray_df_equals_pandas(ray_df, result)


@pytest.fixture
def test_ffill2(num_partitions=2):
    test_data = TestData()
    test_data.tsframe['A'][:5] = np.nan
    test_data.tsframe['A'][-5:] = np.nan
    ray_df = from_pandas(test_data.tsframe, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='ffill'),
        test_data.tsframe.fillna(method='ffill')
    )


@pytest.fixture
def test_bfill2(num_partitions=2):
    test_data = TestData()
    test_data.tsframe['A'][:5] = np.nan
    test_data.tsframe['A'][-5:] = np.nan
    ray_df = from_pandas(test_data.tsframe, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='bfill'),
        test_data.tsframe.fillna(method='bfill')
    )


@pytest.fixture
def test_fillna_inplace(num_partitions=2):
    df = pd.DataFrame(np.random.randn(10, 4))
    df[1][:4] = np.nan
    df[3][-4:] = np.nan

    ray_df = from_pandas(df, num_partitions)
    df.fillna(value=0, inplace=True)
    assert not ray_df_equals_pandas(ray_df, df)

    ray_df.fillna(value=0, inplace=True)
    assert ray_df_equals_pandas(ray_df, df)

    ray_df = from_pandas(df, num_partitions).fillna(value={0: 0},
                                                    inplace=True)
    assert ray_df is None

    df[1][:4] = np.nan
    df[3][-4:] = np.nan
    ray_df = from_pandas(df, num_partitions)
    df.fillna(method='ffill', inplace=True)

    assert not ray_df_equals_pandas(ray_df, df)

    ray_df.fillna(method='ffill', inplace=True)
    assert ray_df_equals_pandas(ray_df, df)


@pytest.fixture
def test_frame_fillna_limit(num_partitions=2):
    index = np.arange(10)
    df = pd.DataFrame(np.random.randn(10, 4), index=index)

    expected = df[:2].reindex(index)
    expected = expected.fillna(method='pad', limit=5)

    ray_df = from_pandas(df[:2].reindex(index), num_partitions).fillna(
        method='pad', limit=5
    )
    assert ray_df_equals_pandas(ray_df, expected)

    expected = df[-2:].reindex(index)
    expected = expected.fillna(method='backfill', limit=5)
    ray_df = from_pandas(df[-2:].reindex(index), num_partitions).fillna(
        method='backfill', limit=5
    )
    assert ray_df_equals_pandas(ray_df, expected)


@pytest.fixture
def test_frame_pad_backfill_limit(num_partitions=2):
    index = np.arange(10)
    df = pd.DataFrame(np.random.randn(10, 4), index=index)

    result = df[:2].reindex(index)
    ray_df = from_pandas(result, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='pad', limit=5),
        result.fillna(method='pad', limit=5)
    )

    result = df[-2:].reindex(index)
    ray_df = from_pandas(result, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='backfill', limit=5),
        result.fillna(method='backfill', limit=5)
    )


@pytest.fixture
def test_fillna_dtype_conversion(num_partitions=2):
    # make sure that fillna on an empty frame works
    df = pd.DataFrame(index=["A", "B", "C"], columns=[1, 2, 3, 4, 5])

    # empty block
    df = pd.DataFrame(index=range(3), columns=['A', 'B'], dtype='float64')
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna('nan'),
        df.fillna('nan')
    )

    # equiv of replace
    df = pd.DataFrame(dict(A=[1, np.nan], B=[1., 2.]))
    ray_df = from_pandas(df, num_partitions)
    for v in ['', 1, np.nan, 1.0]:
        assert ray_df_equals_pandas(
            ray_df.fillna(v),
            df.fillna(v)
        )


@pytest.fixture
def test_fillna_skip_certain_blocks(num_partitions=2):
    # don't try to fill boolean, int blocks

    df = pd.DataFrame(np.random.randn(10, 4).astype(int))
    ray_df = from_pandas(df, num_partitions)

    # it works!
    assert ray_df_equals_pandas(
        ray_df.fillna(np.nan),
        df.fillna(np.nan)
    )


@pytest.fixture
def test_fillna_dict_series(num_partitions=2):
    df = pd.DataFrame({'a': [np.nan, 1, 2, np.nan, np.nan],
                       'b': [1, 2, 3, np.nan, np.nan],
                       'c': [np.nan, 1, 2, 3, 4]})
    ray_df = from_pandas(df, num_partitions)

    assert ray_df_equals_pandas(
        ray_df.fillna({'a': 0, 'b': 5}),
        df.fillna({'a': 0, 'b': 5})
    )

    # it works
    assert ray_df_equals_pandas(
        ray_df.fillna({'a': 0, 'b': 5, 'd': 7}),
        df.fillna({'a': 0, 'b': 5, 'd': 7})
    )

    # Series treated same as dict
    assert ray_df_equals_pandas(
        ray_df.fillna(df.max()),
        df.fillna(df.max())
    )


@pytest.fixture
def test_fillna_dataframe(num_partitions=2):
    # GH 8377
    df = pd.DataFrame({'a': [np.nan, 1, 2, np.nan, np.nan],
                       'b': [1, 2, 3, np.nan, np.nan],
                       'c': [np.nan, 1, 2, 3, 4]},
                      index=list('VWXYZ'))
    ray_df = from_pandas(df, num_partitions)

    # df2 may have different index and columns
    df2 = pd.DataFrame({'a': [np.nan, 10, 20, 30, 40],
                        'b': [50, 60, 70, 80, 90],
                        'foo': ['bar'] * 5},
                       index=list('VWXuZ'))

    # only those columns and indices which are shared get filled
    assert ray_df_equals_pandas(
        ray_df.fillna(df2),
        df.fillna(df2)
    )


@pytest.fixture
def test_fillna_columns(num_partitions=2):
    df = pd.DataFrame(np.random.randn(10, 10))
    df.values[:, ::2] = np.nan
    ray_df = from_pandas(df, num_partitions)

    assert ray_df_equals_pandas(
        ray_df.fillna(method='ffill', axis=1),
        df.fillna(method='ffill', axis=1)
    )

    df.insert(6, 'foo', 5)
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='ffill', axis=1),
        df.fillna(method='ffill', axis=1)
    )


@pytest.fixture
def test_fillna_invalid_method(num_partitions=2):
    test_data = TestData()
    ray_df = from_pandas(test_data.frame, num_partitions)
    with tm.assert_raises_regex(ValueError, 'ffil'):
        ray_df.fillna(method='ffil')


@pytest.fixture
def test_fillna_invalid_value(num_partitions=2):
    test_data = TestData()
    ray_df = from_pandas(test_data.frame, num_partitions)
    # list
    pytest.raises(TypeError, ray_df.fillna, [1, 2])
    # tuple
    pytest.raises(TypeError, ray_df.fillna, (1, 2))
    # TODO: Uncomment when iloc is implemented
    # frame with series
    # pytest.raises(ValueError, ray_df.iloc[:, 0].fillna, ray_df)


@pytest.fixture
def test_fillna_col_reordering(num_partitions=2):
    cols = ["COL." + str(i) for i in range(5, 0, -1)]
    data = np.random.rand(20, 5)
    df = pd.DataFrame(index=range(20), columns=cols, data=data)
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna(method='ffill'),
        df.fillna(method='ffill')
    )


"""
TODO: Use this when Arrow issue resolves:
(https://issues.apache.org/jira/browse/ARROW-2122)
@pytest.fixture
def test_fillna_datetime_columns(num_partitions=2):
    # GH 7095
    df = pd.DataFrame({'A': [-1, -2, np.nan],
                       'B': date_range('20130101', periods=3),
                       'C': ['foo', 'bar', None],
                       'D': ['foo2', 'bar2', None]},
                      index=date_range('20130110', periods=3))
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna('?'),
        df.fillna('?')
    )

    df = pd.DataFrame({'A': [-1, -2, np.nan],
                       'B': [pd.Timestamp('2013-01-01'),
                             pd.Timestamp('2013-01-02'), pd.NaT],
                       'C': ['foo', 'bar', None],
                       'D': ['foo2', 'bar2', None]},
                      index=date_range('20130110', periods=3))
    ray_df = from_pandas(df, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.fillna('?'),
        df.fillna('?')
    )
"""


@pytest.fixture
def test_filter(ray_df, pandas_df, by):
    ray_df_equals_pandas(ray_df.filter(items=by['items']),
                         pandas_df.filter(items=by['items']))

    ray_df_equals_pandas(ray_df.filter(regex=by['regex']),
                         pandas_df.filter(regex=by['regex']))

    ray_df_equals_pandas(ray_df.filter(like=by['like']),
                         pandas_df.filter(like=by['like']))


def test_first():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.first(None)


@pytest.fixture
def test_first_valid_index(ray_df, pandas_df):
    assert(ray_df.first_valid_index() == (pandas_df.first_valid_index()))


def test_floordiv():
    test_inter_df_math("floordiv", simple=False)


def test_from_csv():
    with pytest.raises(NotImplementedError):
        rdf.DataFrame.from_csv(None)


def test_from_dict():
    with pytest.raises(NotImplementedError):
        rdf.DataFrame.from_dict(None)


def test_from_items():
    with pytest.raises(NotImplementedError):
        rdf.DataFrame.from_items(None)


def test_from_records():
    with pytest.raises(NotImplementedError):
        rdf.DataFrame.from_records(None)


def test_ge():
    test_comparison_inter_ops("ge")


def test_get_value():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_value(None, None)


def test_get_values():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_values()


def test_gt():
    test_comparison_inter_ops("gt")


@pytest.fixture
def test_head(ray_df, pandas_df, n=5):
    ray_df_equals_pandas(ray_df.head(n), pandas_df.head(n))


def test_hist():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.hist(None)


@pytest.fixture
def test_idxmax(ray_df, pandas_df):
    assert \
        ray_df.idxmax().sort_index().equals(pandas_df.idxmax().sort_index())


@pytest.fixture
def test_idxmin(ray_df, pandas_df):
    assert \
        ray_df.idxmin().sort_index().equals(pandas_df.idxmin().sort_index())


def test_infer_objects():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.infer_objects()


@pytest.fixture
def test_info(ray_df):
    info_string = ray_df.info()
    assert '<class \'ray.dataframe.dataframe.DataFrame\'>\n' in info_string
    info_string = ray_df.info(memory_usage=True)
    assert 'memory_usage: ' in info_string


@pytest.fixture
def test_insert(ray_df, pandas_df, loc, column, value):
    ray_df_cp = ray_df.copy()
    pd_df_cp = pandas_df.copy()

    ray_df_cp.insert(loc, column, value)
    pd_df_cp.insert(loc, column, value)


def test_interpolate():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.interpolate()


@pytest.fixture
def test_items(ray_df, pandas_df):
    ray_items = ray_df.items()
    pandas_items = pandas_df.items()
    for ray_item, pandas_item in zip(ray_items, pandas_items):
        ray_index, ray_series = ray_item
        pandas_index, pandas_series = pandas_item
        assert pandas_series.equals(ray_series)
        assert pandas_index == ray_index


@pytest.fixture
def test_iteritems(ray_df, pandas_df):
    ray_items = ray_df.iteritems()
    pandas_items = pandas_df.iteritems()
    for ray_item, pandas_item in zip(ray_items, pandas_items):
        ray_index, ray_series = ray_item
        pandas_index, pandas_series = pandas_item
        assert pandas_series.equals(ray_series)
        assert pandas_index == ray_index


@pytest.fixture
def test_iterrows(ray_df, pandas_df):
    ray_iterrows = ray_df.iterrows()
    pandas_iterrows = pandas_df.iterrows()
    for ray_row, pandas_row in zip(ray_iterrows, pandas_iterrows):
        ray_index, ray_series = ray_row
        pandas_index, pandas_series = pandas_row
        assert pandas_series.equals(ray_series)
        assert pandas_index == ray_index


@pytest.fixture
def test_itertuples(ray_df, pandas_df):
    # test default
    ray_it_default = ray_df.itertuples()
    pandas_it_default = pandas_df.itertuples()
    for ray_row, pandas_row in zip(ray_it_default, pandas_it_default):
        np.testing.assert_equal(ray_row, pandas_row)

    # test all combinations of custom params
    indices = [True, False]
    names = [None, 'NotPandas', 'Pandas']

    for index in indices:
        for name in names:
            ray_it_custom = ray_df.itertuples(index=index, name=name)
            pandas_it_custom = pandas_df.itertuples(index=index, name=name)
            for ray_row, pandas_row in zip(ray_it_custom, pandas_it_custom):
                np.testing.assert_equal(ray_row, pandas_row)


def test_join():
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                           "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df2 = rdf.DataFrame({"col5": [0], "col6": [1]})

    pandas_df2 = pd.DataFrame({"col5": [0], "col6": [1]})

    join_types = ["left", "right", "outer", "inner"]
    for how in join_types:
        ray_join = ray_df.join(ray_df2, how=how)
        pandas_join = pandas_df.join(pandas_df2, how=how)
        ray_df_equals_pandas(ray_join, pandas_join)

    ray_df3 = rdf.DataFrame({"col7": [1, 2, 3, 5, 6, 7, 8]})

    pandas_df3 = pd.DataFrame({"col7": [1, 2, 3, 5, 6, 7, 8]})

    join_types = ["left", "outer", "inner"]
    for how in join_types:
        ray_join = ray_df.join([ray_df2, ray_df3], how=how)
        pandas_join = pandas_df.join([pandas_df2, pandas_df3], how=how)
        ray_df_equals_pandas(ray_join, pandas_join)


def test_kurt():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.kurt()


def test_kurtosis():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.kurtosis()


def test_last():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.last(None)


@pytest.fixture
def test_last_valid_index(ray_df, pandas_df):
    assert(ray_df.last_valid_index() == (pandas_df.last_valid_index()))


def test_le():
    test_comparison_inter_ops("le")


def test_lookup():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.lookup(None, None)


def test_lt():
    test_comparison_inter_ops("lt")


def test_mad():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mad()


def test_mask():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mask(None)


@pytest.fixture
def test_max(ray_df, pandas_df):
    assert(ray_series_equals_pandas(ray_df.max(), pandas_df.max()))
    assert(ray_series_equals_pandas(ray_df.max(axis=1), pandas_df.max(axis=1)))


@pytest.fixture
def test_mean(ray_df, pandas_df):
    assert ray_df.mean().equals(pandas_df.mean())


@pytest.fixture
def test_median(ray_df, pandas_df):
    assert(ray_df.median().equals(pandas_df.median()))


def test_melt():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.melt()


@pytest.fixture
def test_memory_usage(ray_df):
    assert type(ray_df.memory_usage()) is pd.core.series.Series
    assert ray_df.memory_usage(index=True).at['Index'] is not None
    assert ray_df.memory_usage(deep=True).sum() >= \
        ray_df.memory_usage(deep=False).sum()


def test_merge():
    ray_df = rdf.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                            "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    pandas_df = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [4, 5, 6, 7],
                              "col3": [8, 9, 0, 1], "col4": [2, 4, 5, 6]})

    ray_df2 = rdf.DataFrame({"col1": [0, 1, 2], "col2": [1, 5, 6]})

    pandas_df2 = pd.DataFrame({"col1": [0, 1, 2], "col2": [1, 5, 6]})

    join_types = ["outer", "inner"]
    for how in join_types:
        # Defaults
        ray_result = ray_df.merge(ray_df2, how=how)
        pandas_result = pandas_df.merge(pandas_df2, how=how)
        ray_df_equals_pandas(ray_result, pandas_result)

        # left_on and right_index
        ray_result = ray_df.merge(ray_df2, how=how, left_on='col1',
                                  right_index=True)
        pandas_result = pandas_df.merge(pandas_df2, how=how, left_on='col1',
                                        right_index=True)
        ray_df_equals_pandas(ray_result, pandas_result)

        # left_index and right_index
        ray_result = ray_df.merge(ray_df2, how=how, left_index=True,
                                  right_index=True)
        pandas_result = pandas_df.merge(pandas_df2, how=how, left_index=True,
                                        right_index=True)
        ray_df_equals_pandas(ray_result, pandas_result)

        # left_index and right_on
        ray_result = ray_df.merge(ray_df2, how=how, left_index=True,
                                  right_on='col1')
        pandas_result = pandas_df.merge(pandas_df2, how=how, left_index=True,
                                        right_on='col1')
        ray_df_equals_pandas(ray_result, pandas_result)

        # left_on and right_on col1
        ray_result = ray_df.merge(ray_df2, how=how, left_on='col1',
                                  right_on='col1')
        pandas_result = pandas_df.merge(pandas_df2, how=how, left_on='col1',
                                        right_on='col1')
        ray_df_equals_pandas(ray_result, pandas_result)

        # left_on and right_on col2
        ray_result = ray_df.merge(ray_df2, how=how, left_on='col2',
                                  right_on='col2')
        pandas_result = pandas_df.merge(pandas_df2, how=how, left_on='col2',
                                        right_on='col2')
        ray_df_equals_pandas(ray_result, pandas_result)


@pytest.fixture
def test_min(ray_df, pandas_df):
    assert(ray_series_equals_pandas(ray_df.min(), pandas_df.min()))
    assert(ray_series_equals_pandas(ray_df.min(axis=1), pandas_df.min(axis=1)))


def test_mod():
    test_inter_df_math("mod", simple=False)


@pytest.fixture
def test_mode(ray_df, pandas_df):
    assert(ray_series_equals_pandas(ray_df.mode(), pandas_df.mode()))
    assert(ray_series_equals_pandas(ray_df.mode(axis=1),
           pandas_df.mode(axis=1)))


def test_mul():
    test_inter_df_math("mul", simple=False)


def test_multiply():
    test_inter_df_math("multiply", simple=False)


def test_ne():
    test_comparison_inter_ops("ne")


def test_nlargest():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.nlargest(None, None)


@pytest.fixture
def test_notna(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.notna(), pandas_df.notna()))


@pytest.fixture
def test_notnull(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.notnull(), pandas_df.notnull()))


def test_nsmallest():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.nsmallest(None, None)


@pytest.fixture
def test_nunique(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.nunique(),
           pandas_df.nunique()))
    assert(ray_df_equals_pandas(ray_df.nunique(axis=1),
           pandas_df.nunique(axis=1)))


def test_pct_change():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pct_change()


@pytest.fixture
def test_pipe(ray_df, pandas_df):
    n = len(ray_df.index)
    a, b, c = 2 % n, 0, 3 % n
    col = ray_df.columns[3 % len(ray_df.columns)]

    def h(x):
        return x.drop(columns=[col])

    def g(x, arg1=0):
        for _ in range(arg1):
            x = x.append(x)
        return x

    def f(x, arg2=0, arg3=0):
        return x.drop([arg2, arg3])

    assert ray_df_equals(f(g(h(ray_df), arg1=a), arg2=b, arg3=c),
                         (ray_df.pipe(h)
                                .pipe(g, arg1=a)
                                .pipe(f, arg2=b, arg3=c)))

    assert ray_df_equals_pandas((ray_df.pipe(h)
                                .pipe(g, arg1=a)
                                .pipe(f, arg2=b, arg3=c)),
                                (pandas_df.pipe(h)
                                .pipe(g, arg1=a)
                                .pipe(f, arg2=b, arg3=c)))


def test_pivot():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pivot()


def test_pivot_table():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pivot_table()


def test_plot():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.plot()


@pytest.fixture
def test_pop(ray_df, pandas_df):
    temp_ray_df = ray_df.copy()
    temp_pandas_df = pandas_df.copy()
    ray_popped = temp_ray_df.pop('col2')
    pandas_popped = temp_pandas_df.pop('col2')
    assert ray_popped.sort_index().equals(pandas_popped.sort_index())
    ray_df_equals_pandas(temp_ray_df, temp_pandas_df)


def test_pow():
    test_inter_df_math("pow", simple=False)


@pytest.fixture
def test_prod(ray_df, pandas_df):
    assert(ray_df.prod().equals(pandas_df.prod()))


@pytest.fixture
def test_product(ray_df, pandas_df):
    assert(ray_df.product().equals(pandas_df.product()))


@pytest.fixture
def test_quantile(ray_df, pandas_df, q):
    assert(ray_df.quantile(q).equals(pandas_df.quantile(q)))


@pytest.fixture
def test_query(ray_df, pandas_df, funcs):
    for f in funcs:
        pandas_df_new, ray_df_new = pandas_df.query(f), ray_df.query(f)
        assert pandas_df_new.equals(to_pandas(ray_df_new))


def test_radd():
    test_inter_df_math_right_ops("radd")


@pytest.fixture
def test_rank(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.rank(), pandas_df.rank()))
    assert(ray_df_equals_pandas(ray_df.rank(axis=1), pandas_df.rank(axis=1)))


def test_rdiv():
    test_inter_df_math_right_ops("rdiv")


def test_reindex():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.reindex()


def test_reindex_axis():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.reindex_axis(None)


def test_reindex_like():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.reindex_like(None)


# Renaming

def test_rename():
    test_rename_sanity()
    test_rename_multiindex()
    # TODO: Uncomment when __setitem__ is implemented
    # test_rename_nocopy()
    test_rename_inplace()
    test_rename_bug()


@pytest.fixture
def test_rename_sanity(num_partitions=2):
    test_data = TestData()
    mapping = {
        'A': 'a',
        'B': 'b',
        'C': 'c',
        'D': 'd'
    }

    ray_df = from_pandas(test_data.frame, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.rename(columns=mapping),
        test_data.frame.rename(columns=mapping)
    )

    renamed2 = test_data.frame.rename(columns=str.lower)
    assert ray_df_equals_pandas(
        ray_df.rename(columns=str.lower),
        renamed2
    )

    ray_df = from_pandas(renamed2, num_partitions)
    assert ray_df_equals_pandas(
        ray_df.rename(columns=str.upper),
        renamed2.rename(columns=str.upper)
    )

    # index
    data = {
        'A': {'foo': 0, 'bar': 1}
    }

    # gets sorted alphabetical
    df = pd.DataFrame(data)
    ray_df = from_pandas(df, num_partitions)
    tm.assert_index_equal(
        ray_df.rename(index={'foo': 'bar', 'bar': 'foo'}).index,
        df.rename(index={'foo': 'bar', 'bar': 'foo'}).index
    )

    tm.assert_index_equal(
        ray_df.rename(index=str.upper).index,
        df.rename(index=str.upper).index
    )

    # have to pass something
    pytest.raises(TypeError, ray_df.rename)

    # partial columns
    renamed = test_data.frame.rename(columns={'C': 'foo', 'D': 'bar'})
    ray_df = from_pandas(test_data.frame, num_partitions)
    tm.assert_index_equal(
        ray_df.rename(columns={'C': 'foo', 'D': 'bar'}).index,
        test_data.frame.rename(columns={'C': 'foo', 'D': 'bar'}).index
    )

    # TODO: Uncomment when transpose works
    # other axis
    # renamed = test_data.frame.T.rename(index={'C': 'foo', 'D': 'bar'})
    # tm.assert_index_equal(
    #     test_data.frame.T.rename(index={'C': 'foo', 'D': 'bar'}).index,
    #     ray_df.T.rename(index={'C': 'foo', 'D': 'bar'}).index
    # )

    # index with name
    index = pd.Index(['foo', 'bar'], name='name')
    renamer = pd.DataFrame(data, index=index)

    ray_df = from_pandas(renamer, num_partitions)
    renamed = renamer.rename(index={'foo': 'bar', 'bar': 'foo'})
    ray_renamed = ray_df.rename(index={'foo': 'bar', 'bar': 'foo'})
    tm.assert_index_equal(
        renamed.index, ray_renamed.index
    )

    assert renamed.index.name == ray_renamed.index.name


@pytest.fixture
def test_rename_multiindex(num_partitions=2):
    tuples_index = [('foo1', 'bar1'), ('foo2', 'bar2')]
    tuples_columns = [('fizz1', 'buzz1'), ('fizz2', 'buzz2')]
    index = pd.MultiIndex.from_tuples(tuples_index, names=['foo', 'bar'])
    columns = pd.MultiIndex.from_tuples(
        tuples_columns, names=['fizz', 'buzz'])
    df = pd.DataFrame([(0, 0), (1, 1)], index=index, columns=columns)
    ray_df = from_pandas(df, num_partitions)

    #
    # without specifying level -> accross all levels
    renamed = df.rename(index={'foo1': 'foo3', 'bar2': 'bar3'},
                        columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'})
    ray_renamed = ray_df.rename(index={'foo1': 'foo3', 'bar2': 'bar3'},
                                columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'})
    tm.assert_index_equal(
        renamed.index, ray_renamed.index
    )

    renamed = df.rename(index={'foo1': 'foo3', 'bar2': 'bar3'},
                        columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'})
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)
    assert renamed.index.names == ray_renamed.index.names
    assert renamed.columns.names == ray_renamed.columns.names

    #
    # with specifying a level (GH13766)

    # dict
    renamed = df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                        level=0)
    ray_renamed = ray_df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                                level=0)
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)
    renamed = df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                        level='fizz')
    ray_renamed = ray_df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                                level='fizz')
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)

    renamed = df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                        level=1)
    ray_renamed = ray_df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                                level=1)
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)
    renamed = df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                        level='buzz')
    ray_renamed = ray_df.rename(columns={'fizz1': 'fizz3', 'buzz2': 'buzz3'},
                                level='buzz')
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)

    # function
    func = str.upper
    renamed = df.rename(columns=func, level=0)
    ray_renamed = ray_df.rename(columns=func, level=0)
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)
    renamed = df.rename(columns=func, level='fizz')
    ray_renamed = ray_df.rename(columns=func, level='fizz')
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)

    renamed = df.rename(columns=func, level=1)
    ray_renamed = ray_df.rename(columns=func, level=1)
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)
    renamed = df.rename(columns=func, level='buzz')
    ray_renamed = ray_df.rename(columns=func, level='buzz')
    tm.assert_index_equal(renamed.columns, ray_renamed.columns)

    # index
    renamed = df.rename(index={'foo1': 'foo3', 'bar2': 'bar3'},
                        level=0)
    ray_renamed = ray_df.rename(index={'foo1': 'foo3', 'bar2': 'bar3'},
                                level=0)
    tm.assert_index_equal(ray_renamed.index, renamed.index)


@pytest.fixture
def test_rename_nocopy(num_partitions=2):
    test_data = TestData().frame
    ray_df = from_pandas(test_data, num_partitions)
    ray_renamed = ray_df.rename(columns={'C': 'foo'}, copy=False)
    ray_renamed['foo'] = 1
    assert (ray_df['C'] == 1).all()


@pytest.fixture
def test_rename_inplace(num_partitions=2):
    test_data = TestData().frame
    ray_df = from_pandas(test_data, num_partitions)

    assert ray_df_equals_pandas(
        ray_df.rename(columns={'C': 'foo'}),
        test_data.rename(columns={'C': 'foo'})
    )

    frame = test_data.copy()
    ray_frame = ray_df.copy()
    frame.rename(columns={'C': 'foo'}, inplace=True)
    ray_frame.rename(columns={'C': 'foo'}, inplace=True)

    assert ray_df_equals_pandas(
        ray_frame,
        frame
    )


@pytest.fixture
def test_rename_bug(num_partitions=2):
    # GH 5344
    # rename set ref_locs, and set_index was not resetting
    df = pd.DataFrame({0: ['foo', 'bar'], 1: ['bah', 'bas'], 2: [1, 2]})
    ray_df = from_pandas(df, num_partitions)
    df = df.rename(columns={0: 'a'})
    df = df.rename(columns={1: 'b'})
    # TODO: Uncomment when set_index is implemented
    # df = df.set_index(['a', 'b'])
    # df.columns = ['2001-01-01']

    ray_df = ray_df.rename(columns={0: 'a'})
    ray_df = ray_df.rename(columns={1: 'b'})
    # TODO: Uncomment when set_index is implemented
    # ray_df = ray_df.set_index(['a', 'b'])
    # ray_df.columns = ['2001-01-01']

    assert ray_df_equals_pandas(
        ray_df,
        df
    )


def test_rename_axis():
    test_rename_axis_inplace()


@pytest.fixture
def test_rename_axis_inplace(num_partitions=2):
    test_frame = TestData().frame
    ray_df = from_pandas(test_frame, num_partitions)

    # GH 15704
    result = test_frame.copy()
    ray_result = ray_df.copy()
    no_return = result.rename_axis('foo', inplace=True)
    ray_no_return = ray_result.rename_axis('foo', inplace=True)

    assert no_return is ray_no_return
    assert ray_df_equals_pandas(
        ray_result,
        result
    )

    result = test_frame.copy()
    ray_result = ray_df.copy()
    no_return = result.rename_axis('bar', axis=1, inplace=True)
    ray_no_return = ray_result.rename_axis('bar', axis=1, inplace=True)

    assert no_return is ray_no_return
    assert ray_df_equals_pandas(
        ray_result,
        result
    )


def test_reorder_levels():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.reorder_levels(None)


def test_replace():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.replace()


def test_resample():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.resample(None)


@pytest.fixture
def test_reset_index(ray_df, pandas_df, inplace=False):
    if not inplace:
        assert to_pandas(ray_df.reset_index(inplace=inplace)).equals(
            pandas_df.reset_index(inplace=inplace))
    else:
        ray_df_cp = ray_df.copy()
        pd_df_cp = pandas_df.copy()
        ray_df_cp.reset_index(inplace=inplace)
        pd_df_cp.reset_index(inplace=inplace)
        assert to_pandas(ray_df_cp).equals(pd_df_cp)


def test_rfloordiv():
    test_inter_df_math_right_ops("rfloordiv")


def test_rmod():
    test_inter_df_math_right_ops("rmod")


def test_rmul():
    test_inter_df_math_right_ops("rmul")


def test_rolling():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rolling(None)


@pytest.fixture
def test_round(ray_df, pd_df):
    assert ray_df_equals_pandas(ray_df.round(), pd_df.round())
    assert ray_df_equals_pandas(ray_df.round(1), pd_df.round(1))


def test_rpow():
    test_inter_df_math_right_ops("rpow")


def test_rsub():
    test_inter_df_math_right_ops("rsub")


def test_rtruediv():
    test_inter_df_math_right_ops("rtruediv")


def test_sample():
    ray_df = create_test_dataframe()
    assert len(ray_df.sample(n=4)) == 4
    assert len(ray_df.sample(frac=0.5)) == 2


def test_select():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.select(None)


def test_select_dtypes():
    df = pd.DataFrame({'test1': list('abc'),
                       'test2': np.arange(3, 6).astype('u1'),
                       'test3': np.arange(8.0, 11.0, dtype='float64'),
                       'test4': [True, False, True],
                       'test5': pd.date_range('now', periods=3).values,
                       'test6': list(range(5, 8))})
    include = np.float, 'integer'
    exclude = np.bool_,
    rd = from_pandas(df, 2)
    r = rd.select_dtypes(include=include, exclude=exclude)

    e = df[["test2", "test3", "test6"]]
    assert(ray_df_equals_pandas(r, e))

    try:
        rdf.DataFrame().select_dtypes()
        assert(False)
    except ValueError:
        assert(True)


def test_sem():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sem()


@pytest.fixture
def test_set_axis(ray_df, pandas_df, label, axis):
    assert to_pandas(ray_df.set_axis(label, axis, inplace=False)).equals(
        pandas_df.set_axis(label, axis, inplace=False))


@pytest.fixture
def test_set_index(ray_df, pandas_df, keys, inplace=False):
    if not inplace:
        assert to_pandas(ray_df.set_index(keys)).equals(
            pandas_df.set_index(keys))
    else:
        ray_df_cp = ray_df.copy()
        pd_df_cp = pandas_df.copy()
        ray_df_cp.set_index(keys, inplace=inplace)
        pd_df_cp.set_index(keys, inplace=inplace)
        assert to_pandas(ray_df_cp).equals(pd_df_cp)


def test_set_value():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.set_value(None, None, None)


def test_shift():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.shift()


@pytest.fixture
def test_skew(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.skew(),
           pandas_df.skew()))
    assert(ray_df_equals_pandas(ray_df.skew(axis=1),
           pandas_df.skew(axis=1)))


def test_slice_shift():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.slice_shift()


def test_sort_index():
    pandas_df = pd.DataFrame(np.random.randint(0, 100, size=(1000, 100)))
    ray_df = rdf.DataFrame(pandas_df)

    pandas_result = pandas_df.sort_index()
    ray_result = ray_df.sort_index()

    ray_df_equals_pandas(ray_result, pandas_result)

    pandas_result = pandas_df.sort_index(ascending=False)
    ray_result = ray_df.sort_index(ascending=False)

    ray_df_equals_pandas(ray_result, pandas_result)


def test_sort_values():
    pandas_df = pd.DataFrame(np.random.randint(0, 100, size=(1000, 100)))
    ray_df = rdf.DataFrame(pandas_df)

    pandas_result = pandas_df.sort_values(by=1)
    ray_result = ray_df.sort_values(by=1)

    ray_df_equals_pandas(ray_result, pandas_result)

    pandas_result = pandas_df.sort_values(by=1, axis=1)
    ray_result = ray_df.sort_values(by=1, axis=1)

    ray_df_equals_pandas(ray_result, pandas_result)

    pandas_result = pandas_df.sort_values(by=[1, 3])
    ray_result = ray_df.sort_values(by=[1, 3])

    ray_df_equals_pandas(ray_result, pandas_result)

    pandas_result = pandas_df.sort_values(by=[1, 67], axis=1)
    ray_result = ray_df.sort_values(by=[1, 67], axis=1)

    ray_df_equals_pandas(ray_result, pandas_result)


def test_sortlevel():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sortlevel()


def test_squeeze():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.squeeze()


def test_stack():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.stack()


@pytest.fixture
def test_std(ray_df, pandas_df):
    assert(ray_df.std().equals(pandas_df.std()))


def test_sub():
    test_inter_df_math("sub", simple=False)


def test_subtract():
    test_inter_df_math("subtract", simple=False)


def test_swapaxes():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.swapaxes(None, None)


def test_swaplevel():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.swaplevel()


@pytest.fixture
def test_tail(ray_df, pandas_df):
    ray_df_equals_pandas(ray_df.tail(), pandas_df.tail())


def test_take():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.take(None)


def test_to_records():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_records()


def test_to_sparse():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_sparse()


def test_to_string():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_string()


def test_to_timestamp():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_timestamp()


def test_to_xarray():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_xarray()


@pytest.fixture
def test_transform(ray_df, pandas_df):
    ray_df_equals_pandas(ray_df.transform(lambda df: df.isna()),
                         pandas_df.transform(lambda df: df.isna()))
    ray_df_equals_pandas(ray_df.transform('isna'),
                         pandas_df.transform('isna'))


def test_truediv():
    test_inter_df_math("truediv", simple=False)


def test_truncate():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.truncate()


def test_tshift():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.tshift()


def test_tz_convert():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.tz_convert(None)


def test_tz_localize():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.tz_localize(None)


def test_unstack():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.unstack()


def test_update():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.update(None)


@pytest.fixture
def test_var(ray_df, pandas_df):
    assert(ray_df.var().equals(pandas_df.var()))


def test_where():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.where(None)


def test_xs():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.xs(None)


@pytest.fixture
def test___getitem__(ray_df, pd_df):
    ray_col = ray_df.__getitem__('col1')
    assert isinstance(ray_col, pd.Series)

    pd_col = pd_df['col1']
    assert pd_col.equals(ray_col)


def test___getattr__():
    df = create_test_dataframe()

    col = df.__getattr__("col1")
    assert isinstance(col, pd.Series)

    col = getattr(df, "col1")
    assert isinstance(col, pd.Series)

    col = df.col1
    assert isinstance(col, pd.Series)

    # Check that lookup in column doesn't override other attributes
    df2 = df.rename(index=str, columns={"col5": "columns"})
    assert isinstance(df2.columns, pd.Index)


def test___setitem__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__setitem__(None, None)


@pytest.fixture
def test___len__(ray_df, pandas_df):
    assert((len(ray_df) == len(pandas_df)))


def test___unicode__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__unicode__()


@pytest.fixture
def test___neg__(ray_df, pd_df):
    ray_df_neg = ray_df.__neg__()
    assert pd_df.__neg__().equals(to_pandas(ray_df_neg))


def test___invert__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__invert__()


def test___hash__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__hash__()


@pytest.fixture
def test___iter__(ray_df, pd_df):
    ray_iterator = ray_df.__iter__()

    # Check that ray_iterator implements the iterator interface
    assert hasattr(ray_iterator, '__iter__')
    assert hasattr(ray_iterator, 'next') or hasattr(ray_iterator, '__next__')

    pd_iterator = pd_df.__iter__()
    assert list(ray_iterator) == list(pd_iterator)


@pytest.fixture
def test___contains__(ray_df, key, result):
    assert result == ray_df.__contains__(key)
    assert result == (key in ray_df)


def test___nonzero__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__nonzero__()


def test___bool__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__bool__()


@pytest.fixture
def test___abs__(ray_df, pandas_df):
    assert(ray_df_equals_pandas(abs(ray_df), abs(pandas_df)))


def test___round__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__round__()


@pytest.fixture
def test___array__(ray_df, pandas_df):
    assert np.array_equal(ray_df.__array__(), pandas_df.__array__())


def test___array_wrap__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__array_wrap__(None)


def test___getstate__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__getstate__()


def test___setstate__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__setstate__(None)


@pytest.fixture
def test___delitem__(ray_df, pd_df):
    ray_df = ray_df.copy()
    pd_df = pd_df.copy()
    ray_df.__delitem__('col1')
    pd_df.__delitem__('col1')
    ray_df_equals_pandas(ray_df, pd_df)


def test___finalize__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__finalize__(None)


@pytest.fixture
def test___copy__(ray_df, pd_df):
    ray_df_copy, pd_df_copy = ray_df.__copy__(), pd_df.__copy__()
    assert ray_df_equals_pandas(ray_df_copy, pd_df_copy)


@pytest.fixture
def test___deepcopy__(ray_df, pd_df):
    ray_df_copy, pd_df_copy = ray_df.__deepcopy__(), pd_df.__deepcopy__()
    assert ray_df_equals_pandas(ray_df_copy, pd_df_copy)


def test_blocks():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.blocks


def test_style():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.style


def test_iat():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.iat()


def test___rsub__():
    test_inter_df_math_right_ops("__rsub__")


@pytest.fixture
def test_loc(ray_df, pd_df):
    # Singleton
    assert ray_df.loc[0].equals(pd_df.loc[0])
    assert ray_df.loc[0, 'col1'] == pd_df.loc[0, 'col1']

    # List
    assert ray_df.loc[[1, 2]].equals(pd_df.loc[[1, 2]])
    assert ray_df.loc[[1, 2], ['col1']].equals(pd_df.loc[[1, 2], ['col1']])

    # Slice
    assert ray_df.loc[1:, 'col1'].equals(pd_df.loc[1:, 'col1'])
    assert ray_df.loc[1:2, 'col1'].equals(pd_df.loc[1:2, 'col1'])
    assert ray_df.loc[1:2, 'col1':'col2'].equals(pd_df.loc[1:2, 'col1':'col2'])


def test_is_copy():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.is_copy


def test___itruediv__():
    test_inter_df_math("__itruediv__", simple=True)


def test___div__():
    test_inter_df_math("__div__", simple=True)


def test_at():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.at()


def test_ix():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ix()


@pytest.fixture
def test_iloc(ray_df, pd_df):
    # Singleton
    assert ray_df.iloc[0].equals(pd_df.iloc[0])
    assert ray_df.iloc[0, 1] == pd_df.iloc[0, 1]

    # List
    assert ray_df.iloc[[1, 2]].equals(pd_df.iloc[[1, 2]])
    assert ray_df.iloc[[1, 2], [1, 0]].equals(pd_df.iloc[[1, 2], [1, 0]])

    # Slice
    assert ray_df.iloc[1:, 0].equals(pd_df.iloc[1:, 0])
    assert ray_df.iloc[1:2, 0].equals(pd_df.iloc[1:2, 0])
    assert ray_df.iloc[1:2, 0:2].equals(pd_df.iloc[1:2, 0:2])


def test__doc__():
    assert rdf.DataFrame.__doc__ != pd.DataFrame.__doc__
    assert rdf.DataFrame.__init__ != pd.DataFrame.__init__
    for attr, obj in rdf.DataFrame.__dict__.items():
        if (callable(obj) or isinstance(obj, property)) \
                and attr != "__init__":
            pd_obj = getattr(pd.DataFrame, attr, None)
            if callable(pd_obj) or isinstance(pd_obj, property):
                assert obj.__doc__ == pd_obj.__doc__


def test_to_datetime():
    ray_df = rdf.DataFrame({'year': [2015, 2016],
                            'month': [2, 3],
                            'day': [4, 5]})
    pd_df = pd.DataFrame({'year': [2015, 2016],
                          'month': [2, 3],
                          'day': [4, 5]})

    rdf.to_datetime(ray_df).equals(pd.to_datetime(pd_df))


def test_get_dummies():
    ray_df = rdf.DataFrame({'A': ['a', 'b', 'a'],
                            'B': ['b', 'a', 'c'],
                            'C': [1, 2, 3]})
    pd_df = pd.DataFrame({'A': ['a', 'b', 'a'],
                          'B': ['b', 'a', 'c'],
                          'C': [1, 2, 3]})

    ray_df_equals_pandas(rdf.get_dummies(ray_df), pd.get_dummies(pd_df))
