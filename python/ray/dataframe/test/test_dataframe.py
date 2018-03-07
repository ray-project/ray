from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas as pd
import ray.dataframe as rdf


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return rdf.to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


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

    assert(new_ray_df is not ray_df)
    assert(new_ray_df._df == ray_df._df)


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

    return rdf.from_pandas(df, 2)


def test_int_dataframe():

    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15],
                              'col5': [0, 0, 0, 0]})
    ray_df = rdf.from_pandas(pandas_df, 2)

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
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

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

    test_loc(ray_df, pandas_df)
    test_iloc(ray_df, pandas_df)

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


def test_float_dataframe():

    pandas_df = pd.DataFrame({'col1': [0.0, 1.0, 2.0, 3.0],
                              'col2': [4.0, 5.0, 6.0, 7.0],
                              'col3': [8.0, 9.0, 10.0, 11.0],
                              'col4': [12.0, 13.0, 14.0, 15.0],
                              'col5': [0.0, 0.0, 0.0, 0.0]})

    ray_df = rdf.from_pandas(pandas_df, 3)

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
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

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

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    test_loc(ray_df, pandas_df)
    test_iloc(ray_df, pandas_df)

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
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])


def test_mixed_dtype_dataframe():
    pandas_df = pd.DataFrame({
        'col1': [1, 2, 3, 4],
        'col2': [4, 5, 6, 7],
        'col3': [8.0, 9.4, 10.1, 11.3],
        'col4': ['a', 'b', 'c', 'd']})

    ray_df = rdf.from_pandas(pandas_df, 2)

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

    with pytest.raises(TypeError):
        test_abs(ray_df, pandas_df)
        test___abs__(ray_df, pandas_df)

    test_keys(ray_df, pandas_df)
    test_transpose(ray_df, pandas_df)
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

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

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    test_loc(ray_df, pandas_df)
    test_iloc(ray_df, pandas_df)

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
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])


def test_nan_dataframe():
    pandas_df = pd.DataFrame({
        'col1': [1, 2, 3, np.nan],
        'col2': [4, 5, np.nan, 7],
        'col3': [8, np.nan, 10, 11],
        'col4': [np.nan, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

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
    test_round(ray_df, pandas_df)
    test_query(ray_df, pandas_df, query_funcs)

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

    for key in keys:
        test_get(ray_df, pandas_df, key)

    test_get_dtype_counts(ray_df, pandas_df)
    test_get_ftype_counts(ray_df, pandas_df)
    test_iterrows(ray_df, pandas_df)
    test_items(ray_df, pandas_df)
    test_iteritems(ray_df, pandas_df)
    test_itertuples(ray_df, pandas_df)

    test_loc(ray_df, pandas_df)
    test_iloc(ray_df, pandas_df)

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
        test_insert(ray_df, pandas_df, 0, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 0, "New Column", pandas_df[key])
        test_insert(ray_df, pandas_df, 1, "New Column", ray_df[key])
        test_insert(ray_df, pandas_df, 4, "New Column", ray_df[key])


def test_add():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.add(None)


def test_agg():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.agg(None)


def test_aggregate():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.aggregate(None)


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.append(None)


def test_apply():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.apply(None)


def test_as_blocks():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.as_blocks()


def test_as_matrix():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.as_matrix()


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.astype(None)


def test_at_time():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.at_time(None)


def test_between_time():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.between_time(None, None)


def test_bfill():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.between_time(None, None)


@pytest.fixture
def test_bool(ray_df, pd_df):
    with pytest.raises(ValueError):
        ray_df.bool()
        pd_df.bool()

    single_bool_pd_df = pd.DataFrame([True])
    single_bool_ray_df = rdf.from_pandas(single_bool_pd_df, 1)

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


def test_cummax():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.cummax()


def test_cummin():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.cummin()


def test_cumprod():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.cumprod()


def test_cumsum():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.cumsum()


def test_describe():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.describe()


def test_diff():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.diff()


def test_div():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.div(None)


def test_divide():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.divide(None)


def test_dot():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.dot(None)


def test_drop():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.drop()


def test_drop_duplicates():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.drop_duplicates()


def test_duplicated():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.duplicated()


def test_eq():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.eq(None)


def test_equals():
    pandas_df1 = pd.DataFrame({'col1': [2.9, 3, 3, 3],
                               'col2': [2, 3, 4, 1]})
    ray_df1 = rdf.from_pandas(pandas_df1, 2)
    ray_df2 = rdf.from_pandas(pandas_df1, 3)

    assert ray_df1.equals(ray_df2)

    pandas_df2 = pd.DataFrame({'col1': [2.9, 3, 3, 3],
                               'col2': [2, 3, 5, 1]})
    ray_df3 = rdf.from_pandas(pandas_df2, 4)

    assert not ray_df3.equals(ray_df1)
    assert not ray_df3.equals(ray_df2)


def test_eval():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.eval(None)


def test_ewm():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ewm()


def test_expanding():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.expanding()


def test_ffill():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ffill()


def test_fillna():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.fillna()


def test_filter():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.filter()


def test_first():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.first(None)


def test_first_valid_index():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.first_valid_index()


def test_floordiv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.floordiv(None)


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ge(None)


def test_get_value():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_value(None, None)


def test_get_values():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_values()


def test_gt():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.gt(None)


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


def test_info():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.info()


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.join(None)


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


def test_last_valid_index():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.last_valid_index()


def test_le():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.le(None)


def test_lookup():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.lookup(None, None)


def test_lt():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.lt(None)


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
    assert(ray_df_equals_pandas(ray_df.max(), pandas_df.max()))


def test_mean():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mean()


def test_median():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.median()


def test_melt():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.melt()


def test_memory_usage():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.memory_usage()


def test_merge():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.merge(None)


@pytest.fixture
def test_min(ray_df, pandas_df):
    assert(ray_df_equals_pandas(ray_df.min(), pandas_df.min()))


def test_mod():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mod(None)


def test_mode():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mode()


def test_mul():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.mul(None)


def test_multiply():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.multiply(None)


def test_ne():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.ne(None)


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


def test_nunique():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.nunique()


def test_pct_change():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pct_change()


def test_pipe():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pipe(None)


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
    temp_ray_df = ray_df._map_partitions(lambda df: df)
    temp_pandas_df = pandas_df.copy()
    ray_popped = temp_ray_df.pop('col2')
    pandas_popped = temp_pandas_df.pop('col2')
    assert ray_popped.sort_index().equals(pandas_popped.sort_index())
    ray_df_equals_pandas(temp_ray_df, temp_pandas_df)


def test_pow():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pow(None)


def test_prod():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.prod()


def test_product():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.product()


def test_quantile():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.quantile()


@pytest.fixture
def test_query(ray_df, pandas_df, funcs):

    for f in funcs:
        pandas_df_new, ray_df_new = pandas_df.query(f), ray_df.query(f)
        assert pandas_df_new.equals(rdf.to_pandas(ray_df_new))


def test_radd():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.radd(None)


def test_rank():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rank()


def test_rdiv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rdiv(None)


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


def test_rename():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rename()


def test_rename_axis():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rename_axis(None)


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
        print(rdf.to_pandas(ray_df.reset_index(inplace=inplace)).index)
        print(pandas_df.reset_index(inplace=inplace))
        assert rdf.to_pandas(ray_df.reset_index(inplace=inplace)).equals(
            pandas_df.reset_index(inplace=inplace))
    else:
        ray_df_cp = ray_df.copy()
        pd_df_cp = pandas_df.copy()
        ray_df_cp.reset_index(inplace=inplace)
        pd_df_cp.reset_index(inplace=inplace)
        assert rdf.to_pandas(ray_df_cp).equals(pd_df_cp)


def test_rfloordiv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rfloordiv(None)


def test_rmod():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rmod(None)


def test_rmul():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rmul(None)


def test_rolling():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rolling(None)


@pytest.fixture
def test_round(ray_df, pd_df):
    assert ray_df_equals_pandas(ray_df.round(), pd_df.round())
    assert ray_df_equals_pandas(ray_df.round(1), pd_df.round(1))


def test_rpow():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rpow(None)


def test_rsub():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rsub(None)


def test_rtruediv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.rtruediv(None)


def test_sample():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sample()


def test_select():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.select(None)


def test_select_dtypes():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.select_dtypes()


def test_sem():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sem()


@pytest.fixture
def test_set_axis(ray_df, pandas_df, label, axis):
    assert rdf.to_pandas(ray_df.set_axis(label, axis, inplace=False)).equals(
        pandas_df.set_axis(label, axis, inplace=False))


@pytest.fixture
def test_set_index(ray_df, pandas_df, keys, inplace=False):
    if not inplace:
        assert rdf.to_pandas(ray_df.set_index(keys)).equals(
            pandas_df.set_index(keys))
    else:
        ray_df_cp = ray_df.copy()
        pd_df_cp = pandas_df.copy()
        ray_df_cp.set_index(keys, inplace=inplace)
        pd_df_cp.set_index(keys, inplace=inplace)
        assert rdf.to_pandas(ray_df_cp).equals(pd_df_cp)


def test_set_value():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.set_value(None, None, None)


def test_shift():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.shift()


def test_skew():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.skew()


def test_slice_shift():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.slice_shift()


def test_sort_index():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sort_index()


def test_sort_values():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sort_values(None)


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


def test_std():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.std()


def test_sub():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.sub(None)


def test_subtract():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.subtract(None)


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


def test_to_clipboard():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_clipboard()


def test_to_csv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_csv()


def test_to_dense():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_dense()


def test_to_dict():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_dict()


def test_to_excel():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_excel(None)


def test_to_feather():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_feather(None)


def test_to_gbq():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_gbq(None, None)


def test_to_hdf():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_hdf(None, None)


def test_to_html():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_html()


def test_to_json():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_json()


def test_to_latex():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_latex()


def test_to_msgpack():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_msgpack()


def test_to_panel():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_panel()


def test_to_parquet():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_parquet(None)


def test_to_period():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_period()


def test_to_pickle():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_pickle(None)


def test_to_records():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_records()


def test_to_sparse():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_sparse()


def test_to_sql():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_sql(None, None)


def test_to_stata():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_stata(None)


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


def test_transform():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.transform(None)


def test_truediv():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.truediv(None)


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


def test_var():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.var()


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


def test___setitem__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__setitem__(None, None)


def test___len__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__len__()


def test___unicode__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__unicode__()


@pytest.fixture
def test___neg__(ray_df, pd_df):
    ray_df_neg = ray_df.__neg__()
    assert pd_df.__neg__().equals(rdf.to_pandas(ray_df_neg))


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


def test___array__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__array__()


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__rsub__(None, None, None)


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__itruediv__()


def test___div__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__div__(None)


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
