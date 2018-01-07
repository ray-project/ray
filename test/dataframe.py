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


def test_add():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.add(None)


def test_agg():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.agg(None)


def test_aggregate():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.aggregate(None)


def test_align():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.align(None)


def test_all():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.all()


def test_any():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.any()


def test_append():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.append(None)


def test_apply():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.apply(None)


def test_as_blocks():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.as_blocks()


def test_as_matrix():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.as_matrix()


def test_asfreq():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.asfreq(None)


def test_asof():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.asof(None)


def test_assign():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.assign()


def test_astype():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.astype(None)


def test_at_time():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.at_time(None)


def test_between_time():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.between_time(None, None)


def test_bfill():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.bfill()


def test_bool():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.bool()


def test_boxplot():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.boxplot()


def test_clip():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.clip()


def test_clip_lower():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.clip_lower(None)


def test_clip_upper():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.clip_upper(None)


def test_combine():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.combine(None, None)


def test_combine_first():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.combine_first(None)


def test_compound():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.compound()


def test_consolidate():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.consolidate()


def test_convert_objects():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.convert_objects()


def test_corr():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.corr()


def test_corrwith():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.corrwith(None)


def test_count():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.count()


def test_cov():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.cov()


def test_cummax():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.cummax()


def test_cummin():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.cummin()


def test_cumprod():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.cumprod()


def test_cumsum():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.cumsum()


def test_describe():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.describe()


def test_diff():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.diff()


def test_div():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.div(None)


def test_divide():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.divide(None)


def test_dot():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.dot(None)


def test_drop():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.drop()


def test_drop_duplicates():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.drop_duplicates()


def test_duplicated():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.duplicated()


def test_eq():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.eq(None)


def test_equals():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.equals(None)


def test_eval():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.eval(None)


def test_ewm():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.ewm()


def test_expanding():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.expanding()


def test_ffill():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.ffill()


def test_fillna():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.fillna()


def test_filter():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.filter()


def test_first():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.first(None)


def test_first_valid_index():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.first_valid_index()


def test_floordiv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

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
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.ge(None)


def test_get():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.get(None)


def test_get_dtype_counts():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.get_dtype_counts()


def test_get_ftype_counts():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.get_ftype_counts()


def test_get_value():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.get_value(None, None)


def test_get_values():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.get_values()


def test_gt():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.gt(None)


def test_head():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.head()


def test_hist():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.hist(None)


def test_idxmax():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.idxmax()


def test_idxmin():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.idxmin()


def test_infer_objects():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.infer_objects()


def test_info():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.info()


def test_insert():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.insert(None, None, None)


def test_interpolate():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.interpolate()


def test_items():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.items()


def test_iteritems():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.iteritems()


def test_iterrows():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.iterrows()


def test_itertuples():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.itertuples()


def test_join():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.join(None)


def test_kurt():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.kurt()


def test_kurtosis():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.kurtosis()


def test_last():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.last(None)


def test_last_valid_index():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.last_valid_index()


def test_le():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.le(None)


def test_lookup():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.lookup(None, None)


def test_lt():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.lt(None)


def test_mad():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mad()


def test_mask():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mask(None)


def test_max():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.max()


def test_mean():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mean()


def test_median():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.median()


def test_melt():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.melt()


def test_memory_usage():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.memory_usage()


def test_merge():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.merge(None)


def test_min():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.min()


def test_mod():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mod(None)


def test_mode():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mode()


def test_mul():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.mul(None)


def test_multiply():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.multiply(None)


def test_ne():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.ne(None)


def test_nlargest():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.nlargest(None, None)


def test_notna():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.notna()


def test_notnull():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.notnull()


def test_nsmallest():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.nsmallest(None, None)


def test_nunique():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.nunique()


def test_pct_change():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pct_change()


def test_pipe():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pipe(None)


def test_pivot():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pivot()


def test_pivot_table():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pivot_table()


def test_plot():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.plot()


def test_pop():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pop(None)


def test_pow():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.pow(None)


def test_prod():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.prod()


def test_product():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.product()


def test_quantile():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.quantile()


def test_query():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.query(None)


def test_radd():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.radd(None)


def test_rank():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rank()


def test_rdiv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rdiv(None)


def test_reindex():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.reindex()


def test_reindex_axis():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.reindex_axis(None)


def test_reindex_like():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.reindex_like(None)


def test_rename():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rename()


def test_rename_axis():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rename_axis(None)


def test_reorder_levels():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.reorder_levels(None)


def test_replace():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.replace()


def test_resample():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.resample(None)


def test_reset_index():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.reset_index()


def test_rfloordiv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rfloordiv(None)


def test_rmod():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rmod(None)


def test_rmul():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rmul(None)


def test_rolling():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rolling(None)


def test_round():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.round()


def test_rpow():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rpow(None)


def test_rsub():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rsub(None)


def test_rtruediv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.rtruediv(None)


def test_sample():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sample()


def test_select():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.select(None)


def test_select_dtypes():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.select_dtypes()


def test_sem():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sem()


def test_set_axis():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.set_axis(None)


def test_set_index():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.set_index(None)


def test_set_value():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.set_value(None, None, None)


def test_shift():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.shift()


def test_skew():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.skew()


def test_slice_shift():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.slice_shift()


def test_sort_index():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sort_index()


def test_sort_values():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sort_values(None)


def test_sortlevel():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sortlevel()


def test_squeeze():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.squeeze()


def test_stack():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.stack()


def test_std():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.std()


def test_sub():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.sub(None)


def test_subtract():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.subtract(None)


def test_swapaxes():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.swapaxes(None, None)


def test_swaplevel():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.swaplevel()


def test_tail():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.tail()


def test_take():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.take(None)


def test_to_clipboard():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_clipboard()


def test_to_csv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_csv()


def test_to_dense():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_dense()


def test_to_dict():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_dict()


def test_to_excel():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_excel(None)


def test_to_feather():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_feather(None)


def test_to_gbq():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_gbq(None, None)


def test_to_hdf():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_hdf(None, None)


def test_to_html():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_html()


def test_to_json():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_json()


def test_to_latex():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_latex()


def test_to_msgpack():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_msgpack()


def test_to_panel():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_panel()


def test_to_parquet():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_parquet(None)


def test_to_period():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_period()


def test_to_pickle():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_pickle(None)


def test_to_records():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_records()


def test_to_sparse():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_sparse()


def test_to_sql():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_sql(None, None)


def test_to_stata():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_stata(None)


def test_to_string():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_string()


def test_to_timestamp():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_timestamp()


def test_to_xarray():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.to_xarray()


def test_transform():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.transform(None)


def test_truediv():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.truediv(None)


def test_truncate():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.truncate()


def test_tshift():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.tshift()


def test_tz_convert():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.tz_convert(None)


def test_tz_localize():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.tz_localize(None)


def test_unstack():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.unstack()


def test_update():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.update(None)


def test_var():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.var()


def test_where():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.where(None)


def test_xs():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.xs(None)


def test___getitem__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__getitem__(None)


def test___setitem__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__setitem__(None, None)


def test___len__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__len__()


def test___unicode__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__unicode__()


def test___neg__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__neg__()


def test___invert__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__invert__()


def test___hash__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__hash__()


def test___iter__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__iter__()


def test___contains__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__contains__(None)


def test___nonzero__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__nonzero__()


def test___bool__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__bool__()


def test___abs__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__abs__()


def test___round__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__round__()


def test___array__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__array__()


def test___array_wrap__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__array_wrap__(None)


def test___getstate__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__getstate__()


def test___setstate__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__setstate__(None)


def test___delitem__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__delitem__(None)


def test___finalize__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__finalize__(None)


def test___copy__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__copy__()


def test___deepcopy__():
    pandas_df = pd.DataFrame({'col1': [0, 1, 2, 3],
                              'col2': [4, 5, 6, 7],
                              'col3': [8, 9, 10, 11],
                              'col4': [12, 13, 14, 15]})

    ray_df = rdf.from_pandas(pandas_df, 2)

    with pytest.raises(NotImplementedError):
        ray_df.__deepcopy__()
