from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import re
import numpy as np
import pandas as pd
import ray
import pandas.util.testing as tm
from pandas import (compat, isna, notna, DataFrame, Series,
                    MultiIndex, date_range, Timestamp)
from pandas.compat import StringIO, lrange, product
from pandas import Series
from pandas.util.testing import assert_series_equal
import pandas.core.algorithms as algorithms
import ray.dataframe as rdf
from datetime import datetime
from datetime import timedelta
from ray.dataframe.utils import (
    to_pandas,
    from_pandas
)

from pandas.tests.frame.common import TestData

@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


def test_cummin():
    num_partitions = 2
    test_data = TestData()
    tf = test_data.tsframe
    tf.loc[5:10, 0] = np.nan
    tf.loc[10:15, 1] = np.nan
    tf.loc[15:, 2] = np.nan

    # axis = 0
    cummin_ray = from_pandas(tf, num_partitions).cummin()
    cummin = tf.cummin()
    expected = tf.apply(Series.cummin)
    assert ray_df_equals_pandas(cummin_ray, cummin)
    assert ray_df_equals_pandas(cummin_ray, expected)

    axis = 1
    cummin_ray = from_pandas(tf, num_partitions).cummin(axis=1)
    cummin = tf.cummin(axis=1)
    expected = tf.apply(Series.cummin, axis=1)
    assert ray_df_equals_pandas(cummin_ray, cummin)
    assert ray_df_equals_pandas(cummin_ray, expected)

    # it works
    df = pd.DataFrame({'A': np.arange(20)}, index=np.arange(20))
    ray_df = from_pandas(df, num_partitions)
    result = ray_df.cummin()  # noqa

    # fix issue
    cummin_ray = from_pandas(tf, num_partitions)
    cummin_xs = cummin_ray.cummin(axis=1)
    assert cummin_xs.shape == cummin_ray.shape


def test_cummax():
    num_partitions = 2
    test_data = TestData()
    tf = test_data.tsframe
    tf.loc[5:10, 0] = np.nan
    tf.loc[10:15, 1] = np.nan
    tf.loc[15:, 2] = np.nan

    # axis = 0
    cummax_ray = from_pandas(tf, num_partitions).cummax()
    cummax = tf.cummax()
    expected = tf.apply(Series.cummax)
    assert ray_df_equals_pandas(cummax_ray, cummax)
    assert ray_df_equals_pandas(cummax_ray, expected)

    # axis = 1
    cummax_ray = from_pandas(tf, num_partitions).cummax(axis=1)
    cummax = tf.cummax(axis=1)
    expected = tf.apply(Series.cummax, axis=1)
    assert ray_df_equals_pandas(cummax_ray, cummax)
    assert ray_df_equals_pandas(cummax_ray, expected)

    # it works
    df = pd.DataFrame({'A': np.arange(20)}, index=np.arange(20))
    ray_df = from_pandas(df, num_partitions)
    result = ray_df.cummax()  # noqa

    # fix issue
    cummax_ray = from_pandas(tf, num_partitions).cummax(axis=1)
    cummax_xs = tf.cummax(axis=1)
    assert cummax_xs.shape == cummax_ray.shape

def test_cumsum():
    num_partitions = 2
    test_data = TestData()
    tf = test_data.tsframe
    tf.loc[5:10, 0] = np.nan
    tf.loc[10:15, 1] = np.nan
    tf.loc[15:, 2] = np.nan

    # axis = 0
    cumsum_ray = from_pandas(tf, num_partitions).cumsum()
    cumsum = tf.cumsum()
    expected = tf.apply(Series.cumsum)
    assert ray_df_equals_pandas(cumsum_ray, cumsum)
    assert ray_df_equals_pandas(cumsum_ray, expected)

    # axis = 1
    cumsum_ray = from_pandas(tf, num_partitions).cumsum(axis=1)
    cumsum = tf.cumsum(axis=1)
    expected = tf.apply(Series.cumsum, axis=1)
    assert ray_df_equals_pandas(cumsum_ray, cumsum)
    assert ray_df_equals_pandas(cumsum_ray, expected)

    # it works
    df = pd.DataFrame({'A': np.arange(20)}, index=np.arange(20))
    ray_df = from_pandas(df, num_partitions)
    result = ray_df.cumsum()  # noqa

    # fix issue
    cumsum_ray = from_pandas(tf, num_partitions).cumsum(axis=1)
    cumsum_xs = tf.cumsum(axis=1)
    assert cumsum_xs.shape == cumsum_ray.shape

def test_cumsum_corner():
    test_data = TestData()
    num_partitions = 2
    dm = DataFrame(np.arange(20).reshape(4, 5),
                   index=lrange(4), columns=lrange(5))
    ray_dm = from_pandas(dm, num_partitions)
    # ?(wesm)
    result = ray_dm.cumsum()  # noqa

def test_cumprod():
    num_partitions = 2
    test_data = TestData()
    tf = test_data.tsframe
    tf.loc[5:10, 0] = np.nan
    tf.loc[10:15, 1] = np.nan
    tf.loc[15:, 2] = np.nan

    # axis = 0
    cumprod_ray = from_pandas(tf, num_partitions).cumprod()
    cumprod = tf.cumprod()
    expected = tf.apply(Series.cumprod)
    assert ray_df_equals_pandas(cumprod_ray, cumprod)
    assert ray_df_equals_pandas(cumprod_ray, expected)

    # axis = 1
    cumprod_ray = from_pandas(tf, num_partitions).cumprod(axis=1)
    cumprod = tf.cumprod(axis=1)
    expected = tf.apply(Series.cumprod, axis=1)
    tm.assert_frame_equal(cumprod, expected)
    assert ray_df_equals_pandas(cumprod_ray, cumprod)
    assert ray_df_equals_pandas(cumprod_ray, expected)

    # fix issue
    cumprod_ray = from_pandas(tf, num_partitions).cummax(axis=1)
    cumprod_xs = tf.cumprod(axis=1)
    assert cumprod_xs.shape == cumprod_ray.shape

    # ints
    df = tf.fillna(0).astype(int)
    ray_df = from_pandas(df, num_partitions)
    df.cumprod(0)
    df.cumprod(1)
    ray_df.cumprod(0)
    ray_df.cumprod(1)
    assert ray_df_equals_pandas(ray_df, df)

    # ints32
    df = tf.fillna(0).astype(np.int32)
    ray_df = from_pandas(df, num_partitions)
    df.cumprod(0)
    df.cumprod(1)
    ray_df.cumprod(0)
    ray_df.cumprod(1)
    assert ray_df_equals_pandas(ray_df, df)

def test_min():
    test_data = TestData()
    _check_stat_op(test_data, 'min', np.min, check_dates=True)
    _check_stat_op(test_data, 'min', np.min, frame=test_data.intframe)

def test_max():
    test_data = TestData()
    _check_stat_op(test_data, 'max', np.max, check_dates=True)
    _check_stat_op(test_data, 'max', np.max, frame=test_data.intframe)


def test_sum():
    test_data = TestData()
    _check_stat_op(test_data, 'sum', np.sum, has_numeric_only=True,
                        skipna_alternative=np.nansum)

    # mixed types (with upcasting happening)
    _check_stat_op(test_data, 'sum', np.sum,
                        frame=test_data.mixed_float.astype('float32'),
                        has_numeric_only=True, check_dtype=False,
                        check_less_precise=True)

def test_sum_corner():
    test_data = TestData()
    num_partitions = 2
    ray_test_data = from_pandas(test_data.empty, num_partitions)
    ray_axis0 = ray_test_data.sum(0)
    ray_axis1 = ray_test_data.sum(1)
    assert isinstance(ray_axis0, Series)
    assert isinstance(ray_axis1, Series)
    assert len(ray_axis0) == 0
    assert len(ray_axis1) == 0

def test_sum_object():
    test_data = TestData()
    num_partitions = 2
    ray_df = from_pandas(test_data.frame, num_partitions)
    values = ray_df.values.astype(int)
    frame = DataFrame(values, index=ray_df.index,
                      columns=ray_df.columns)
    ray_frame = from_pandas(frame, num_partitions)
    deltas = frame * timedelta(1)
    deltas.sum()


@pytest.mark.parametrize('method, unit', [
        ('sum', 0),
        ('prod', 1),
    ])
def test_sum_prod_nanops(method, unit):
    test_data = TestData()
    num_partitions = 2
    idx = ['a', 'b', 'c']
    df = pd.DataFrame({"a": [unit, unit],
                       "b": [unit, np.nan],
                       "c": [np.nan, np.nan]})
    ray_df = from_pandas(df, num_partitions)
    # The default
    result = getattr(ray_df, method)
    expected = pd.Series([unit, unit, unit], index=idx, dtype='float64')

    # min_count=1
    result = getattr(ray_df, method)(min_count=1)
    expected = pd.Series([unit, unit, np.nan], index=idx)
    tm.assert_series_equal(result, expected)

    # min_count=0
    result = getattr(ray_df, method)(min_count=0)
    expected = pd.Series([unit, unit, unit], index=idx, dtype='float64')
    tm.assert_series_equal(result, expected)

    # min_count > 1
    df = pd.DataFrame({"A": [unit] * 10, "B": [unit] * 5 + [np.nan] * 5})
    ray_df = from_pandas(df, num_partitions)
    result = getattr(ray_df, method)(min_count=5)
    expected = pd.Series(result, index=['A', 'B'])
    tm.assert_series_equal(result, expected)

    result = getattr(ray_df, method)(min_count=6)
    expected = pd.Series(result, index=['A', 'B'])
    tm.assert_series_equal(result, expected)

def test_product():
    test_data = TestData()
    _check_stat_op(test_data, 'product', np.prod)

def test_mean():
    test_data = TestData()
    _check_stat_op(test_data, 'mean', np.mean, check_dates=True)

@pytest.fixture
def _check_stat_op(self, name, alternative, frame=None, has_skipna=True,
                       has_numeric_only=False, check_dtype=True,
                       check_dates=False, check_less_precise=False,
                       skipna_alternative=None):
        num_partitions = 2
        if frame is None:
            frame = self.frame
            # set some NAs
            frame.loc[5:10] = np.nan
            frame.loc[15:20, -2:] = np.nan
        ray_frame = from_pandas(frame, num_partitions)
        f = getattr(ray_frame, name)

        if check_dates:
            df = pd.DataFrame({'b': date_range('1/1/2001', periods=2)})
            ray_df = from_pandas(df, num_partitions)
            print(ray_df)
            _f = getattr(ray_df, name)
            result = _f()
            assert isinstance(result, Series)

        if has_skipna:
            def wrapper(x):
                return alternative(x.values)

            skipna_wrapper = tm._make_skipna_wrapper(alternative,
                                                     skipna_alternative)
            result0 = f(axis=0, skipna=False)
            result1 = f(axis=1, skipna=False)
            tm.assert_series_equal(result0, ray_frame.apply(wrapper),
                                   check_dtype=check_dtype,
                                   check_less_precise=check_less_precise)
            # HACK: win32
            tm.assert_series_equal(result1, ray_frame.apply(wrapper, axis=1),
                                   check_dtype=False,
                                   check_less_precise=check_less_precise)
        else:
            skipna_wrapper = alternative
            wrapper = alternative

        result0 = f(axis=0)
        result1 = f(axis=1)
        tm.assert_series_equal(result0, ray_frame.apply(skipna_wrapper),
                               check_dtype=check_dtype,
                               check_less_precise=check_less_precise)
        if name in ['sum', 'prod']:
            exp = ray_frame.apply(skipna_wrapper, axis=1)
            print("RAY FRAME")
            print(ray_frame)
            print("EXPECTED")
            print(exp)
            print(len(exp))
            print("RESULT1")
            print(result1)
            print(len(result1))
            tm.assert_series_equal(result1, exp, check_dtype=False,
                                   check_less_precise=check_less_precise)

        # check dtypes
        if check_dtype:
            lcd_dtype = ray_frame.values.dtype
            assert lcd_dtype == result0.dtype
            assert lcd_dtype == result1.dtype


        # bad axis
        tm.assert_raises_regex(ValueError, 'No axis named 2', f, axis=2)
        # make sure works on mixed-type frame
        ray_mixed = from_pandas(self.mixed_frame, num_partitions)
        ray_frame_numeric = from_pandas(self.frame, num_partitions)
        getattr(ray_mixed, name)(axis=0)
        getattr(ray_mixed, name)(axis=1)

        if has_numeric_only:
            getattr(ray_mixed, name)(axis=0, numeric_only=True)
            getattr(ray_mixed, name)(axis=1, numeric_only=True)
            getattr(ray_frame_numeric, name)(axis=0, numeric_only=False)
            getattr(ray_frame_numeric, name)(axis=1, numeric_only=False)

