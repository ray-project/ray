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


@pytest.fixture
def create_test_dataframe():
    df = pd.DataFrame({'col1': [0, 1, 2, 3],
                       'col2': [4, 5, 6, 7],
                       'col3': [8, 9, 10, 11],
                       'col4': [12, 13, 14, 15]})

    return rdf.from_pandas(df, 2)


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


def test_all():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.all()


def test_any():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.any()


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
        ray_df.bfill()


def test_bool():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.bool()


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


def test_count():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.count()


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
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.equals(None)


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


def test_get():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get(None)


def test_get_dtype_counts():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_dtype_counts()


def test_get_ftype_counts():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.get_ftype_counts()


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


def test_head():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.head()


def test_hist():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.hist(None)


def test_idxmax():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.idxmax()


def test_idxmin():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.idxmin()


def test_infer_objects():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.infer_objects()


def test_info():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.info()


def test_insert():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.insert(None, None, None)


def test_interpolate():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.interpolate()


def test_items():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.items()


def test_iteritems():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.iteritems()


def test_iterrows():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.iterrows()


def test_itertuples():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.itertuples()


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


def test_max():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.max()


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


def test_min():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.min()


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


def test_notna():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.notna()


def test_notnull():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.notnull()


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


def test_pop():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.pop(None)


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


def test_query():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.query(None)


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


def test_reset_index():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.reset_index()


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


def test_round():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.round()


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


def test_set_axis():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.set_axis(None)


def test_set_index():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.set_index(None)


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


def test_tail():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.tail()


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


def test___getitem__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__getitem__(None)


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


def test___neg__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__neg__()


def test___invert__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__invert__()


def test___hash__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__hash__()


def test___iter__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__iter__()


def test___contains__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__contains__(None)


def test___nonzero__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__nonzero__()


def test___bool__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__bool__()


def test___abs__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__abs__()


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


def test___delitem__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__delitem__(None)


def test___finalize__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__finalize__(None)


def test___copy__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__copy__()


def test___deepcopy__():
    ray_df = create_test_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.__deepcopy__()
