# Licensed to Modin Development Team under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.  The Modin Development Team
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# This file is copied and adapted from:
# http://github.com/modin-project/modin/master/modin/pandas/test/test_general.py
# http://github.com/modin-project/modin/master/modin/pandas/test/utils.py

import pandas
import pytest
import modin
import modin.pandas as pd
import numpy as np
from numpy.testing import assert_array_equal
from modin.utils import to_pandas
from pandas.testing import (assert_series_equal, assert_frame_equal,
                            assert_extension_array_equal, assert_index_equal)
from packaging import version
import ray
from ray.util.client.ray_client_helpers import ray_start_client_server

# Versions of modin prior to 0.9.1 are incompatible with ray
# client. See https://github.com/modin-project/modin/pull/2851
modin_version = version.parse(modin.__version__)
pytestmark = pytest.mark.skipif(
    modin_version <= version.parse("0.9.1"),
    reason="Incompatible modin version")


# Module scoped fixture. Will first run all tests without ray
# client, then rerun all tests with a single ray client session.
@pytest.fixture(params=[False, True], autouse=True, scope="module")
def run_ray_client(request):
    if request.param:
        with ray_start_client_server() as client:
            yield client
    else:
        # Run without ray client (do nothing)
        yield
        # Cleanup before moving rerunning tests with client
        ray.shutdown()


random_state = np.random.RandomState(seed=42)

DATASET_SIZE_DICT = {
    "Small": (2**2, 2**3),
    "Normal": (2**6, 2**8),
    "Big": (2**7, 2**12),
}

# Size of test dataframes
NCOLS, NROWS = DATASET_SIZE_DICT["Normal"]

# Range for values for test data
RAND_LOW = 0
RAND_HIGH = 100

# Input data and functions for the tests
# The test data that we will test our code against
test_data = {
    "int_data": {
        "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): random_state.randint(
            RAND_LOW, RAND_HIGH, size=(NROWS))
        for i in range(NCOLS)
    },
    "float_nan_data": {
        "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): [
            x if (j % 4 == 0 and i > NCOLS // 2)
            or (j != i and i <= NCOLS // 2) else np.NaN for j, x in enumerate(
                random_state.uniform(RAND_LOW, RAND_HIGH, size=(NROWS)))
        ]
        for i in range(NCOLS)
    },
}

test_data["int_data"]["index"] = test_data["int_data"].pop("col{}".format(
    int(NCOLS / 2)))

for col in test_data["float_nan_data"]:
    for row in range(NROWS // 2):
        if row % 16 == 0:
            test_data["float_nan_data"][col][row] = np.NaN

test_data_values = list(test_data.values())
test_data_keys = list(test_data.keys())


def categories_equals(left, right):
    assert (left.ordered and right.ordered) or (not left.ordered
                                                and not right.ordered)
    assert_extension_array_equal(left, right)


def df_categories_equals(df1, df2):
    if not hasattr(df1, "select_dtypes"):
        if isinstance(df1, pandas.CategoricalDtype):
            return categories_equals(df1, df2)
        elif isinstance(getattr(df1, "dtype"),
                        pandas.CategoricalDtype) and isinstance(
                            getattr(df1, "dtype"), pandas.CategoricalDtype):
            return categories_equals(df1.dtype, df2.dtype)
        else:
            return True

    categories_columns = df1.select_dtypes(include="category").columns
    for column in categories_columns:
        assert_extension_array_equal(
            df1[column].values,
            df2[column].values,
            check_dtype=False,
        )


def df_equals(df1, df2):
    """Tests if df1 and df2 are equal.

    Args:
        df1: (pandas or modin DataFrame or series) dataframe to test if equal.
        df2: (pandas or modin DataFrame or series) dataframe to test if equal.

    Returns:
        True if df1 is equal to df2.
    """
    # Gets AttributError if modin's groupby object is not import like this
    from modin.pandas.groupby import DataFrameGroupBy

    groupby_types = (pandas.core.groupby.DataFrameGroupBy, DataFrameGroupBy)

    # The typing behavior of how pandas treats its index is not consistent when
    # the length of the DataFrame or Series is 0, so we just verify that the
    # contents are the same.
    if (hasattr(df1, "index") and hasattr(df2, "index") and len(df1) == 0
            and len(df2) == 0):
        if type(df1).__name__ == type(df2).__name__:
            if hasattr(df1, "name") and hasattr(
                    df2, "name") and df1.name == df2.name:
                return
            if (hasattr(df1, "columns") and hasattr(df2, "columns")
                    and df1.columns.equals(df2.columns)):
                return
        assert False

    if isinstance(df1, (list, tuple)) and all(
            isinstance(d, (pd.DataFrame, pd.Series, pandas.DataFrame,
                           pandas.Series)) for d in df1):
        assert isinstance(df2, type(df1)), "Different type of collection"
        assert len(df1) == len(df2), "Different length result"
        return (df_equals(d1, d2) for d1, d2 in zip(df1, df2))

    # Convert to pandas
    if isinstance(df1, (pd.DataFrame, pd.Series)):
        df1 = to_pandas(df1)
    if isinstance(df2, (pd.DataFrame, pd.Series)):
        df2 = to_pandas(df2)

    if isinstance(df1, pandas.DataFrame) and isinstance(df2, pandas.DataFrame):
        if (df1.empty and not df2.empty) or (df2.empty and not df1.empty):
            assert False, "One of the passed frames is empty, when other isn't"
        elif df1.empty and df2.empty and type(df1) != type(df2):
            assert (
                False
            ), f"Empty frames have different types: {type(df1)} != {type(df2)}"

    if isinstance(df1, pandas.DataFrame) and isinstance(df2, pandas.DataFrame):
        assert_frame_equal(
            df1,
            df2,
            check_dtype=False,
            check_datetimelike_compat=True,
            check_index_type=False,
            check_column_type=False,
            check_categorical=False,
        )
        df_categories_equals(df1, df2)
    elif isinstance(df1, pandas.Index) and isinstance(df2, pandas.Index):
        assert_index_equal(df1, df2)
    elif isinstance(df1, pandas.Series) and isinstance(df2, pandas.Series):
        assert_series_equal(
            df1, df2, check_dtype=False, check_series_type=False)
    elif isinstance(df1, groupby_types) and isinstance(df2, groupby_types):
        for g1, g2 in zip(df1, df2):
            assert g1[0] == g2[0]
            df_equals(g1[1], g2[1])
    elif (isinstance(df1, pandas.Series) and isinstance(df2, pandas.Series)
          and df1.empty and df2.empty):
        assert all(df1.index == df2.index)
        assert df1.dtypes == df2.dtypes
    elif isinstance(df1, pandas.core.arrays.numpy_.PandasArray):
        assert isinstance(df2, pandas.core.arrays.numpy_.PandasArray)
        assert df1 == df2
    elif isinstance(df1, np.recarray) and isinstance(df2, np.recarray):
        np.testing.assert_array_equal(df1, df2)
    else:
        if df1 != df2:
            np.testing.assert_almost_equal(df1, df2)


@pytest.mark.parametrize("data", test_data_values, ids=test_data_keys)
def test_isna(data):
    pandas_df = pandas.DataFrame(data)
    modin_df = pd.DataFrame(data)

    pandas_result = pandas.isna(pandas_df)
    modin_result = pd.isna(modin_df)
    df_equals(modin_result, pandas_result)

    modin_result = pd.isna(pd.Series([1, np.nan, 2]))
    pandas_result = pandas.isna(pandas.Series([1, np.nan, 2]))
    df_equals(modin_result, pandas_result)

    assert pd.isna(np.nan) == pandas.isna(np.nan)


@pytest.mark.parametrize("data", test_data_values, ids=test_data_keys)
def test_isnull(data):
    pandas_df = pandas.DataFrame(data)
    modin_df = pd.DataFrame(data)

    pandas_result = pandas.isnull(pandas_df)
    modin_result = pd.isnull(modin_df)
    df_equals(modin_result, pandas_result)

    modin_result = pd.isnull(pd.Series([1, np.nan, 2]))
    pandas_result = pandas.isnull(pandas.Series([1, np.nan, 2]))
    df_equals(modin_result, pandas_result)

    assert pd.isna(np.nan) == pandas.isna(np.nan)


@pytest.mark.parametrize("data", test_data_values, ids=test_data_keys)
def test_notna(data):
    pandas_df = pandas.DataFrame(data)
    modin_df = pd.DataFrame(data)

    pandas_result = pandas.notna(pandas_df)
    modin_result = pd.notna(modin_df)
    df_equals(modin_result, pandas_result)

    modin_result = pd.notna(pd.Series([1, np.nan, 2]))
    pandas_result = pandas.notna(pandas.Series([1, np.nan, 2]))
    df_equals(modin_result, pandas_result)

    assert pd.isna(np.nan) == pandas.isna(np.nan)


@pytest.mark.parametrize("data", test_data_values, ids=test_data_keys)
def test_notnull(data):
    pandas_df = pandas.DataFrame(data)
    modin_df = pd.DataFrame(data)

    pandas_result = pandas.notnull(pandas_df)
    modin_result = pd.notnull(modin_df)
    df_equals(modin_result, pandas_result)

    modin_result = pd.notnull(pd.Series([1, np.nan, 2]))
    pandas_result = pandas.notnull(pandas.Series([1, np.nan, 2]))
    df_equals(modin_result, pandas_result)

    assert pd.isna(np.nan) == pandas.isna(np.nan)


def test_merge():
    frame_data = {
        "col1": [0, 1, 2, 3],
        "col2": [4, 5, 6, 7],
        "col3": [8, 9, 0, 1],
        "col4": [2, 4, 5, 6],
    }

    modin_df = pd.DataFrame(frame_data)
    pandas_df = pandas.DataFrame(frame_data)

    frame_data2 = {"col1": [0, 1, 2], "col2": [1, 5, 6]}
    modin_df2 = pd.DataFrame(frame_data2)
    pandas_df2 = pandas.DataFrame(frame_data2)

    join_types = ["outer", "inner"]
    for how in join_types:
        # Defaults
        modin_result = pd.merge(modin_df, modin_df2, how=how)
        pandas_result = pandas.merge(pandas_df, pandas_df2, how=how)
        df_equals(modin_result, pandas_result)

        # left_on and right_index
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_on="col1", right_index=True)
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col1", right_index=True)
        df_equals(modin_result, pandas_result)

        # left_index and right_on
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_index=True, right_on="col1")
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_index=True, right_on="col1")
        df_equals(modin_result, pandas_result)

        # left_on and right_on col1
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_on="col1", right_on="col1")
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col1", right_on="col1")
        df_equals(modin_result, pandas_result)

        # left_on and right_on col2
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_on="col2", right_on="col2")
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col2", right_on="col2")
        df_equals(modin_result, pandas_result)

        # left_index and right_index
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_index=True, right_index=True)
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_index=True, right_index=True)
        df_equals(modin_result, pandas_result)

    s = pd.Series(frame_data.get("col1"))
    with pytest.raises(ValueError):
        pd.merge(s, modin_df2)

    with pytest.raises(TypeError):
        pd.merge("Non-valid type", modin_df2)


def test_pivot():
    test_df = pd.DataFrame({
        "foo": ["one", "one", "one", "two", "two", "two"],
        "bar": ["A", "B", "C", "A", "B", "C"],
        "baz": [1, 2, 3, 4, 5, 6],
        "zoo": ["x", "y", "z", "q", "w", "t"],
    })

    df = pd.pivot(test_df, index="foo", columns="bar", values="baz")
    assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.pivot(test_df["bar"], index="foo", columns="bar", values="baz")


def test_pivot_table():
    test_df = pd.DataFrame({
        "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
        "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
        "C": [
            "small",
            "large",
            "large",
            "small",
            "small",
            "large",
            "small",
            "small",
            "large",
        ],
        "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
        "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
    })

    df = pd.pivot_table(
        test_df, values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum)
    assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.pivot_table(
            test_df["C"],
            values="D",
            index=["A", "B"],
            columns=["C"],
            aggfunc=np.sum)


def test_unique():
    modin_result = pd.unique([2, 1, 3, 3])
    pandas_result = pandas.unique([2, 1, 3, 3])
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(pd.Series([2] + [1] * 5))
    pandas_result = pandas.unique(pandas.Series([2] + [1] * 5))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(
        pd.Series([pd.Timestamp("20160101"),
                   pd.Timestamp("20160101")]))
    pandas_result = pandas.unique(
        pandas.Series(
            [pandas.Timestamp("20160101"),
             pandas.Timestamp("20160101")]))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(
        pd.Series([
            pd.Timestamp("20160101", tz="US/Eastern"),
            pd.Timestamp("20160101", tz="US/Eastern"),
        ]))
    pandas_result = pandas.unique(
        pandas.Series([
            pandas.Timestamp("20160101", tz="US/Eastern"),
            pandas.Timestamp("20160101", tz="US/Eastern"),
        ]))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(
        pd.Index([
            pd.Timestamp("20160101", tz="US/Eastern"),
            pd.Timestamp("20160101", tz="US/Eastern"),
        ]))
    pandas_result = pandas.unique(
        pandas.Index([
            pandas.Timestamp("20160101", tz="US/Eastern"),
            pandas.Timestamp("20160101", tz="US/Eastern"),
        ]))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(pd.Series(pd.Categorical(list("baabc"))))
    pandas_result = pandas.unique(
        pandas.Series(pandas.Categorical(list("baabc"))))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape


def test_to_datetime():
    # DataFrame input for to_datetime
    modin_df = pd.DataFrame({
        "year": [2015, 2016],
        "month": [2, 3],
        "day": [4, 5]
    })
    pandas_df = pandas.DataFrame({
        "year": [2015, 2016],
        "month": [2, 3],
        "day": [4, 5]
    })
    df_equals(pd.to_datetime(modin_df), pandas.to_datetime(pandas_df))

    # Series input for to_datetime
    modin_s = pd.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 1000)
    pandas_s = pandas.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 1000)
    df_equals(pd.to_datetime(modin_s), pandas.to_datetime(pandas_s))

    # Other inputs for to_datetime
    value = 1490195805
    assert pd.to_datetime(
        value, unit="s") == pandas.to_datetime(
            value, unit="s")
    value = 1490195805433502912
    assert pd.to_datetime(
        value, unit="ns") == pandas.to_datetime(
            value, unit="ns")
    value = [1, 2, 3]
    assert pd.to_datetime(
        value, unit="D", origin=pd.Timestamp("2000-01-01")).equals(
            pandas.to_datetime(
                value, unit="D", origin=pandas.Timestamp("2000-01-01")))


@pytest.mark.parametrize(
    "data, errors, downcast",
    [
        (["1.0", "2", -3], "raise", None),
        (["1.0", "2", -3], "raise", "float"),
        (["1.0", "2", -3], "raise", "signed"),
        (["apple", "1.0", "2", -3], "ignore", None),
        (["apple", "1.0", "2", -3], "coerce", None),
    ],
)
def test_to_numeric(data, errors, downcast):
    modin_series = pd.Series(data)
    pandas_series = pandas.Series(data)
    modin_result = pd.to_numeric(
        modin_series, errors=errors, downcast=downcast)
    pandas_result = pandas.to_numeric(
        pandas_series, errors=errors, downcast=downcast)
    df_equals(modin_result, pandas_result)


def test_to_pandas_indices():
    data = test_data_values[0]

    md_df = pd.DataFrame(data)
    index = pandas.MultiIndex.from_tuples(
        [(i, i * 2) for i in np.arange(len(md_df) + 1)], names=["A",
                                                                "B"]).drop(0)
    columns = pandas.MultiIndex.from_tuples(
        [(i, i * 2) for i in np.arange(len(md_df.columns) + 1)],
        names=["A", "B"]).drop(0)

    md_df.index = index
    md_df.columns = columns

    pd_df = md_df._to_pandas()

    for axis in [0, 1]:
        assert md_df.axes[axis].equals(
            pd_df.axes[axis]), f"Indices at axis {axis} are different!"
        assert md_df.axes[axis].equal_levels(pd_df.axes[
            axis]), f"Levels of indices at axis {axis} are different!"


def test_empty_dataframe():
    df = pd.DataFrame(columns=["a", "b"])
    df[(df.a == 1) & (df.b == 2)]
