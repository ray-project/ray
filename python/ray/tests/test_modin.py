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


def test_merge_ordered():
    data_a = {
        "key": list("aceace"),
        "lvalue": [1, 2, 3, 1, 2, 3],
        "group": list("aaabbb"),
    }
    data_b = {"key": list("bcd"), "rvalue": [1, 2, 3]}

    modin_df_a = pd.DataFrame(data_a)
    modin_df_b = pd.DataFrame(data_b)

    with pytest.warns(UserWarning):
        df = pd.merge_ordered(
            modin_df_a, modin_df_b, fill_method="ffill", left_by="group")
        assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.merge_ordered(data_a, data_b, fill_method="ffill", left_by="group")


def test_merge_asof():
    left = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    right = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})

    with pytest.warns(UserWarning):
        df = pd.merge_asof(left, right, on="a")
        assert isinstance(df, pd.DataFrame)

    with pytest.warns(UserWarning):
        df = pd.merge_asof(left, right, on="a", allow_exact_matches=False)
        assert isinstance(df, pd.DataFrame)

    with pytest.warns(UserWarning):
        df = pd.merge_asof(left, right, on="a", direction="forward")
        assert isinstance(df, pd.DataFrame)

    with pytest.warns(UserWarning):
        df = pd.merge_asof(left, right, on="a", direction="nearest")
        assert isinstance(df, pd.DataFrame)

    left = pd.DataFrame({"left_val": ["a", "b", "c"]}, index=[1, 5, 10])
    right = pd.DataFrame({"right_val": [1, 2, 3, 6, 7]}, index=[1, 2, 3, 6, 7])

    with pytest.warns(UserWarning):
        df = pd.merge_asof(left, right, left_index=True, right_index=True)
        assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.merge_asof(
            {
                "left_val": ["a", "b", "c"]
            },
            {"right_val": [1, 2, 3, 6, 7]},
            left_index=True,
            right_index=True,
        )


def test_merge_asof_on_variations():
    """on=,left_on=,right_on=,right_index=,left_index= options match Pandas."""
    left = {"a": [1, 5, 10], "left_val": ["a", "b", "c"]}
    left_index = [6, 8, 12]
    right = {"a": [1, 2, 3, 6, 7], "right_val": ["d", "e", "f", "g", "h"]}
    right_index = [6, 7, 8, 9, 15]
    pandas_left, pandas_right = (
        pandas.DataFrame(left, index=left_index),
        pandas.DataFrame(right, index=right_index),
    )
    modin_left, modin_right = pd.DataFrame(
        left, index=left_index), pd.DataFrame(
            right, index=right_index)
    for on_arguments in [
        {
            "on": "a"
        },
        {
            "left_on": "a",
            "right_on": "a"
        },
        {
            "left_on": "a",
            "right_index": True
        },
        {
            "left_index": True,
            "right_on": "a"
        },
        {
            "left_index": True,
            "right_index": True
        },
    ]:
        pandas_merged = pandas.merge_asof(pandas_left, pandas_right,
                                          **on_arguments)
        modin_merged = pd.merge_asof(modin_left, modin_right, **on_arguments)
        df_equals(pandas_merged, modin_merged)


def test_merge_asof_suffixes():
    """Suffix variations are handled the same as Pandas."""
    left = {"a": [1, 5, 10]}
    right = {"a": [2, 3, 6]}
    pandas_left, pandas_right = (pandas.DataFrame(left),
                                 pandas.DataFrame(right))
    modin_left, modin_right = pd.DataFrame(left), pd.DataFrame(right)
    for suffixes in [("a", "b"), (False, "c"), ("d", False)]:
        pandas_merged = pandas.merge_asof(
            pandas_left,
            pandas_right,
            left_index=True,
            right_index=True,
            suffixes=suffixes,
        )
        modin_merged = pd.merge_asof(
            modin_left,
            modin_right,
            left_index=True,
            right_index=True,
            suffixes=suffixes,
        )
        df_equals(pandas_merged, modin_merged)

    with pytest.raises(ValueError):
        pandas.merge_asof(
            pandas_left,
            pandas_right,
            left_index=True,
            right_index=True,
            suffixes=(False, False),
        )
    with pytest.raises(ValueError):
        modin_merged = pd.merge_asof(
            modin_left,
            modin_right,
            left_index=True,
            right_index=True,
            suffixes=(False, False),
        )


def test_merge_asof_bad_arguments():
    left = {"a": [1, 5, 10], "b": [5, 7, 9]}
    right = {"a": [2, 3, 6], "b": [6, 5, 20]}
    pandas_left, pandas_right = (pandas.DataFrame(left),
                                 pandas.DataFrame(right))
    modin_left, modin_right = pd.DataFrame(left), pd.DataFrame(right)

    # Can't mix by with left_by/right_by
    with pytest.raises(ValueError):
        pandas.merge_asof(
            pandas_left,
            pandas_right,
            on="a",
            by="b",
            left_by="can't do with by")
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left,
            modin_right,
            on="a",
            by="b",
            left_by="can't do with by")
    with pytest.raises(ValueError):
        pandas.merge_asof(
            pandas_left,
            pandas_right,
            by="b",
            on="a",
            right_by="can't do with by")
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left,
            modin_right,
            by="b",
            on="a",
            right_by="can't do with by")

    # Can't mix on with left_on/right_on
    with pytest.raises(ValueError):
        pandas.merge_asof(
            pandas_left, pandas_right, on="a", left_on="can't do with by")
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left, modin_right, on="a", left_on="can't do with by")
    with pytest.raises(ValueError):
        pandas.merge_asof(
            pandas_left, pandas_right, on="a", right_on="can't do with by")
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left, modin_right, on="a", right_on="can't do with by")

    # Can't mix left_index with left_on or on, similarly for right.
    with pytest.raises(ValueError):
        pd.merge_asof(modin_left, modin_right, on="a", right_index=True)
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left,
            modin_right,
            left_on="a",
            right_on="a",
            right_index=True)
    with pytest.raises(ValueError):
        pd.merge_asof(modin_left, modin_right, on="a", left_index=True)
    with pytest.raises(ValueError):
        pd.merge_asof(
            modin_left,
            modin_right,
            left_on="a",
            right_on="a",
            left_index=True)

    # Need both left and right
    with pytest.raises(
            Exception):  # Pandas bug, didn't validate inputs sufficiently
        pandas.merge_asof(pandas_left, pandas_right, left_on="a")
    with pytest.raises(ValueError):
        pd.merge_asof(modin_left, modin_right, left_on="a")
    with pytest.raises(
            Exception):  # Pandas bug, didn't validate inputs sufficiently
        pandas.merge_asof(pandas_left, pandas_right, right_on="a")
    with pytest.raises(ValueError):
        pd.merge_asof(modin_left, modin_right, right_on="a")
    with pytest.raises(ValueError):
        pandas.merge_asof(pandas_left, pandas_right)
    with pytest.raises(ValueError):
        pd.merge_asof(modin_left, modin_right)


def test_merge_asof_merge_options():
    modin_quotes = pd.DataFrame({
        "time": [
            pd.Timestamp("2016-05-25 13:30:00.023"),
            pd.Timestamp("2016-05-25 13:30:00.023"),
            pd.Timestamp("2016-05-25 13:30:00.030"),
            pd.Timestamp("2016-05-25 13:30:00.041"),
            pd.Timestamp("2016-05-25 13:30:00.048"),
            pd.Timestamp("2016-05-25 13:30:00.049"),
            pd.Timestamp("2016-05-25 13:30:00.072"),
            pd.Timestamp("2016-05-25 13:30:00.075"),
        ],
        "ticker": [
            "GOOG", "MSFT", "MSFT", "MSFT", "GOOG", "AAPL", "GOOG", "MSFT"
        ],
        "bid": [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01],
        "ask": [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03],
    })
    modin_trades = pd.DataFrame({
        "time": [
            pd.Timestamp("2016-05-25 13:30:00.023"),
            pd.Timestamp("2016-05-25 13:30:00.038"),
            pd.Timestamp("2016-05-25 13:30:00.048"),
            pd.Timestamp("2016-05-25 13:30:00.048"),
            pd.Timestamp("2016-05-25 13:30:00.048"),
        ],
        "ticker2": ["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"],
        "price": [51.95, 51.95, 720.77, 720.92, 98.0],
        "quantity": [75, 155, 100, 100, 100],
    })
    pandas_quotes, pandas_trades = to_pandas(modin_quotes), to_pandas(
        modin_trades)

    # left_by + right_by
    df_equals(
        pandas.merge_asof(
            pandas_quotes,
            pandas_trades,
            on="time",
            left_by="ticker",
            right_by="ticker2",
        ),
        pd.merge_asof(
            modin_quotes,
            modin_trades,
            on="time",
            left_by="ticker",
            right_by="ticker2",
        ),
    )

    # Just by:
    pandas_trades["ticker"] = pandas_trades["ticker2"]
    modin_trades["ticker"] = modin_trades["ticker2"]
    df_equals(
        pandas.merge_asof(
            pandas_quotes,
            pandas_trades,
            on="time",
            by="ticker",
        ),
        pd.merge_asof(
            modin_quotes,
            modin_trades,
            on="time",
            by="ticker",
        ),
    )

    # Tolerance
    df_equals(
        pandas.merge_asof(
            pandas_quotes,
            pandas_trades,
            on="time",
            by="ticker",
            tolerance=pd.Timedelta("2ms"),
        ),
        pd.merge_asof(
            modin_quotes,
            modin_trades,
            on="time",
            by="ticker",
            tolerance=pd.Timedelta("2ms"),
        ),
    )

    # Direction
    df_equals(
        pandas.merge_asof(
            pandas_quotes,
            pandas_trades,
            on="time",
            by="ticker",
            direction="forward",
        ),
        pd.merge_asof(
            modin_quotes,
            modin_trades,
            on="time",
            by="ticker",
            direction="forward",
        ),
    )

    # Allow exact matches
    df_equals(
        pandas.merge_asof(
            pandas_quotes,
            pandas_trades,
            on="time",
            by="ticker",
            tolerance=pd.Timedelta("10ms"),
            allow_exact_matches=False,
        ),
        pd.merge_asof(
            modin_quotes,
            modin_trades,
            on="time",
            by="ticker",
            tolerance=pd.Timedelta("10ms"),
            allow_exact_matches=False,
        ),
    )


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
