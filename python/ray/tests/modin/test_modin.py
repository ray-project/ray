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

import sys
import pytest
import pandas
import numpy as np
from numpy.testing import assert_array_equal
import ray
from ray.util.client.ray_client_helpers import ray_start_client_server

modin_compatible_version = sys.version_info >= (3, 7, 0)
modin_installed = True

if modin_compatible_version:
    try:
        import modin  # noqa: F401
    except ModuleNotFoundError:
        modin_installed = False

skip = not modin_compatible_version or not modin_installed

# These tests are written for versions of Modin that require python 3.7+
pytestmark = pytest.mark.skipif(skip, reason="Outdated or missing Modin dependency")

if not skip:
    from ray.tests.modin.modin_test_utils import df_equals
    import modin.pandas as pd


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
        # Cleanup state before rerunning tests with client
        ray.shutdown()


random_state = np.random.RandomState(seed=42)

# Size of test dataframes
NCOLS, NROWS = (2 ** 6, 2 ** 8)

# Range for values for test data
RAND_LOW = 0
RAND_HIGH = 100

# Input data and functions for the tests
# The test data that we will test our code against
test_data = {
    "int_data": {
        "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): random_state.randint(
            RAND_LOW, RAND_HIGH, size=(NROWS)
        )
        for i in range(NCOLS)
    },
    "float_nan_data": {
        "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): [
            x
            if (j % 4 == 0 and i > NCOLS // 2) or (j != i and i <= NCOLS // 2)
            else np.NaN
            for j, x in enumerate(
                random_state.uniform(RAND_LOW, RAND_HIGH, size=(NROWS))
            )
        ]
        for i in range(NCOLS)
    },
}

test_data["int_data"]["index"] = test_data["int_data"].pop(
    "col{}".format(int(NCOLS / 2))
)

for col in test_data["float_nan_data"]:
    for row in range(NROWS // 2):
        if row % 16 == 0:
            test_data["float_nan_data"][col][row] = np.NaN

test_data_values = list(test_data.values())
test_data_keys = list(test_data.keys())


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
            modin_df, modin_df2, how=how, left_on="col1", right_index=True
        )
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col1", right_index=True
        )
        df_equals(modin_result, pandas_result)

        # left_index and right_on
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_index=True, right_on="col1"
        )
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_index=True, right_on="col1"
        )
        df_equals(modin_result, pandas_result)

        # left_on and right_on col1
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_on="col1", right_on="col1"
        )
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col1", right_on="col1"
        )
        df_equals(modin_result, pandas_result)

        # left_on and right_on col2
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_on="col2", right_on="col2"
        )
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_on="col2", right_on="col2"
        )
        df_equals(modin_result, pandas_result)

        # left_index and right_index
        modin_result = pd.merge(
            modin_df, modin_df2, how=how, left_index=True, right_index=True
        )
        pandas_result = pandas.merge(
            pandas_df, pandas_df2, how=how, left_index=True, right_index=True
        )
        df_equals(modin_result, pandas_result)

    s = pd.Series(frame_data.get("col1"))
    with pytest.raises(ValueError):
        pd.merge(s, modin_df2)

    with pytest.raises(TypeError):
        pd.merge("Non-valid type", modin_df2)


def test_pivot():
    test_df = pd.DataFrame(
        {
            "foo": ["one", "one", "one", "two", "two", "two"],
            "bar": ["A", "B", "C", "A", "B", "C"],
            "baz": [1, 2, 3, 4, 5, 6],
            "zoo": ["x", "y", "z", "q", "w", "t"],
        }
    )

    df = pd.pivot(test_df, index="foo", columns="bar", values="baz")
    assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.pivot(test_df["bar"], index="foo", columns="bar", values="baz")


def test_pivot_table():
    test_df = pd.DataFrame(
        {
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
        }
    )

    df = pd.pivot_table(
        test_df, values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
    )
    assert isinstance(df, pd.DataFrame)

    with pytest.raises(ValueError):
        pd.pivot_table(
            test_df["C"], values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
        )


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
        pd.Series([pd.Timestamp("20160101"), pd.Timestamp("20160101")])
    )
    pandas_result = pandas.unique(
        pandas.Series([pandas.Timestamp("20160101"), pandas.Timestamp("20160101")])
    )
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(
        pd.Series(
            [
                pd.Timestamp("20160101", tz="US/Eastern"),
                pd.Timestamp("20160101", tz="US/Eastern"),
            ]
        )
    )
    pandas_result = pandas.unique(
        pandas.Series(
            [
                pandas.Timestamp("20160101", tz="US/Eastern"),
                pandas.Timestamp("20160101", tz="US/Eastern"),
            ]
        )
    )
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(
        pd.Index(
            [
                pd.Timestamp("20160101", tz="US/Eastern"),
                pd.Timestamp("20160101", tz="US/Eastern"),
            ]
        )
    )
    pandas_result = pandas.unique(
        pandas.Index(
            [
                pandas.Timestamp("20160101", tz="US/Eastern"),
                pandas.Timestamp("20160101", tz="US/Eastern"),
            ]
        )
    )
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape

    modin_result = pd.unique(pd.Series(pd.Categorical(list("baabc"))))
    pandas_result = pandas.unique(pandas.Series(pandas.Categorical(list("baabc"))))
    assert_array_equal(modin_result, pandas_result)
    assert modin_result.shape == pandas_result.shape


def test_to_datetime():
    # DataFrame input for to_datetime
    modin_df = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
    pandas_df = pandas.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
    df_equals(pd.to_datetime(modin_df), pandas.to_datetime(pandas_df))

    # Series input for to_datetime
    modin_s = pd.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 1000)
    pandas_s = pandas.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 1000)
    df_equals(pd.to_datetime(modin_s), pandas.to_datetime(pandas_s))

    # Other inputs for to_datetime
    value = 1490195805
    assert pd.to_datetime(value, unit="s") == pandas.to_datetime(value, unit="s")
    value = 1490195805433502912
    assert pd.to_datetime(value, unit="ns") == pandas.to_datetime(value, unit="ns")
    value = [1, 2, 3]
    assert pd.to_datetime(value, unit="D", origin=pd.Timestamp("2000-01-01")).equals(
        pandas.to_datetime(value, unit="D", origin=pandas.Timestamp("2000-01-01"))
    )


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
    modin_result = pd.to_numeric(modin_series, errors=errors, downcast=downcast)
    pandas_result = pandas.to_numeric(pandas_series, errors=errors, downcast=downcast)
    df_equals(modin_result, pandas_result)


def test_to_pandas_indices():
    data = test_data_values[0]

    md_df = pd.DataFrame(data)
    index = pandas.MultiIndex.from_tuples(
        [(i, i * 2) for i in np.arange(len(md_df) + 1)], names=["A", "B"]
    ).drop(0)
    columns = pandas.MultiIndex.from_tuples(
        [(i, i * 2) for i in np.arange(len(md_df.columns) + 1)], names=["A", "B"]
    ).drop(0)

    md_df.index = index
    md_df.columns = columns

    pd_df = md_df._to_pandas()

    for axis in [0, 1]:
        assert md_df.axes[axis].equals(
            pd_df.axes[axis]
        ), f"Indices at axis {axis} are different!"
        assert md_df.axes[axis].equal_levels(
            pd_df.axes[axis]
        ), f"Levels of indices at axis {axis} are different!"


def test_empty_dataframe():
    df = pd.DataFrame(columns=["a", "b"])
    df[(df.a == 1) & (df.b == 2)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
