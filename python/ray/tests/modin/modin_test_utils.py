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
# This file is copied and adapted from
# http://github.com/modin-project/modin/master/modin/pandas/test/utils.py

import pandas
import modin.pandas as pd
from modin.utils import to_pandas
from pandas.testing import (
    assert_series_equal,
    assert_frame_equal,
    assert_extension_array_equal,
    assert_index_equal,
)
import numpy as np


def categories_equals(left, right):
    assert (left.ordered and right.ordered) or (not left.ordered and not right.ordered)
    assert_extension_array_equal(left, right)


def df_categories_equals(df1, df2):
    if not hasattr(df1, "select_dtypes"):
        if isinstance(df1, pandas.CategoricalDtype):
            return categories_equals(df1, df2)
        elif isinstance(getattr(df1, "dtype"), pandas.CategoricalDtype) and isinstance(
            getattr(df1, "dtype"), pandas.CategoricalDtype
        ):
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
    if (
        hasattr(df1, "index")
        and hasattr(df2, "index")
        and len(df1) == 0
        and len(df2) == 0
    ):
        if type(df1).__name__ == type(df2).__name__:
            if hasattr(df1, "name") and hasattr(df2, "name") and df1.name == df2.name:
                return
            if (
                hasattr(df1, "columns")
                and hasattr(df2, "columns")
                and df1.columns.equals(df2.columns)
            ):
                return
        assert False

    if isinstance(df1, (list, tuple)) and all(
        isinstance(d, (pd.DataFrame, pd.Series, pandas.DataFrame, pandas.Series))
        for d in df1
    ):
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
        assert_series_equal(df1, df2, check_dtype=False, check_series_type=False)
    elif isinstance(df1, groupby_types) and isinstance(df2, groupby_types):
        for g1, g2 in zip(df1, df2):
            assert g1[0] == g2[0]
            df_equals(g1[1], g2[1])
    elif (
        isinstance(df1, pandas.Series)
        and isinstance(df2, pandas.Series)
        and df1.empty
        and df2.empty
    ):
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
