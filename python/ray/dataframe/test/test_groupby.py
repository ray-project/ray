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
