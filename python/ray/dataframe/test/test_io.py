from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas as pd
import ray.dataframe as rdf
import ray.dataframe.io as io
import os

TEST_PARQUET_FILENAME = 'test.parquet'
TEST_CSV_FILENAME = 'test.csv'
SMALL_ROW_SIZE = 2000
LARGE_ROW_SIZE = 7e6


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return rdf.to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


@pytest.fixture
def setup_parquet_file(row_size, force=False):
    if os.path.exists(TEST_PARQUET_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_parquet(TEST_PARQUET_FILENAME)


@pytest.fixture
def teardown_parquet_file():
    if os.path.exists(TEST_PARQUET_FILENAME):
        os.remove(TEST_PARQUET_FILENAME)


@pytest.fixture
def setup_csv_file(row_size, force=False):
    if os.path.exists(TEST_CSV_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_csv(TEST_CSV_FILENAME)


@pytest.fixture
def teardown_csv_file():
    if os.path.exists(TEST_CSV_FILENAME):
        os.remove(TEST_CSV_FILENAME)


def test_from_parquet_small():

    setup_parquet_file(SMALL_ROW_SIZE)

    pd_df = pd.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = io.read_parquet(TEST_PARQUET_FILENAME)
    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_parquet_file()


def test_from_parquet_large():
    setup_parquet_file(LARGE_ROW_SIZE)

    pd_df = pd.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = io.read_parquet(TEST_PARQUET_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_parquet_file()


def test_from_csv():
    setup_csv_file(SMALL_ROW_SIZE)

    pd_df = pd.read_csv(TEST_CSV_FILENAME)
    ray_df = io.read_csv(TEST_CSV_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_csv_file()
