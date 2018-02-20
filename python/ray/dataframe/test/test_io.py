from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas as pd
import ray
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
def ray_df_equals_pandas_wo_index(ray_df, pandas_df):
    """Compare two df as-is. Strip away the index"""
    ray_df = rdf.to_pandas(ray_df)
    pd_df = pandas_df

    same_content = np.array_equal(ray_df.as_matrix(), pd_df.as_matrix())
    same_columns = all(ray_df.columns == pd_df.columns)
    return same_content and same_columns


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
    ray.init()

    setup_parquet_file(SMALL_ROW_SIZE)

    pd_df = pd.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = io.read_parquet(TEST_PARQUET_FILENAME, npartitions=20)
    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_parquet_file()


def test_from_parquet_large():
    setup_parquet_file(LARGE_ROW_SIZE)

    pd_df = pd.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = io.read_parquet(TEST_PARQUET_FILENAME, npartitions=80)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_parquet_file()


def test_from_csv():
    setup_csv_file(SMALL_ROW_SIZE)

    pd_df = pd.read_csv(TEST_CSV_FILENAME)
    ray_df = io.read_csv(TEST_CSV_FILENAME, npartitions=20)
    assert ray_df_equals_pandas_wo_index(ray_df, pd_df)

    teardown_csv_file()


def test__vertical_concat():
    pd_df1 = pd.DataFrame({'col1': np.arange(10), 'col2': np.arange(10)})
    pd_df2 = pd.DataFrame({'col1': np.arange(10), 'col2': np.arange(10)})

    ray_df1 = rdf.from_pandas(pd_df1, npartitions=2)
    ray_df2 = rdf.from_pandas(pd_df2, npartitions=2)

    ray_con_singleton = io._vertical_concat([ray_df1])
    ray_con_list = io._vertical_concat([ray_df1, ray_df2])
    pd_con_singleton = pd.concat([pd_df1])
    pd_con_list = pd.concat([pd_df1, pd_df2])

    assert ray_df_equals_pandas(ray_con_singleton, pd_con_singleton)
    assert ray_df_equals_pandas(ray_con_list, pd_con_list)
