from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas as pd
from ray.dataframe.utils import to_pandas
import ray.dataframe.io as io
import os
import sqlite3

from ray.dataframe.utils import to_pandas

TEST_PARQUET_FILENAME = 'test.parquet'
TEST_CSV_FILENAME = 'test.csv'
TEST_JSON_FILENAME = 'test.json'
TEST_HTML_FILENAME = 'test.html'
TEST_EXCEL_FILENAME = 'test.xlsx'
TEST_FEATHER_FILENAME = 'test.feather'
TEST_HDF_FILENAME = 'test.hdf'
TEST_MSGPACK_FILENAME = 'test.msg'
TEST_STATA_FILENAME = 'test.dta'
TEST_PICKLE_FILENAME = 'test.pkl'
TEST_SAS_FILENAME = 'data/test1.sas7bdat'
TEST_SQL_FILENAME = 'test.db'
SMALL_ROW_SIZE = 2000
LARGE_ROW_SIZE = 7e6


@pytest.fixture
def ray_df_equals_pandas(ray_df, pandas_df):
    return to_pandas(ray_df).sort_index().equals(pandas_df.sort_index())


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
def setup_csv_file(row_size, force=False, delimiter=','):
    if os.path.exists(TEST_CSV_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_csv(TEST_CSV_FILENAME, sep=delimiter)


@pytest.fixture
def teardown_csv_file():
    if os.path.exists(TEST_CSV_FILENAME):
        os.remove(TEST_CSV_FILENAME)


@pytest.fixture
def setup_json_file(row_size, force=False):
    if os.path.exists(TEST_JSON_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_json(TEST_JSON_FILENAME)


@pytest.fixture
def teardown_json_file():
    if os.path.exists(TEST_JSON_FILENAME):
        os.remove(TEST_JSON_FILENAME)


@pytest.fixture
def setup_html_file(row_size, force=False):
    if os.path.exists(TEST_HTML_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_html(TEST_HTML_FILENAME)


@pytest.fixture
def teardown_html_file():
    if os.path.exists(TEST_HTML_FILENAME):
        os.remove(TEST_HTML_FILENAME)


@pytest.fixture
def setup_clipboard(row_size, force=False):
    df = pd.DataFrame({
        'col1': np.arange(row_size),
        'col2': np.arange(row_size)
    })
    df.to_clipboard()


@pytest.fixture
def setup_excel_file(row_size, force=False):
    if os.path.exists(TEST_EXCEL_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_excel(TEST_EXCEL_FILENAME)


@pytest.fixture
def teardown_excel_file():
    if os.path.exists(TEST_EXCEL_FILENAME):
        os.remove(TEST_EXCEL_FILENAME)


@pytest.fixture
def setup_feather_file(row_size, force=False):
    if os.path.exists(TEST_FEATHER_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_feather(TEST_FEATHER_FILENAME)


@pytest.fixture
def teardown_feather_file():
    if os.path.exists(TEST_FEATHER_FILENAME):
        os.remove(TEST_FEATHER_FILENAME)


@pytest.fixture
def setup_hdf_file(row_size, force=False):
    if os.path.exists(TEST_HDF_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_hdf(TEST_HDF_FILENAME, 'test')


@pytest.fixture
def teardown_hdf_file():
    if os.path.exists(TEST_HDF_FILENAME):
        os.remove(TEST_HDF_FILENAME)


@pytest.fixture
def setup_msgpack_file(row_size, force=False):
    if os.path.exists(TEST_MSGPACK_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_msgpack(TEST_MSGPACK_FILENAME)


@pytest.fixture
def teardown_msgpack_file():
    if os.path.exists(TEST_MSGPACK_FILENAME):
        os.remove(TEST_MSGPACK_FILENAME)


@pytest.fixture
def setup_stata_file(row_size, force=False):
    if os.path.exists(TEST_STATA_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_stata(TEST_STATA_FILENAME)


@pytest.fixture
def teardown_stata_file():
    if os.path.exists(TEST_STATA_FILENAME):
        os.remove(TEST_STATA_FILENAME)


@pytest.fixture
def setup_pickle_file(row_size, force=False):
    if os.path.exists(TEST_PICKLE_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_pickle(TEST_PICKLE_FILENAME)


@pytest.fixture
def teardown_pickle_file():
    if os.path.exists(TEST_PICKLE_FILENAME):
        os.remove(TEST_PICKLE_FILENAME)


@pytest.fixture
def setup_sql_file(conn, force=False):
    if os.path.exists(TEST_SQL_FILENAME) and not force:
        pass
    else:
        df = pd.DataFrame({'col1': [0, 1, 2, 3],
                           'col2': [4, 5, 6, 7],
                           'col3': [8, 9, 10, 11],
                           'col4': [12, 13, 14, 15],
                           'col5': [0, 0, 0, 0]})
        df.to_sql(TEST_SQL_FILENAME.split(".")[0], conn)


@pytest.fixture
def teardown_sql_file():
    if os.path.exists(TEST_SQL_FILENAME):
        os.remove(TEST_SQL_FILENAME)


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


def test_from_json():
    setup_json_file(SMALL_ROW_SIZE)

    pd_df = pd.read_json(TEST_JSON_FILENAME)
    ray_df = io.read_json(TEST_JSON_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_json_file()


def test_from_html():
    setup_html_file(SMALL_ROW_SIZE)

    pd_df = pd.read_html(TEST_HTML_FILENAME)[0]
    ray_df = io.read_html(TEST_HTML_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_html_file()


def test_from_clipboard():
    setup_clipboard(SMALL_ROW_SIZE)

    pd_df = pd.read_clipboard()
    ray_df = io.read_clipboard()

    assert ray_df_equals_pandas(ray_df, pd_df)


def test_from_excel():
    setup_excel_file(SMALL_ROW_SIZE)

    pd_df = pd.read_excel(TEST_EXCEL_FILENAME)
    ray_df = io.read_excel(TEST_EXCEL_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_excel_file()


def test_from_feather():
    setup_feather_file(SMALL_ROW_SIZE)

    pd_df = pd.read_feather(TEST_FEATHER_FILENAME)
    ray_df = io.read_feather(TEST_FEATHER_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_feather_file()


def test_from_hdf():
    setup_hdf_file(SMALL_ROW_SIZE)

    pd_df = pd.read_hdf(TEST_HDF_FILENAME, key='test')
    ray_df = io.read_hdf(TEST_HDF_FILENAME, key='test')

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_hdf_file()


def test_from_msgpack():
    setup_msgpack_file(SMALL_ROW_SIZE)

    pd_df = pd.read_msgpack(TEST_MSGPACK_FILENAME)
    ray_df = io.read_msgpack(TEST_MSGPACK_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_msgpack_file()


def test_from_stata():
    setup_stata_file(SMALL_ROW_SIZE)

    pd_df = pd.read_stata(TEST_STATA_FILENAME)
    ray_df = io.read_stata(TEST_STATA_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_stata_file()


def test_from_pickle():
    setup_pickle_file(SMALL_ROW_SIZE)

    pd_df = pd.read_pickle(TEST_PICKLE_FILENAME)
    ray_df = io.read_pickle(TEST_PICKLE_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_pickle_file()


def test_from_sql():
    conn = sqlite3.connect(TEST_SQL_FILENAME)
    setup_sql_file(conn, True)

    pd_df = pd.read_sql("select * from test", conn)
    ray_df = io.read_sql("select * from test", conn)

    assert ray_df_equals_pandas(ray_df, pd_df)

    teardown_sql_file()


def test_from_sas():
    pd_df = pd.read_sas(TEST_SAS_FILENAME)
    ray_df = io.read_sas(TEST_SAS_FILENAME)

    assert ray_df_equals_pandas(ray_df, pd_df)
