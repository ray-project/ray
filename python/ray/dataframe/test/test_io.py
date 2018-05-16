from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import numpy as np
import pandas
from ray.dataframe.utils import to_pandas
import ray.dataframe as pd
import os
import sqlite3

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
TEST_SAS_FILENAME = os.getcwd() + '/data/test1.sas7bdat'
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
        df = pandas.DataFrame({
            'col1': np.arange(row_size),
            'col2': np.arange(row_size)
        })
        df.to_parquet(TEST_PARQUET_FILENAME)


@pytest.fixture
def create_test_ray_dataframe():
    df = pd.DataFrame({
        'col1': [0, 1, 2, 3],
        'col2': [4, 5, 6, 7],
        'col3': [8, 9, 10, 11],
        'col4': [12, 13, 14, 15],
        'col5': [0, 0, 0, 0]
    })

    return df


@pytest.fixture
def create_test_pandas_dataframe():
    df = pandas.DataFrame({
        'col1': [0, 1, 2, 3],
        'col2': [4, 5, 6, 7],
        'col3': [8, 9, 10, 11],
        'col4': [12, 13, 14, 15],
        'col5': [0, 0, 0, 0]
    })

    return df


@pytest.fixture
def test_files_eq(path1, path2):
    with open(path1, 'rb') as file1, open(path2, 'rb') as file2:
        file1_content = file1.read()
        file2_content = file2.read()

        if file1_content == file2_content:
            return True
        else:
            return False


@pytest.fixture
def teardown_test_file(test_path):
    if os.path.exists(test_path):
        os.remove(test_path)


@pytest.fixture
def teardown_parquet_file():
    if os.path.exists(TEST_PARQUET_FILENAME):
        os.remove(TEST_PARQUET_FILENAME)


@pytest.fixture
def setup_csv_file(row_size, force=False, delimiter=','):
    if os.path.exists(TEST_CSV_FILENAME) and not force:
        pass
    else:
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
    df = pandas.DataFrame({
        'col1': np.arange(row_size),
        'col2': np.arange(row_size)
    })
    df.to_clipboard()


@pytest.fixture
def setup_excel_file(row_size, force=False):
    if os.path.exists(TEST_EXCEL_FILENAME) and not force:
        pass
    else:
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
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
        df = pandas.DataFrame({
            'col1': [0, 1, 2, 3],
            'col2': [4, 5, 6, 7],
            'col3': [8, 9, 10, 11],
            'col4': [12, 13, 14, 15],
            'col5': [0, 0, 0, 0]
        })
        df.to_sql(TEST_SQL_FILENAME.split(".")[0], conn)


@pytest.fixture
def teardown_sql_file():
    if os.path.exists(TEST_SQL_FILENAME):
        os.remove(TEST_SQL_FILENAME)


def test_from_parquet_small():

    setup_parquet_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = pd.read_parquet(TEST_PARQUET_FILENAME)
    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_parquet_file()


def test_from_parquet_large():
    setup_parquet_file(LARGE_ROW_SIZE)

    pandas_df = pandas.read_parquet(TEST_PARQUET_FILENAME)
    ray_df = pd.read_parquet(TEST_PARQUET_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_parquet_file()


def test_from_csv():
    setup_csv_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_csv(TEST_CSV_FILENAME)
    ray_df = pd.read_csv(TEST_CSV_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_csv_file()


def test_from_json():
    setup_json_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_json(TEST_JSON_FILENAME)
    ray_df = pd.read_json(TEST_JSON_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_json_file()


def test_from_html():
    setup_html_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_html(TEST_HTML_FILENAME)[0]
    ray_df = pd.read_html(TEST_HTML_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_html_file()


@pytest.mark.skip(reason="No clipboard on Travis")
def test_from_clipboard():
    setup_clipboard(SMALL_ROW_SIZE)

    pandas_df = pandas.read_clipboard()
    ray_df = pd.read_clipboard()

    assert ray_df_equals_pandas(ray_df, pandas_df)


def test_from_excel():
    setup_excel_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_excel(TEST_EXCEL_FILENAME)
    ray_df = pd.read_excel(TEST_EXCEL_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_excel_file()


def test_from_feather():
    setup_feather_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_feather(TEST_FEATHER_FILENAME)
    ray_df = pd.read_feather(TEST_FEATHER_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_feather_file()


@pytest.mark.skip(reason="Memory overflow on Travis")
def test_from_hdf():
    setup_hdf_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_hdf(TEST_HDF_FILENAME, key='test')
    ray_df = pd.read_hdf(TEST_HDF_FILENAME, key='test')

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_hdf_file()


def test_from_msgpack():
    setup_msgpack_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_msgpack(TEST_MSGPACK_FILENAME)
    ray_df = pd.read_msgpack(TEST_MSGPACK_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_msgpack_file()


def test_from_stata():
    setup_stata_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_stata(TEST_STATA_FILENAME)
    ray_df = pd.read_stata(TEST_STATA_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_stata_file()


def test_from_pickle():
    setup_pickle_file(SMALL_ROW_SIZE)

    pandas_df = pandas.read_pickle(TEST_PICKLE_FILENAME)
    ray_df = pd.read_pickle(TEST_PICKLE_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_pickle_file()


def test_from_sql():
    conn = sqlite3.connect(TEST_SQL_FILENAME)
    setup_sql_file(conn, True)

    pandas_df = pandas.read_sql("select * from test", conn)
    ray_df = pd.read_sql("select * from test", conn)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_sql_file()


@pytest.mark.skip(reason="No SAS write methods in Pandas")
def test_from_sas():
    pandas_df = pandas.read_sas(TEST_SAS_FILENAME)
    ray_df = pd.read_sas(TEST_SAS_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)


def test_from_csv_delimiter():
    setup_csv_file(SMALL_ROW_SIZE, delimiter='|')

    pandas_df = pandas.read_csv(TEST_CSV_FILENAME)
    ray_df = pd.read_csv(TEST_CSV_FILENAME)

    assert ray_df_equals_pandas(ray_df, pandas_df)

    teardown_csv_file()


@pytest.mark.skip(reason="No clipboard on Travis")
def test_to_clipboard():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    ray_df.to_clipboard()
    ray_as_clip = pandas.read_clipboard()

    pandas_df.to_clipboard()
    pandas_as_clip = pandas.read_clipboard()

    assert (ray_as_clip.equals(pandas_as_clip))


def test_to_csv():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_CSV_DF_FILENAME = "test_df.csv"
    TEST_CSV_pandas_FILENAME = "test_pandas.csv"

    ray_df.to_csv(TEST_CSV_DF_FILENAME)
    pandas_df.to_csv(TEST_CSV_pandas_FILENAME)

    assert (test_files_eq(TEST_CSV_DF_FILENAME, TEST_CSV_pandas_FILENAME))

    teardown_test_file(TEST_CSV_pandas_FILENAME)
    teardown_test_file(TEST_CSV_DF_FILENAME)


def test_to_dense():
    ray_df = create_test_ray_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_dense()


def test_to_dict():
    ray_df = create_test_ray_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_dict()


def test_to_excel():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_EXCEL_DF_FILENAME = "test_df.xlsx"
    TEST_EXCEL_pandas_FILENAME = "test_pandas.xlsx"

    ray_writer = pandas.ExcelWriter(TEST_EXCEL_DF_FILENAME)
    pandas_writer = pandas.ExcelWriter(TEST_EXCEL_pandas_FILENAME)

    ray_df.to_excel(ray_writer)
    pandas_df.to_excel(pandas_writer)

    ray_writer.save()
    pandas_writer.save()

    assert (test_files_eq(TEST_EXCEL_DF_FILENAME, TEST_EXCEL_pandas_FILENAME))

    teardown_test_file(TEST_EXCEL_DF_FILENAME)
    teardown_test_file(TEST_EXCEL_pandas_FILENAME)


def test_to_feather():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_FEATHER_DF_FILENAME = "test_df.feather"
    TEST_FEATHER_pandas_FILENAME = "test_pandas.feather"

    ray_df.to_feather(TEST_FEATHER_DF_FILENAME)
    pandas_df.to_feather(TEST_FEATHER_pandas_FILENAME)

    assert (
        test_files_eq(TEST_FEATHER_DF_FILENAME, TEST_FEATHER_pandas_FILENAME)
    )

    teardown_test_file(TEST_FEATHER_pandas_FILENAME)
    teardown_test_file(TEST_FEATHER_DF_FILENAME)


def test_to_gbq():
    ray_df = create_test_ray_dataframe()

    TEST_GBQ_DF_FILENAME = "test_df.gbq"
    with pytest.raises(NotImplementedError):
        ray_df.to_gbq(TEST_GBQ_DF_FILENAME, None)


def test_to_html():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_HTML_DF_FILENAME = "test_df.html"
    TEST_HTML_pandas_FILENAME = "test_pandas.html"

    ray_df.to_html(TEST_HTML_DF_FILENAME)
    pandas_df.to_html(TEST_HTML_pandas_FILENAME)

    assert (test_files_eq(TEST_HTML_DF_FILENAME, TEST_HTML_pandas_FILENAME))

    teardown_test_file(TEST_HTML_pandas_FILENAME)
    teardown_test_file(TEST_HTML_DF_FILENAME)


def test_to_json():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_JSON_DF_FILENAME = "test_df.json"
    TEST_JSON_pandas_FILENAME = "test_pandas.json"

    ray_df.to_json(TEST_JSON_DF_FILENAME)
    pandas_df.to_json(TEST_JSON_pandas_FILENAME)

    assert (test_files_eq(TEST_JSON_DF_FILENAME, TEST_JSON_pandas_FILENAME))

    teardown_test_file(TEST_JSON_pandas_FILENAME)
    teardown_test_file(TEST_JSON_DF_FILENAME)


def test_to_latex():
    ray_df = create_test_ray_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_latex()


def test_to_msgpack():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_MSGPACK_DF_FILENAME = "test_df.msgpack"
    TEST_MSGPACK_pandas_FILENAME = "test_pandas.msgpack"

    ray_df.to_msgpack(TEST_MSGPACK_DF_FILENAME)
    pandas_df.to_msgpack(TEST_MSGPACK_pandas_FILENAME)

    assert (
        test_files_eq(TEST_MSGPACK_DF_FILENAME, TEST_MSGPACK_pandas_FILENAME)
    )

    teardown_test_file(TEST_MSGPACK_pandas_FILENAME)
    teardown_test_file(TEST_MSGPACK_DF_FILENAME)


def test_to_panel():
    ray_df = create_test_ray_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_panel()


def test_to_parquet():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_PARQUET_DF_FILENAME = "test_df.parquet"
    TEST_PARQUET_pandas_FILENAME = "test_pandas.parquet"

    ray_df.to_parquet(TEST_PARQUET_DF_FILENAME)
    pandas_df.to_parquet(TEST_PARQUET_pandas_FILENAME)

    assert (
        test_files_eq(TEST_PARQUET_DF_FILENAME, TEST_PARQUET_pandas_FILENAME)
    )

    teardown_test_file(TEST_PARQUET_pandas_FILENAME)
    teardown_test_file(TEST_PARQUET_DF_FILENAME)


def test_to_period():
    ray_df = create_test_ray_dataframe()

    with pytest.raises(NotImplementedError):
        ray_df.to_period()


def test_to_pickle():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_PICKLE_DF_FILENAME = "test_df.pkl"
    TEST_PICKLE_pandas_FILENAME = "test_pandas.pkl"

    ray_df.to_pickle(TEST_PICKLE_DF_FILENAME)
    pandas_df.to_pickle(TEST_PICKLE_pandas_FILENAME)

    assert (test_files_eq(TEST_PICKLE_DF_FILENAME, TEST_PICKLE_pandas_FILENAME))

    teardown_test_file(TEST_PICKLE_pandas_FILENAME)
    teardown_test_file(TEST_PICKLE_DF_FILENAME)


def test_to_sql():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_SQL_DF_FILENAME = "test_df.sql"
    TEST_SQL_pandas_FILENAME = "test_pandas.sql"

    ray_df.to_pickle(TEST_SQL_DF_FILENAME)
    pandas_df.to_pickle(TEST_SQL_pandas_FILENAME)

    assert (test_files_eq(TEST_SQL_DF_FILENAME, TEST_SQL_pandas_FILENAME))

    teardown_test_file(TEST_SQL_DF_FILENAME)
    teardown_test_file(TEST_SQL_pandas_FILENAME)


def test_to_stata():
    ray_df = create_test_ray_dataframe()
    pandas_df = create_test_pandas_dataframe()

    TEST_STATA_DF_FILENAME = "test_df.stata"
    TEST_STATA_pandas_FILENAME = "test_pandas.stata"

    ray_df.to_stata(TEST_STATA_DF_FILENAME)
    pandas_df.to_stata(TEST_STATA_pandas_FILENAME)

    assert (test_files_eq(TEST_STATA_DF_FILENAME, TEST_STATA_pandas_FILENAME))

    teardown_test_file(TEST_STATA_pandas_FILENAME)
    teardown_test_file(TEST_STATA_DF_FILENAME)
