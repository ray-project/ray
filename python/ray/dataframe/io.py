from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
from io import BytesIO
import os
import re
import warnings

from pyarrow.parquet import ParquetFile
import pandas as pd

from .dataframe import ray, DataFrame
from . import get_npartitions
from .utils import from_pandas


# Parquet
def read_parquet(path, engine='auto', columns=None, **kwargs):
    """Load a parquet object from the file path, returning a DataFrame.
    Ray DataFrame only supports pyarrow engine for now.

    Args:
        path: The filepath of the parquet file.
              We only support local files for now.
        engine: Ray only support pyarrow reader.
                This argument doesn't do anything for now.
        kwargs: Pass into parquet's read_row_group function.
    """
    pf = ParquetFile(path)

    n_rows = pf.metadata.num_rows
    chunksize = n_rows // get_npartitions()
    n_row_groups = pf.metadata.num_row_groups

    idx_regex = re.compile('__index_level_\d+__')
    columns = [
        name for name in pf.metadata.schema.names if not idx_regex.match(name)
    ]

    df_from_row_groups = [
        _read_parquet_row_group.remote(path, columns, i, kwargs)
        for i in range(n_row_groups)
    ]
    splited_dfs = ray.get(
        [_split_df.remote(df, chunksize) for df in df_from_row_groups])
    df_remotes = list(chain.from_iterable(splited_dfs))

    return DataFrame(row_partitions=df_remotes, columns=columns)


@ray.remote
def _read_parquet_row_group(path, columns, row_group_id, kwargs={}):
    """Read a parquet row_group given file_path.
    """
    pf = ParquetFile(path)
    df = pf.read_row_group(row_group_id, columns=columns, **kwargs).to_pandas()
    return df


@ray.remote
def _split_df(pd_df, chunksize):
    """Split a pd_df into partitions.

    Returns:
        remote_df_ids ([ObjectID])
    """
    dataframes = []

    while len(pd_df) > chunksize:
        t_df = pd_df[:chunksize]
        t_df.reset_index(drop=True)
        top = ray.put(t_df)
        dataframes.append(top)
        pd_df = pd_df[chunksize:]
    else:
        pd_df = pd_df.reset_index(drop=True)
        dataframes.append(ray.put(pd_df))

    return dataframes


# CSV
def _compute_offset(fn, npartitions):
    """
    Calculate the currect bytes offsets for a csv file.
    Return a list of (start, end) tuple where the end == \n or EOF.
    """
    total_bytes = os.path.getsize(fn)
    chunksize = total_bytes // npartitions
    if chunksize == 0:
        chunksize = 1

    bio = open(fn, 'rb')

    offsets = []
    start = 0
    while start <= total_bytes:
        bio.seek(chunksize, 1)  # Move forward {chunksize} bytes
        extend_line = bio.readline()  # Move after the next \n
        total_offset = chunksize + len(extend_line)
        # The position of the \n we just crossed.
        new_line_cursor = start + total_offset - 1
        offsets.append((start, new_line_cursor))
        start = new_line_cursor + 1

    bio.close()
    return offsets


def _get_firstline(file_path):
    bio = open(file_path, 'rb')
    first = bio.readline()
    bio.close()
    return first


def _infer_column(first_line, kwargs={}):
    return pd.read_csv(BytesIO(first_line), **kwargs).columns


@ray.remote
def _read_csv_with_offset(fn, start, end, header=b'', kwargs={}):
    bio = open(fn, 'rb')
    bio.seek(start)
    to_read = header + bio.read(end - start)
    bio.close()
    return pd.read_csv(BytesIO(to_read), **kwargs)


def read_csv(filepath,
             sep=',',
             delimiter=None,
             header='infer',
             names=None,
             index_col=None,
             usecols=None,
             squeeze=False,
             prefix=None,
             mangle_dupe_cols=True,
             dtype=None,
             engine=None,
             converters=None,
             true_values=None,
             false_values=None,
             skipinitialspace=False,
             skiprows=None,
             nrows=None,
             na_values=None,
             keep_default_na=True,
             na_filter=True,
             verbose=False,
             skip_blank_lines=True,
             parse_dates=False,
             infer_datetime_format=False,
             keep_date_col=False,
             date_parser=None,
             dayfirst=False,
             iterator=False,
             chunksize=None,
             compression='infer',
             thousands=None,
             decimal=b'.',
             lineterminator=None,
             quotechar='"',
             quoting=0,
             escapechar=None,
             comment=None,
             encoding=None,
             dialect=None,
             tupleize_cols=None,
             error_bad_lines=True,
             warn_bad_lines=True,
             skipfooter=0,
             skip_footer=0,
             doublequote=True,
             delim_whitespace=False,
             as_recarray=None,
             compact_ints=None,
             use_unsigned=None,
             low_memory=True,
             buffer_lines=None,
             memory_map=False,
             float_precision=None):
    """Read csv file from local disk.

    Args:
        filepath:
              The filepath of the csv file.
              We only support local files for now.
        kwargs: Keyword arguments in pandas::from_csv
    """

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_csv(filepath,
                             sep=',',
                             delimiter=None,
                             header='infer',
                             names=None,
                             index_col=None,
                             usecols=None,
                             squeeze=False,
                             prefix=None,
                             mangle_dupe_cols=True,
                             dtype=None,
                             engine=None,
                             converters=None,
                             true_values=None,
                             false_values=None,
                             skipinitialspace=False,
                             skiprows=None,
                             nrows=None,
                             na_values=None,
                             keep_default_na=True,
                             na_filter=True,
                             verbose=False,
                             skip_blank_lines=True,
                             parse_dates=False,
                             infer_datetime_format=False,
                             keep_date_col=False,
                             date_parser=None,
                             dayfirst=False,
                             iterator=False,
                             chunksize=None,
                             compression='infer',
                             thousands=None,
                             decimal=b'.',
                             lineterminator=None,
                             quotechar='"',
                             quoting=0,
                             escapechar=None,
                             comment=None,
                             encoding=None,
                             dialect=None,
                             tupleize_cols=None,
                             error_bad_lines=True,
                             warn_bad_lines=True,
                             skipfooter=0,
                             skip_footer=0,
                             doublequote=True,
                             delim_whitespace=False,
                             as_recarray=None,
                             compact_ints=None,
                             use_unsigned=None,
                             low_memory=True,
                             buffer_lines=None,
                             memory_map=False,
                             float_precision=None)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_json(path_or_buf=None,
              orient=None,
              typ='frame',
              dtype=True,
              convert_axes=True,
              convert_dates=True,
              keep_default_dates=True,
              numpy=False,
              precise_float=False,
              date_unit=None,
              encoding=None,
              lines=False,
              chunksize=None,
              compression='infer'):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_json(path_or_buf, orient, typ, dtype,
                              convert_axes, convert_dates, keep_default_dates,
                              numpy, precise_float, date_unit, encoding,
                              lines, chunksize, compression)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_html(io,
              match='.+',
              flavor=None,
              header=None,
              index_col=None,
              skiprows=None,
              attrs=None,
              parse_dates=False,
              tupleize_cols=None,
              thousands=',',
              encoding=None,
              decimal='.',
              converters=None,
              na_values=None,
              keep_default_na=True):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_html(io, match, flavor, header, index_col,
                              skiprows, attrs, parse_dates, tupleize_cols,
                              thousands, encoding, decimal, converters,
                              na_values, keep_default_na)
    ray_frame = from_pandas(port_frame[0], get_npartitions())

    return ray_frame


def read_clipboard(sep=r'\s+'):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_clipboard(sep)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_excel(io,
               sheet_name=0,
               header=0,
               skiprows=None,
               skip_footer=0,
               index_col=None,
               names=None,
               usecols=None,
               parse_dates=False,
               date_parser=None,
               na_values=None,
               thousands=None,
               convert_float=True,
               converters=None,
               dtype=None,
               true_values=None,
               false_values=None,
               engine=None,
               squeeze=False):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_excel(io, sheet_name, header, skiprows, skip_footer,
                               index_col, names, usecols, parse_dates,
                               date_parser, na_values, thousands,
                               convert_float, converters, dtype, true_values,
                               false_values, engine, squeeze)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_hdf(path_or_buf,
             key=None,
             mode='r'):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_hdf(path_or_buf, key, mode)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_feather(path,
                 nthreads=1):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_feather(path)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_msgpack(path_or_buf,
                 encoding='utf-8',
                 iterator=False):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_msgpack(path_or_buf, encoding, iterator)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_stata(filepath_or_buffer,
               convert_dates=True,
               convert_categoricals=True,
               encoding=None,
               index_col=None,
               convert_missing=False,
               preserve_dtypes=True,
               columns=None,
               order_categoricals=True,
               chunksize=None,
               iterator=False):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_stata(filepath_or_buffer, convert_dates,
                               convert_categoricals, encoding, index_col,
                               convert_missing, preserve_dtypes, columns,
                               order_categoricals, chunksize, iterator)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_sas(filepath_or_buffer,
             format=None,
             index=None,
             encoding=None,
             chunksize=None,
             iterator=False):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_sas(filepath_or_buffer, format, index, encoding,
                             chunksize, iterator)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_pickle(path,
                compression='infer'):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_pickle(path, compression)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame


def read_sql(sql,
             con,
             index_col=None,
             coerce_float=True,
             params=None,
             parse_dates=None,
             columns=None,
             chunksize=None):

    warnings.warn("Defaulting to Pandas implementation",
                  PendingDeprecationWarning)

    port_frame = pd.read_sql(sql, con, index_col, coerce_float, params,
                             parse_dates, columns, chunksize)
    ray_frame = from_pandas(port_frame, get_npartitions())

    return ray_frame
