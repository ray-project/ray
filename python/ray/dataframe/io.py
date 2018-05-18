from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
from io import BytesIO
import os
import pathlib
import py
import re
import warnings

from pyarrow.parquet import ParquetFile
import pandas as pd
from pandas.io.common import _infer_compression  # don't depend on internal API


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
def _skip_header(fp, kwargs={}):
    lines_read = 0
    comment = kwargs["comment"]
    skiprows = kwargs["skiprows"]
    encoding = kwargs["encoding"]
    header = kwargs["header"]
    names = kwargs["names"]

    if header is None:
        return lines_read

    if header == "infer":
        if names is not None:
            return lines_read
        else:
            header = 0

    # Skip lines before the header
    if isinstance(skiprows, int):
        lines_read += skiprows
        for _ in range(skiprows):
            fp.readline()
        skiprows = None

    header_lines = header + 1 if isinstance(header, int) else max(header) + 1

    header_lines_skipped = 0
    for line in fp:
        lines_read += 1
        skip = False
        if not skip and comment is not None:
            if encoding is not None:
                skip |= line.decode(encoding)[0] == comment
            else:
                skip |= line.decode()[0] == comment
        if not skip and callable(skiprows):
            skip |= skiprows(lines_read)
        elif not skip and hasattr(skiprows, "__contains__"):
            skip |= lines_read in skiprows

        if not skip:
            header_lines_skipped += 1
            if header_lines_skipped == header_lines:
                return lines_read

    return lines_read


def _read_csv_from_file(filepath, npartitions, kwargs={}):
    """Constructs a FataFrame from a CSV file.

    Args:
        filepath (str): path to the CSV file.
        npartitions (int): number of partitions for the DataFrame.
        kwargs (dict): args excluding filepath provided to read_csv.

    Returns:
        DataFrame constructed from CSV file.
    """
    empty_pd_df = pd.read_csv(filepath, **dict(kwargs, nrows=0))
    names = empty_pd_df.columns
    partition_kwargs = dict(kwargs, header=None, names=names)
    with open(filepath, "rb") as f:
        # Get the BOM if necessary
        prefix = b""
        if kwargs["encoding"] is not None:
            prefix = f.readline()
            partition_kwargs["skiprows"] = 1
            f.seek(0, os.SEEK_SET)  # Return to beginning of file

        prefix_id = ray.put(prefix)
        partition_kwargs_id = ray.put(partition_kwargs)

        # Skip the header since we already have the header information
        _skip_header(f, kwargs)

        # Launch tasks to read partitions
        partition_ids = []
        index_ids = []
        total_bytes = os.path.getsize(filepath)
        chunk_size = max(1, (total_bytes - f.tell()) // npartitions)
        while f.tell() < total_bytes:
            start = f.tell()
            f.seek(chunk_size, os.SEEK_CUR)
            f.readline()    # Read a whole number of lines
            partition_id, index_id = _read_csv_with_offset._submit(
                    args=(filepath, start, f.tell(), partition_kwargs_id,
                          prefix_id),
                    num_return_vals=2)
            partition_ids.append(partition_id)
            index_ids.append(index_id)

        # Construct index
        index_id = get_index.remote([empty_pd_df.index.name], *index_ids) \
            if kwargs["index_col"] is not None else None

        return DataFrame(row_partitions=partition_ids, columns=names,
                         index=index_id)


@ray.remote
def _read_csv_with_offset(fn, start, end, kwargs={}, header=b''):
    kwargs["quoting"] = int(kwargs["quoting"])  # See issue #2078

    bio = open(fn, 'rb')
    bio.seek(start)
    to_read = header + bio.read(end - start)
    bio.close()
    pd_df = pd.read_csv(BytesIO(to_read), **kwargs)
    index = pd_df.index
    # Partitions must have RangeIndex
    pd_df.index = pd.RangeIndex(0, len(pd_df))
    return pd_df, index


def _read_csv_from_pandas(filepath_or_buffer, kwargs):
    pd_obj = pd.read_csv(filepath_or_buffer, **kwargs)

    if isinstance(pd_obj, pd.DataFrame):
        return from_pandas(pd_obj, get_npartitions())
    elif isinstance(pd_obj, pd.io.parsers.TextFileReader):
        # Overwriting the read method should return a ray DataFrame for calls
        # to __next__ and get_chunk
        pd_read = pd_obj.read
        pd_obj.read = lambda *args, **kwargs: \
            from_pandas(pd_read(*args, **kwargs), get_npartitions())

    return pd_obj


@ray.remote
def get_index(index_name, *partition_indices):
    index = partition_indices[0].append(partition_indices[1:])
    index.names = index_name
    return index


def read_csv(filepath_or_buffer,
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
             doublequote=True,
             delim_whitespace=False,
             low_memory=True,
             memory_map=False,
             float_precision=None):
    """Read csv file from local disk.

    Args:
        filepath:
              The filepath of the csv file.
              We only support local files for now.
        kwargs: Keyword arguments in pandas::from_csv
    """

    kwargs = dict(
            sep=sep,
            delimiter=delimiter,
            header=header,
            names=names,
            index_col=index_col,
            usecols=usecols,
            squeeze=squeeze,
            prefix=prefix,
            mangle_dupe_cols=mangle_dupe_cols,
            dtype=dtype,
            engine=engine,
            converters=converters,
            true_values=true_values,
            false_values=false_values,
            skipinitialspace=skipinitialspace,
            skiprows=skiprows,
            nrows=nrows,
            na_values=na_values,
            keep_default_na=keep_default_na,
            na_filter=na_filter,
            verbose=verbose,
            skip_blank_lines=skip_blank_lines,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
            keep_date_col=keep_date_col,
            date_parser=date_parser,
            dayfirst=dayfirst,
            iterator=iterator,
            chunksize=chunksize,
            compression=compression,
            thousands=thousands,
            decimal=decimal,
            lineterminator=lineterminator,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            comment=comment,
            encoding=encoding,
            dialect=dialect,
            tupleize_cols=tupleize_cols,
            error_bad_lines=error_bad_lines,
            warn_bad_lines=warn_bad_lines,
            skipfooter=skipfooter,
            doublequote=doublequote,
            delim_whitespace=delim_whitespace,
            low_memory=low_memory,
            memory_map=memory_map,
            float_precision=float_precision)

    if isinstance(filepath_or_buffer, str):
        if not os.path.exists(filepath_or_buffer):
            warnings.warn(("File not found on disk. "
                           "Defaulting to Pandas implementation."),
                          PendingDeprecationWarning)

            return _read_csv_from_pandas(filepath_or_buffer, kwargs)
    elif not isinstance(filepath_or_buffer, pathlib.Path) and \
            not isinstance(filepath_or_buffer, py.path.local):
        warnings.warn(("Reading from buffer. "
                       "Defaulting to Pandas implementation."),
                      PendingDeprecationWarning)

        return _read_csv_from_pandas(filepath_or_buffer, kwargs)

    if _infer_compression(filepath_or_buffer, compression) is not None:
        warnings.warn(("Compression detected. "
                       "Defaulting to Pandas implementation."),
                      PendingDeprecationWarning)

        return _read_csv_from_pandas(filepath_or_buffer, kwargs)

    if chunksize is not None:
        warnings.warn(("Reading chunks from a file. "
                       "Defaulting to Pandas implementation."),
                      PendingDeprecationWarning)

        return _read_csv_from_pandas(filepath_or_buffer, kwargs)

    if skiprows is not None and not isinstance(skiprows, int):
        warnings.warn(("Defaulting to Pandas implementation. To speed up "
                       "read_csv through the Pandas on Ray implementation, "
                       "comment the rows to skip instead."))

        return _read_csv_from_pandas(filepath_or_buffer, kwargs)

    return _read_csv_from_file(filepath_or_buffer, get_npartitions(), kwargs)


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
