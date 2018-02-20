from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
from io import BytesIO
import os

from pyarrow.parquet import ParquetFile
import pandas as pd

from .dataframe import from_pandas, ray, DataFrame


def read_parquet(path, columns=None, npartitions=None, chunksize=None):
    """Load a parquet object from the file path, returning a DataFrame.
    Ray DataFrame only supports pyarrow engine for now.

    Args:
        path: The filepath of the parquet file.
              We only support local files for now.
        columns: The columns to read from the parquet file.
                 If columns != None, only the subset of columsn will be read.
        npartitions: See dataframe::from_pandas
        chunksize: See dataframe::from_pandas
    """
    pf = ParquetFile(path)
    n_row_groups = pf.metadata.num_row_groups
    npartitions = npartitions // n_row_groups
    ray_row_groups = [
        _read_parquet_row_group.remote(path, columns, i, npartitions,
                                       chunksize) for i in range(n_row_groups)
    ]
    return _vertical_concat(ray.get(ray_row_groups))


@ray.remote
def _read_parquet_row_group(path, columns, row_group, npartitions, chunksize):
    """Read a parquet row_group given file_path. This function returns
    a Ray DataFrame
    """
    pf = ParquetFile(path)
    df = pf.read_row_group(row_group, columns=columns).to_pandas()
    return from_pandas(df, npartitions=npartitions, chunksize=chunksize)


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


def _get_first_line(fn):
    bio = open(fn, 'rb')
    first = bio.readline()
    bio.close()
    return first


@ray.remote
def _read_csv_with_offset(fn, start, end, header=b''):
    bio = open(fn, 'rb')
    bio.seek(start)
    to_read = header + bio.read(end - start)
    bio.close()
    return from_pandas(pd.read_csv(BytesIO(to_read)), npartitions=1)


def read_csv(path, npartitions, **kwargs):
    """Read csv file from local disk.

    Args:
        path: The filepath of the csv file.
              We only support local files for now.
        npartitions: See dataframe::from_pandas
        chunksize: See dataframe::from_pandas
        kwargs: Keyword arguments in pandas::from_csv
    """
    offsets = _compute_offset(path, npartitions)
    first_line = _get_first_line(path)

    df_obj_ids = []
    for start, end in offsets:
        if start != 0:
            df = _read_csv_with_offset.remote(
                path, start, end, header=first_line)
        else:
            df = _read_csv_with_offset.remote(path, start, end)
        df_obj_ids.append(df)

    return _vertical_concat(ray.get(df_obj_ids))


def _vertical_concat(ray_dfs):
    """Concatenate a list of Ray DataFrame objects.
    Given they all share the same columns.
    """
    assert isinstance(ray_dfs, list), "Input must be a list of Ray DataFrames"

    if len(ray_dfs) == 1:
        return ray_dfs[0]

    dfs = map(lambda ray_df: ray_df._df, ray_dfs)
    flattened = list(chain.from_iterable(dfs))

    return DataFrame(flattened, ray_dfs[0].columns)
