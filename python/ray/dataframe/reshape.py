from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import pandas
import numpy as np

from pandas import compat
from pandas.core.dtypes.common import is_list_like
from itertools import cycle

from .dataframe import DataFrame
from .utils import _deploy_func


def get_dummies(data, prefix=None, prefix_sep='_', dummy_na=False,
                columns=None, sparse=False, drop_first=False):
    """Convert categorical variable into indicator variables.

    Args:
        data (array-like, Series, or DataFrame): data to encode.
        prefix (string, [string]): Prefix to apply to each encoded column
                                   label.
        prefix_sep (string, [string]): Separator between prefix and value.
        dummy_na (bool): Add a column to indicate NaNs.
        columns: Which columns to encode.
        sparse (bool): Not Implemented: If True, returns SparseDataFrame.
        drop_first (bool): Whether to remove the first level of encoded data.

    Returns:
        DataFrame or one-hot encoded data.
    """
    if not isinstance(data, DataFrame):
        return pandas.get_dummies(data, prefix=prefix, prefix_sep=prefix_sep,
                                  dummy_na=dummy_na, columns=columns,
                                  sparse=sparse, drop_first=drop_first)

    if sparse:
        raise NotImplementedError(
            "SparseDataFrame is not implemented. "
            "To contribute to Pandas on Ray, please visit "
            "github.com/ray-project/ray.")

    if columns is None:
        columns_to_encode = data.dtypes.isin([np.dtype("O"), 'category'])
        columns_to_encode = data.columns[columns_to_encode]
    else:
        columns_to_encode = columns

    def check_len(item, name):
        len_msg = ("Length of '{name}' ({len_item}) did not match the "
                   "length of the columns being encoded ({len_enc}).")

        if is_list_like(item):
            if not len(item) == len(columns_to_encode):
                len_msg = len_msg.format(name=name, len_item=len(item),
                                         len_enc=len(columns_to_encode))
                raise ValueError(len_msg)

    check_len(prefix, 'prefix')
    check_len(prefix_sep, 'prefix_sep')
    if isinstance(prefix, compat.string_types):
        prefix = cycle([prefix])
        prefix = [next(prefix) for i in range(len(columns_to_encode))]
    if isinstance(prefix, dict):
        prefix = [prefix[col] for col in columns_to_encode]

    if prefix is None:
        prefix = columns_to_encode

    # validate separators
    if isinstance(prefix_sep, compat.string_types):
        prefix_sep = cycle([prefix_sep])
        prefix_sep = [next(prefix_sep) for i in range(len(columns_to_encode))]
    elif isinstance(prefix_sep, dict):
        prefix_sep = [prefix_sep[col] for col in columns_to_encode]

    if set(columns_to_encode) == set(data.columns):
        with_dummies = []
        dropped_columns = pandas.Index()
    else:
        with_dummies = data.drop(columns_to_encode, axis=1)._col_partitions
        dropped_columns = data.columns.drop(columns_to_encode)

    def get_dummies_remote(df, to_drop, prefix, prefix_sep):
        df = df.drop(to_drop, axis=1)

        if df.size == 0:
            return df, df.columns

        df = pandas.get_dummies(df, prefix=prefix, prefix_sep=prefix_sep,
                                dummy_na=dummy_na, columns=None, sparse=sparse,
                                drop_first=drop_first)
        columns = df.columns
        df.columns = pandas.RangeIndex(0, len(df.columns))
        return df, columns

    total = 0
    columns = []
    for i, part in enumerate(data._col_partitions):
        col_index = data._col_metadata.partition_series(i)

        # TODO(kunalgosar): Handle the case of duplicate columns here
        to_encode = col_index.index.isin(columns_to_encode)

        to_encode = col_index[to_encode]
        to_drop = col_index.drop(to_encode.index)

        result = _deploy_func._submit(
            args=(get_dummies_remote, part, to_drop,
                  prefix[total:total + len(to_encode)],
                  prefix_sep[total:total + len(to_encode)]),
            num_return_vals=2)

        with_dummies.append(result[0])
        columns.append(result[1])
        total += len(to_encode)

    columns = ray.get(columns)
    dropped_columns = dropped_columns.append(columns)

    return DataFrame(col_partitions=with_dummies,
                     columns=dropped_columns,
                     index=data.index)
