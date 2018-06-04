from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas
import numpy as np
from .dataframe import DataFrame
from .utils import _reindex_helper


def concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,
           keys=None, levels=None, names=None, verify_integrity=False,
           copy=True):

    if keys is not None:
        objs = [objs[k] for k in keys]
    else:
        objs = list(objs)

    if len(objs) == 0:
        raise ValueError("No objects to concatenate")

    objs = [obj for obj in objs if obj is not None]

    if len(objs) == 0:
        raise ValueError("All objects passed were None")

    try:
        type_check = next(obj for obj in objs
                          if not isinstance(obj, (pandas.Series,
                                                  pandas.DataFrame,
                                                  DataFrame)))
    except StopIteration:
        type_check = None
    if type_check is not None:
        raise ValueError("cannot concatenate object of type \"{0}\"; only "
                         "pandas.Series, pandas.DataFrame, "
                         "and ray.dataframe.DataFrame objs are "
                         "valid", type(type_check))

    all_series = all(isinstance(obj, pandas.Series)
                     for obj in objs)
    if all_series:
        return DataFrame(pandas.concat(objs, axis, join, join_axes,
                                       ignore_index, keys, levels, names,
                                       verify_integrity, copy))

    if isinstance(objs, dict):
        raise NotImplementedError(
            "Obj as dicts not implemented. To contribute to "
            "Pandas on Ray, please visit github.com/ray-project/ray.")

    axis = pandas.DataFrame()._get_axis_number(axis)

    if join not in ['inner', 'outer']:
        raise ValueError("Only can inner (intersect) or outer (union) join the"
                         " other axis")

    # We need this in a list because we use it later.
    all_index, all_columns = list(zip(*[(obj.index, obj.columns)
                                        for obj in objs]))

    def series_to_df(series, columns):
        df = pandas.DataFrame(series)
        df.columns = columns
        return DataFrame(df)

    # Pandas puts all of the Series in a single column named 0. This is
    # true regardless of the existence of another column named 0 in the
    # concat.
    if axis == 0:
        objs = [series_to_df(obj, [0])
                if isinstance(obj, pandas.Series) else obj for obj in objs]
    else:
        # Pandas starts the count at 0 so this will increment the names as
        # long as there's a new nameless Series being added.
        def name_incrementer(i):
            val = i[0]
            i[0] += 1
            return val

        i = [0]
        objs = [series_to_df(obj, obj.name if obj.name is not None
                             else name_incrementer(i))
                if isinstance(obj, pandas.Series) else obj for obj in objs]

    # Using concat on the columns and index is fast because they're empty,
    # and it forces the error checking. It also puts the columns in the
    # correct order for us.
    final_index = \
        pandas.concat([pandas.DataFrame(index=idx) for idx in all_index],
                      axis=axis, join=join, join_axes=join_axes,
                      ignore_index=ignore_index, keys=keys, levels=levels,
                      names=names, verify_integrity=verify_integrity,
                      copy=False).index
    final_columns = \
        pandas.concat([pandas.DataFrame(columns=col)
                       for col in all_columns],
                      axis=axis, join=join, join_axes=join_axes,
                      ignore_index=ignore_index, keys=keys, levels=levels,
                      names=names, verify_integrity=verify_integrity,
                      copy=False).columns

    # Put all of the DataFrames into Ray format
    # TODO just partition the DataFrames instead of building a new Ray DF.
    objs = [DataFrame(obj) if isinstance(obj, (pandas.DataFrame,
                                               pandas.Series)) else obj
            for obj in objs]

    # Here we reuse all_columns/index so we don't have to materialize objects
    # from remote memory built in the previous line. In the future, we won't be
    # building new DataFrames, rather just partitioning the DataFrames.
    if axis == 0:
        new_blocks = np.array([_reindex_helper._submit(
            args=tuple([all_columns[i], final_columns, axis,
                       len(objs[0]._block_partitions)] + part.tolist()),
            num_return_vals=len(objs[0]._block_partitions))
            for i in range(len(objs))
            for part in objs[i]._block_partitions])
    else:
        # Transposing the columns is necessary because the remote task treats
        # everything like rows and returns in row-major format. Luckily, this
        # operation is cheap in numpy.
        new_blocks = np.array([_reindex_helper._submit(
            args=tuple([all_index[i], final_index, axis,
                       len(objs[0]._block_partitions.T)] + part.tolist()),
            num_return_vals=len(objs[0]._block_partitions.T))
            for i in range(len(objs))
            for part in objs[i]._block_partitions.T]).T

    return DataFrame(block_partitions=new_blocks,
                     columns=final_columns,
                     index=final_index)
