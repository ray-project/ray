import pandas as pd
import numpy as np
from .dataframe import DataFrame as rdf
from .utils import (
    from_pandas,
    _deploy_func)
from functools import reduce


def concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,
           keys=None, levels=None, names=None, verify_integrity=False,
           copy=True):

    def _concat(frame1, frame2):
        # Check type on objects
        # Case 1: Both are Pandas DF
        if isinstance(frame1, pd.DataFrame) and \
           isinstance(frame2, pd.DataFrame):

            return pd.concat((frame1, frame2), axis, join, join_axes,
                             ignore_index, keys, levels, names,
                             verify_integrity, copy)

        if not (isinstance(frame1, rdf) and
           isinstance(frame2, rdf)) and join == 'inner':
            raise NotImplementedError(
                  "Obj as dicts not implemented. To contribute to "
                  "Pandas on Ray, please visit github.com/ray-project/ray."
                  )

        # Case 2: Both are different types
        if isinstance(frame1, pd.DataFrame):
            frame1 = from_pandas(frame1, len(frame1) / 2**16 + 1)
        if isinstance(frame2, pd.DataFrame):
            frame2 = from_pandas(frame2, len(frame2) / 2**16 + 1)

        # Case 3: Both are Ray DF
        if isinstance(frame1, rdf) and \
           isinstance(frame2, rdf):

            new_columns = frame1.columns.join(frame2.columns, how=join)

            def _reindex_helper(pdf, old_columns, join):
                pdf.columns = old_columns
                if join == 'outer':
                    pdf = pdf.reindex(columns=new_columns)
                else:
                    pdf = pdf[new_columns]
                pdf.columns = pd.RangeIndex(len(new_columns))

                return pdf

            f1_columns, f2_columns = frame1.columns, frame2.columns
            new_f1 = [_deploy_func.remote(lambda p: _reindex_helper(p,
                                          f1_columns, join), part) for
                      part in frame1._row_partitions]
            new_f2 = [_deploy_func.remote(lambda p: _reindex_helper(p,
                                          f2_columns, join), part) for
                      part in frame2._row_partitions]

            return rdf(row_partitions=new_f1 + new_f2, columns=new_columns,
                       index=frame1.index.append(frame2.index))

    # (TODO) Group all the pandas dataframes

    if isinstance(objs, dict):
        raise NotImplementedError(
              "Obj as dicts not implemented. To contribute to "
              "Pandas on Ray, please visit github.com/ray-project/ray."
              )

    axis = pd.DataFrame()._get_axis_number(axis)
    if axis == 1:
        raise NotImplementedError(
              "Concat not implemented for axis=1. To contribute to "
              "Pandas on Ray, please visit github.com/ray-project/ray."
              )

    all_pd = np.all([isinstance(obj, pd.DataFrame) for obj in objs])
    if all_pd:
        result = pd.concat(objs, axis, join, join_axes,
                           ignore_index, keys, levels, names,
                           verify_integrity, copy)
    else:
        result = reduce(_concat, objs)

    if isinstance(result, pd.DataFrame):
        return from_pandas(result, len(result) / 2**16 + 1)

    return result
