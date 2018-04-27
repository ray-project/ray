from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas
import ray

from .dataframe import DataFrame
from .utils import _map_partitions


def to_datetime(arg, errors='raise', dayfirst=False, yearfirst=False, utc=None,
                box=True, format=None, exact=True, unit=None,
                infer_datetime_format=False, origin='unix'):
    if not isinstance(arg, DataFrame):
        return pandas.to_datetime(arg, errors=errors, dayfirst=dayfirst,
                                  yearfirst=yearfirst, utc=utc, box=box,
                                  format=format, exact=exact, unit=unit,
                                  infer_datetime_format=infer_datetime_format,
                                  origin=origin)
    if errors == 'raise':
        pandas.to_datetime(pandas.DataFrame(columns=arg.columns),
                           errors=errors, dayfirst=dayfirst,
                           yearfirst=yearfirst, utc=utc, box=box,
                           format=format, exact=exact, unit=unit,
                           infer_datetime_format=infer_datetime_format,
                           origin=origin)

    def datetime_helper(df, cols):
        df.columns = cols
        return pandas.to_datetime(df, errors=errors, dayfirst=dayfirst,
                                  yearfirst=yearfirst, utc=utc, box=box,
                                  format=format, exact=exact, unit=unit,
                                  infer_datetime_format=infer_datetime_format,
                                  origin=origin)

    datetime_series = _map_partitions(datetime_helper, arg._row_partitions,
                                      arg.columns)
    result = pandas.concat(ray.get(datetime_series), copy=False)
    result.index = arg.index

    return result
