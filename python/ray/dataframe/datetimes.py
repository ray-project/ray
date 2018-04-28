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
    """Convert the arg to datetime format. If not Ray DataFrame, this falls
       back on pandas.

    Args:
        errors ('raise' or 'ignore'): If 'ignore', errors are silenced.
        dayfirst (bool): Date format is passed in as day first.
        yearfirst (bool): Date format is passed in as year first.
        utc (bool): retuns a UTC DatetimeIndex if True.
        box (bool): If True, returns a DatetimeIndex.
        format (string): strftime to parse time, eg "%d/%m/%Y".
        exact (bool): If True, require an exact format match.
        unit (string, default 'ns'): unit of the arg.
        infer_datetime_format (bool): Whether or not to infer the format.
        origin (string): Define the reference date.

    Returns:
        Type depends on input:

        - list-like: DatetimeIndex
        - Series: Series of datetime64 dtype
        - scalar: Timestamp
    """
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
