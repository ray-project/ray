from typing import Any, Callable, Dict, Optional

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.compute as pc
from packaging.version import Version

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink

_PANDAS_LT_3 = Version(pd.__version__) < Version("3.0.0")


class JSONDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        pandas_json_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        pandas_json_args: Optional[Dict[str, Any]] = None,
        file_format: str = "json",
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        if pandas_json_args_fn is None:
            pandas_json_args_fn = lambda: {}  # noqa: E731

        if pandas_json_args is None:
            pandas_json_args = {}

        self.pandas_json_args_fn = pandas_json_args_fn
        self.pandas_json_args = pandas_json_args

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        writer_args = _resolve_kwargs(self.pandas_json_args_fn, **self.pandas_json_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        df = block.to_pandas()
        date_unit = writer_args.get("date_unit", "ms")
        date_format = writer_args.get("date_format")
        # Same default as to_json(): ISO for orient="table", epoch otherwise.
        is_epoch = date_format == "epoch" or (date_format is None and orient != "table")
        # Invalid date_unit values are left for to_json() to reject.
        valid_unit = date_unit in ("s", "ms", "us", "ns")
        if is_epoch and valid_unit:
            df = _cast_arrow_dates_to_epoch(df, date_unit)
        # TODO: Remove this workaround once Ray requires pandas >= 3.
        # orient="table" is skipped: it writes dates as ISO strings by default,
        # which pandas < 3 already gets right, and the epoch cast below would
        # turn them into numbers.
        if _PANDAS_LT_3 and orient != "table" and valid_unit:
            df = _fix_non_nanosecond_columns(
                df, epoch_unit=date_unit if is_epoch else None
            )

        df.to_json(file, orient=orient, lines=lines, **writer_args)


def _cast_arrow_dates_to_epoch(df: pd.DataFrame, date_unit: str) -> pd.DataFrame:
    """Cast ``pd.ArrowDtype`` date32 and date64 columns to integers in ``date_unit``.

    pandas 3 ``to_json`` raises ``NotImplementedError`` on these columns when it
    writes epoch values. The integers are what pandas < 3 writes for them.
    """
    # Go by position because column names can repeat.
    positions = [
        i
        for i, dtype in enumerate(df.dtypes)
        if isinstance(dtype, pd.ArrowDtype)
        and pyarrow.types.is_date(dtype.pyarrow_dtype)
    ]
    if not positions:
        return df

    df = df.copy(deep=False)
    for i in positions:
        values = (
            pyarrow.array(df.iloc[:, i])
            .cast(pyarrow.timestamp(date_unit))
            .cast(pyarrow.int64())
        )
        df.isetitem(
            i, pd.Series(values, index=df.index, dtype=pd.ArrowDtype(pyarrow.int64()))
        )
    return df


def _fix_non_nanosecond_columns(
    df: pd.DataFrame, epoch_unit: Optional[str]
) -> pd.DataFrame:
    """Work around pandas < 3 writing non-nanosecond timestamps and durations as
    if they were nanoseconds (https://github.com/pandas-dev/pandas/issues/55827).

    For epoch output, tz-naive timestamps and durations are cast to integers in
    ``epoch_unit``, so dates after 2262 still work. For ISO output
    (``epoch_unit`` is None), only durations are wrong, so they are cast to
    nanoseconds.
    """
    df = df.copy(deep=False)
    # Go by position because column names can repeat.
    for i, dtype in enumerate(df.dtypes):
        if isinstance(dtype, pd.ArrowDtype):
            arrow_type = dtype.pyarrow_dtype
        elif isinstance(dtype, np.dtype) and dtype.kind in "mM":
            arrow_type = pyarrow.from_numpy_dtype(dtype)
        else:
            continue
        is_duration = pyarrow.types.is_duration(arrow_type)
        is_naive_timestamp = (
            pyarrow.types.is_timestamp(arrow_type) and arrow_type.tz is None
        )
        needs_fix = is_duration or (epoch_unit is not None and is_naive_timestamp)
        if not needs_fix or arrow_type.unit == "ns":
            continue

        if epoch_unit is None:
            target = pyarrow.duration("ns")
        elif is_duration:
            target = pyarrow.duration(epoch_unit)
        else:
            target = pyarrow.timestamp(epoch_unit)
        values = pc.cast(
            pyarrow.array(df.iloc[:, i]),
            options=pc.CastOptions(target, allow_time_truncate=True),
        )
        if epoch_unit is not None:
            values = values.cast(pyarrow.int64())
        df.isetitem(
            i, pd.Series(values, index=df.index, dtype=pd.ArrowDtype(values.type))
        )
    return df
