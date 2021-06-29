import itertools
try:
    import pyarrow as pa
except ImportError:
    pa = None
try:
    import pandas as pd
except ImportError:
    pd = None

from functools import reduce
from typing import List, Any, Union, Iterable, TypeVar

T = TypeVar("T")
U = TypeVar("U")

from ray.experimental.data.impl.arrow_block import ArrowBlockBuilder, ArrowBlock
from ray.experimental.data.impl.block import Block, ListBlock

def _import_check():
    if pa is None:
        raise ImportError("Run `pip install pyarrow` for Arrow support")
    if pd is None:
        raise ImportError("Run `pip install pandas` for Pandas support")


def concat_batches(batches: Iterable[Union["pandas.DataFrame", List, "pyarrow.Table"]]) -> Block[U]:
    _import_check()
    assert len(batches) >= 1
    batch_sample = batches[0]
    
    if isinstance(batch_sample, list):
        return ListBlock(list(itertools.chain.from_iterable(batches)))
    elif isinstance(batch_sample, pd.core.frame.DataFrame):
        builder = ArrowBlockBuilder()
        builder.add_pandas_df(reduce(lambda df1, df2: df1.append(df2), batches))
        return builder.build()
    elif isinstance(batch_sample, pa.Table):
        return ArrowBlock(pa.concat_tables(batches))
    else:
        raise ValueError(
            f"The map batch UDF returns a type {type(batch_sample)}, "
            "which is not allowed. The return type must be either list, "
            "pandas.DataFrame, or pyarrow.Table")
