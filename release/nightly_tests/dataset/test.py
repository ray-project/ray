from typing import Dict, Iterator, Optional, Union

import ray
import pandas as pd
import pyarrow as pa
from ray.data.aggregate import _AggregateOnKeyBase
from ray.data.block import Block, BlockAccessor, KeyFn
from ray.data.row import TableRow
from tdigest import TDigest

RowType = Union[int, float, TableRow]


def _initialize_tdigest(k: Optional[float]) -> TDigest:
    digest = TDigest()
    if k is not None:
        digest.update(k)
    return digest


### Aggregator Class
class PercentileAggregator(_AggregateOnKeyBase):
    def __init__(self, on: Optional[KeyFn] = None):
        self._set_key_fn(on)

        def init(row: RowType) -> TDigest:
            if isinstance(row, TableRow):
                return _initialize_tdigest(row.get(on, None))
            elif isinstance(row, int) or isinstance(row, float) or row is None:
                return _initialize_tdigest(row)
            else:
                raise TypeError(
                    "Not a supported data row type: {} ({})".format(row, type(row))
                )

        def merge(left: TDigest, right: TDigest) -> TDigest:
            sum = left + right
            return sum

        def accumulate_block(aggregator: TDigest, block: Block) -> TDigest:
            rows: Iterator[RowType] = BlockAccessor.for_block(block).iter_rows()
            for row in rows:
                if isinstance(row, TableRow):
                    v = row.get(on, None)
                    if v is not None:
                        aggregator.update(v)
                elif isinstance(row, int) or isinstance(row, float) or row is None:
                    if row is not None:
                        aggregator.update(row)
                else:
                    raise TypeError(
                        "Not a supported data row type: {} ({})".format(row, type(row))
                    )

            aggregator.compress()
            return aggregator

        super().__init__(
            init=init,
            accumulate_block=accumulate_block,
            merge=merge,
            name=f"t-digest({str(on)})",
        )


### Code to use the custom aggregator
ray.init(num_cpus=2)
df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [6, 7, 8, 9, 10]})
table = pa.Table.from_pandas(df)
data = ray.data.from_arrow(table)
aggregator_a = PercentileAggregator("a")
aggregator_b = PercentileAggregator("b")
digests = data.aggregate(aggregator_a, aggregator_b)

assert digest_a.n == 5
assert digest_b.n == 5
