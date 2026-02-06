"""Block coalescer for streaming datasources.

This utility helps streaming datasources produce well-sized blocks that align
with Ray Data's target block size, improving downstream throughput and memory behavior.

Uses PyArrow (https://arrow.apache.org/docs/python/) for efficient table concatenation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

import pyarrow as pa  # https://arrow.apache.org/docs/python/


@dataclass
class BlockCoalescer:
    """Coalesces small tables into larger blocks that meet target size requirements.

    This is useful for streaming datasources (Kafka, Kinesis, etc.) that naturally
    produce small record batches. By coalescing them, downstream operators see
    block sizes aligned with Ray Data settings, improving throughput.

    Example:
        .. testcode::
            :skipif: True

            from ray.data._internal.streaming.block_coalescer import BlockCoalescer
            from ray.data.context import DataContext

            ctx = DataContext.get_current()
            target_bytes = ctx.target_max_block_size or (128 * 1024 * 1024)
            coalescer = BlockCoalescer(target_max_bytes=target_bytes)

            # Small tables from Kafka poll loop
            def poll_tables():
                yield pa.table({"a": [1, 2, 3]})
                yield pa.table({"a": [4, 5]})
                yield pa.table({"a": [6, 7, 8, 9]})

            # Coalesced into target-sized blocks
            for out_table in coalescer.coalesce_tables(poll_tables()):
                yield out_table
    """

    target_max_bytes: int
    target_max_rows: Optional[int] = None

    def coalesce_tables(self, tables: Iterable[pa.Table]) -> Iterator[pa.Table]:
        """Coalesce small tables into larger blocks.

        Args:
            tables: Iterable of PyArrow tables to coalesce.

        Yields:
            PyArrow tables that meet target size requirements.
        """
        buf = []
        rows = 0
        bytes_ = 0

        def flush():
            nonlocal buf, rows, bytes_
            if not buf:
                return None
            out = pa.concat_tables(buf, promote=True)
            buf = []
            rows = 0
            bytes_ = 0
            return out

        for t in tables:
            # Approximate bytes; Arrow doesn't always expose exact memory, but this is good enough
            t_bytes = t.nbytes if hasattr(t, "nbytes") else 0
            if buf and (
                (bytes_ + t_bytes) >= self.target_max_bytes
                or (
                    self.target_max_rows is not None
                    and (rows + t.num_rows) >= self.target_max_rows
                )
            ):
                out = flush()
                if out is not None:
                    yield out

            buf.append(t)
            rows += t.num_rows
            bytes_ += t_bytes

        out = flush()
        if out is not None:
            yield out
