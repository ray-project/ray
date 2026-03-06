from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Generator, Iterable, List, Optional, Tuple

import numpy as np

import ray
from ray.data.block import Block, BlockAccessor, CallableClass

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


def make_ref_bundles(simple_data: List[List[Any]]) -> List["RefBundle"]:
    """Create ref bundles from a list of block data.

    One bundle is created for each input block.
    """
    import pandas as pd
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import RefBundle

    output = []
    for block in simple_data:
        block = pd.DataFrame({"id": block})
        output.append(
            RefBundle(
                [
                    (
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata(),
                    )
                ],
                owns_blocks=True,
                schema=pa.lib.Schema.from_pandas(block, preserve_index=False),
            )
        )
    return output


memory_units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]


def memory_string(num_bytes: float) -> str:
    """Return a human-readable memory string for the given amount of bytes."""
    k = 0
    while num_bytes >= 1024 and k < len(memory_units) - 1:
        num_bytes /= 1024
        k += 1
    return f"{num_bytes:.1f}{memory_units[k]}"


def locality_string(locality_hits: int, locality_misses) -> str:
    """Return a human-readable string for object locality stats."""
    if not locality_misses:
        return "[all objects local]"
    return f"[{locality_hits}/{locality_hits + locality_misses} objects local]"


def concat_and_shuffle(
    blocks: Iterable[Block],
    row_block_size: int,
    seed: Optional[int] = None,
) -> Generator[Block, None, None]:
    """Concatenate blocks into one Arrow table and shuffle rows.

    Rows are shuffled in chunks of ``row_block_size`` to avoid per-row
    overhead. The shuffled chunks are concatenated back into one table.

    Args:
        blocks: Input Arrow tables to concatenate.
        row_block_size: Number of rows per shuffle chunk.
        seed: Optional RNG seed for deterministic shuffling.

    Yields:
        Block: A single Arrow table with shuffled rows.
    """
    import numpy as np
    import pyarrow as pa

    tables = list(blocks)
    if not tables:
        return
    combined = pa.concat_tables(tables)
    n = combined.num_rows
    if n <= 1:
        yield combined
        return
    rng = np.random.default_rng(seed)
    starts = np.arange(0, n, row_block_size)
    rng.shuffle(starts)
    slices = [combined.slice(int(s), min(row_block_size, n - int(s))) for s in starts]
    yield pa.concat_tables(slices)


class ShuffleRefBundler:
    """Groups small blocks from the read operator for sub-file shuffle.

    Accumulates single-block RefBundles produced by the read operator. Once
    ``sample_ratio * merge_window`` bundles are buffered, randomly samples
    ``merge_window`` of them, merges them into one multi-block RefBundle, and
    emits it as a single shuffle task input. On finalization, remaining
    bundles are flushed in groups of ``merge_window``.
    """

    def __init__(
        self,
        merge_window: int,
        sample_ratio: int,
        seed: Optional[int] = None,
    ):
        self._merge_window = merge_window
        self._sample_window = merge_window * sample_ratio
        self._buffer: List["RefBundle"] = []
        self._buffer_size_bytes: int = 0
        self._finalized = False
        self._rng = np.random.default_rng(seed)

    def num_blocks(self) -> int:
        return sum(len(b.block_refs) for b in self._buffer)

    def add_bundle(self, bundle: "RefBundle"):
        self._buffer.append(bundle)
        self._buffer_size_bytes += bundle.size_bytes()

    def has_bundle(self) -> bool:
        if self._finalized:
            return len(self._buffer) > 0
        return len(self._buffer) >= self._sample_window

    def size_bytes(self) -> int:
        return self._buffer_size_bytes

    def get_next_bundle(self) -> Tuple[List["RefBundle"], "RefBundle"]:
        from ray.data._internal.execution.operators.map_operator import (
            _merge_ref_bundles,
        )

        assert self.has_bundle()

        n = len(self._buffer)
        take = min(self._merge_window, n)

        if take >= n:
            # Take everything (flush or exact fit).
            selected = self._buffer
            self._buffer = []
        else:
            # Randomly sample merge_window bundles from the buffer.
            indices = self._rng.choice(n, size=take, replace=False)
            indices_set = set(indices.tolist())
            selected = [self._buffer[i] for i in indices]
            self._buffer = [
                b for i, b in enumerate(self._buffer) if i not in indices_set
            ]

        self._buffer_size_bytes = sum(b.size_bytes() for b in self._buffer)
        return list(selected), _merge_ref_bundles(*selected)

    def done_adding_bundles(self):
        self._finalized = True


def make_callable_class_single_threaded(callable_cls: CallableClass) -> CallableClass:
    """Returns a thread-safe CallableClass with the same logic as the provided
    `callable_cls`.

    This function allows the usage of concurrent actors by safeguarding user logic
    behind a separate thread.

    This allows batch slicing and formatting to occur concurrently, to overlap with the
    user provided UDF.
    """

    class _SingleThreadedWrapper(callable_cls):
        def __init__(self, *args, **kwargs):
            self.thread_pool_executor = ThreadPoolExecutor(max_workers=1)
            super().__init__(*args, **kwargs)

        def __repr__(self):
            return super().__repr__()

        def __call__(self, *args, **kwargs):
            # ThreadPoolExecutor will reuse the same thread for every submit call.
            future = self.thread_pool_executor.submit(super().__call__, *args, **kwargs)
            return future.result()

    return _SingleThreadedWrapper
