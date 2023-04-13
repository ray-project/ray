from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, List, TYPE_CHECKING

import ray
from ray.data.block import Block, BlockAccessor, CallableClass
from ray.data._internal.block_list import BlockList

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


def make_ref_bundles(simple_data: List[Block]) -> List["RefBundle"]:
    """Create ref bundles from a list of block data.

    One bundle is created for each input block.
    """
    from ray.data._internal.execution.interfaces import RefBundle

    output = []
    for block in simple_data:
        output.append(
            RefBundle(
                [
                    (
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata([], None),
                    )
                ],
                owns_blocks=True,
            )
        )
    return output


def bundles_to_block_list(bundles: Iterator["RefBundle"]) -> BlockList:
    blocks, metadata = [], []
    owns_blocks = True
    for ref_bundle in bundles:
        if not ref_bundle.owns_blocks:
            owns_blocks = False
        for block, meta in ref_bundle.blocks:
            blocks.append(block)
            metadata.append(meta)
    return BlockList(blocks, metadata, owned_by_consumer=owns_blocks)


def block_list_to_bundles(blocks: BlockList, owns_blocks: bool) -> List["RefBundle"]:
    from ray.data._internal.execution.interfaces import RefBundle

    output = []
    for block, meta in blocks.iter_blocks_with_metadata():
        output.append(
            RefBundle(
                [
                    (
                        block,
                        meta,
                    )
                ],
                owns_blocks=owns_blocks,
            )
        )
    return output


def memory_string(num_bytes: int) -> str:
    """Return a human-readable memory string for the given amount of bytes."""
    if num_bytes >= 1024 * 1024 * 1024:
        mem = str(round(num_bytes / (1024 * 1024 * 1024), 2)) + " GiB"
    else:
        mem = str(round(num_bytes / (1024 * 1024), 2)) + " MiB"
    return mem


def locality_string(locality_hits: int, locality_misses) -> str:
    """Return a human-readable string for object locality stats."""
    if not locality_misses:
        return "[all objects local]"
    return f"[{locality_hits}/{locality_hits + locality_misses} objects local]"


def make_callable_class_concurrent(callable_cls: CallableClass) -> CallableClass:
    """Returns a thread-safe CallableClass with the same logic as the provided
    `callable_cls`.

    This function allows the usage of concurrent actors by safeguarding user logic
    behind a separate thread.

    This allows batch slicing and formatting to occur concurrently, to overlap with the
    user provided UDF.
    """

    class _Wrapper(callable_cls):
        def __init__(self, *args, **kwargs):
            self.thread_pool_executor = ThreadPoolExecutor(max_workers=1)
            super().__init__(*args, **kwargs)

        def __repr__(self):
            return super().__repr__()

        def __call__(self, *args, **kwargs):
            # ThreadPoolExecutor will reuse the same thread for every submit call.
            future = self.thread_pool_executor.submit(super().__call__, *args, **kwargs)
            return future.result()

    return _Wrapper
