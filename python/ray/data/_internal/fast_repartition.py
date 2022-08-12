import ray

from ray.data.block import BlockAccessor
from ray.data._internal.block_list import BlockList
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.shuffle_and_partition import _ShufflePartitionOp
from ray.data._internal.stats import DatasetStats


def fast_repartition(blocks, num_blocks):
    from ray.data.dataset import Dataset

    wrapped_ds = Dataset(
        ExecutionPlan(
            blocks,
            DatasetStats(stages={}, parent=None),
            run_by_consumer=blocks._owned_by_consumer,
        ),
        0,
        lazy=False,
    )
    # Compute the (n-1) indices needed for an equal split of the data.
    count = wrapped_ds.count()
    dataset_format = wrapped_ds._dataset_format()
    indices = []
    cur_idx = 0
    for _ in range(num_blocks - 1):
        cur_idx += count / num_blocks
        indices.append(int(cur_idx))
    assert len(indices) < num_blocks, (indices, num_blocks)
    if indices:
        splits = wrapped_ds.split_at_indices(indices)
    else:
        splits = [wrapped_ds]
    # TODO(ekl) include stats for the split tasks. We may also want to
    # consider combining the split and coalesce tasks as an optimization.

    # Coalesce each split into a single block.
    reduce_task = cached_remote_fn(_ShufflePartitionOp.reduce).options(num_returns=2)
    reduce_bar = ProgressBar("Repartition", position=0, total=len(splits))
    reduce_out = [
        reduce_task.remote(False, None, *s.get_internal_block_refs())
        for s in splits
        if s.num_blocks() > 0
    ]

    owned_by_consumer = blocks._owned_by_consumer

    # Early-release memory.
    del splits, blocks, wrapped_ds

    new_blocks, new_metadata = zip(*reduce_out)
    new_blocks, new_metadata = list(new_blocks), list(new_metadata)
    new_metadata = reduce_bar.fetch_until_complete(new_metadata)
    reduce_bar.close()

    # Handle empty blocks.
    if len(new_blocks) < num_blocks:
        from ray.data._internal.arrow_block import ArrowBlockBuilder
        from ray.data._internal.pandas_block import PandasBlockBuilder
        from ray.data._internal.simple_block import SimpleBlockBuilder

        num_empties = num_blocks - len(new_blocks)
        if dataset_format == "arrow":
            builder = ArrowBlockBuilder()
        elif dataset_format == "pandas":
            builder = PandasBlockBuilder()
        else:
            builder = SimpleBlockBuilder()
        empty_block = builder.build()
        empty_meta = BlockAccessor.for_block(empty_block).get_metadata(
            input_files=None, exec_stats=None
        )  # No stats for empty block.
        empty_blocks, empty_metadata = zip(
            *[(ray.put(empty_block), empty_meta) for _ in range(num_empties)]
        )
        new_blocks += empty_blocks
        new_metadata += empty_metadata

    return BlockList(new_blocks, new_metadata, owned_by_consumer=owned_by_consumer), {}
