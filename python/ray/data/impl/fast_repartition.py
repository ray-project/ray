import ray

from ray.data.block import BlockAccessor
from ray.data.impl.block_list import BlockList
from ray.data.impl.plan import ExecutionPlan
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.shuffle import _shuffle_reduce
from ray.data.impl.stats import DatasetStats


def fast_repartition(blocks, num_blocks):
    from ray.data.dataset import Dataset

    wrapped_ds = Dataset(
        ExecutionPlan(blocks, DatasetStats(stages={}, parent=None)), 0, lazy=False
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
    reduce_task = cached_remote_fn(_shuffle_reduce).options(num_returns=2)
    reduce_bar = ProgressBar("Repartition", position=0, total=len(splits))
    reduce_out = [
        reduce_task.remote(*s.get_internal_block_refs())
        for s in splits
        if s.num_blocks() > 0
    ]

    # Early-release memory.
    del splits, blocks, wrapped_ds

    new_blocks, new_metadata = zip(*reduce_out)
    new_blocks, new_metadata = list(new_blocks), list(new_metadata)
    new_metadata = reduce_bar.fetch_until_complete(new_metadata)
    reduce_bar.close()

    # Handle empty blocks.
    if len(new_blocks) < num_blocks:
        from ray.data.impl.arrow_block import ArrowBlockBuilder
        from ray.data.impl.pandas_block import PandasBlockBuilder
        from ray.data.impl.simple_block import SimpleBlockBuilder

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

    return BlockList(new_blocks, new_metadata), {}
