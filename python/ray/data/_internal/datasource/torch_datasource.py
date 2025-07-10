from typing import TYPE_CHECKING

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import torch


TORCH_DATASOURCE_READER_BATCH_SIZE = 32


class TorchDatasource(Datasource):
    """Torch datasource, for reading from `Torch
    datasets <https://pytorch.org/docs/stable/data.html/>`_.
    This datasource implements a streaming read using a single read task.
    """

    def __init__(
        self,
        dataset: "torch.utils.data.Dataset",
    ):
        self._dataset = dataset

    def get_read_tasks(self, parallelism):
        assert parallelism == 1

        meta = BlockMetadata(
            # Note: avoid len(self._dataset) because it will trigger
            # iterating through IterableDataset, which can cause OOM.
            num_rows=None,
            size_bytes=None,
            input_files=None,
            exec_stats=None,
        )
        read_task = ReadTask(
            lambda subset=self._dataset: _read_subset(
                subset,
            ),
            metadata=meta,
        )

        return [read_task]

    def estimate_inmemory_data_size(self):
        return None


def _read_subset(subset: "torch.utils.data.Subset"):
    batch = []

    # Get items from dataset based on its type
    if hasattr(subset, "__iter__"):
        # IterableDataset: Use the iterator directly
        items = subset
    else:
        # Map-style dataset: Respect __len__
        items = (subset[i] for i in range(len(subset)))

    # Process items in batches
    for item in items:
        batch.append(item)
        if len(batch) == TORCH_DATASOURCE_READER_BATCH_SIZE:
            builder = DelegatingBlockBuilder()
            builder.add_batch({"item": batch})
            yield builder.build()
            batch.clear()

    # Handle any remaining items
    if len(batch) > 0:
        builder = DelegatingBlockBuilder()
        builder.add_batch({"item": batch})
        yield builder.build()
