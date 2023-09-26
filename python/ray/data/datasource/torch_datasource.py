import math
from typing import TYPE_CHECKING

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch


@DeveloperAPI
class TorchDatasource(Datasource):
    """Torch datasource, for reading from `map-style
    Torch datasets <https://pytorch.org/docs/stable/data.html#map-style-datasets/>`_.
    This datasource implements a parallel read that partitions the dataset based on
    input parallelism and creates read tasks for each partitions.
    """

    def create_reader(
        self,
        dataset: "torch.utils.data.Dataset",
        shuffle: bool = False,
        batch_size: int = 32,
    ):
        return _TorchDatasourceReader(dataset, shuffle, batch_size)


class _TorchDatasourceReader(Reader):
    def __init__(
        self, dataset: "torch.utils.data.Dataset", shuffle: bool, batch_size: int
    ):
        self._dataset = dataset
        self._shuffle = shuffle
        self._batch_size = batch_size

    def get_read_tasks(self, parallelism):
        import torch

        rows = len(self._dataset)
        subsets = None
        if self._shuffle:
            lengths = [rows // parallelism] * parallelism
            remainder = rows - sum(lengths)
            # spread remainder across lengths
            for i in range(remainder):
                lengths[i] += 1
            subsets = torch.utils.data.random_split(
                self._dataset,
                # use lengths array instead of fractions to avoid floating point error
                lengths,
            )
        else:
            rows_per_worker = math.ceil(rows / parallelism)
            subsets = [
                torch.utils.data.Subset(
                    self._dataset,
                    range(i * rows_per_worker, min((i + 1) * rows_per_worker, rows)),
                )
                for i in range(parallelism)
            ]

        read_tasks = []
        for i in range(parallelism):
            num_rows = len(subsets[i])
            meta = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda subset=subsets[i], batch_size=self._batch_size: _read_subset(
                        subset,
                        batch_size,
                    ),
                    metadata=meta,
                ),
            )

        return read_tasks

    def estimate_inmemory_data_size(self):
        return None


def _read_subset(subset: "torch.utils.data.Subset", batch_size: int):
    batch = []
    for item in subset:
        batch.append(item)
        if len(batch) == batch_size:
            builder = DelegatingBlockBuilder()
            builder.add_batch({"item": batch})
            yield builder.build()
            batch.clear()

    if len(batch) > 0:
        builder = DelegatingBlockBuilder()
        builder.add_batch({"item": batch})
        yield builder.build()
