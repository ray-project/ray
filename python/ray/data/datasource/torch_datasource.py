import math
from typing import TYPE_CHECKING

import cloudpickle

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask

if TYPE_CHECKING:
    import torch


class TorchDatasource(Datasource):
    def create_reader(
        self,
        dataset: "torch.utils.data.Dataset",
        batch_size: int = 32,
        random_split: bool = False,
    ):
        return _TorchDatasourceReader(dataset, batch_size, random_split)


class _TorchDatasourceReader(Reader):
    def __init__(
        self, dataset: "torch.utils.data.Dataset", batch_size: int, random_split: bool
    ):
        self._dataset = dataset
        self._batch_size = batch_size
        self._random_split = random_split
        self._size = len(cloudpickle.dumps(self._dataset))

    def get_read_tasks(self, parallelism):
        import torch

        rows = len(self._dataset)
        subsets = None
        if self._random_split:
            subsets = torch.utils.data.random_split(
                self._dataset, [1 / parallelism] * parallelism
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
                size_bytes=self._size * num_rows / rows,
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
        return self._size


def _read_subset(subset: "torch.utils.data.Subset", batch_size):
    import torch

    data_loader = torch.utils.data.DataLoader(
        subset,
        # default_collate does not accept `PIL.Image.Image`s
        collate_fn=lambda x: x,
        batch_size=batch_size,
    )

    for batch in data_loader:
        builder = DelegatingBlockBuilder()
        builder.add_batch({"item": batch})
        yield builder.build()
