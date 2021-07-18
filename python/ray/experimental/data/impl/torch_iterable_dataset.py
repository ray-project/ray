import torch
from torch.utils.data import IterableDataset


class TorchIterableDataset(IterableDataset):
    def __init__(self, generator_func):
        self.generator_func = generator_func

    def __iter__(self):
        it = self.generator_func()
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:
            yield from it
        else:
            # Multiple workers are doing dataloading.
            # Each worker has a copy of the data.
            # Avoid duplicates.
            import itertools
            it = itertools.islice(it, worker_info.id, None,
                                  worker_info.num_workers)
            yield from it
