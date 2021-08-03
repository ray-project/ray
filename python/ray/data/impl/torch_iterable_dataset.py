from torch.utils.data import IterableDataset


class TorchIterableDataset(IterableDataset):
    def __init__(self, generator_func):
        self.generator_func = generator_func

    def __iter__(self):
        it = self.generator_func()
        yield from it
