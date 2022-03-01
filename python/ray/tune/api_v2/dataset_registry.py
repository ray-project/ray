from ray.data import Dataset


class DatasetRegistry:
    def __init__(self):
        self.set = set()

    def execute_if_needed(self, ds: Dataset) -> Dataset:
        if ds._uuid not in self.set:
            ds = ds.fully_executed()
            self.set.add(ds._uuid)
        return ds


dataset_registry = DatasetRegistry()
