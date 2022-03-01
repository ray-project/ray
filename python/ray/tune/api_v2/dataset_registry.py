from ray.data import Dataset


class DatasetRegistry:
    def __init__(self):
        self.map = set()

    def execute_if_needed(self, ds: Dataset):
        if ds._uuid not in self.map:
            ds.fully_executed()
            self.map.add(ds._uuid)


dataset_registry = DatasetRegistry()
