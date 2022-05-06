from ray.data import Dataset


class DatasetExecutionRegistry:
    """A class that makes sure each dataset is executed only once.

    Tune's driver process needs to make sure that dataset is fully executed
    before sent to trials. Multiple trials can all use the same dataset.
    In such case, the dataset should only be executed once (instead of once
    per trial).

    This class is only used when resuming trials from checkpoints."""

    def __init__(self):
        self.set = set()

    def execute_if_needed(self, ds: Dataset) -> Dataset:
        if ds._uuid not in self.set:
            ds = ds.fully_executed()
            self.set.add(ds._uuid)
        return ds


dataset_execution_registry = DatasetExecutionRegistry()
