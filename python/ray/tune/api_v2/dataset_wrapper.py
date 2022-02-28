import uuid

from ray.data import Dataset

# A wrapper of Dataset with explicit lineage (until datasets implements it itself)
class DatasetWrapper:
    def __init__(self, dataset: Dataset, gen_fn: callable):
        self.dataset = dataset
        self.gen_fn = gen_fn

    def __deepcopy__(self, memodict={}):
        return DatasetWrapper(self.dataset, self.gen_fn)


class DatasetRegistry:
    # ID to DatasetWrapper
    # Upon pickling, replace all Dataset objects with corresponding IDs,
    # and save the mapping between ID to datasets.
    # Upon unpickling, construct the Dataset objects and evaluate them.
    # Replace IDs with Dataset objects.
    def __init__(self):
        self.map = {}

    def register_if_needed(self, dataset_wrapper: DatasetWrapper) -> Dataset:
        """This also makes sure that fully_executed is called only once per dataset."""
        for v in self.map.values():
            if dataset_wrapper.dataset._uuid == v.dataset._uuid:
                return dataset_wrapper.dataset
        self.map[str(uuid.uuid4())] = dataset_wrapper
        dataset_wrapper.dataset.fully_executed()
        return dataset_wrapper.dataset

    def get_id(self, dataset: Dataset) -> str:
        for k in self.map.keys():
            if self.map[k].dataset._uuid == dataset._uuid:
                return k
        assert False, "the dataset is not registered with DatasetRegistry before."

    def get_dataset(self, id) -> Dataset:
        assert (
            id in self.map.keys()
        ), "the key is not registered with DatasetRegistry before."
        if not self.map[id].dataset:  # not generated yet
            # !!!!!!!!!!!! THIS DOES NOT WORK FOR SOME REASON !!!!!!!!!!!!!!
            # BUT IT IS OK AS THIS IS HACK AROUND DATASETS LINEAGE ANYWAY.
            self.map[id].dataset = self.map[id].gen_fn()
            self.map[id].dataset.fully_executed()
        return self.map[id].dataset

    def __getstate__(self):
        result = self.__dict__.copy()
        print("================================")
        print(result)
        print("================================")
        return result

    def __setstate__(self, state):
        for v in state["map"].values():
            v.dataset = None
        self.__dict__.update(state)


dataset_registry = DatasetRegistry()
