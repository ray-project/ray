from typing import Dict, TYPE_CHECKING

from ray.data import Dataset


class IngestStrategy:
    pass


class BulkIngest(IngestStrategy):

    def __init__(self, local_shuffle_buffer_size: int = 0, global_shuffle: bool = False):
        self.local_shuffle_buffer_size = local_shuffle_buffer_size
        self.global_shuffle = global_shuffle
        if self.global_shuffle and self.local_shuffle_buffer_size:
            raise ValueError("Cannot specify both global and local shuffle.")

    def preprocess_datasets(self, preprocessor, datasets):
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)
        if train_dataset:
            preprocessor.fit(train_dataset)

        # Execute dataset transformations serially for now.
        # Cannot execute them in remote tasks due to dataset ownership model:
        # if datasets are created on a remote node, then if that node fails,
        # we cannot recover the dataset.
        new_datasets = {}
        for key, dataset in datasets.items():
            new_datasets[key] = preprocessor.transform(dataset)

        return new_datasets
    
    def set_world_rank(self, i) -> None:
        pass

    def get_reader_for_rank(self, dataset, i) -> DatasetPipeline:
        if self.local_shuffle_buffer_size > 0:
            raise NotImplementedError
        pipe = dataset.repeat()
        if self.global_shuffle:
            pipe = pipe.random_shuffle_each_window()
        return pipe


def _choose_ingest_strategy(dataset: Dict[str, Dataset]) -> IngestStrategy:
    return BulkIngest()
