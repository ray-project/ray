from typing import List, Dict, TYPE_CHECKING

from ray.data import Dataset, DatasetPipeline


class IngestStrategy:
    def preprocess_datasets(self, preprocessor, datasets):
        """Call to preprocess datasets."""
        raise NotImplementedError

    def prepare_readers(self, datasets, worker_handles) -> None:
        """Must be called before get_reader"""
        raise NotImplementedError

    def get_reader_for_rank(self, dataset, i) -> DatasetPipeline:
        """Called in start of train loop of trainer."""
        raise NotImplementedError


class StreamIngest(IngestStrategy):
    def __init__(self):
        self._window_size_bytes = 0.25 * local_object_store_memory()

    def preprocess_datasets(self, preprocessor, datasets):
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)
        if train_dataset:
            preprocessor.fit_pipeline(train_dataset)

#        new_datasets = {}
#        for key, dataset in datasets.items():
#            # TODO exclude train one for streaming read
#            new_datasets[key] = preprocessor.transform(dataset)

        # Return original datasets? Transforms will be applied on the fly at read time?
        return datasets

    def prepare_readers(self, datasets, worker_handles) -> None:
        """Must be called before get_reader"""
        raise NotImplementedError

    def get_reader_for_rank(self, dataset, i) -> DatasetPipeline:
        """Called in start of train loop of trainer."""
        raise NotImplementedError


class BulkIngest(IngestStrategy):
    def __init__(
        self,
        local_shuffle_buffer_size: int = 0,
        global_shuffle: bool = False,
        split: bool = True,
    ):
        self._local_shuffle_buffer_size = local_shuffle_buffer_size
        self._global_shuffle = global_shuffle
        if self._global_shuffle and self._local_shuffle_buffer_size:
            raise ValueError("Cannot specify both global and local shuffle.")
        self._split = split
        self._world_size = 1
        self._dataset_splits: List[Dict[str, Dataset]] = []

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

    def prepare_readers(self, datasets, worker_handles) -> None:
        self._world_size = len(worker_handles)
        if self._split:
            splits = _default_dataset_split_fn(datasets, worker_handles)
        else:
            splits = [datasets.copy() for _ in worker_handles]
        self._dataset_splits = splits

    def get_reader_for_rank(self, dataset_name, i) -> DatasetPipeline:
        assert 0 <= i < self._world_size
        dataset = self.splits[i][dataset_name]
        pipe = dataset.repeat()
        if self._global_shuffle:
            pipe = pipe.random_shuffle_each_window()
        if self._local_shuffle_buffer_size > 0:
            raise NotImplementedError
        return pipe


def _choose_ingest_strategy(dataset: Dict[str, Dataset]) -> IngestStrategy:
    # TODO: if small enough, use bulk ingest
    # if train dataset < max(1gb, 0.25 * object_store_memory)
    # else stream with window = 0.25 * object_store_memory
    return BulkIngest()


def _default_dataset_split_fn(
    dataset_dict: Dict[str, "Dataset"], training_worker_handles: List[ActorHandle]
) -> List[Dict[str, "Dataset"]]:
    """Defines splitting logic of Datasets passed into ``DataParallelTrainer``.

    By default only training dataset will be split. All other datasets will not be
    split and passed through directly to the training workers. This is because
    validation implementation is often done on just the rank 0 worker.

    Args:
        dataset_dict: A dictionary of Datasets.
        training_worker_handles: The actor handles of the training workers to use for
            locality hints.

    Returns:
        A list of dataset dictionaries for each training worker.
    """
    dataset_dict_splits = [{} for _ in range(len(training_worker_handles))]

    for key, dataset in dataset_dict.items():
        if key == TRAIN_DATASET_KEY:
            dataset_splits = dataset.split(
                len(training_worker_handles),
                equal=True,
                locality_hints=training_worker_handles,
            )
        else:
            # Only shard the training dataset.
            dataset_splits = [dataset] * len(training_worker_handles)

        for i in range(len(dataset_splits)):
            dataset_dict_splits[i][key] = dataset_splits[i]

    return dataset_dict_splits
