from typing import List, Dict

from ray.actor import ActorHandle
from ray.data import Dataset, DatasetPipeline
from ray.ml.constants import TRAIN_DATASET_KEY


class IngestStrategy:
    def preprocess_datasets(self, preprocessor, datasets):
        """Call to preprocess datasets."""
        raise NotImplementedError

    def create_readers(self, datasets, workers) -> Dict[str, DatasetPipeline]:
        """Call to create dataset readers."""
        raise NotImplementedError


class StreamedIngest(IngestStrategy):
    def __init__(self, global_shuffle: bool = False):
        # TODO: calculate this as 1GiB per worker.
        self._window_size_bytes = 1e9
        self._fitted_preprocessor = None
        self._global_shuffle = global_shuffle

    def preprocess_datasets(self, preprocessor, datasets):
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)
        if train_dataset:
            # TODO(ekl) support streaming fit?
            preprocessor.fit(train_dataset)
            self._fitted_preprocessor = train_dataset

        # Only transform non-train datasets. In the future, we might support streaming
        # transform of those as well.
        for k, dataset in datasets.items():
            if k == TRAIN_DATASET_KEY:
                pass
            else:
                datasets[k] = self._fitted_preprocessor.transform(dataset)

        return datasets

    def create_readers(self, datasets, worker_handles) -> Dict[str, DatasetPipeline]:
        # TODO: implement independent reads per worker
        self._world_size = len(worker_handles)
        splits = [datasets.copy() for _ in worker_handles]

        train_pipe = (
            datasets[TRAIN_DATASET_KEY]
            .window(window_size_bytes=self._window_size_bytes)
            .repeat()
        )

        if self._global_shuffle:
            train_pipe = train_pipe.random_shuffle_each_window()

        train_pipe_splits = train_pipe.split(
            self._world_size, equal=True, locality_hints=worker_handles
        )

        def to_reader(i, k, ds):
            if k == TRAIN_DATASET_KEY:
                return train_pipe_splits[i]
            else:
                return ds.repeat()

        for i in range(self._world_size):
            splits[i] = {k: to_reader(i, k, v) for k, v in splits[i].items()}
        return splits


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

    def create_readers(self, datasets, worker_handles) -> Dict[str, DatasetPipeline]:
        self._world_size = len(worker_handles)
        if self._split:
            splits = _default_dataset_split_fn(datasets, worker_handles)
        else:
            splits = [datasets.copy() for _ in worker_handles]

        def to_reader(k, ds):
            pipe = ds.repeat()
            if self._global_shuffle and k == TRAIN_DATASET_KEY:
                pipe = pipe.random_shuffle_each_window()
            if self._local_shuffle_buffer_size > 0:
                raise NotImplementedError
            return pipe

        for i in range(self._world_size):
            splits[i] = {k: to_reader(k, v) for k, v in splits[i].items()}
        return splits


def _choose_ingest_strategy(dataset: Dict[str, Dataset]) -> IngestStrategy:
    sz = dataset.size_bytes()
    if sz < 1e9:
        print("Chose bulk ingest by default, dataset size", sz)
        return BulkIngest()
    else:
        print("Chose streamed ingest by default, dataset size", sz)
        return StreamedIngest()


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
