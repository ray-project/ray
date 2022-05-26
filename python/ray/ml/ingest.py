from typing import List, Dict

import ray
from ray.actor import ActorHandle
from ray.data import Dataset, DatasetPipeline
from ray.data.context import DatasetContext
from ray.ml.constants import TRAIN_DATASET_KEY
from ray.ml import Preprocessor


class IngestStrategy:
    """Defines how AIR loads and transforms data for a Trainer.

    You can specify a custom ingest strategy for an AIR Trainer by specifying
    ``YourTrainer(ingest=IngestStrategy)``. By default, the ``BulkIngest`` strategy
    is used, which loads the entire source Dataset in memory in bulk. The
    ``StreamingIngest`` strategy loads data into memory in fixed sized windows,
    reducing the peak memory usage.
    """

    def preprocess_datasets(
        self, prep: Preprocessor, datasets: Dict[str, Dataset]
    ) -> Dict[str, Dataset]:
        """Fit and transform the given datasets.

        This method is called prior to the start of training. The ingest strategy can
        decide to preprocess the datasets at this point, or return them unprocessed
        for later processing (i.e., for streaming ingest).

        Args:
            prep: The preprocessor to fit and transform datasets with.
            dataset: The dict of datasets to preprocessor.
        """
        raise NotImplementedError

    def create_readers(
        self, datasets: Dict[str, Dataset], workers: List[ActorHandle]
    ) -> Dict[str, DatasetPipeline]:
        """Create pipeline readers for each training worker.

        This method is called after training workers have been launched to generate
        dataset readers. Each reader is a DatasetPipeline, which in the simplest case
        independently loops over the source dataset. In the most general case, the
        readers can share a common pipeline structure (e.g., data is globally shuffled
        and then split between training workers).
        """
        raise NotImplementedError


class BulkIngest(IngestStrategy):
    """A simple ingest strategy that loads all data blocks into memory.

    Data is loaded into memory, and then split equally between training workers. Each
    worker sees a different sub-set of the original data.

    This strategy is optimal for small datasets, and also the best if you want to
    perform per-epoch global shuffles over the entire Dataset (which involves loading
    the entire thing into memory in any case).

    However, Datasets larger than memory can lead to disk spilling, slowing ingest.
    """

    def preprocess_datasets(
        self, prep: Preprocessor, datasets: Dict[str, Dataset]
    ) -> Dict[str, Dataset]:

        # Fit only the training dataset.
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)
        if train_dataset:
            prep.fit(train_dataset)

        # Transform the auxiliary datasets after fit.
        new_datasets = {}
        for key, dataset in datasets.items():
            new_datasets[key] = prep.transform(dataset)
        return new_datasets

    def create_readers(
        self, datasets: Dict[str, Dataset], workers: List[ActorHandle]
    ) -> Dict[str, DatasetPipeline]:

        # Divide the dataset blocks up among training workers.
        splits = _default_dataset_split_fn(datasets, workers)

        # Wrap each split in a trivial pipeline that just loops over its blocks.
        for i in range(len(splits)):
            splits[i] = {k: ds.repeat() for k, ds in splits[i].items()}
        return splits


class StreamingIngest(IngestStrategy):
    def __init__(self, global_shuffle: bool = False):
        # TODO: calculate this as 1GiB per worker.
        self._window_size_bytes = 1024 * 1024 * 1024
        self._fitted_preprocessor = None
        self._global_shuffle = global_shuffle

    def preprocess_datasets(self, preprocessor, datasets):
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)
        if train_dataset:
            # TODO(ekl) support streaming fit?
            preprocessor.fit(train_dataset)
            self._fitted_preprocessor = preprocessor

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

        if datasets[TRAIN_DATASET_KEY].size_bytes() <= 2 * self._window_size_bytes:
            print("Disabling re-read")
            context = DatasetContext.get_current()
            context.optimize_fuse_read_stages = False
        else:
            print("Re-read")

        prep = self._fitted_preprocessor

        def transform_fn(x):
            return prep.transform_batch(x)

        train_pipe = (
            datasets[TRAIN_DATASET_KEY]
            .window(bytes_per_window=self._window_size_bytes)
            .map_batches(transform_fn, batch_format="pandas")
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


def _choose_ingest_strategy(dataset: Dict[str, Dataset]) -> IngestStrategy:
    sz = dataset.size_bytes()
    if sz > 1e9 and sz > 0.2 * ray.available_resources["object_store_memory"]:
        print("WARNING: large size, consider streamed ingest")
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
