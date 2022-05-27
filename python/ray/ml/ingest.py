import logging
from typing import List, Dict, Optional

import ray
from ray.util.annotations import PublicAPI
from ray.actor import ActorHandle
from ray.data import Dataset, DatasetPipeline
from ray.data.context import DatasetContext
from ray.ml.constants import TRAIN_DATASET_KEY
from ray.ml.train.data_parallel_trainer import _default_dataset_split_fn
from ray.ml import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class IngestStrategy:
    """Defines how AIR loads and transforms data for a Trainer.

    You can specify a custom ingest strategy for an AIR Trainer by specifying
    ``YourTrainer(ingest=IngestStrategy)``. By default, the ``BulkIngest`` strategy
    is used, which loads the entire source Dataset in memory in bulk. The
    ``PipelinedIngest`` strategy loads data into memory in fixed sized windows,
    reducing the peak memory usage.
    """

    def preprocess_datasets(
        self, prep: Preprocessor, datasets: Dict[str, Dataset]
    ) -> Dict[str, Dataset]:
        """Fit and transform the given datasets.

        This method is called prior to the start of training. The ingest strategy can
        decide to preprocess the datasets at this point, or return them unprocessed
        for later processing (i.e., for pipelined ingest).

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


@PublicAPI(stability="alpha")
class BulkIngest(IngestStrategy):
    """A simple ingest strategy that loads all data blocks into memory.

    Data is loaded into memory, and then split equally between training workers. Each
    worker sees a different sub-set of the original data.

    This strategy is optimal for small datasets, and also the best if you want to
    perform per-epoch global shuffles over the entire Dataset (which involves loading
    the entire thing into memory in any case).

    However, Datasets larger than memory can lead to disk spilling, slowing ingest.
    """

    def __init__(self, global_shuffle: bool = False):
        """Create a bulk ingest strategy.

        Args:
            global_shuffle: Whether to shuffle the dataset globally each epoch. Note
                that this is an expensive distributed operation. Prefer to use local
                shuffles instead unless you really need this.
        """
        self._global_shuffle = global_shuffle

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

        def to_reader(k: str, ds: Dataset) -> DatasetPipeline:
            pipe = ds.repeat()
            if k == TRAIN_DATASET_KEY and self._global_shuffle:
                pipe = pipe.random_shuffle_each_window()
            return pipe

        # Wrap each split in a trivial pipeline that just loops over its blocks.
        # If global shuffle is enabled, the dataset is also shuffled each epoch.
        for i in range(len(splits)):
            splits[i] = {k: to_reader(k, ds) for k, ds in splits[i].items()}
        return splits


@PublicAPI(stability="alpha")
class PipelinedIngest(IngestStrategy):
    """A pipelined ingest strategy.

    This strategy loads *windows* of data at a time for processing in a pipelined way.
    Each window can optionally be shuffled. Then, each window is split into equal
    sized pieces and routed to the training workers for read.

    When the window size is set to infinity, this is similar to BulkIngest.

    Note that pipelined reads are only made available for the main "train" dataset.
    Other datasets are still loaded via the bulk strategy. Additionally, it is not
    allowed to call `train.get_dataset_shard` on the "train" dataset, to avoid
    accidental use of non-pipelined reads for the base dataset.
    """

    def __init__(
        self,
        window_size_bytes: int = 1024 * 1024 * 1024,
        shuffle_each_window: bool = False,
    ):
        """Create a pipelined ingest strategy.

        Args:
            window_size_bytes: Set the pipeline window size. The ingest strategy will
                target loading this many bytes at a time from storage for processing.
                Set this to a value that comfortably fits into object store memory,
                e.g., 20-25%, to avoid unnecessary disk spilling.
            shuffle_each_window: Whether to shuffle each window loaded from disk. Note
                that this is an expensive distributed operation. Prefer to use local
                shuffles instead unless you really need this.
        """
        self._window_size_bytes = window_size_bytes
        self._shuffle_each_window = shuffle_each_window
        # Set after preprocessing.
        self._fitted_preprocessor: Optional[Preprocessor] = None
        self._train_dataset: Optional[Dataset] = None

    def preprocess_datasets(
        self, prep: Preprocessor, datasets: Dict[str, Dataset]
    ) -> Dict[str, Dataset]:
        train_dataset = datasets.get(TRAIN_DATASET_KEY, None)

        # Fit only the training dataset, but do not transform it yet.
        if train_dataset:
            if prep.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE:
                # TODO(ekl) implement and remove this check.
                raise NotImplementedError(
                    "PipelinedIngest does not support fittable preprocessors yet."
                )
            prep.fit(train_dataset)
            self._fitted_preprocessor = prep
            self._train_dataset = train_dataset

        # Only transform auxiliary datasets. In the future, we might support pipelined
        # transform of these as well.
        for k, dataset in datasets.items():
            if k != TRAIN_DATASET_KEY:
                datasets[k] = self._fitted_preprocessor.transform(dataset)

        datasets = datasets.copy()
        del datasets[TRAIN_DATASET_KEY]
        return datasets

    def create_readers(
        self, datasets: Dict[str, Dataset], workers: List[ActorHandle]
    ) -> Dict[str, DatasetPipeline]:
        splits = [datasets.copy() for _ in workers]

        train_ds_size = self._train_dataset.size_bytes()
        if train_ds_size < self._window_size_bytes:
            logger.warning(
                f"The `train` dataset is {_in_gb(train_ds_size)}, which is less than "
                f"the pipeline window size of {_in_gb(self._window_size_bytes)}. "
                "PipelinedIngest will act the same as BulkIngest in this case."
            )
            # The user should really use BulkIngest, but disable re-reads from
            # external storage in this case to help optimize.
            context = DatasetContext.get_current()
            context.optimize_fuse_read_stages = False

        # Setup the preprocessing pipeline for the train dataset.
        prep = self._fitted_preprocessor
        train_pipe = (
            self._train_dataset.window(bytes_per_window=self._window_size_bytes)
            .map_batches(prep.transform_batch, batch_format="pandas")
            .repeat()
        )
        if self._shuffle_each_window:
            train_pipe = train_pipe.random_shuffle_each_window()

        train_pipe_splits = train_pipe.split(
            len(workers), equal=True, locality_hints=workers
        )

        for i in range(len(splits)):
            splits[i] = {k: ds.repeat() for k, ds in splits[i].items()}
            splits[i][TRAIN_DATASET_KEY] = train_pipe_splits[i]
        return splits


def _in_gb(num: int) -> str:
    gb = round(num / (1024 * 1024 * 1024), 2)
    return f"{gb} GiB"


def _choose_ingest_strategy(datasets: Dict[str, Dataset]) -> IngestStrategy:
    train_ds = datasets.get(TRAIN_DATASET_KEY)
    if train_ds:
        sz = train_ds.size_bytes()
        obj_mem = ray.available_resources().get("object_store_memory", 0)
        if sz > 1024 * 1024 * 1024 and sz > 0.2 * obj_mem:
            logger.warning(
                f"The `train` dataset ({_in_gb(sz)}) is larger than 20% of available "
                f"object store memory ({_in_gb(obj_mem)}). Disk spilling may occur. "
                "To optimize memory usage, use `ingest=PipelinedIngest()`, which "
                "streams data from storage."
            )
    return BulkIngest()
