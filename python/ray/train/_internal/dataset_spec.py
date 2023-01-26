from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

from ray.actor import ActorHandle
from ray.air.config import DatasetConfig

from ray.data import Dataset, DatasetPipeline
from ray.air._internal.util import _estimate_avail_object_store_memory

if TYPE_CHECKING:
    from ray.data import DatasetIterator
    from ray.data.preprocessor import Preprocessor

RayDataset = Union["Dataset", "DatasetPipeline"]


@dataclass
class RayDatasetSpec:
    """Configuration for Ray Datasets to pass to the training workers.

    dataset_or_dict: An optional Ray Dataset (or DatasetPipeline) or a dictionary of
        datasets to be sharded across all the training workers, which can be accessed
        from the training function via ``session.get_dataset_shard()``. Multiple
        Datasets can be passed in as a dictionary that maps each name key to a
        Dataset value, and each Dataset can be accessed from the training function
        by passing in a `dataset_name` argument to ``session.get_dataset_shard()``.
    dataset_split_fn: An optional callable to specify how the provided ``dataset``
        should be split across the training workers. It is expected to take in two
        arguments. The first one is the ``dataset``, just as is passed in to the
        ``_RayDatasetSpec``. The second argument is a list of the ActorHandles of the
        training workers (to use as locality hints). The Callable is expected to
        return a list of RayDatasets or a list of dictionaries of RayDatasets,
        with the length of the list equal to the length of the list of actor handles.
        If None is provided, the provided Ray Dataset(s) will be equally split.

    """

    dataset_or_dict: Optional[Union[RayDataset, Dict[str, RayDataset]]]
    dataset_split_fn: Optional[
        Callable[
            [Union[RayDataset, Dict[str, RayDataset]], List[ActorHandle]],
            List[Union[RayDataset, Dict[str, RayDataset]]],
        ]
    ] = None

    def _default_split_fn(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Optional[Union[RayDataset, Dict[str, RayDataset]]]]:
        def split_dataset(dataset_or_pipeline):
            return dataset_or_pipeline.split(
                len(training_worker_handles),
                equal=True,
                locality_hints=training_worker_handles,
            )

        if isinstance(self.dataset_or_dict, dict):
            # Return a smaller dict for each shard.
            dataset_shards = [{} for _ in range(len(training_worker_handles))]
            for key, dataset in self.dataset_or_dict.items():
                split_datasets = split_dataset(dataset)
                assert len(split_datasets) == len(training_worker_handles)
                for i in range(len(split_datasets)):
                    dataset_shards[i][key] = split_datasets[i]
            return dataset_shards
        else:
            # return a smaller RayDataset for each shard.
            return split_dataset(self.dataset_or_dict)

    def get_dataset_shards(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Optional[Union[RayDataset, Dict[str, RayDataset]]]]:
        """Returns Dataset splits based off the spec and the given training workers

        Args:
            training_worker_handles: A list of the training worker actor handles.

        Returns:
            A list of RayDataset shards or list of dictionaries of RayDataset shards,
                one for each training worker.

        """
        if not self.dataset_or_dict:
            return [None] * len(training_worker_handles)

        if self.dataset_split_fn is None:
            return self._default_split_fn(training_worker_handles)
        else:
            splits = self.dataset_split_fn(
                self.dataset_or_dict, training_worker_handles
            )
            if not len(splits) == len(training_worker_handles):
                raise RuntimeError(
                    "The list of Datasets returned by the "
                    f"`dataset_split_fn`: {len(splits)} does not match "
                    f"the number of training workers: {len(training_worker_handles)}"
                )
            return splits


class DataParallelIngestSpec:
    """Implements the execution of DatasetConfig preprocessing and ingest."""

    def __init__(self, dataset_config: Dict[str, DatasetConfig]):
        """Construct an ingest spec.

        Args:
            dataset_config: The merged default + user config dict for the trainer
                with all defaults filled in.
        """
        self.dataset_config = dataset_config
        self.preprocessed_datasets: Optional[Dict[str, "Dataset"]] = None
        self.preprocessor: Optional["Preprocessor"] = None

    def preprocess_datasets(
        self, prep: "Preprocessor", datasets: Dict[str, "Dataset"]
    ) -> Dict[str, "Dataset"]:
        """Preprocess the given datasets.

        This will be called prior to `get_dataset_shards()`.

        Args:
            prep: The preprocessor to fit, if needed.
            dataset: The datasets to fit and transform.

        Returns:
            Dict of transformed datasets.
        """

        for key, dataset in list(datasets.items()):
            conf = self._config(key)
            # If globally shuffling, don't randomize unless using the stream API.
            local_window = 1 > conf.max_object_store_memory_fraction >= 0
            if conf.randomize_block_order and (not conf.global_shuffle or local_window):
                datasets[key] = dataset.randomize_block_order()

        if prep:
            ds_to_fit = None
            for k, conf in self.dataset_config.items():
                if k not in datasets:
                    assert not conf.required, "Missing dataset post-validation"
                    continue
                if conf.fit:
                    ds_to_fit = datasets[k]
            if ds_to_fit:
                prep.fit(ds_to_fit)
            new_datasets = {}

            for key, dataset in datasets.items():
                conf = self._config(key)
                if conf.transform:
                    if conf.max_object_store_memory_fraction >= 0:
                        # In windowed mode, preprocessor is applied in streaming way.
                        new_datasets[key] = dataset
                    else:
                        # Window size of infinity is treated same as bulk mode.
                        new_datasets[key] = prep.transform(dataset)
                else:
                    new_datasets[key] = dataset
        else:
            new_datasets = datasets
        self.preprocessed_datasets = new_datasets
        self.preprocessor = prep
        return new_datasets

    def get_dataset_shards(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Dict[str, "DatasetIterator"]]:
        """Get the shards to pass to training workers.

        Note: this has to match the signature of DatasetSpec in legacy train.

        Args:
            training_worker_handles: Actor handles of the workers, which can be used
                for locality-aware splitting.

        Returns:
            List of dataset shard dicts, one for each training worker.
        """
        dataset_dict_splits = [{} for _ in range(len(training_worker_handles))]

        for key, dataset in self.preprocessed_datasets.items():
            config = self._config(key)

            if config.max_object_store_memory_fraction >= 0:
                object_store_memory = _estimate_avail_object_store_memory()
                stream_window_size = max(
                    object_store_memory * config.max_object_store_memory_fraction, 1
                )
                dataset = dataset.window(bytes_per_window=stream_window_size).repeat()
                # In windowed mode, we re-apply the preprocessor on each iteration.
                if self.preprocessor:
                    dataset = self.preprocessor._transform_pipeline(dataset)
                # Always re-randomize each window; this doesn't help with reducing
                # cluster hot-spots since we already randomized the based blocks, but
                # can help with improving randomness in combination with local shuffle.
                if config.randomize_block_order and not config.global_shuffle:
                    # TODO(swang): Should randomize block order across the
                    # original dataset, not the window.
                    dataset = dataset.randomize_block_order_each_window()

            if config.global_shuffle:
                # If global shuffle is requested, then we should try to overlap
                # this with other computation, so convert to a DatasetPipeline
                # if not already being used.
                if isinstance(dataset, Dataset):
                    dataset = dataset.repeat()
                dataset = dataset.random_shuffle_each_window()

            if config.split and len(training_worker_handles) > 1:
                dataset_splits = dataset.split(
                    len(training_worker_handles),
                    equal=True,
                    locality_hints=training_worker_handles,
                )
            else:
                dataset_splits = [dataset] * len(training_worker_handles)

            for i, dataset_split in enumerate(dataset_splits):
                dataset_splits[i] = dataset_split.iterator()._with_backward_compat()

            for i in range(len(dataset_splits)):
                dataset_dict_splits[i][key] = dataset_splits[i]

        return dataset_dict_splits

    def _config(self, key: str) -> "DatasetConfig":
        """Get the dataset config for the given dataset name."""
        if key in self.dataset_config:
            return self.dataset_config[key]
        return self.dataset_config["*"]
