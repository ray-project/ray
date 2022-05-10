from dataclasses import dataclass
from typing import Optional, Union, Dict, Callable, List, TYPE_CHECKING

from ray.actor import ActorHandle

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline

RayDataset = Union["Dataset", "DatasetPipeline"]


@dataclass
class _RayDatasetSpec:
    """Configuration for Ray Datasets to pass to the training workers.

    dataset_or_dict: An optional Ray Dataset (or DatasetPipeline) or a dictionary of
        datasets to be sharded across all the training workers, which can be accessed
        from the training function via ``train.get_dataset_shard()``. Multiple Datasets
        can be passed in as a dictionary that maps each name key to a Dataset value,
        and each Dataset can be accessed from the training function by passing in a
        `dataset_name` argument to ``train.get_dataset_shard()``.
    dataset_split_fn: An optional callable to specify how the provided ``dataset``
        should be split across the training workers. It is expected to take in two
        arguments. The first one is the ``dataset``, just as is passed in to the
        ``_RayDatasetSpec``. The second argument is a list of the ActorHandles of the
        training workers (to use as locality hints). The Callable is expected to
        return a list of RayDatasets or a list of dictionaries of RayDatasets,
        with the length of the list equal to the length of the list of actor handles.
        If None is provided, the provided Ray Dataset(s) will be simply be split using
        the actor handles as locality hints.

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
