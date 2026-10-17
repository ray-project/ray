"""DataConfig variant that shards datasets with the push-based split.

Drop-in replacement for :class:`ray.train.DataConfig`::

    trainer = TorchTrainer(
        ...,
        datasets={"train": ds},
        dataset_config=PushBasedDataConfig(),
    )

Split datasets are served by a ``PushSplitCoordinator`` that pushes blocks
to each train worker (see
``ray/data/_internal/iterator/push_based_split_iterator.py``). Each returned
``PushBasedDataIterator`` registers its train worker with the coordinator on
first iteration; deliveries arrive through the ``PushSplitReceiverMixin``
methods that ``RayTrainWorker`` mixes in.
"""

from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.actor import ActorHandle
from ray.train._internal.data_config import DataConfig

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr


class PushBasedDataConfig(DataConfig):
    """Shard Train datasets via the push-based streaming split."""

    def __init__(
        self,
        datasets_to_split: Union[Literal["all"], List[str]] = "all",
        execution_options: Optional[
            Union["ExecutionOptions", Dict[str, "ExecutionOptions"]]
        ] = None,
        enable_shard_locality: bool = True,
    ):
        """Construct a PushBasedDataConfig.

        Args:
            datasets_to_split: Same as :class:`~ray.train.DataConfig`.
            execution_options: Same as :class:`~ray.train.DataConfig`.
            enable_shard_locality: Same as :class:`~ray.train.DataConfig`.
        """
        super().__init__(
            datasets_to_split=datasets_to_split,
            execution_options=execution_options,
            enable_shard_locality=enable_shard_locality,
        )

    def configure(
        self,
        datasets: Dict[str, "Dataset"],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List["NodeIdStr"]],
        **kwargs,
    ) -> List[Dict[str, "DataIterator"]]:
        from ray.data._internal.iterator.push_based_split_iterator import (
            streaming_split_push_based,
        )

        # Mirrors DataConfig.configure, swapping Dataset.streaming_split for
        # the push-based split.
        output = [{} for _ in range(world_size)]

        for dataset_name, dataset in datasets.items():
            if dataset.name is None:
                dataset.set_name(dataset_name)

        if self._datasets_to_split == "all":
            datasets_to_split = set(datasets.keys())
        else:
            datasets_to_split = set(self._datasets_to_split)

        locality_hints = worker_node_ids if self._enable_shard_locality else None
        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._resolve_execution_options(name)

            if name in datasets_to_split:
                for i, split in enumerate(
                    streaming_split_push_based(
                        ds,
                        world_size,
                        equal=True,
                        locality_hints=locality_hints,
                    )
                ):
                    output[i][name] = split
            else:
                for i in range(world_size):
                    output[i][name] = ds.iterator()

        return output
