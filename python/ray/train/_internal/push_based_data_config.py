"""PROTOTYPE: DataConfig variant that shards datasets with the push-based split.

Drop-in replacement for :class:`ray.train.DataConfig`::

    trainer = TorchTrainer(
        ...,
        datasets={"train": ds},
        dataset_config=PushBasedDataConfig(),
    )

Instead of ``Dataset.streaming_split`` (pull model), split datasets are
served by a ``PushSplitCoordinator`` that pushes block refs to each train
worker (see ``ray/data/_internal/iterator/push_based_split_iterator.py``).
No Ray Train core changes are needed: the returned ``PushBasedDataIterator``
self-registers the hosting train worker actor with the coordinator on first
iteration, and delivery goes through the actor's built-in ``__ray_call__``.
"""

from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.actor import ActorHandle
from ray.data._internal.iterator.push_based_split_iterator import (
    PushBasedDataIterator,
    create_push_split,
)
from ray.train._internal.data_config import DataConfig

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr


class PushBasedDataConfig(DataConfig):
    """PROTOTYPE: shard Train datasets via the push-based streaming split."""

    def __init__(
        self,
        datasets_to_split: Union[Literal["all"], List[str]] = "all",
        execution_options: Optional[
            Union["ExecutionOptions", Dict[str, "ExecutionOptions"]]
        ] = None,
        enable_shard_locality: bool = True,
        target_buffer_rows: Optional[int] = None,
    ):
        """Construct a PushBasedDataConfig.

        Args:
            datasets_to_split: Same as :class:`~ray.train.DataConfig`.
            execution_options: Same as :class:`~ray.train.DataConfig`.
            enable_shard_locality: Same as :class:`~ray.train.DataConfig`.
            target_buffer_rows: How many rows each train worker keeps
                buffered locally (the push credit). Should cover at least
                ~2 blocks; defaults to
                ``DEFAULT_GENERIC_TARGET_BUFFER_ROWS``.
        """
        super().__init__(
            datasets_to_split=datasets_to_split,
            execution_options=execution_options,
            enable_shard_locality=enable_shard_locality,
        )
        self._target_buffer_rows = target_buffer_rows

    def configure(
        self,
        datasets: Dict[str, "Dataset"],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List["NodeIdStr"]],
        **kwargs,
    ) -> List[Dict[str, "DataIterator"]]:
        # Mirrors DataConfig.configure, swapping streaming_split for
        # create_push_split + PushBasedDataIterator.
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
                coord_actor = create_push_split(
                    ds, world_size, equal=True, locality_hints=locality_hints
                )
                for i in range(world_size):
                    output[i][name] = PushBasedDataIterator(
                        coord_actor,
                        i,
                        world_size,
                        target_buffer_rows=self._target_buffer_rows,
                    )
            else:
                for i in range(world_size):
                    output[i][name] = ds.iterator()

        return output
