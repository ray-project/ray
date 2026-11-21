"""DataConfig variant that shards datasets with the push-based split.

Drop-in replacement for :class:`ray.train.DataConfig`::

    trainer = TorchTrainer(
        ...,
        datasets={"train": ds},
        dataset_config=PushBasedDataConfig(),
    )

``PushBasedDataConfig`` always uses the push-based split. The plain
``DataConfig`` uses it by default on Ray Train V2, unless the
``RAY_TRAIN_USE_PUSH_BASED_STREAMING_SPLIT=0`` environment variable is set.

Split datasets are served by a ``PushSplitCoordinator`` that pushes blocks
to each train worker (see
``ray/data/_internal/iterator/push_split_iterator.py``). Each returned
``PushBasedDataIterator`` registers its train worker with the coordinator on
first iteration; deliveries arrive through the ``PushSplitReceiverMixin``
methods that ``RayTrainWorker`` mixes in.
"""

from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.train._internal.data_config import DataConfig

if TYPE_CHECKING:
    from ray.data import ExecutionOptions
    from ray.data.checkpoint.interfaces import DatasetCheckpointConfig


class PushBasedDataConfig(DataConfig):
    """Shard Train datasets via the push-based streaming split."""

    def __init__(
        self,
        datasets_to_split: Union[Literal["all"], List[str]] = "all",
        execution_options: Optional[
            Union["ExecutionOptions", Dict[str, "ExecutionOptions"]]
        ] = None,
        enable_shard_locality: bool = True,
        dataset_checkpoint_configs: Optional[
            Dict[str, "DatasetCheckpointConfig"]
        ] = None,
    ):
        """Construct a PushBasedDataConfig.

        Args:
            datasets_to_split: Same as :class:`~ray.train.DataConfig`.
            execution_options: Same as :class:`~ray.train.DataConfig`.
            enable_shard_locality: Same as :class:`~ray.train.DataConfig`.
            dataset_checkpoint_configs: Same as :class:`~ray.train.DataConfig`.
        """
        super().__init__(
            datasets_to_split=datasets_to_split,
            execution_options=execution_options,
            enable_shard_locality=enable_shard_locality,
            dataset_checkpoint_configs=dataset_checkpoint_configs,
        )

    def _use_push_based_streaming_split(self) -> bool:
        return True
