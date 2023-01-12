import warnings
from typing import TYPE_CHECKING, Dict, Optional

from ray.air._internal.session import Session
from ray.air.checkpoint import Checkpoint

if TYPE_CHECKING:
    # avoid circular import
    from ray.data import DatasetIterator
    from ray.train._internal.session import _TrainSession
    from ray.tune.execution.placement_groups import PlacementGroupFactory


class _TrainSessionImpl(Session):
    """Session client that "per worker train loop" can interact with.

    Notice that each worker will automatically switch to its working
    directory on entering the train loop. This is to ensure that
    each worker can safely write to a local directory without racing
    and overwriting each other."""

    def __init__(self, session: "_TrainSession"):
        self._session = session

    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
        self._session.report(metrics, checkpoint)

    @property
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        ckpt = self._session.loaded_checkpoint
        if ckpt:
            # The new API should only interact with Checkpoint object.
            assert isinstance(ckpt, Checkpoint)
        return ckpt

    @property
    def experiment_name(self) -> str:
        return self._session.trial_info.experiment_name

    @property
    def trial_name(self) -> str:
        return self._session.trial_info.name

    @property
    def trial_id(self) -> str:
        return self._session.trial_info.id

    @property
    def trial_resources(self) -> "PlacementGroupFactory":
        return self._session.trial_info.resources

    @property
    def trial_dir(self) -> str:
        return self._session.trial_info.logdir

    @property
    def world_size(self) -> int:
        return self._session.world_size

    @property
    def world_rank(self) -> int:
        return self._session.world_rank

    @property
    def local_rank(self) -> int:
        return self._session.local_rank

    @property
    def local_world_size(self) -> int:
        return self._session.local_world_size

    @property
    def node_rank(self) -> int:
        return self._session.node_rank

    def get_dataset_shard(
        self,
        dataset_name: Optional[str] = None,
    ) -> Optional["DatasetIterator"]:
        shard = self._session.dataset_shard
        if shard is None:
            warnings.warn(
                "No dataset passed in. Returning None. Make sure to "
                "pass in a Ray Dataset to Trainer.run to use this "
                "function."
            )
        elif isinstance(shard, dict):
            if not dataset_name:
                raise RuntimeError(
                    "Multiple datasets were passed into ``Trainer``, "
                    "but no ``dataset_name`` is passed into "
                    "``get_dataset_shard``. Please specify which "
                    "dataset shard to retrieve."
                )
            return shard.get(dataset_name)
        return shard
