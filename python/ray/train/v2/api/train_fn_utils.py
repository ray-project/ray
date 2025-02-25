from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.train import Checkpoint
from ray.train._internal import session
from ray.train.context import get_context as get_context_v1
from ray.train.v2._internal.execution.context import (
    get_train_context as internal_get_train_context,
)
from ray.train.v2._internal.util import _copy_doc

if TYPE_CHECKING:
    from ray.data import DataIterator


class TrainContext:
    @_copy_doc(session.get_metadata)
    def get_metadata(self) -> Dict[str, Any]:
        return internal_get_train_context().get_metadata()

    @_copy_doc(session.get_trial_name)
    def get_trial_name(self) -> str:
        return internal_get_train_context().get_trial_name()

    @_copy_doc(session.get_trial_id)
    def get_trial_id(self) -> str:
        return internal_get_train_context().get_trial_id()

    @_copy_doc(session.get_trial_resources)
    def get_trial_resources(self):
        return internal_get_train_context().get_trial_resources()

    @_copy_doc(session.get_trial_dir)
    def get_trial_dir(self) -> str:
        return internal_get_train_context().get_trial_dir()

    @_copy_doc(session.get_experiment_name)
    def get_experiment_name(self) -> str:
        return internal_get_train_context().get_experiment_name()

    @_copy_doc(session.get_world_size)
    def get_world_size(self) -> int:
        return internal_get_train_context().get_world_size()

    @_copy_doc(session.get_world_rank)
    def get_world_rank(self) -> int:
        return internal_get_train_context().get_world_rank()

    @_copy_doc(session.get_local_rank)
    def get_local_rank(self) -> int:
        return internal_get_train_context().get_local_rank()

    @_copy_doc(session.get_local_world_size)
    def get_local_world_size(self) -> int:
        return internal_get_train_context().get_local_world_size()

    @_copy_doc(session.get_node_rank)
    def get_node_rank(self) -> int:
        return internal_get_train_context().get_node_rank()

    @_copy_doc(session.get_storage)
    def get_storage(self):
        return internal_get_train_context().get_storage()


@_copy_doc(session.report)
def report(
    metrics: Dict[str, Any],
    checkpoint: Optional[Checkpoint] = None,
    checkpoint_dir_name: Optional[str] = None,
):
    internal_get_train_context().report(
        metrics=metrics, checkpoint=checkpoint, checkpoint_dir_name=checkpoint_dir_name
    )


@_copy_doc(get_context_v1)
def get_context() -> TrainContext:
    return TrainContext()


@_copy_doc(session.get_checkpoint)
def get_checkpoint() -> Optional[Checkpoint]:
    return internal_get_train_context().get_checkpoint()


@_copy_doc(session.get_dataset_shard)
def get_dataset_shard(dataset_name: Optional[str] = None) -> Optional["DataIterator"]:
    return internal_get_train_context().get_dataset_shard(dataset_name)
