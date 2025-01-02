from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.train import Checkpoint
from ray.train._internal import session
from ray.train.context import get_context as get_context_v1
from ray.train.v2._internal.execution.context import TrainContext, get_train_context
from ray.train.v2._internal.util import _copy_doc

if TYPE_CHECKING:
    from ray.data import DataIterator


@_copy_doc(session.report)
def report(
    metrics: Dict[str, Any],
    checkpoint: Optional[Checkpoint] = None,
    checkpoint_dir_name: Optional[str] = None,
):
    get_train_context().report(
        metrics=metrics, checkpoint=checkpoint, checkpoint_dir_name=checkpoint_dir_name
    )


@_copy_doc(get_context_v1)
def get_context() -> TrainContext:
    return get_train_context()


@_copy_doc(session.get_checkpoint)
def get_checkpoint() -> Optional[Checkpoint]:
    return get_train_context().get_checkpoint()


@_copy_doc(session.get_dataset_shard)
def get_dataset_shard(dataset_name: Optional[str] = None) -> Optional["DataIterator"]:
    return get_train_context().get_dataset_shard(dataset_name)
