from typing import Dict, Optional

from ray.train._checkpoint import Checkpoint as TrainCheckpoint
from ray.train._internal.session import _warn_session_misuse, get_session
from ray.train.context import _copy_doc
from ray.util.annotations import PublicAPI


@_copy_doc(TrainCheckpoint)
class Checkpoint(TrainCheckpoint):
    # NOTE: This is just a pass-through wrapper around `ray.train.Checkpoint`
    # in order to detect whether the import module was correct `ray.tune.Checkpoint`.
    pass


@PublicAPI(stability="stable")
@_warn_session_misuse()
def report(metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
    """Report metrics and optionally save and register a checkpoint to Ray Tune.

    If a checkpoint is provided, it will be
    :ref:`persisted to storage <persistent-storage-guide>`.

    .. note::

        Each invocation of this method will automatically increment the underlying
        ``training_iteration`` number. The physical meaning of this "iteration" is
        defined by user depending on how often they call ``report``.
        It does not necessarily map to one epoch.

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
    """
    get_session().report(metrics, checkpoint=checkpoint)


@PublicAPI(stability="stable")
@_warn_session_misuse()
def get_checkpoint() -> Optional[Checkpoint]:
    """Access the latest reported checkpoint to resume from if one exists."""

    return get_session().loaded_checkpoint
