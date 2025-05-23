from typing import Dict, Optional

from ray.train._checkpoint import Checkpoint as TrainCheckpoint
from ray.train._internal.session import _warn_session_misuse, get_session
from ray.train.constants import (
    V2_MIGRATION_GUIDE_MESSAGE,
    _v2_migration_warnings_enabled,
)
from ray.train.utils import _copy_doc, _log_deprecation_warning
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
    if checkpoint and not isinstance(checkpoint, Checkpoint):
        if _v2_migration_warnings_enabled():
            _log_deprecation_warning(
                "The `Checkpoint` class should be imported from `ray.tune` "
                "when passing it to `ray.tune.report` in a Tune function. "
                "Please update your imports. "
                f"{V2_MIGRATION_GUIDE_MESSAGE}"
            )

    get_session().report(metrics, checkpoint=checkpoint)


@PublicAPI(stability="stable")
@_warn_session_misuse()
def get_checkpoint() -> Optional[Checkpoint]:
    """Access the latest reported checkpoint to resume from if one exists."""

    return get_session().loaded_checkpoint


def _in_tune_session() -> bool:
    return get_session() and get_session().world_rank is None
