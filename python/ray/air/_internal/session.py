import abc
import logging
from typing import TYPE_CHECKING, Dict, Optional

from ray.air.checkpoint import Checkpoint

if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


class Session(abc.ABC):
    """The canonical session interface that both Tune and Train session implements.

    User can interact with this interface to get session information,
    as well as reporting metrics and saving checkpoint.
    """

    @abc.abstractmethod
    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
        """Report metrics and optionally save checkpoint.

        Each invocation of this method will automatically increment the underlying
        iteration number. The physical meaning of this "iteration" is defined by
        user (or more specifically the way they call ``report``).
        It does not necessarily map to one epoch.

        This API is supposed to replace the legacy ``tune.report``,
        ``with tune.checkpoint_dir``, ``train.report`` and ``train.save_checkpoint``.
        Please avoid mixing them together.

        There is no requirement on what is the underlying representation of the
        checkpoint.

        All forms are accepted and (will be) handled by AIR in an efficient way.

        Specifically, if you are passing in a directory checkpoint, AIR will move
        the content of the directory to AIR managed directory. By the return of this
        method, one may safely write new content to the original directory without
        interfering with AIR checkpointing flow.

        Args:
            metrics: The metrics you want to report.
            checkpoint: The optional checkpoint you want to report.
        """

        raise NotImplementedError

    @property
    @abc.abstractmethod
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        """Access the session's loaded checkpoint to resume from if applicable.

        Returns:
            Checkpoint object if the session is currently being resumed.
            Otherwise, return None.
        """

        raise NotImplementedError

    @property
    def experiment_name(self) -> str:
        """Experiment name for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_name(self) -> str:
        """Trial name for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_id(self) -> str:
        """Trial id for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_resources(self) -> "PlacementGroupFactory":
        """Trial resources for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_dir(self) -> str:
        """Trial-level log directory for the corresponding trial."""
        raise NotImplementedError


def _get_session(warn: bool = True) -> Optional[Session]:
    from ray.train._internal.session import _session_v2 as train_session
    from ray.tune.trainable.session import _session_v2 as tune_session

    if train_session and tune_session:
        if warn:
            logger.warning(
                "Expected to be either in tune session or train session but not both."
            )
        return None
    if not (train_session or tune_session):
        if warn:
            logger.warning("In neither tune session nor train session!")
        return None
    return train_session or tune_session
