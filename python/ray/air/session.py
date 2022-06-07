import abc
import logging
from typing import Optional, Dict

from ray.air.checkpoint import Checkpoint

logger = logging.getLogger(__name__)


class Session(abc.ABC):
    """The canonical session interface that both Tune and Train session implements.

    User can interact with this interface to get session information,
    as well as reporting metrics and saving checkpoint.
    """

    @abc.abstractmethod
    def report(self, metrics: Dict, checkpoint: Optional[Checkpoint] = None) -> None:
        """Report metrics and optionally save checkpoint.

        The checkpoint is guaranteed to be saved onto Driver node.
        Each invocation of this method will automatically increment the underlying
        epoch number.
        This API is supposed to replace the legacy ``tune.report``,
        ``with tune.checkpoint_dir``, ``train.report`` and ``train.save_checkpoint``.
        Please avoid mixing them together.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def remote_checkpoint_dir(self) -> str:
        """Get remote checkpoint dir to save to cloud.

        This checkpoint dir is tracked by Ray Air so that the same
        checkpoint dir is used for resuming training or tuning."""

        raise NotImplementedError


def get_session() -> Optional[Session]:
    from ray.tune.session import _session as tune_session
    from ray.train.session import _session as train_session

    if train_session and tune_session:
        logger.warning(
            "Expected to be either in tune session or train session but not both."
        )
        return None
    if not (train_session or tune_session):
        logger.warning("In neither tune session nor train session!")
        return None
    return train_session or tune_session
