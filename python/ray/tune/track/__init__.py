import logging
import os

from ray.tune.result import FUNCTION_SAVE as SAVE
from ray.tune.track.session import TrackSession, TuneSession

logger = logging.getLogger(__name__)

_session = None


def get_session():
    global _session
    if not _session:
        raise ValueError("Session not detected. Try `track.init()`?")
    return _session


def init(ignore_reinit_error=True, **session_kwargs):
    """Initializes the global trial context for this process.

    This creates a TrackSession object and the corresponding hooks for logging.

    Examples:
        >>> from ray.tune import track
        >>> track.init()
    """
    global _session

    if _session:
        # TODO(ng): would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        reinit_msg = "A session already exists in the current context."
        if ignore_reinit_error:
            if not isinstance(_session, TuneSession):
                logger.warning(reinit_msg)
            return
        else:
            raise ValueError(reinit_msg)

    if "_tune_reporter" in session_kwargs:
        _session = TuneSession(**session_kwargs)
    else:
        _session = TrackSession(**session_kwargs)


def shutdown():
    """Cleans up the trial and removes it from the global context."""
    global _session
    if _session:
        _session.close()
    _session = None


def log(**kwargs):
    """Applies TrackSession.log to the trial in the current context.

    Checkpoints can be saved using the `tune.track.SAVE` parameter.

    Examples:
        >>> track.log({"mean_accuracy": acc, "mean_loss": loss})

        >>> track.log(mean_accuracy=acc, mean_loss=loss)

        >>> checkpoint = {"state": state}
        >>> track.log(mean_accuracy=acc, **{tune.track.SAVE: checkpoint})
    """
    return get_session().log(**kwargs)


def get_checkpoint_dir():
    """Returns the checkpoint directory.

    This is the directory in which the checkpoint corresponding to the next
    reported result, if any, should be placed.

    Examples:
        >>> checkpoint_path = track.get_checkpoint_dir() + "/model.pkl")
        >>> pickle.dump(model, open(checkpoint_path, "wb"))
        >>> track.log({"mean_accuracy": acc, tune.track.SAVE: checkpoint_path})
    """
    checkpoint_dir = get_session().get_next_iter_checkpoint_dir()
    os.makedirs(checkpoint_dir, exist_ok=True)
    return checkpoint_dir


def is_pending_restore():
    """Returns whether the trial can be restored from a checkpoint."""
    return get_session().is_pending_restore


def restore():
    """Returns a checkpoint to restore from, if restorable.

    Examples:
        >>> if track.is_pending_restore():
        >>>   checkpoint_path = track.restore()
        >>>   model = pickle.load(open(checkpoint_path, "rb"))
    """
    chkpt = get_session().restore()
    return chkpt


def trial_dir():
    """Returns the directory where trial results are saved.

    This includes json data containing the session's parameters and metrics.
    """
    return get_session().logdir


__all__ = [
    "TrackSession", "init", "is_pending_restore", "log", "restore", "SAVE",
    "session", "shutdown", "trial_dir"
]
