from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.tune.track.session import TrackSession

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
            if not _session.is_tune_session:
                logger.warning(reinit_msg)
            return
        else:
            raise ValueError(reinit_msg)

    _session = TrackSession(**session_kwargs)


def shutdown():
    """Cleans up the trial and removes it from the global context."""
    global _session
    if _session:
        _session.close()
    _session = None


def log(**kwargs):
    """Applies TrackSession.log to the trial in the current context."""
    return get_session().log(**kwargs)


def save(checkpoint):
    """Saves a checkpoint.

    Args:
        checkpoint (str|dict): checkpoint to save.
    """
    get_session().save(checkpoint)


def restore():
    """Returns a checkpoint to restore from, if restorable."""
    return get_session().restore()


def is_restorable():
    """Returns whether the trial is restorable."""
    return get_session().is_restorable


def current_iter_checkpoint_dir():
    """Current iteration's directory in which to save checkpoint."""
    return get_session().current_iter_checkpoint_dir


def trial_dir():
    """Returns the directory where trial results are saved.

    This includes json data containing the session's parameters and metrics.
    """
    return get_session().logdir


__all__ = ["TrackSession", "session", "init", "shutdown", "log", "save",
           "restore", "is_restorable", "current_iter_checkpoint_dir",
           "trial_dir"]
