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
    _session = get_session()
    return _session.log(**kwargs)


def trial_dir():
    """Returns the directory where trial results are saved.

    This includes json data containing the session's parameters and metrics.
    """
    _session = get_session()
    return _session.logdir


__all__ = ["TrackSession", "session", "log", "trial_dir", "init", "shutdown"]
