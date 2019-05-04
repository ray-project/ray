from ray.tune.track.session import TrackSession

__name__ = 'track'

_session = None


def init(log_dir=None,
         upload_dir=None,
         sync_period=None,
         trial_prefix="",
         param_map=None):
    """
    Initializes the global trial context for this process.
    This creates a TrackSession object and the corresponding hooks for logging.
    """
    global _session  # pylint: disable=global-statement
    if _session:
        # TODO: would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        raise ValueError("A session already exists in the current context")
    local_session = TrackSession(
        log_dir=log_dir,
        upload_dir=upload_dir,
        sync_period=sync_period,
        trial_prefix=trial_prefix,
        param_map=param_map)
    # try:
    _session = local_session
    _session.start()


def shutdown():
    """Cleans up the trial and removes it from the global context.
    """
    global _session  # pylint: disable=global-statement
    if not _session:
        raise ValueError("Tried to stop session, but no session exists")
    _session.close()
    _session = None


def metric(iteration=None, **kwargs):
    """Applies TrackSession.metric to the trial in the current context."""
    return _session.metric(iteration=iteration, **kwargs)


def trial_dir():
    """Returns the directory where trial results are saved, including

    json data containing the session's parameters an stored metrics.
    """
    return _session.trial_dir()


__all__ = ["TrackSession", "session", "metric", "trial_dir",
           "init", "shutdown"]
