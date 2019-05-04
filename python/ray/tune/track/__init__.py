from ray.tune.track.session import TrackSession

_session = None


def init(log_dir=None,
         upload_dir=None,
         sync_period=None,
         trial_prefix="",
         param_map=None,
         init_logging=True):
    """
    Initializes the global trial context for this process.
    This creates a TrackSession object and the corresponding hooks for logging.
    """
    global _session  # pylint: disable=global-statement
    if _session:
        # TODO: would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        raise ValueError("A trial already exists in the current context")
    local_session = TrackSession(
        log_dir=log_dir,
        upload_dir=upload_dir,
        sync_period=sync_period,
        trial_prefix=trial_prefix,
        param_map=param_map,
        init_logging=True)
    # try:
    _session = local_session
    _session.start()


def shutdown():
    """
    Cleans up the trial and removes it from the global context.
    """
    global _session  # pylint: disable=global-statement
    if not _session:
        raise ValueError("Tried to stop trial, but no trial exists")
    _session.close()
    _session = None


def metric(iteration=None, **kwargs):
    """Applies TrackSession.metric to the trial in the current context."""
    return _session.metric(iteration=iteration, **kwargs)


__all__ = ["TrackSession", "trial", "metric", "trial_dir"]
