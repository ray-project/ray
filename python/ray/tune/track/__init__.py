import pickle

from .trial import Trial


_trial = None


def init(log_dir=None,
         upload_dir=None,
         sync_period=None,
         trial_prefix="",
         param_map=None,
         init_logging=True):
    """
    Initializes the global trial context for this process.
    This creates a Trial object and the corresponding hooks for logging.
    """
    global _trial  # pylint: disable=global-statement
    if _trial:
        # TODO: would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        raise ValueError("A trial already exists in the current context")
    local_trial = Trial(
        log_dir=log_dir,
        upload_dir=upload_dir,
        sync_period=sync_period,
        trial_prefix=trial_prefix,
        param_map=param_map,
        init_logging=True)
    # try:
    _trial = local_trial
    _trial.start()


def shutdown():
    """
    Cleans up the trial and removes it from the global context.
    """
    global _trial  # pylint: disable=global-statement
    if not _trial:
        raise ValueError("Tried to stop trial, but no trial exists")
    _trial.close()
    _trial = None


def save(obj, obj_name, iteration=None, save_fn=pickle.dump, **kwargs):
    """ Applies Trial.save to the trial in the current context """
    return _trial.save(obj=obj, obj_name=obj_name, iteration=iteration,
                       save_fn=save_fn, **kwargs)


def metric(*, iteration=None, **kwargs):
    """Applies Trial.metric to the trial in the current context."""
    return _trial.metric(iteration=iteration, **kwargs)


def load(obj_name, iteration=None, load_fn=pickle.load, **kwargs):
    """Applies Trial.load to the trial in the current context."""
    return _trial.load(obj_name=obj_name, iteration=iteration,
                       load_fn=load_fn, **kwargs)


def trial_dir():
    """Retrieves the trial directory for the trial in the current context."""
    return _trial.trial_dir()


__all__ = ["Trial", "trial", "metric",
           "save", "load", "trial_dir"]
