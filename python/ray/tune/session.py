from contextlib import contextmanager
import os
import logging

logger = logging.getLogger(__name__)

_session = None


def get_session():
    global _session
    if not _session:
        logger.warning(
            "Session not detected. You should not be calling this function "
            "outside `tune.run` or while using the class API. ")
    return _session


def init(reporter, ignore_reinit_error=True):
    """Initializes the global trial context for this process."""
    global _session

    if _session is not None:
        # TODO(ng): would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        reinit_msg = (
            "A Tune session already exists in the current process. "
            "If you are using ray.init(local_mode=True), "
            "you must set ray.init(..., num_cpus=1, num_gpus=1) to limit "
            "available concurrency.")
        if ignore_reinit_error:
            logger.warning(reinit_msg)
            return
        else:
            raise ValueError(reinit_msg)

    if reporter is None:
        logger.warning("You are using a Tune session outside of Tune. "
                       "Most session commands will have no effect.")

    _session = reporter


def shutdown():
    """Cleans up the trial and removes it from the global context."""

    global _session
    _session = None


def report(**kwargs):
    """Logs all keyword arguments.

    .. code-block:: python

        import time
        from ray import tune

        def run_me(config):
            for iter in range(100):
                time.sleep(1)
                tune.report(hello="world", ray="tune")

        analysis = tune.run(run_me)

    Args:
        **kwargs: Any key value pair to be logged by Tune. Any of these
            metrics can be used for early stopping or optimization.
    """
    _session = get_session()
    if _session:
        return _session(**kwargs)


def make_checkpoint_dir(step=None):
    """Gets the next checkpoint dir.

    .. versionadded:: 0.8.6

    .. deprecated:: 0.8.7
        Use tune.checkpoint_dir instead.
    """
    raise DeprecationWarning(
        "Deprecated method. Use `tune.checkpoint_dir` instead.")


def save_checkpoint(checkpoint):
    """Register the given checkpoint.

    .. versionadded:: 0.8.6

    .. deprecated:: 0.8.7
        Use tune.checkpoint_dir instead.
    """
    raise DeprecationWarning(
        "Deprecated method. Use `tune.checkpoint_dir` instead.")


@contextmanager
def checkpoint_dir(step=None):
    """Returns a checkpoint dir inside a context.

    Store any files related to restoring state within the
    provided checkpoint dir.

    .. code-block:: python

        import os
        import json
        import time
        from ray import tune

        def func(config, checkpoint_dir=None):
            start = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
                    state = json.loads(f.read())
                    accuracy = state["acc"]
                    start = state["step"] + 1

            for iter in range(start, 10):
                time.sleep(1)

                with tune.checkpoint_dir(step=iter) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir, "checkpoint")
                    with open(path, "w") as f:
                        f.write(json.dumps({"step": start}))

                tune.report(hello="world", ray="tune")

    Yields:
        checkpoint_dir (str): Directory for checkpointing.

    .. versionadded:: 0.8.7
    """
    _session = get_session()

    if _session:
        _checkpoint_dir = _session.make_checkpoint_dir(step=step)
    else:
        _checkpoint_dir = os.path.abspath("./")

    yield _checkpoint_dir

    if _session:
        _session.save_checkpoint(_checkpoint_dir)


def get_trial_dir():
    """Returns the directory where trial results are saved.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.logdir


def get_trial_name():
    """Trial name for the corresponding trial.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.trial_name


def get_trial_id():
    """Trial id for the corresponding trial.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.trial_id


__all__ = ["report", "get_trial_dir", "get_trial_name", "get_trial_id"]
