from contextlib import contextmanager
import inspect
import os
import logging
import traceback

from ray.util.debug import log_once
from ray.util.annotations import PublicAPI, DeveloperAPI

logger = logging.getLogger(__name__)

_session = None


@PublicAPI
def is_session_enabled() -> bool:
    """Returns True if running within an Tune process."""
    global _session
    return _session is not None


@PublicAPI
def get_session():
    global _session
    if not _session:
        function_name = inspect.stack()[1].function
        # Log traceback so the user knows where the offending func is called.
        # E.g. ... -> tune.report() -> get_session() -> logger.warning(...)
        # So we shouldn't print the last 2 functions in the trace.
        stack_trace_str = "".join(traceback.extract_stack().format()[:-2])
        if log_once(stack_trace_str):
            logger.warning(
                "Session not detected. You should not be calling `{}` "
                "outside `tune.run` or while using the class API. ".format(
                    function_name
                )
            )
            logger.warning(stack_trace_str)
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
            "available concurrency. If you are supplying a wrapped "
            "Searcher(concurrency, repeating) or customized SearchAlgo. "
            "Please try limiting the concurrency to 1 there."
        )
        if ignore_reinit_error:
            logger.warning(reinit_msg)
            return
        else:
            raise ValueError(reinit_msg)

    if reporter is None:
        logger.warning(
            "You are using a Tune session outside of Tune. "
            "Most session commands will have no effect."
        )

    _session = reporter


def shutdown():
    """Cleans up the trial and removes it from the global context."""

    global _session
    _session = None


@PublicAPI
def report(_metric=None, **kwargs):
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
        _metric: Optional default anonymous metric for ``tune.report(value)``
        **kwargs: Any key value pair to be logged by Tune. Any of these
            metrics can be used for early stopping or optimization.
    """
    _session = get_session()
    if _session:
        return _session(_metric, **kwargs)


@PublicAPI
@contextmanager
def checkpoint_dir(step: int):
    """Returns a checkpoint dir inside a context.

    Store any files related to restoring state within the
    provided checkpoint dir.

    You should call this *before* calling ``tune.report``. The reason is
    because we want checkpoints to be correlated with the result
    (i.e., be able to retrieve the best checkpoint, etc). Many algorithms
    depend on this behavior too.

    Calling ``checkpoint_dir`` after report could introduce
    inconsistencies.

    Args:
        step: Index for the checkpoint. Expected to be a
            monotonically increasing quantity.

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
        checkpoint_dir: Directory for checkpointing.

    .. versionadded:: 0.8.7
    """
    _session = get_session()

    if step is None:
        raise ValueError("checkpoint_dir(step) must be provided - got None.")

    if _session:
        _checkpoint_dir = _session.make_checkpoint_dir(step=step)
    else:
        _checkpoint_dir = os.path.abspath("./")

    yield _checkpoint_dir

    if _session:
        _session.set_checkpoint(_checkpoint_dir)


@DeveloperAPI
def get_trial_dir():
    """Returns the directory where trial results are saved.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.logdir


@DeveloperAPI
def get_trial_name():
    """Trial name for the corresponding trial.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.trial_name


@DeveloperAPI
def get_trial_id():
    """Trial id for the corresponding trial.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.trial_id


@DeveloperAPI
def get_trial_resources():
    """Trial resources for the corresponding trial.

    Will be a PlacementGroupFactory if trial uses those,
    otherwise a Resources instance.

    For function API use only.
    """
    _session = get_session()
    if _session:
        return _session.trial_resources


__all__ = [
    "report",
    "get_trial_dir",
    "get_trial_name",
    "get_trial_id",
    "get_trial_resources",
]
