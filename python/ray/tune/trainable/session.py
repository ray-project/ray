import inspect
import logging
import os
import traceback
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, Optional, Set

import ray
from ray.air._internal.session import Session
from ray.air.checkpoint import Checkpoint
from ray.tune.error import TuneError
from ray.tune.trainable.function_trainable import _StatusReporter
from ray.tune.trainable.util import TrainableUtil
from ray.util.annotations import PublicAPI, Deprecated
from ray.util.debug import log_once
from ray.util.placement_group import _valid_resource_shape
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)

if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)

_session: Optional[_StatusReporter] = None
# V2 Session API.
_session_v2: Optional["_TuneSessionImpl"] = None

_deprecation_msg = (
    "`tune.report` and `tune.checkpoint_dir` APIs are deprecated in Ray "
    "2.0, and is replaced by `ray.air.session`. This will provide an easy-"
    "to-use API across Tune session and Data parallel worker sessions."
    "The old APIs will be removed in the future. "
)


class _TuneSessionImpl(Session):
    """Session client that function trainable can interact with."""

    def __init__(self, status_reporter: _StatusReporter):
        self._status_reporter = status_reporter

    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
        self._status_reporter.report(metrics, checkpoint=checkpoint)

    @property
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        return self._status_reporter.loaded_checkpoint

    @property
    def experiment_name(self) -> str:
        return self._status_reporter.experiment_name

    @property
    def trial_name(self) -> str:
        return self._status_reporter.trial_name

    @property
    def trial_id(self) -> str:
        return self._status_reporter.trial_id

    @property
    def trial_resources(self) -> "PlacementGroupFactory":
        return self._status_reporter.trial_resources

    @property
    def trial_dir(self) -> str:
        return self._status_reporter.logdir


@Deprecated(message=_deprecation_msg)
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
                "outside `tuner.fit()` or while using the class API. ".format(
                    function_name
                )
            )
            logger.warning(stack_trace_str)
    return _session


def _init(reporter, ignore_reinit_error=True):
    """Initializes the global trial context for this process."""
    global _session
    global _session_v2

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

    # Setup hooks for generating placement group resource deadlock warnings.
    from ray import actor, remote_function

    if "TUNE_DISABLE_RESOURCE_CHECKS" not in os.environ:
        actor._actor_launch_hook = _tune_task_and_actor_launch_hook
        remote_function._task_launch_hook = _tune_task_and_actor_launch_hook

    _session = reporter
    _session_v2 = _TuneSessionImpl(status_reporter=reporter)


# Cache of resource dicts that have been checked by the launch hook already.
_checked_resources: Set[frozenset] = set()


def _tune_task_and_actor_launch_hook(
    fn, resources: Dict[str, float], strategy: Optional[SchedulingStrategyT]
):
    """Launch hook to catch nested tasks that can't fit in the placement group.

    This gives users a nice warning in case they launch a nested task in a Tune trial
    without reserving resources in the trial placement group to fit it.
    """

    # Already checked, skip for performance reasons.
    key = frozenset({(k, v) for k, v in resources.items() if v > 0})
    if not key or key in _checked_resources:
        return

    # No need to check if placement group is None.
    if (
        not isinstance(strategy, PlacementGroupSchedulingStrategy)
        or strategy.placement_group is None
    ):
        return

    # Check if the resource request is targeting the current placement group.
    cur_pg = ray.util.get_current_placement_group()
    if not cur_pg or strategy.placement_group.id != cur_pg.id:
        return

    _checked_resources.add(key)

    # Check if the request can be fulfilled by the current placement group.
    pgf = get_trial_resources()

    if pgf.head_bundle_is_empty:
        available_bundles = cur_pg.bundle_specs[0:]
    else:
        available_bundles = cur_pg.bundle_specs[1:]

    # Check if the request can be fulfilled by the current placement group.
    if _valid_resource_shape(resources, available_bundles):
        return

    if fn.class_name:
        submitted = "actor"
        name = fn.module_name + "." + fn.class_name + "." + fn.function_name
    else:
        submitted = "task"
        name = fn.module_name + "." + fn.function_name

    # Normalize the resource spec so it looks the same as the placement group bundle.
    main_resources = cur_pg.bundle_specs[0]
    resources = {k: float(v) for k, v in resources.items() if v > 0}

    raise TuneError(
        f"No trial resources are available for launching the {submitted} `{name}`. "
        "To resolve this, specify the Tune option:\n\n"
        ">  resources_per_trial=tune.PlacementGroupFactory(\n"
        f">    [{main_resources}] + [{resources}] * N\n"
        ">  )\n\n"
        f"Where `N` is the number of slots to reserve for trial {submitted}s. "
        "If you are using a Ray training library, there might be a utility function "
        "to set this automatically for you. For more information, refer to "
        "https://docs.ray.io/en/latest/tune/tutorials/tune-resources.html"
    )


def _shutdown():
    """Cleans up the trial and removes it from the global context."""

    global _session
    _session = None


@Deprecated(message=_deprecation_msg)
def report(_metric=None, **kwargs):
    """Logs all keyword arguments.

    .. code-block:: python

        import time
        from ray import tune

        def run_me(config):
            for iter in range(100):
                time.sleep(1)
                tune.report(hello="world", ray="tune")

        tuner = Tuner(run_me)
        results = tuner.fit()

    Args:
        _metric: Optional default anonymous metric for ``tune.report(value)``
        **kwargs: Any key value pair to be logged by Tune. Any of these
            metrics can be used for early stopping or optimization.
    """
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )
    _session = get_session()
    if _session:
        if _session._air_session_has_reported:
            raise ValueError(
                "It is not allowed to mix `tune.report` with `session.report`."
            )

        return _session(_metric, **kwargs)


@Deprecated(message=_deprecation_msg)
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
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )

    _session = get_session()

    if step is None:
        raise ValueError("checkpoint_dir(step) must be provided - got None.")

    if _session:
        if _session._air_session_has_reported:
            raise ValueError(
                "It is not allowed to mix `with tune.checkpoint_dir` "
                "with `session.report`."
            )
        _checkpoint_dir = _session.make_checkpoint_dir(step=step)
    else:
        _checkpoint_dir = os.path.abspath("./")

    yield _checkpoint_dir

    # Drop marker again in case it was deleted.
    TrainableUtil.mark_as_checkpoint_dir(_checkpoint_dir)

    if _session:
        _session.set_checkpoint(_checkpoint_dir)


@Deprecated(message=_deprecation_msg)
def get_trial_dir():
    """Returns the directory where trial results are saved.

    For function API use only.
    """
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )
    _session = get_session()
    if _session:
        return _session.logdir


@Deprecated(message=_deprecation_msg)
def get_trial_name():
    """Trial name for the corresponding trial.

    For function API use only.
    """
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )
    _session = get_session()
    if _session:
        return _session.trial_name


@Deprecated(message=_deprecation_msg)
def get_trial_id():
    """Trial id for the corresponding trial.

    For function API use only.
    """
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )
    _session = get_session()
    if _session:
        return _session.trial_id


@Deprecated(message=_deprecation_msg)
def get_trial_resources():
    """Trial resources for the corresponding trial.

    Will be a PlacementGroupFactory if trial uses those,
    otherwise a Resources instance.

    For function API use only.
    """
    warnings.warn(
        _deprecation_msg,
        DeprecationWarning,
    )
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
