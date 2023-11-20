# TODO(justinvyu): [code_removal] Delete this file after all dependencies are gone.
import logging
from contextlib import contextmanager
from typing import Dict, Optional, Set

import ray
from ray.tune.error import TuneError
from ray.util.annotations import Deprecated
from ray.util.placement_group import _valid_resource_shape
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)


logger = logging.getLogger(__name__)


_TUNE_REPORT_DEPRECATION_MSG = """`tune.report` is deprecated.
Use `ray.train.report` instead -- see the example below:

from ray import tune     ->     from ray import train
tune.report(metric=1)    ->     train.report({'metric': 1})"""


_TUNE_CHECKPOINT_DIR_DEPRECATION_MSG = """`tune.checkpoint_dir` is deprecated.
Use `ray.train.report` instead -- see the example below:

Before
------

with tune.checkpoint_dir(step=1) as checkpoint_dir:
    torch.save(state_dict, os.path.join(checkpoint_dir, 'model.pt'))
tune.report(metric=1)

After
-----

from ray.train import Checkpoint

with tempfile.TemporaryDirectory as temp_checkpoint_dir:
    torch.save(state_dict, os.path.join(temp_checkpoint_dir, 'model.pt'))
    ray.train.report({'metric': 1}, checkpoint=Checkpoint.from_directory(temp_checkpoint_dir))"""  # noqa: E501


@Deprecated(message=_TUNE_REPORT_DEPRECATION_MSG)
def get_session():
    raise DeprecationWarning


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


@Deprecated
def report(_metric=None, **kwargs):
    raise DeprecationWarning(_TUNE_REPORT_DEPRECATION_MSG)


@Deprecated
@contextmanager
def checkpoint_dir(step: int):
    raise DeprecationWarning(_TUNE_CHECKPOINT_DIR_DEPRECATION_MSG)


@Deprecated
def get_trial_dir():
    raise DeprecationWarning(
        "`tune.get_trial_dir()` is deprecated. "
        "Use `ray.train.get_context().get_trial_dir()` instead."
    )


@Deprecated
def get_trial_name():
    raise DeprecationWarning(
        "`tune.get_trial_name()` is deprecated. "
        "Use `ray.train.get_context().get_trial_name()` instead."
    )


@Deprecated
def get_trial_id():
    raise DeprecationWarning(
        "`tune.get_trial_id()` is deprecated. "
        "Use `ray.train.get_context().get_trial_id()` instead."
    )


@Deprecated
def get_trial_resources():
    raise DeprecationWarning(
        "`tune.get_trial_resources()` is deprecated. "
        "Use `ray.train.get_context().get_trial_resources()` instead."
    )


# TODO(justinvyu): [code_removal] Remove after xgboost_ray dependency is updated.
@Deprecated
def is_session_enabled() -> bool:
    return False


__all__ = [
    "report",
    "get_trial_dir",
    "get_trial_name",
    "get_trial_id",
    "get_trial_resources",
]
