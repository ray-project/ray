"""``nccl_hang_check()``, the in-flight check that runs the NCCL hang detector."""
import os
from typing import TYPE_CHECKING, Callable, Optional

from ray._private.ray_constants import env_float
from ray.train.health.check import PeriodicHealthCheck
from ray.train.health.nccl_ras.evaluators import NCCLHangEvaluator
from ray.train.health.nccl_ras.probes import NCCLRASProbe
from ray.train.v2._internal.constants import (
    DEFAULT_NCCL_RAS_ACTION,
    DEFAULT_NCCL_RAS_CONFIRM_DURATION_S,
    DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
)

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.worker_group import WorkerGroup


def nccl_hang_check(
    train_run_context: "TrainRunContext",
    get_worker_group: Callable[[], Optional["WorkerGroup"]],
) -> PeriodicHealthCheck:
    """The NCCL hang detector's in-flight check, configured by
    ``RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S``,
    ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S`` and ``RAY_TRAIN_NCCL_RAS_ACTION``.

    Args:
        train_run_context: The run, which tags the Grafana metrics and stores
            the diagnostics.
        get_worker_group: Returns the attempt's worker group, whose workers
            name the ranks.

    Returns:
        The check.

    Raises:
        ValueError: If one of the env vars is invalid.
    """
    poll_interval_s = env_float(
        NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S
    )
    confirm_duration_s = env_float(
        NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, DEFAULT_NCCL_RAS_CONFIRM_DURATION_S
    )
    for env_var, value_s in [
        (NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, poll_interval_s),
        (NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, confirm_duration_s),
    ]:
        if value_s <= 0:
            raise ValueError(
                f"{env_var} must be a positive number of seconds, got {value_s}."
            )
    action = os.environ.get(NCCL_RAS_ACTION_ENV_VAR, DEFAULT_NCCL_RAS_ACTION).lower()
    if action not in (NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE):
        raise ValueError(
            f"{NCCL_RAS_ACTION_ENV_VAR} must be one of {NCCL_RAS_ACTION_FAIL!r} or "
            f"{NCCL_RAS_ACTION_OBSERVE!r}, got {action!r}."
        )
    return PeriodicHealthCheck(
        probe_creator=lambda: [NCCLRASProbe()],
        evaluator_creator=lambda: [
            NCCLHangEvaluator(
                poll_interval_s,
                confirm_duration_s,
                action,
                train_run_context,
                get_worker_group,
            )
        ],
        poll_interval_s=poll_interval_s,
    )
