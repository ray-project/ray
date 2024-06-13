import logging
import os
import time
from functools import lru_cache
from typing import Dict, Optional, Tuple

import ray
from ray.tune.execution.cluster_info import _is_ray_cluster
from ray.tune.experiment import Trial

logger = logging.getLogger(__name__)


# Ideally we want to use @cache; but it's only available for python 3.9.
# Caching is only helpful/correct for no autoscaler case.
@lru_cache()
def _get_cluster_resources_no_autoscaler() -> Dict:
    return ray.cluster_resources()


def _get_trial_cpu_and_gpu(trial: Trial) -> Tuple[int, int]:
    cpu = trial.placement_group_factory.required_resources.get("CPU", 0)
    gpu = trial.placement_group_factory.required_resources.get("GPU", 0)
    return cpu, gpu


def _can_fulfill_no_autoscaler(trial: Trial) -> bool:
    """Calculates if there is enough resources for a PENDING trial.

    For no autoscaler case.
    """
    assert trial.status == Trial.PENDING
    asked_cpus, asked_gpus = _get_trial_cpu_and_gpu(trial)

    return asked_cpus <= _get_cluster_resources_no_autoscaler().get(
        "CPU", 0
    ) and asked_gpus <= _get_cluster_resources_no_autoscaler().get("GPU", 0)


@lru_cache()
def _get_insufficient_resources_warning_threshold() -> float:
    if _is_ray_cluster():
        return float(
            os.environ.get(
                "TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S_AUTOSCALER", "60"
            )
        )
    else:
        # Set the default to 10s so that we don't prematurely determine that
        # a cluster cannot fulfill the resources requirements.
        # TODO(xwjiang): Change it back once #18608 is resolved.
        return float(os.environ.get("TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S", "60"))


MSG_TRAIN_START = (
    "Training has not started in the last {wait_time:.0f} seconds. "
    "This could be due to the cluster not having enough resources available. "
)
MSG_TRAIN_INSUFFICIENT = (
    "You asked for {asked_cpus} CPUs and {asked_gpus} GPUs, but the cluster only "
    "has {cluster_cpus} CPUs and {cluster_gpus} GPUs available. "
)
MSG_TRAIN_END = (
    "Stop the training and adjust the required resources (e.g. via the "
    "`ScalingConfig` or `resources_per_trial`, or `num_workers` for rllib), "
    "or add more resources to your cluster."
)

MSG_TUNE_START = (
    "No trial is running and no new trial has been started within "
    "the last {wait_time:.0f} seconds. "
    "This could be due to the cluster not having enough resources available. "
)
MSG_TUNE_INSUFFICIENT = (
    "You asked for {asked_cpus} CPUs and {asked_gpus} GPUs per trial, "
    "but the cluster only has {cluster_cpus} CPUs and {cluster_gpus} GPUs available. "
)
MSG_TUNE_END = (
    "Stop the tuning and adjust the required resources (e.g. via the "
    "`ScalingConfig` or `resources_per_trial`, or `num_workers` for rllib), "
    "or add more resources to your cluster."
)


# TODO(xwjiang): Consider having a help page with more detailed instructions.
@lru_cache()
def _get_insufficient_resources_warning_msg(
    for_train: bool = False, trial: Optional[Trial] = None
) -> str:
    msg = "Ignore this message if the cluster is autoscaling. "

    if for_train:
        start = MSG_TRAIN_START
        insufficient = MSG_TRAIN_INSUFFICIENT
        end = MSG_TRAIN_END
    else:
        start = MSG_TUNE_START
        insufficient = MSG_TUNE_INSUFFICIENT
        end = MSG_TUNE_END

    msg += start.format(wait_time=_get_insufficient_resources_warning_threshold())

    if trial:
        asked_cpus, asked_gpus = _get_trial_cpu_and_gpu(trial)
        cluster_resources = _get_cluster_resources_no_autoscaler()

        msg += insufficient.format(
            asked_cpus=asked_cpus,
            asked_gpus=asked_gpus,
            cluster_cpus=cluster_resources.get("CPU", 0),
            cluster_gpus=cluster_resources.get("GPU", 0),
        )

    msg += end

    return msg


class _InsufficientResourcesManager:
    """Insufficient resources manager.

    Makes best effort, conservative guesses about if Tune loop is stuck due to
    infeasible resources. If so, outputs usability messages for users to
    act upon.
    """

    def __init__(self, for_train: bool = False):
        # The information tracked across the life time of Tune loop.
        self._no_running_trials_since = -1
        self._last_trial_num = -1
        self._for_train = for_train

    def on_no_available_trials(self, all_trials):
        """Tracks information across the life of Tune loop and makes guesses
        about if Tune loop is stuck due to infeasible resources.
        If so, outputs certain warning messages.
        The logic should be conservative, non-intrusive and informative.
        For example, rate limiting is applied so that the message is not
        spammy.
        """
        # This is approximately saying we are not making progress.
        if len(all_trials) == self._last_trial_num:
            if self._no_running_trials_since == -1:
                self._no_running_trials_since = time.monotonic()
            elif (
                time.monotonic() - self._no_running_trials_since
                > _get_insufficient_resources_warning_threshold()
            ):
                can_fulfill_any = any(
                    trial.status == Trial.PENDING and _can_fulfill_no_autoscaler(trial)
                    for trial in all_trials
                )

                if can_fulfill_any:
                    # If one trial can be fulfilled, it will be fulfilled eventually
                    self._no_running_trials_since = -1
                    return

                # Otherwise, can fulfill none
                msg = _get_insufficient_resources_warning_msg(
                    for_train=self._for_train, trial=all_trials[0]
                )
                logger.warning(msg)
                self._no_running_trials_since = time.monotonic()
        else:
            self._no_running_trials_since = -1
        self._last_trial_num = len(all_trials)
