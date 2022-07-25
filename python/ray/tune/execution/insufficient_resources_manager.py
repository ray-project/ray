import logging
from functools import lru_cache
import os
import ray
import time
from typing import Dict

from ray.tune.execution.cluster_info import is_ray_cluster
from ray.tune.experiment import Trial

logger = logging.getLogger(__name__)


# Ideally we want to use @cache; but it's only available for python 3.9.
# Caching is only helpful/correct for no autoscaler case.
@lru_cache()
def _get_cluster_resources_no_autoscaler() -> Dict:
    return ray.cluster_resources()


def _get_trial_cpu_and_gpu(trial: Trial) -> Dict:
    cpu = trial.placement_group_factory.required_resources.get("CPU", 0)
    gpu = trial.placement_group_factory.required_resources.get("GPU", 0)
    return {"CPU": cpu, "GPU": gpu}


def _can_fulfill_no_autoscaler(trial: Trial) -> bool:
    """Calculates if there is enough resources for a PENDING trial.

    For no autoscaler case.
    """
    assert trial.status == Trial.PENDING
    trial_cpu_gpu = _get_trial_cpu_and_gpu(trial)

    return trial_cpu_gpu["CPU"] <= _get_cluster_resources_no_autoscaler().get(
        "CPU", 0
    ) and trial_cpu_gpu["GPU"] <= _get_cluster_resources_no_autoscaler().get("GPU", 0)


@lru_cache()
def _get_insufficient_resources_warning_threshold() -> float:
    if is_ray_cluster():
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


# TODO(xwjiang): Consider having a help page with more detailed instructions.
@lru_cache()
def _get_insufficient_resources_warning_msg() -> str:
    msg = (
        f"No trial is running and no new trial has been started within"
        f" at least the last "
        f"{_get_insufficient_resources_warning_threshold()} seconds. "
        f"This could be due to the cluster not having enough "
        f"resources available to start the next trial. "
        f"Stop the tuning job and adjust the resources requested per trial "
        f"(possibly via `resources_per_trial` or via `num_workers` for rllib) "
        f"and/or add more resources to your Ray runtime."
    )
    if is_ray_cluster():
        return "Ignore this message if the cluster is autoscaling. " + msg
    else:
        return msg


# A beefed up version when Tune Error is raised.
def _get_insufficient_resources_error_msg(trial: Trial) -> str:
    trial_cpu_gpu = _get_trial_cpu_and_gpu(trial)
    return (
        f"Ignore this message if the cluster is autoscaling. "
        f"You asked for {trial_cpu_gpu['CPU']} cpu and "
        f"{trial_cpu_gpu['GPU']} gpu per trial, but the cluster only has "
        f"{_get_cluster_resources_no_autoscaler().get('CPU', 0)} cpu and "
        f"{_get_cluster_resources_no_autoscaler().get('GPU', 0)} gpu. "
        f"Stop the tuning job and adjust the resources requested per trial "
        f"(possibly via `resources_per_trial` or via `num_workers` for rllib) "
        f"and/or add more resources to your Ray runtime."
    )


class _InsufficientResourcesManager:
    """Insufficient resources manager.

    Makes best effort, conservative guesses about if Tune loop is stuck due to
    infeasible resources. If so, outputs usability messages for users to
    act upon.
    """

    def __init__(self):
        # The information tracked across the life time of Tune loop.
        self._no_running_trials_since = -1
        self._last_trial_num = -1

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
                if not is_ray_cluster():  # autoscaler not enabled
                    # If any of the pending trial cannot be fulfilled,
                    # that's a good enough hint of trial resources not enough.
                    for trial in all_trials:
                        if (
                            trial.status is Trial.PENDING
                            and not _can_fulfill_no_autoscaler(trial)
                        ):
                            # TODO(xwjiang):
                            #  Raise an Error once #18608 is resolved.
                            logger.warning(_get_insufficient_resources_error_msg(trial))
                            break
                else:
                    # TODO(xwjiang): #17799.
                    #  Output a more helpful msg for autoscaler.
                    logger.warning(_get_insufficient_resources_warning_msg())
                self._no_running_trials_since = time.monotonic()
        else:
            self._no_running_trials_since = -1
        self._last_trial_num = len(all_trials)
