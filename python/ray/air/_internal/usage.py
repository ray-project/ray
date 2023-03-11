from typing import (
    Callable,
    Optional,
    Sequence as Typing_Seq,
    Type,
    TYPE_CHECKING,
    Union,
)

from collections.abc import Sequence
from dataclasses import dataclass
import os
import json

import ray
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.tune import Trainable
    from ray.tune.callback import Callback
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import SearchAlgorithm, Searcher
    from ray.tune.search import ConcurrencyLimiter
    from ray.train.trainer import BaseTrainer

    TrainableType = Union[str, Callable, Type[Trainable]]
    TrainableTypeOrTrainer = Union[TrainableType, BaseTrainer]


# All possible entrypoint combinations...
# * Trainer.fit() --> Tuner.fit()
# * Trainer.fit() --> Tuner.fit() --> remote --> tuner_internal.fit()
# * Tuner.fit()
# * Tuner.fit() --> remote --> tuner_internal.fit()
# * Tuner.restore() --> Tuner.fit() --> tuner_internal.fit()
# * Tuner.restore() --> remote --> tuner_internal.fit()
# * tune.run()
# * tune.run() --> remote
# * run_experiment()
# * run_experiment() --> remote


# Entrypoint types
TRAINER_FIT = "Trainer.fit"
TUNER_RESTORE = "Tuner.restore"
TUNER_FIT = "Tuner.fit"
TUNE_RUN = "tune.run"
TUNE_RUN_EXP = "tune.run_experiment"


# A class that holds information about AIR workload scenario (tune v.s. train)
# and entrypoint. At various places in the air/train/tune code base, we may access
# this data structure as a global variable.
# Ray client mode makes things a bit tricky - caller should understand that this
# data structure cannot be accessed across different processes and certainly not
# between client and head node. See below for env vars to bridge client and head.
# Mainly for telemetry purposes but may also be used for formatting air console output.
@dataclass
class AIRScenario:
    # One of TRAINER_FIT, TUNER_FIT, TUNE_RUN, and TUNE_RUN_EXP.
    entrypoint = None

    ray_client_mode = None

    trainable = None
    more_than_one_trial = None

    # only meaningful with "more than one trial" case.
    search_alg = None
    scheduler_alg = None


# The following three are fields specifically set by ray client and accessed at the
# head node to set the corresponding field in `AIRScenario`.
AIR_RAY_CLIENT_MODE_ENV_VAR = "AIR_RAY_CLIENT_MODE"
AIR_TRAINABLE_ENV_VAR = "AIR_TRAINABLE"
AIR_ENTRYPOINT_ENV_VAR = "AIR_ENTRYPOINT"


_air_scenario: AIRScenario = AIRScenario()


def get_air_scenario():
    return _air_scenario


def get_air_scenario_env_vars_to_propagate():
    # only expected to be called in ray client, right before ray.remote() calls.
    assert ray.util.client.ray.is_connected()
    air_scenario_env_vars = {AIR_RAY_CLIENT_MODE_ENV_VAR: "1"}

    assert _air_scenario.entrypoint
    air_scenario_env_vars.update({AIR_ENTRYPOINT_ENV_VAR: _air_scenario.entrypoint})

    # the original entrypoint is `trainer.fit`.
    if _air_scenario.entrypoint == TRAINER_FIT:
        assert _air_scenario.trainable
        air_scenario_env_vars.update(
            {AIR_RAY_CLIENT_MODE_ENV_VAR: _air_scenario.trainable}
        )
    return air_scenario_env_vars


def set_air_scenario_in_head_node():
    # only expected to be called on head node.
    # may be called multiple times, but the following times are no-op.
    # And unset all env vars.
    assert not ray.util.client.ray.is_connected()
    if (
        not os.environ.get(AIR_RAY_CLIENT_MODE_ENV_VAR) == "1"
    ):  # not coming from ray client
        assert (
            AIR_TRAINABLE_ENV_VAR not in os.environ
            and AIR_ENTRYPOINT_ENV_VAR not in os.environ
        )
        return
    _air_scenario.ray_client_mode = True
    del os.environ[AIR_RAY_CLIENT_MODE_ENV_VAR]
    assert AIR_ENTRYPOINT_ENV_VAR in os.environ  # must have entrypoint already set
    _air_scenario.entrypoint = os.environ.get(AIR_ENTRYPOINT_ENV_VAR)
    del os.environ[AIR_ENTRYPOINT_ENV_VAR]
    if AIR_TRAINABLE_ENV_VAR in os.environ:  # trainer.fit + ray client mode
        assert _air_scenario.entrypoint == TRAINER_FIT
        _air_scenario.trainable = os.environ.get(AIR_TRAINABLE_ENV_VAR)
        del os.environ[AIR_TRAINABLE_ENV_VAR]
    # TODO: how to delete those env vars from runtime_env?


def set_entrypoint(entrypoint: str):
    assert entrypoint in (TRAINER_FIT, TUNER_RESTORE, TUNER_FIT, TUNE_RUN, TUNE_RUN_EXP)
    # can be called on both client and driver node.
    if _air_scenario.ray_client_mode:
        # already filled together with ray client mode.
        return
    if entrypoint in (TRAINER_FIT, TUNE_RUN_EXP, TUNER_RESTORE):
        # these should be first-line entrypoints - they are not used internally.
        assert not _air_scenario.entrypoint
        _air_scenario.entrypoint = entrypoint
    else:  # TUNER_FIT or TUNE_RUN
        _air_scenario.entrypoint = _air_scenario.entrypoint or entrypoint


def parse_and_set_trainable(trainable: "TrainableTypeOrTrainer"):
    if _air_scenario.trainable is not None:
        # already parsed in the original entrypoint.
        return

    from ray.train.trainer import BaseTrainer

    if isinstance(trainable, BaseTrainer):
        result = f"AIR_{trainable.__class__.__name__}"
    elif callable(trainable):
        result = "custom_function_trainable"
    else:
        from ray.rllib.algorithms.registry import ALGORITHMS
        from ray.tune import Trainable

        # Deal with native RLlib cases.
        if (
            isinstance(trainable, type(Trainable)) and trainable.__name__ in ALGORITHMS
        ) or (isinstance(trainable, str) and trainable in ALGORITHMS):
            result = "RLlib_native_trainable"
        elif isinstance(trainable, Trainable):
            result = "custom_class_trainable"
        else:
            result = "other"
    _air_scenario.trainable = result


# intended to be called only once upon entering `tune.run()`.
def parse_and_set_search_algorithm(
    algorithm: Optional[Union["Searcher", "SearchAlgorithm", str, "ConcurrencyLimiter"]]
):
    if algorithm is None:
        result = "basic_variant"

    from ray.tune.search import ConcurrencyLimiter
    from ray.tune.search import SearchAlgorithm, Searcher

    if isinstance(algorithm, ConcurrencyLimiter):
        algorithm = algorithm.searcher
    if isinstance(algorithm, str):
        from ray.tune.search import SEARCH_ALG_IMPORT

        if algorithm in SEARCH_ALG_IMPORT:
            result = SEARCH_ALG_IMPORT[algorithm].__class__.__name__
        else:
            result = "invalid"
    elif isinstance(algorithm, Searcher):
        if algorithm.__class__.startswith("ray.tune.search"):
            result = algorithm.__class__.__name__
        else:
            # We don't want to expose custom search/scheduler functions.
            result = "custom"
    elif isinstance(algorithm, SearchAlgorithm):
        from ray.tune.search.basic_variant import BasicVariantGenerator

        if isinstance(algorithm, BasicVariantGenerator):
            result = "basic_variant"
        else:
            result = "custom"
    else:
        result = "other"
    _air_scenario.search_alg = result


# intended to be called only once upon entering `tune.run()`.
def parse_and_set_scheduler_algorithm(
    scheduler: Optional[Union[str, "TrialScheduler"]]
):
    from ray.tune.schedulers import TrialScheduler

    if scheduler is None:
        result = "none"
    elif isinstance(scheduler, str):
        from ray.tune.schedulers import SCHEDULER_IMPORT

        if scheduler in SCHEDULER_IMPORT:
            result = SCHEDULER_IMPORT[scheduler].__class__.__name__
        else:
            result = "invalid"
    elif isinstance(scheduler, TrialScheduler):
        if scheduler.__class__.startswith("ray.tune.schedulers"):
            result = scheduler.__class__.__name__
        else:
            # We don't want to expose custom search/scheduler functions.
            result = "custom_scheduler"
    else:
        result = "other"
    _air_scenario.scheduler_alg = result


# intended to be called when search algorithm finishes setting up.
def set_more_than_one_trial(num_samples: int):
    _air_scenario.more_than_one_trial = num_samples > 1


# Intended to be called when `trial_runner.step()` is about to be called.
def record_trainable_usage():
    # by this time, it should already be set.
    assert _air_scenario.trainable
    record_extra_usage_tag(TagKey.TRAINER, _air_scenario.trainable)


# Intended to be called when `trial_runner.step()` is about to be called.
def record_search_algorithm():
    # by this time, it should already be set.
    assert _air_scenario.search_alg
    assert _air_scenario.more_than_one_trial is not None
    if not _air_scenario.more_than_one_trial:
        _air_scenario.search_alg = "na"
    record_extra_usage_tag(TagKey.SEARCH_ALGORITHM, _air_scenario.search_alg)


# Intended to be called when `trial_runner.step()` is about to be called.
def record_scheduler_algorithm():
    # by this time, it should already be set.
    assert _air_scenario.scheduler_alg
    assert _air_scenario.more_than_one_trial is not None
    if not _air_scenario.more_than_one_trial:
        _air_scenario.scheduler_alg = "na"
    record_extra_usage_tag(TagKey.SCHEDULER_ALGORITHM, _air_scenario.scheduler_alg)


# intended to be called only once upon entering `tune.run()`.
def record_experiment_tracking_framework(callbacks: Optional[Typing_Seq["Callback"]]):
    if not callbacks:
        record_extra_usage_tag(TagKey.EXPERIMENT_TRACKING_FRAMEWORK, "none")
    elif isinstance(callbacks, Sequence):
        result = []
        from ray.air.integrations.mlflow import MLflowLoggerCallback
        from ray.air.integrations.wandb import WandbLoggerCallback

        for callback in callbacks:
            if isinstance(callback, MLflowLoggerCallback):
                result.append("mlflow")
            elif isinstance(callback, WandbLoggerCallback):
                result.append("wandb")
        if not result:
            record_extra_usage_tag(TagKey.EXPERIMENT_TRACKING_FRAMEWORK, "none")
        else:
            record_extra_usage_tag(
                TagKey.EXPERIMENT_TRACKING_FRAMEWORK, json.dumps(result)
            )
    else:
        record_extra_usage_tag(TagKey.EXPERIMENT_TRACKING_FRAMEWORK, "other")
