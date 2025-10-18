from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import ray
from ray.data.iterator import DataIterator
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.offline.offline_evaluation_runner import OfflineEvaluationRunner
from ray.rllib.offline.offline_policy_evaluation_runner import (
    OfflinePolicyEvaluationRunner,
    OfflinePolicyPreEvaluator,
)
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.runners.runner_group import RunnerGroup

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class OfflineEvaluationRunnerGroup(RunnerGroup):
    def __init__(
        self,
        config: "AlgorithmConfig",
        local_runner: Optional[bool] = False,
        logdir: Optional[str] = None,
        tune_trial_id: Optional[str] = None,
        pg_offset: int = 0,
        _setup: bool = True,
        spaces: Optional[Dict[str, Any]] = None,
        module_state: Dict[str, Any] = None,
        module_spec: Optional[MultiRLModuleSpec] = None,
        **kwargs: Dict[str, Any],
    ) -> None:

        # TODO (simon): Check, if this should happen later when the dataset
        # is created. Maybe just overriding _setup.
        # First initialize the super class.
        super().__init__(
            config=config,
            local_runner=local_runner,
            logdir=logdir,
            tune_trial_id=tune_trial_id,
            pg_offset=pg_offset,
            _setup=_setup,
            module_state=module_state,
            module_spec=module_spec,
            spaces=spaces,
        )

    @override(RunnerGroup)
    def _setup(
        self,
        *,
        config: Optional["AlgorithmConfig"] = None,
        num_runners: int = 0,
        local_runner: Optional[bool] = False,
        module_state: Dict[str, Any] = None,
        module_spec: Optional[MultiRLModuleSpec] = None,
        spaces: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[str, Any],
    ) -> None:

        # Define the offline evaluation runner class.
        self._runner_cls = config.offline_eval_runner_class or (
            OfflineEvaluationRunner
            if config.offline_evaluation_type == "eval_loss"
            else OfflinePolicyEvaluationRunner
        )
        # Define
        self._pre_learner_or_evaluator_cls = self.config.prelearner_class or (
            OfflinePreLearner
            if config.offline_evaluation_type == "eval_loss"
            else OfflinePolicyPreEvaluator
        )
        self.config._is_frozen = False
        self.config.prelearner_class = self._pre_learner_or_evaluator_cls
        self.config._is_frozen = True

        # We can either run on a local runner or on remote runners only b/c
        # streaming split needs remote runners.
        if num_runners > 0 and local_runner:
            raise ValueError(
                f"Cannot run `OfflineEvaluationRunnerGroup with {num_runners=} "
                "and a local runner. Either use no remote runners or only "
                "remote runners."
            )
        # Create all workers.
        super()._setup(
            config=config,
            num_runners=num_runners,
            local_runner=local_runner,
            # Do not validate until the `DataIterators` are distributed.
            validate=False,
            module_spec=module_spec,
            module_state=module_state,
            spaces=spaces,
        )

        # Setup the evaluation offline dataset and return an iterator.
        self._offline_data: OfflineData = OfflineData(config=config)
        # We need the spaces to be defined for the `OfflinePreLearner`.
        spaces = spaces or {
            INPUT_ENV_SPACES: (config.observation_space, config.action_space)
        }
        self._offline_data.spaces = spaces
        # The `OfflinePreLearner` also needs the module spec.
        module_spec: MultiRLModuleSpec = module_spec or self.config.get_multi_rl_module_spec(
            # TODO (simon): this needs merely the spaces defined via the connectors.
            spaces={DEFAULT_MODULE_ID: spaces[INPUT_ENV_SPACES]},
            inference_only=self.config.offline_eval_rl_module_inference_only,
        )
        self._offline_data.module_spec = module_spec
        # If we have remote runners set the locality hints for the streaming split
        # dataset iterators.
        if self.num_remote_runners > 0:
            runner_node_ids = self.foreach_runner(
                lambda _: ray.get_runtime_context().get_node_id()
            )
            if self.local_runner is not None:
                runner_node_ids.insert(0, ray.get_runtime_context().get_node_id())
            self._offline_data.locality_hints = runner_node_ids
        # Return a data iterator for each `Runner`.
        self._offline_data_iterators: List[DataIterator] = self.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=num_runners,
            module_state=module_state,
        )
        # Provide each `Runner` with a `DataIterator`.
        self.foreach_runner(
            func="set_dataset_iterator",
            local_runner=local_runner,
            kwargs=[
                {"iterator": iterator} for iterator in self._offline_data_iterators
            ],
        )
        # Now validate healthiness.
        self.validate()

    @property
    def runner_health_probe_timeout_s(self):
        """Number of seconds to wait for health probe calls to `Runner`s."""
        return self.config.offline_eval_runner_health_probe_timeout_s

    @property
    def runner_cls(self) -> Callable:
        """Class for each runner."""
        return self._runner_cls

    @property
    def num_runners(self) -> int:
        """Number of runners to schedule and manage."""
        return self.config.num_offline_eval_runners

    @property
    def offline_data(self) -> OfflineData:
        return self._offline_data

    @property
    def _remote_args(self):
        """Remote arguments for each runner."""
        return {
            "num_cpus": self._remote_config.num_cpus_per_offline_eval_runner,
            "num_gpus": self._remote_config.num_gpus_per_offline_eval_runner,
            "resources": self._remote_config.custom_resources_per_offline_eval_runner,
            "max_restarts": (
                self.config.max_num_offline_eval_runner_restarts
                if self.config.restart_failed_offline_eval_runners
                else 0
            ),
        }

    @property
    def _ignore_ray_errors_on_runners(self):
        """If errors in runners should be ignored."""
        return (
            self.config.ignore_offline_eval_runner_failures
            or self.config.restart_failed_offline_eval_runners
        )

    @property
    def _max_requests_in_flight_per_runner(self):
        """Maximum requests in flight per runner."""
        return self.config.max_requests_in_flight_per_offline_eval_runner

    @property
    def _validate_runners_after_construction(self):
        """If runners should validated after constructed."""
        return self.config.validate_offline_eval_runners_after_construction
