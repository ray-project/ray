import tree
from typing import Any, Dict, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.rl_algorithm import RLAlgorithm
from ray.rllib.algorithms.rl_algorithm_apis.learner_api import SimpleLearnerGroupAPI
from ray.rllib.algorithms.rl_algorithm_apis.online_sampling_api import (
    SyncOnlineSamplingAPI,
)
from ray.rllib.core.learner.training_data import TrainingData


class PPOAlgorithm(SyncOnlineSamplingAPI, SimpleLearnerGroupAPI, RLAlgorithm):
    """Defines the PPO algorithm.

    Note, this algorithm includes mixins for policy rollout,
    model updates and derives directly from the base algorithm.
    """

    def __init__(self, config: AlgorithmConfig, logger_creator=None, **kwargs):
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)

    def _setup(self, config: AlgorithmConfig):

        # This sets up the `RLAlgorithm` base class and the `EnvRunner`s.
        print("Setup PPOAlgorithm ... ")
        # It is important to call the `super` method here to activate
        # the MRO.
        super()._setup(config=config)

    def training_step(self) -> None:
        # Implement the PPO training logic.

        # Sample from the `EnvRunner`s (synchronously).
        episodes, metrics = self.sample(
            env_steps=(
                self.config.total_train_batch_size
                if self.config.count_steps_by == "env_steps"
                else None
            ),
            agent_steps=(
                self.config.total_train_batch_size
                if self.config.count_steps_by == "agent_steps"
                else None
            ),
        )

        # TODO (simon): Implement Metrics collection via Mixin in base algo.
        #   Like this, it can be a mixin that is derived into two forms:
        #       1. MetricsConcreteMixin
        #       2. MetricsActorConcreteMixin

        # TODO (simon): Maybe returning from EnvRunnerGroup(s) already a TrainingData?
        learner_results = self.update(
            training_data=TrainingData(episodes=tree.flatten(episodes)),
            num_epochs=self.config.num_epochs,
            minibatch_size=self.config.minibatch_size,
            shuffle_batch_per_epoch=self.config.shuffle_batch_per_epoch,
        )

        state = self._provide_sync_state({})
        self.sync()

        return learner_results, metrics

    def sync(self, state: Optional[Dict[str, Any]] = None, **kwargs):
        # Synchronize the `EnvRunner`s via the learner group.
        # TODO (simon): This is bad style, having one mixin being dependent
        #   on another one. Better: use state.
        state = super().sync(state, learner_group=self._learner_group)

        # state = self._provide_sync_state(state, **kwargs)

        # return state
