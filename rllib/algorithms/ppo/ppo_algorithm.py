import numpy as np
import tree
from typing import Any, Dict, List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.rl_algorithm import RLAlgorithm
from ray.rllib.algorithms.rl_algorithm_apis.data_api import OfflineDataAPI
from ray.rllib.algorithms.rl_algorithm_apis.learner_api import SimpleLearnerGroupAPI
from ray.rllib.algorithms.rl_algorithm_apis.online_sampling_api import (
    SyncOnlineSamplingAPI,
)
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import OfflinePreLearner, SCHEMA
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, LEARNER_RESULTS


class PPOAlgorithm(SyncOnlineSamplingAPI, SimpleLearnerGroupAPI, RLAlgorithm):
    """Defines the PPO algorithm.

    Note, this algorithm includes apis for policy rollout,
    model updates and derives directly from the base algorithm.
    """

    def __init__(self, config: AlgorithmConfig, logger_creator=None, **kwargs):
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)

    def _setup(self, config: AlgorithmConfig):

        # This sets up the `RLAlgorithm` base class and the `EnvRunner`s.
        self.logger.info("Setup PPOAlgorithm ... ")
        # It is important to call the `super` method here to activate
        # the MRO.
        super()._setup(config=config)

    def training_step(self) -> None:
        # Implement the PPO training logic.

        # Sample from the `EnvRunner`s (synchronously).
        episodes, env_runner_metrics = self.sample(
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
            _return_metrics=True,
        )

        # TODO (simon): Implement Metrics collection via Mixin in base algo.
        #   Like this, it can be a mixin that is derived into two forms:
        #       1. MetricsConcreteMixin
        #       2. MetricsActorConcreteMixin

        # TODO (simon): Maybe returning from EnvRunnerGroup(s) already a TrainingData?
        learner_metrics = self.update(
            training_data=TrainingData(episodes=tree.flatten(episodes)),
            num_epochs=self.config.num_epochs,
            minibatch_size=self.config.minibatch_size,
            shuffle_batch_per_epoch=self.config.shuffle_batch_per_epoch,
        )

        # state = self._provide_sync_state({})
        self.sync()

        return {
            ENV_RUNNER_RESULTS: env_runner_metrics,
            LEARNER_RESULTS: learner_metrics,
        }

    def sync(self, state: Optional[Dict[str, Any]] = None, **kwargs):
        # Synchronize the `EnvRunner`s via the learner group.
        # TODO (simon): This is bad style, having one mixin being dependent
        #   on another one. Better: use state.
        state = super().sync(state, learner_group=self._learner_group)

        # state = self._provide_sync_state(state, **kwargs)

        # return state


class OfflinePreLearnerReturnEpisodes(OfflinePreLearner):
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        episodes: List[SingleAgentEpisode] = self._map_to_episodes(
            self._is_multi_agent,
            batch,
            schema=SCHEMA | self.config.input_read_schema,
            to_numpy=True,
            input_compress_columns=self.config.input_compress_columns,
            observation_space=self.observation_space,
            action_space=self.action_space,
        )["episodes"]

        # Flatten the dictionary to increase serialization performance.
        return {"episodes": episodes}


class PPOAlgorithmWithData(OfflineDataAPI, PPOAlgorithm):

    _offline_mixing_rate: float = None

    def __init__(self, config: AlgorithmConfig, logger_creator=None, **kwargs):
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)

    def _setup(self, config: AlgorithmConfig):

        self._offline_mixing_rate = config.get("offline_mixing_rate", 0.2)
        # This sets up the `RLAlgorithm` base class and the `EnvRunner`s.
        self.logger.info("Setup PPOAlgorithmWithData ... ")
        # It is important to call the `super` method here to activate
        # the MRO.
        super()._setup(config=config)

        # Now add the learner handles to the `OfflineData`.
        self._data_source.learner_handles = [self._learner_group._learner]
        self._data_source.module_spec = self._learner_group._module_spec
        self._data_source.spaces = self._spaces

    def training_step(self) -> None:
        # Implement the PPO training logic using online and offline experiences.
        offline_batch_size = round(
            self.config.total_train_batch_size * self._offline_mixing_rate
        )
        offline_episodes = self.sample_from_data(
            offline_batch_size,
            num_shards=self.config.num_learners,
            return_iterator=False,
            as_multi_agent_batch=False,
        )["episodes"].tolist()

        # Sample from the `EnvRunner`s (synchronously).
        online_episodes, env_runner_metrics = self.sample(
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
            _return_metrics=True,
        )
        online_episodes = tree.flatten(online_episodes)
        episodes = online_episodes + offline_episodes
        # TODO (simon): Implement Metrics collection via Mixin in base algo.
        #   Like this, it can be a mixin that is derived into two forms:
        #       1. MetricsConcreteMixin
        #       2. MetricsActorConcreteMixin

        # TODO (simon): Maybe returning from EnvRunnerGroup(s) already a TrainingData?
        learner_metrics = self.update(
            training_data=TrainingData(episodes=episodes),
            num_epochs=self.config.num_epochs,
            minibatch_size=self.config.minibatch_size,
            shuffle_batch_per_epoch=self.config.shuffle_batch_per_epoch,
        )

        # state = self._provide_sync_state({})
        self.sync()

        return {
            ENV_RUNNER_RESULTS: env_runner_metrics,
            LEARNER_RESULTS: learner_metrics,
        }
