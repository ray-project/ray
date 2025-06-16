from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.rl_algorithm import RLAlgorithm
from ray.rllib.algorithms.rl_algorithm_mixins.online_mixin import (
    SyncEnvRunnerConcreteMixin,
)


class PPOAlgorithm(RLAlgorithm, SyncEnvRunnerConcreteMixin):
    def __init__(self, config: AlgorithmConfig, logger_creator=None, **kwargs):
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)
        self._setup(config=config)

    def _setup(self, config: AlgorithmConfig):

        # This sets up the `RLAlgorithm` base class and the `EnvRunner`s.
        print("Setup PPOAlgorithm ... ")
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
