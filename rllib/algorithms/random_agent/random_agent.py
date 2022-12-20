import numpy as np
from typing import Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override


class RandomAgentConfig(AlgorithmConfig):
    """Defines a configuration class from which a RandomAgent Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.random_agent import RandomAgentConfig
        >>> config = RandomAgentConfig().rollouts(rollouts_per_iteration=20)
        >>> print(config.to_dict()) # doctest: +SKIP
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")
        >>> algo.train() # doctest: +SKIP
    """

    def __init__(self, algo_class=None):
        """Initializes a RandomAgentConfig instance."""
        super().__init__(algo_class=algo_class or RandomAgent)

        self.rollouts_per_iteration = 10

    def rollouts(
        self,
        *,
        rollouts_per_iteration: Optional[int] = NotProvided,
        **kwargs,
    ) -> "RandomAgentConfig":
        """Sets the rollout configuration.

        Args:
            rollouts_per_iteration: How many episodes to run per training iteration.

        Returns:
            This updated AlgorithmConfig object.
        """
        super().rollouts(**kwargs)

        if rollouts_per_iteration is not NotProvided:
            self.rollouts_per_iteration = rollouts_per_iteration

        return self


# fmt: off
# __sphinx_doc_begin__
class RandomAgent(Algorithm):
    """Algo that produces random actions and never learns."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        config = AlgorithmConfig()
        config.rollouts_per_iteration = 10
        return config

    @override(Algorithm)
    def _init(self, config, env_creator):
        self.env = env_creator(config["env_config"])

    @override(Algorithm)
    def step(self):
        rewards = []
        steps = 0
        for _ in range(self.config.rollouts_per_iteration):
            self.env.reset()
            terminated = truncated = False
            reward = 0.0
            while not terminated and not truncated:
                action = self.env.action_space.sample()
                _, rew, terminated, truncated, _ = self.env.step(action)
                reward += rew
                steps += 1
            rewards.append(reward)
        return {
            "episode_reward_mean": np.mean(rewards),
            "timesteps_this_iter": steps,
        }
# __sphinx_doc_end__


if __name__ == "__main__":
    # Define a config object.
    config = (
        RandomAgentConfig()
        .environment("CartPole-v1")
        .rollouts(rollouts_per_iteration=10)
    )
    # Build the agent.
    algo = config.build()
    # "Train" one iteration.
    result = algo.train()
    assert result["episode_reward_mean"] > 10, result
    algo.stop()

    print("Test: OK")
