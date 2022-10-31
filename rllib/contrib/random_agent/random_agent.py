import numpy as np

from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.annotations import override


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
            obs = self.env.reset()
            done = False
            reward = 0.0
            while not done:
                action = self.env.action_space.sample()
                obs, r, done, info = self.env.step(action)
                reward += r
                steps += 1
            rewards.append(reward)
        return {
            "episode_reward_mean": np.mean(rewards),
            "timesteps_this_iter": steps,
        }
# __sphinx_doc_end__
# FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
# formatting on this module and update the comment below. See issue #21318.
# don't enable yapf after, it's buggy here


if __name__ == "__main__":
    # New way of configuring Algorithms.
    config = AlgorithmConfig().environment("CartPole-v1")
    config.rollouts_per_iteration = 10
    algo = RandomAgent(config=config)
    result = algo.train()
    assert result["episode_reward_mean"] > 10, result
    algo.stop()

    # Old style via config dict.
    config = {"rollouts_per_iteration": 10}
    algo = RandomAgent(config=config, env="CartPole-v1")
    result = algo.train()
    assert result["episode_reward_mean"] > 10, result
    algo.stop()

    print("Test: OK")
