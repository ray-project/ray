from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.utils.annotations import override


# yapf: disable
# __sphinx_doc_begin__
class RandomAgent(Agent):
    """Agent that takes random actions and never learns."""

    _agent_name = "RandomAgent"
    _default_config = with_common_config({
        "rollouts_per_iteration": 10,
    })

    @override(Agent)
    def _init(self):
        self.env = self.env_creator(self.config["env_config"])

    @override(Agent)
    def _train(self):
        rewards = []
        steps = 0
        for _ in range(self.config["rollouts_per_iteration"]):
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
# don't enable yapf after, it's buggy here


if __name__ == "__main__":
    agent = RandomAgent(
        env="CartPole-v0", config={"rollouts_per_iteration": 10})
    result = agent.train()
    assert result["episode_reward_mean"] > 10, result
    print("Test: OK")
