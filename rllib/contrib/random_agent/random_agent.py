import numpy as np

from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict


# fmt: off
# __sphinx_doc_begin__
class RandomAgent(Trainer):
    """Trainer that produces random actions and never learns."""

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return with_common_config({
            "rollouts_per_iteration": 10,
            "framework": "tf",  # not used
        })

    @override(Trainer)
    def _init(self, config, env_creator):
        self.env = env_creator(config["env_config"])

    @override(Trainer)
    def step(self):
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
# FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
# formatting on this module and update the comment below. See issue #21318.
# don't enable yapf after, it's buggy here


if __name__ == "__main__":
    trainer = RandomAgent(
        env="CartPole-v0", config={"rollouts_per_iteration": 10})
    result = trainer.train()
    assert result["episode_reward_mean"] > 10, result
    print("Test: OK")
