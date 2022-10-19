import unittest
import gym
from ray.rllib.core.rl_module.examples.simple_ppo_rl_module import (
    SimplePPOModule, PPOModuleConfig, FCConfig
)

class TestRLModule(unittest.TestCase):

    def test_simple_ppo_module_compilation_cartpole(self):

        env = gym.make("CartPole-v0")
        config1 = PPOModuleConfig(
            observation_space=env.observation_space,
            action_space=env.action_space,
            pi_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
            vf_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
        ) 

        module = SimplePPOModule(config1)
        breakpoint()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

