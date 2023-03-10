import gymnasium as gym
import tempfile
import torch
from typing import Mapping
import unittest

from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.utils.test_utils import check


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        self.assertIsInstance(module, TorchRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((bsize,) + obs_shape)
        actions = torch.stack(
            [torch.tensor(env.action_space.sample()) for _ in range(bsize)]
        )
        output = module.forward_train({"obs": obs})

        self.assertIsInstance(output, Mapping)
        self.assertIn("action_dist", output)
        self.assertIsInstance(output["action_dist"], torch.distributions.Categorical)

        loss = -output["action_dist"].log_prob(actions.view(-1)).mean()
        loss.backward()

        # check that all neural net parameters have gradients
        for param in module.parameters():
            self.assertIsNotNone(param.grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        state2 = module2.get_state()
        check(state, state2, false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_checkpointing(self):
        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            module.save_to_checkpoint(tmpdir)
            new_module = DiscreteBCTorchModule.from_checkpoint(tmpdir)

        check(module.get_state(), new_module.get_state())
        self.assertNotEqual(id(module), id(new_module))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
