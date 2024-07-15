import tempfile
import unittest

import gymnasium as gym
import tensorflow as tf

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.utils.test_utils import check


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        self.assertIsInstance(module, TfRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        obs_shape = env.observation_space.shape
        obs = tf.random.uniform((bsize,) + obs_shape)
        actions = tf.stack(
            [
                tf.convert_to_tensor(env.action_space.sample(), dtype=tf.float32)
                for _ in range(bsize)
            ]
        )
        with tf.GradientTape() as tape:
            output = module.forward_train({"obs": obs})
            action_dist_class = module.get_train_action_dist_cls()
            action_dist = action_dist_class.from_logits(
                output[Columns.ACTION_DIST_INPUTS]
            )
            loss = -tf.math.reduce_mean(action_dist.logp(actions))

        self.assertIsInstance(output, dict)

        grads = tape.gradient(loss, module.trainable_variables)

        # check that all neural net parameters have gradients
        for grad in grads:
            self.assertIsNotNone(grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        obs_shape = env.observation_space.shape
        obs = tf.random.uniform((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        state2 = module2.get_state()
        check(state["policy"][0], state2["policy"][0], false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_checkpointing(self):
        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule(
            config=RLModuleConfig(
                env.observation_space,
                env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            module.save_to_path(tmpdir)
            new_module = DiscreteBCTFModule.from_checkpoint(tmpdir)

        check(module.get_state(), new_module.get_state())
        self.assertNotEqual(id(module), id(new_module))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
