import gym
import tensorflow as tf
import tensorflow_probability as tfp
import unittest
from typing import Mapping

from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule

from ray.rllib.utils.test_utils import check


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_env(env)

        self.assertIsInstance(module, TfRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_env(env)

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
            loss = -tf.math.reduce_mean(output["action_dist"].log_prob(actions))

        self.assertIsInstance(output, Mapping)
        self.assertIn("action_dist", output)
        self.assertIsInstance(output["action_dist"], tfp.distributions.Categorical)

        grads = tape.gradient(loss, module.trainable_variables())

        # check that all neural net parameters have gradients
        for grad in grads["policy"]:
            self.assertIsNotNone(grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_env(env)

        obs_shape = env.observation_space.shape
        obs = tf.random.uniform((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_env(env)

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = DiscreteBCTFModule.from_env(env)
        state2 = module2.get_state()
        check(state["policy"][0], state2["policy"][0], false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
