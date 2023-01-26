import gymnasium as gym
import tensorflow as tf
import tensorflow_probability as tfp
import threading
from typing import Mapping
import unittest

from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.utils.error import NotSerializable
from ray.rllib.utils.test_utils import check


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        self.assertIsInstance(module, TfRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
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
            loss = -tf.math.reduce_mean(output["action_dist"].log_prob(actions))

        self.assertIsInstance(output, Mapping)
        self.assertIn("action_dist", output)
        self.assertIsInstance(output["action_dist"], tfp.distributions.Categorical)

        grads = tape.gradient(loss, module.trainable_variables)

        # check that all neural net parameters have gradients
        for grad in grads:
            self.assertIsNotNone(grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        obs_shape = env.observation_space.shape
        obs = tf.random.uniform((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        state2 = module2.get_state()
        check(state["policy"][0], state2["policy"][0], false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_serialize_deserialize(self):
        env = gym.make("CartPole-v1")
        module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        # create a new module from the old module
        new_module = module.deserialize(module.serialize())

        # check that the new module is the same type
        self.assertIsInstance(new_module, type(module))

        # check that a parameter of their's is the same
        self.assertEqual(new_module._input_dim, module._input_dim)

        # check that their states are the same
        check(module.get_state(), new_module.get_state())

        # check that these 2 objects are not the same object
        self.assertNotEqual(id(module), id(new_module))

        # check that unpickleable parameters are not allowed by the RL Module
        # constructor
        unpickleable_param = threading.Thread()

        def bad_constructor():
            return DiscreteBCTFModule(
                input_dim=unpickleable_param,
                hidden_dim=unpickleable_param,
                output_dim=unpickleable_param,
            )

        self.assertRaises(NotSerializable, bad_constructor)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
