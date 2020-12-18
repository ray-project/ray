from gym.spaces import Box, Discrete
import unittest

from ray.rllib.models.jax.fcnet import FullyConnectedNetwork
from ray.rllib.utils.framework import try_import_jax


jax, flax = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp


class TestJAXModels(unittest.TestCase):

    def test_jax_fcnet(self):
        """Tests the JAX FCNet class."""
        batch_size = 1000
        obs_space = Box(-10.0, 10.0, shape=(4, ))
        action_space = Discrete(2)
        fc_net = FullyConnectedNetwork(
            obs_space,
            action_space,
            num_outputs=2,
            model_config={
                "fcnet_hiddens": [10],
                "fcnet_activation": "relu",
            },
            name="jax_model"
        )
        inputs = jnp.array([obs_space.sample()])
        print(fc_net({"obs": inputs}))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
