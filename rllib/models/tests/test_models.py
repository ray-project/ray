from gym.spaces import Box
import numpy as np
import unittest

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


class TestTFModel(TFModelV2):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        input_ = tf.keras.layers.Input(shape=(3, ))
        output = tf.keras.layers.Dense(2)(input_)
        # A keras model inside.
        self.keras_model = tf.keras.models.Model([input_], [output])
        # A RLlib FullyConnectedNetwork (tf) inside (which is also a keras
        # Model).
        self.fc_net = FullyConnectedNetwork(obs_space, action_space, 3, {},
                                            "fc1")

    def forward(self, input_dict, state, seq_lens):
        obs = input_dict["obs_flat"]
        out1 = self.keras_model(obs)
        out2, _ = self.fc_net({"obs": obs})
        return tf.concat([out1, out2], axis=-1), []


class TestModels(unittest.TestCase):
    """Tests ModelV2 classes and their modularization capabilities."""

    def test_tf_modelv2(self):
        obs_space = Box(-1.0, 1.0, (3, ))
        action_space = Box(-1.0, 1.0, (2, ))
        my_tf_model = TestTFModel(obs_space, action_space, 5, {},
                                  "my_tf_model")
        # Call the model.
        out, states = my_tf_model({"obs": np.array([obs_space.sample()])})
        self.assertTrue(out.shape == (1, 5))
        self.assertTrue(out.dtype == tf.float32)
        self.assertTrue(states == [])
        vars = my_tf_model.variables(as_dict=True)
        self.assertTrue(len(vars) == 6)
        self.assertTrue("keras_model.dense.kernel:0" in vars)
        self.assertTrue("keras_model.dense.bias:0" in vars)
        self.assertTrue("fc_net.base_model.fc_out.kernel:0" in vars)
        self.assertTrue("fc_net.base_model.fc_out.bias:0" in vars)
        self.assertTrue("fc_net.base_model.value_out.kernel:0" in vars)
        self.assertTrue("fc_net.base_model.value_out.bias:0" in vars)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
