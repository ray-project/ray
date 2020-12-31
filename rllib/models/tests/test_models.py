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
        self.base_model = tf.keras.models.Model([input_], [output])
        # A RLlib FullyConnectedNetwork (tf) inside (which is also a keras
        # Model).
        self.fc_net = FullyConnectedNetwork(
            obs_space, action_space, 3, {}, "fc1")

    def forward(self, input_dict, state, seq_lens):
        obs = input_dict["obs_flat"]
        out1 = self.base_model(obs)
        out2, _ = self.fc_net({"obs": obs})
        return tf.concat([out1, out2], axis=-1), []


class TestModels(unittest.TestCase):
    """Tests ModelV2 classes."""

    def test_tf_modelv2(self):
        obs_space = Box(-1.0, 1.0, (3, ))
        action_space = Box(-1.0, 1.0, (2, ))
        my_tf_model = TestTFModel(
            obs_space, action_space, 5, {}, "my_tf_model")
        # Call the model.
        test = my_tf_model({"obs": np.array([obs_space.sample()])})


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
