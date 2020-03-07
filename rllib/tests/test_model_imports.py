#!/usr/bin/env python

import gym
import numpy as np
import unittest
import ray

from ray.rllib.agents.registry import get_agent_class
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class MyKerasModel(TFModelV2):
    """Custom model for policy gradient algorithms."""
    
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(MyKerasModel, self).__init__(obs_space, action_space,
                                           num_outputs, model_config, name)
        self.inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            16,
            name="layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0))(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(layer_1)
        if self.model_config["vf_share_layers"]:
            value_out = tf.keras.layers.Dense(
                1,
                name="value",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(layer_1)
            self.base_model = tf.keras.Model(
                self.inputs, [layer_out, value_out])
        else:
            self.base_model = tf.keras.Model(self.inputs, layer_out)
        
        self.register_variables(self.base_model.variables)
    
    def forward(self, input_dict, state, seq_lens):
        if self.model_config["vf_share_layers"]:
            model_out, self._value_out = self.base_model(input_dict["obs"])
        else:
            model_out = self.base_model(input_dict["obs"])
            self._value_out = tf.zeros(shape=(tf.shape(input_dict["obs"])[0], ))
        return model_out, state
    
    def value_function(self):
        return tf.reshape(self._value_out, [-1])
    
    def import_from_h5(self, import_file):
        # Override this to define custom weight loading behavior from h5 files.
        self.base_model.load_weights(import_file)


def test_model_import(run, config, env):
    import_file = "data/model_weights/weights.h5"
    agent_cls = get_agent_class(run)
    agent = agent_cls(config, env)
    
    def current_weight(agent):
        return agent.get_weights()["default_policy"][
            "default_policy/value/kernel"][0]
    
    # Import weights for our custom model from an h5 file.
    weight_before_import = current_weight(agent)
    agent.import_model(import_file=import_file)
    weight_after_import = current_weight(agent)
    check(weight_before_import, weight_after_import, false=True)

    # Train for a while.
    for _ in range(2):
        agent.train()
    weight_after_train = current_weight(agent)
    # Weights should have changed.
    check(weight_before_import, weight_after_train, false=True)
    check(weight_after_import, weight_after_train, false=True)

    # We can save the entire Agent and restore, weights should remain the same.
    file = agent.save("after_train")
    check(weight_after_train, current_weight(agent))
    agent.restore(file)
    check(weight_after_train, current_weight(agent))

    # Import (untrained) weights again.
    agent.import_model(import_file=import_file)
    check(current_weight(agent), weight_after_import)


class TestModelImport(unittest.TestCase):
    def setUp(self):
        ray.init()
        ModelCatalog.register_custom_model(
            "keras_model", MyKerasModel,
        )

    def tearDown(self):
        ray.shutdown()

    def test_dqn(self):
        test_model_import(
            "DQN", config={
            "num_workers": 0,
            "model": {
                "vf_share_layers": True,
                "custom_model": "keras_model",
            }},
            env="CartPole-v0")

    def test_ppo(self):
        test_model_import(
            "PPO", config={
            "num_workers": 0,
            "vf_share_layers": True,
            "model": {
                "custom_model": "keras_model",
            }},
            env="CartPole-v0")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
