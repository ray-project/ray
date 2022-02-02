#!/usr/bin/env python

import h5py
import numpy as np
from pathlib import Path
import unittest

import ray
from ray.rllib.agents.registry import get_trainer_class
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class MyKerasModel(TFModelV2):
    """Custom model for policy gradient algorithms."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super(MyKerasModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name
        )
        self.inputs = tf.keras.layers.Input(shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            16,
            name="layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0),
        )(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="out",
            activation=None,
            kernel_initializer=normc_initializer(0.01),
        )(layer_1)
        if self.model_config["vf_share_layers"]:
            value_out = tf.keras.layers.Dense(
                1,
                name="value",
                activation=None,
                kernel_initializer=normc_initializer(0.01),
            )(layer_1)
            self.base_model = tf.keras.Model(self.inputs, [layer_out, value_out])
        else:
            self.base_model = tf.keras.Model(self.inputs, layer_out)

    def forward(self, input_dict, state, seq_lens):
        if self.model_config["vf_share_layers"]:
            model_out, self._value_out = self.base_model(input_dict["obs"])
        else:
            model_out = self.base_model(input_dict["obs"])
            self._value_out = tf.zeros(shape=(tf.shape(input_dict["obs"])[0],))
        return model_out, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    def import_from_h5(self, import_file):
        # Override this to define custom weight loading behavior from h5 files.
        self.base_model.load_weights(import_file)


class MyTorchModel(TorchModelV2, nn.Module):
    """Generic vision network."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.layer_1 = nn.Linear(obs_space.shape[0], 16).to(self.device)
        self.layer_out = nn.Linear(16, num_outputs).to(self.device)
        self.value_branch = nn.Linear(16, 1).to(self.device)
        self.cur_value = None

    def forward(self, input_dict, state, seq_lens):
        layer_1_out = self.layer_1(input_dict["obs"])
        logits = self.layer_out(layer_1_out)
        self.cur_value = self.value_branch(layer_1_out).squeeze(1)
        return logits, state

    def value_function(self):
        assert self.cur_value is not None, "Must call `forward()` first!"
        return self.cur_value

    def import_from_h5(self, import_file):
        # Override this to define custom weight loading behavior from h5 files.
        f = h5py.File(import_file)
        layer1 = f["layer1"][DEFAULT_POLICY_ID]["layer1"]
        out = f["out"][DEFAULT_POLICY_ID]["out"]
        value = f["value"][DEFAULT_POLICY_ID]["value"]

        try:
            self.layer_1.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(layer1["kernel:0"])),
                    "bias": torch.Tensor(np.transpose(layer1["bias:0"])),
                }
            )
            self.layer_out.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(out["kernel:0"])),
                    "bias": torch.Tensor(np.transpose(out["bias:0"])),
                }
            )
            self.value_branch.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(value["kernel:0"])),
                    "bias": torch.Tensor(np.transpose(value["bias:0"])),
                }
            )
        except AttributeError:
            self.layer_1.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(layer1["kernel:0"].value)),
                    "bias": torch.Tensor(np.transpose(layer1["bias:0"].value)),
                }
            )
            self.layer_out.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(out["kernel:0"].value)),
                    "bias": torch.Tensor(np.transpose(out["bias:0"].value)),
                }
            )
            self.value_branch.load_state_dict(
                {
                    "weight": torch.Tensor(np.transpose(value["kernel:0"].value)),
                    "bias": torch.Tensor(np.transpose(value["bias:0"].value)),
                }
            )


def model_import_test(algo, config, env):
    # Get the abs-path to use (bazel-friendly).
    rllib_dir = Path(__file__).parent.parent
    import_file = str(rllib_dir) + "/tests/data/model_weights/weights.h5"

    agent_cls = get_trainer_class(algo)

    for fw in framework_iterator(config, ["tf", "torch"]):
        config["model"]["custom_model"] = (
            "keras_model" if fw != "torch" else "torch_model"
        )

        agent = agent_cls(config, env)

        def current_weight(agent):
            if fw == "tf":
                return agent.get_weights()[DEFAULT_POLICY_ID][
                    "default_policy/value/kernel"
                ][0]
            elif fw == "torch":
                return float(
                    agent.get_weights()[DEFAULT_POLICY_ID]["value_branch.weight"][0][0]
                )
            else:
                return agent.get_weights()[DEFAULT_POLICY_ID][4][0]

        # Import weights for our custom model from an h5 file.
        weight_before_import = current_weight(agent)
        agent.import_model(import_file=import_file)
        weight_after_import = current_weight(agent)
        check(weight_before_import, weight_after_import, false=True)

        # Train for a while.
        for _ in range(1):
            agent.train()
        weight_after_train = current_weight(agent)
        # Weights should have changed.
        check(weight_before_import, weight_after_train, false=True)
        check(weight_after_import, weight_after_train, false=True)

        # We can save the entire Agent and restore, weights should remain the
        # same.
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
        ModelCatalog.register_custom_model("keras_model", MyKerasModel)
        ModelCatalog.register_custom_model("torch_model", MyTorchModel)

    def tearDown(self):
        ray.shutdown()

    def test_ppo(self):
        model_import_test(
            "PPO",
            config={
                "num_workers": 0,
                "model": {
                    "vf_share_layers": True,
                },
            },
            env="CartPole-v0",
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
