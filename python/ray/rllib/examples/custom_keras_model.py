"""Example of using a custom ModelV2 Keras-stlye model.

TODO(ekl): add this to docs once ModelV2 is fully implemented.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class MyKerasModel(TFModelV2):
    def __init__(self, obs_space, action_space, output_spec, model_config,
                 name):
        TFModelV2.__init__(self, obs_space, action_space, output_spec,
                           model_config, name)

        self.layer_1 = tf.keras.layers.Dense(64)
        self.layer_2 = tf.keras.layers.Dense(64)
        self.layer_out = tf.keras.layers.Dense(
            output_spec.size, activation=None)
        self.vnet_layer_1 = tf.keras.layers.Dense(64)
        self.vnet_layer_2 = tf.keras.layers.Dense(64)
        self.vnet_out = tf.keras.layers.Dense(1, activation=None)

    def forward(self, input_dict, state, seq_lens):
        self.prev_input = input_dict
        x1 = self.layer_1(input_dict["obs"])
        x2 = self.layer_2(x1)
        return self.layer_out(x2), x2, state

    def get_branch_output(self,
                          branch_type,
                          output_spec=None,
                          feature_layer=None,
                          default_impl=None):
        if branch_type == "value":
            if feature_layer is not None:
                return self.vnet_out(feature_layer)
            else:
                y1 = self.vnet_layer_1(self.prev_input["obs"])
                y2 = self.vnet_layer_2(y1)
                return self.vnet_out(y2)
        else:
            raise NotImplementedError(branch_type)


if __name__ == "__main__":
    ray.init()
    ModelCatalog.register_custom_model("keras_model", MyKerasModel)
    tune.run(
        "PPO",
        stop={"training_iteration": 2},
        config={
            "env": "CartPole-v0",
            "vf_share_layers": False,
            "model": {
                "custom_model": "keras_model"
            },
        })
