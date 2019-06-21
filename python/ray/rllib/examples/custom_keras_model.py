"""Example of using a custom ModelV2 Keras-stlye model.

TODO(ekl): add this to docs once ModelV2 is fully implemented.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.agents.dqn.simple_q_model import SimpleQModel
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="DQN")
parser.add_argument("--stop", type=int, default=200)


class MyKerasModel(SimpleQModel):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(MyKerasModel, self).__init__(self, obs_space, action_space, num_outputs,
                           model_config, name)

        self.inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="observations")

        layer_1 = tf.keras.layers.Dense(64, name="layer1")(self.inputs)
        layer_2 = tf.keras.layers.Dense(64, name="layer2")(layer_1)
        layer_out = tf.keras.layers.Dense(
            num_outputs, name="out", activation=None)(layer_2)
        self.base_model = tf.keras.Model(self.inputs, [layer_out, layer_2])
        self.value_model = None

        self.register_model_variables(self.base_model)

    def forward(self, input_dict, state, seq_lens):
        self.prev_input = input_dict
        model_out, feature_out = self.base_model(input_dict["obs"])
        return model_out, feature_out, state

#    def get_branch_output(self,
#                          branch_type,
#                          output_spec=None,
#                          feature_layer=None,
#                          default_impl=None):
#        if branch_type == "value":
#            # e.g., for PPO
#            if feature_layer is not None:
#                # sharing variables
#                value_out = tf.keras.layers.Dense(
#                    1, name="value_out", activation=None)(feature_layer)
#                self.value_model = value_out
#                return value_out
#            else:
#                # non-shared case
#                value_1 = tf.keras.layers.Dense(64, name="value1")(self.inputs)
#                value_2 = tf.keras.layers.Dense(64, name="value2")(value_1)
#                value_out = tf.keras.layers.Dense(
#                    1, name="value_out", activation=None)(value_2)
#                self.value_model = tf.keras.Model(self.inputs, value_out)
#                return self.value_model(self.prev_input["obs"])
#        else:
#            # this path is hit for DQN
#            return TFModelV2.get_branch_output(self, branch_type, output_spec,
#                                               feature_layer, default_impl)
#
#    def variables(self):
#        # combine any scope variables with our keras model variables
#        var_list = TFModelV2.variables(self) + self.actions_model.variables
#        if self.value_model:
#            self.value_model.extend(self.value_model.variables)
#        return var_list


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    ModelCatalog.register_custom_model("keras_model", MyKerasModel)
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": "CartPole-v0",
            "model": {
                "custom_model": "keras_model"
            },
        })
