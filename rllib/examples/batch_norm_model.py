from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of using a custom model with batch norm."""

import argparse

import ray
from ray import tune
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--num-iters", type=int, default=200)
parser.add_argument("--run", type=str, default="PPO")


class BatchNormModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        last_layer = input_dict["obs"]
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = tf.layers.dense(
                last_layer,
                size,
                kernel_initializer=normc_initializer(1.0),
                activation=tf.nn.tanh,
                name=label)
            # Add a batch norm layer
            last_layer = tf.layers.batch_normalization(
                last_layer, training=input_dict["is_training"])
        output = tf.layers.dense(
            last_layer,
            num_outputs,
            kernel_initializer=normc_initializer(0.01),
            activation=None,
            name="fc_out")
        return output, last_layer


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model("bn_model", BatchNormModel)
    tune.run(
        args.run,
        stop={"training_iteration": args.num_iters},
        config={
            "env": "Pendulum-v0" if args.run == "DDPG" else "CartPole-v0",
            "model": {
                "custom_model": "bn_model",
            },
            "num_workers": 0,
        },
    )
