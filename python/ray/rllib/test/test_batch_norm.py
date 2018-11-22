from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of using a custom model with batch norm."""

import argparse

import tensorflow as tf
import tensorflow.contrib.slim as slim

import ray
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.misc import normc_initializer
from ray.tune import run_experiments

parser = argparse.ArgumentParser()
parser.add_argument("--num-iters", type=int, default=200)
parser.add_argument("--run", type=str, default="PPO")


class BatchNormModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        last_layer = input_dict["obs"]
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = slim.fully_connected(
                last_layer,
                size,
                weights_initializer=normc_initializer(1.0),
                activation_fn=tf.nn.tanh,
                scope=label)
            # Add a batch norm layer
            last_layer = tf.layers.batch_normalization(
                last_layer, training=input_dict["is_training"])
        output = slim.fully_connected(
            last_layer,
            num_outputs,
            weights_initializer=normc_initializer(0.01),
            activation_fn=None,
            scope="fc_out")
        return output, last_layer


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model("bn_model", BatchNormModel)
    run_experiments({
        "batch_norm_demo": {
            "run": args.run,
            "env": "Pendulum-v0" if args.run == "DDPG" else "CartPole-v0",
            "stop": {
                "training_iteration": args.num_iters
            },
            "config": {
                "model": {
                    "custom_model": "bn_model",
                },
                "num_workers": 0,
            },
        },
    })
