# @OldAPIStack
"""Example of using a custom ModelV2 Keras-style model."""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.distributional_q_tf_model import DistributionalQTFModel
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.visionnet import VisionNetwork as MyVisionNetwork
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.tune.registry import get_trainable_cls

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="DQN", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--stop", type=int, default=200)
parser.add_argument("--use-vision-network", action="store_true")
parser.add_argument("--num-cpus", type=int, default=0)


class MyKerasModel(TFModelV2):
    """Custom model for policy gradient algorithms."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super(MyKerasModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name
        )
        self.inputs = tf.keras.layers.Input(shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            128,
            name="my_layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0),
        )(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="my_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01),
        )(layer_1)
        value_out = tf.keras.layers.Dense(
            1,
            name="value_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01),
        )(layer_1)
        self.base_model = tf.keras.Model(self.inputs, [layer_out, value_out])

    def forward(self, input_dict, state, seq_lens):
        model_out, self._value_out = self.base_model(input_dict["obs"])
        return model_out, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    def metrics(self):
        return {"foo": tf.constant(42.0)}


class MyKerasQModel(DistributionalQTFModel):
    """Custom model for DQN."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name, **kw):
        super(MyKerasQModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name, **kw
        )

        # Define the core model layers which are used by the other
        # output heads of DistributionalQModel
        self.inputs = tf.keras.layers.Input(shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            128,
            name="my_layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0),
        )(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="my_out",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0),
        )(layer_1)
        self.base_model = tf.keras.Model(self.inputs, layer_out)

    # Implement the core forward method.
    def forward(self, input_dict, state, seq_lens):
        model_out = self.base_model(input_dict["obs"])
        return model_out, state

    def metrics(self):
        return {"foo": tf.constant(42.0)}


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model(
        "keras_model", MyVisionNetwork if args.use_vision_network else MyKerasModel
    )
    ModelCatalog.register_custom_model(
        "keras_q_model", MyVisionNetwork if args.use_vision_network else MyKerasQModel
    )

    # Tests https://github.com/ray-project/ray/issues/7293
    class MyCallbacks(DefaultCallbacks):
        def on_train_result(self, *, algorithm, result, **kwargs):
            r = result["result"]["info"][LEARNER_INFO]
            if DEFAULT_POLICY_ID in r:
                r = r[DEFAULT_POLICY_ID].get(LEARNER_STATS_KEY, r[DEFAULT_POLICY_ID])
            assert r["model"]["foo"] == 42, result

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment("ALE/Breakout-v5" if args.use_vision_network else "CartPole-v1")
        .framework("tf")
        .callbacks(MyCallbacks)
        .training(
            model={
                "custom_model": "keras_q_model" if args.run == "DQN" else "keras_model"
            }
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    if args.run == "DQN":
        config = (
            DQNConfig()
            .update_from_dict(config.to_dict())
            .training(num_steps_sampled_before_learning_starts=0)
        )

    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop,
    }

    tuner = tune.Tuner(
        args.run,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
    )
    tuner.fit()
