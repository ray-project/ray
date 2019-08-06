"""Example of using a custom image env and model.

Both the model and env are trivial (and super-fast), so they are useful
for running perf microbenchmarks.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete, Box
import gym
import numpy as np

import ray
from ray.rllib.models import Model, ModelCatalog
from ray.tune import run_experiments, sample_from
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class FastModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        bias = tf.get_variable(
            dtype=tf.float32,
            name="bias",
            initializer=tf.zeros_initializer,
            shape=())
        output = bias + tf.zeros([tf.shape(input_dict["obs"])[0], num_outputs])
        return output, output


class FastImageEnv(gym.Env):
    def __init__(self, config):
        self.zeros = np.zeros((84, 84, 4))
        self.action_space = Discrete(2)
        self.observation_space = Box(
            0.0, 1.0, shape=(84, 84, 4), dtype=np.float32)
        self.i = 0

    def reset(self):
        self.i = 0
        return self.zeros

    def step(self, action):
        self.i += 1
        return self.zeros, 1, self.i > 1000, {}


if __name__ == "__main__":
    ray.init()
    ModelCatalog.register_custom_model("fast_model", FastModel)
    run_experiments({
        "demo": {
            "run": "IMPALA",
            "env": FastImageEnv,
            "config": {
                "compress_observations": True,
                "model": {
                    "custom_model": "fast_model"
                },
                "num_gpus": 0,
                "num_workers": 2,
                "num_envs_per_worker": 10,
                "num_data_loader_buffers": 1,
                "num_aggregation_workers": 1,
                "broadcast_interval": 50,
                "sample_batch_size": 100,
                "train_batch_size": sample_from(
                    lambda spec: 1000 * max(1, spec.config.num_gpus)),
                "_fake_sampler": True,
            },
        },
    })
