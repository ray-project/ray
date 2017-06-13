from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import argparse
import time

import ray
import tensorflow as tf

from reinforce.env import (NoPreprocessor, AtariRamPreprocessor,
                           AtariPixelPreprocessor)
from reinforce.train import train


config = {"kl_coeff": 0.2,
          "num_sgd_iter": 30,
          "max_iterations": 1000,
          "sgd_stepsize": 5e-5,
          # TODO(pcm): Expose the choice between gpus and cpus
          # as a command line argument.
          "devices": ["/cpu:%d" % i for i in range(1)],
          "tf_session_args": {
              "device_count": {"CPU": 1},
              "log_device_placement": False,
              "allow_soft_placement": True,
          },
          "sgd_batchsize": 128,  # total size across all devices
          "entropy_coeff": 0.0,
          "clip_param": 0.3,
          "kl_target": 0.01,
          "timesteps_per_batch": 10000,
          "num_agents": 3,
          "tensorboard_log_dir": "/tmp/ray",
          "full_trace_nth_sgd_batch": -1,
          "full_trace_data_load": False}


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Run the policy gradient "
                                               "algorithm.")
  parser.add_argument("--environment", default="Pong-v0", type=str,
                      help="The gym environment to use.")
  parser.add_argument("--redis-address", default=None, type=str,
                      help="The Redis address of the cluster.")

  args = parser.parse_args()

  ray.init(redis_address=args.redis_address)

  mdp_name = args.environment
  if args.environment == "Pong-v0":
    preprocessor = AtariPixelPreprocessor()
  elif mdp_name == "Pong-ram-v3":
    preprocessor = AtariRamPreprocessor()
  elif mdp_name == "CartPole-v0":
    preprocessor = NoPreprocessor()
  elif mdp_name == "Walker2d-v1":
    preprocessor = NoPreprocessor()
  else:
    print("No environment was chosen, so defaulting to Pong-v0.")
    mdp_name = "Pong-v0"
    preprocessor = AtariPixelPreprocessor()

  train(mdp_name, preprocessor, config)
