from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Note: do not introduce unnecessary library dependencies here, e.g. gym
from ray.tune.registry import register_trainable
from ray.rllib.agent import get_agent_class


def _register_all():
    for key in ["PPO", "ES", "DQN", "A3C", "__fake", "__sigmoid_fake_data"]:
        try:
            register_trainable(key, get_agent_class(key))
        except ImportError as e:
            print("Warning: could not import {}: {}".format(key, e))


_register_all()
