from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable


def _register_all():
    for key in ["PPO", "ES", "DQN", "A3C", "BC", "__fake",
                "__sigmoid_fake_data", "__parameter_tuning"]:
        try:
            from ray.rllib.agent import get_agent_class
            register_trainable(key, get_agent_class(key))
        except ImportError as e:
            print("Warning: could not import {}: {}".format(key, e))


_register_all()
