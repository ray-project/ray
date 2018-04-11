from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable


def _register_all():
    for key in ["PPO", "ES", "DQN", "APEX", "A3C", "BC", "PG", "DDPG",
                "__fake", "__sigmoid_fake_data", "__parameter_tuning"]:
        from ray.rllib.agent import get_agent_class
        register_trainable(key, get_agent_class(key))


_register_all()
