from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune.registry import register_trainable
from ray.rllib import ppo, es, dqn, a3c
from ray.rllib.agent import _MockAgent, _SigmoidFakeData


def _register_all():
    register_trainable("PPO", ppo.PPOAgent)
    register_trainable("ES", es.ESAgent)
    register_trainable("DQN", dqn.DQNAgent)
    register_trainable("A3C", a3c.A3CAgent)
    register_trainable("__fake", _MockAgent)
    register_trainable("__sigmoid_fake_data", _SigmoidFakeData)


_register_all()
