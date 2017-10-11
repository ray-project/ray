from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib import a3c, dqn, es, ppo


def get_agent_class(alg):
    if alg == "PPO":
        return ppo.PPOAgent
    elif alg == "ES":
        return es.ESAgent
    elif alg == "DQN":
        return dqn.DQNAgent
    elif alg == "A3C":
        return a3c.A3CAgent
    else:
        raise Exception(
            ("Unknown algorithm {}, check --alg argument. Valid choices " +
             "are PPO, ES, DQN, and A3C.").format(alg))
