from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.common import Agent
from ray.tune.result import TrainingResult


class _MockAgent(Agent):
    """Mock agent for use in tests"""

    _agent_name = "MockAgent"
    _default_config = {}

    def _init(self):
        pass

    def _train(self):
        return TrainingResult(
            episode_reward_mean=10, episode_len_mean=10,
            timesteps_this_iter=10, info={})


def get_agent_class(alg):
    """Returns the class of an known agent given its name."""

    if alg == "PPO":
        from ray.rllib import ppo
        return ppo.PPOAgent
    elif alg == "ES":
        from ray.rllib import es
        return es.ESAgent
    elif alg == "DQN":
        from ray.rllib import dqn
        return dqn.DQNAgent
    elif alg == "A3C":
        from ray.rllib import a3c
        return a3c.A3CAgent
    elif alg == "script":
        from ray.tune import script_runner
        return script_runner._ScriptRunner
    elif alg == "__fake":
        return _MockAgent
    else:
        raise Exception(
            ("Unknown algorithm {}, check --alg argument. Valid choices " +
             "are PPO, ES, DQN, and A3C.").format(alg))
