"""Wrap Google's RecSim environment for RLlib

RecSim is a configurable recommender systems simulation platform.
Source: https://github.com/google-research/recsim
"""

from recsim.environments import long_term_satisfaction
from ray.tune.registry import register_env


def _make_recsim_env(config):
    DEFAULT_ENV_CONFIG = {
        "num_candidates": 10,
        "slate_size": 2,
        "resample_documents": True,
        "seed": 0,
        "convert_to_discrete_action_space": False,
    }
    env_config = DEFAULT_ENV_CONFIG.copy()
    env_config.update(config)
    env = long_term_satisfaction.create_environment(env_config)
    env = RecSimResetWrapper(env)
    env = RecSimObservationSpaceWrapper(env)
    if env_config and env_config["convert_to_discrete_action_space"]:
        env = MultiDiscreteToDiscreteActionWrapper(env)
    return env


MultiAgentCartPole = make_recsim_env(make_recsim_env)
