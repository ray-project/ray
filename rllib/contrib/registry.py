"""Registry of algorithm names for `rllib train --run=contrib/<alg_name>`"""


def _import_random_agent():
    from ray.rllib.contrib.random_agent.random_agent import RandomAgent

    return RandomAgent, RandomAgent.get_default_config()


def _import_maddpg():
    from ray.rllib.agents.maddpg import maddpg

    return maddpg.MADDPGTrainer, maddpg.DEFAULT_CONFIG


def _import_alphazero():
    from ray.rllib.contrib.alpha_zero.core.alpha_zero_trainer import (
        AlphaZeroTrainer,
        DEFAULT_CONFIG,
    )

    return AlphaZeroTrainer, DEFAULT_CONFIG


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomAgent": _import_random_agent,
    "contrib/AlphaZero": _import_alphazero,
    # Deprecated: Use `MADDPG`, instead.
    "contrib/MADDPG": _import_maddpg,
}
