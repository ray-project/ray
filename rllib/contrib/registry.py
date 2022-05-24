"""Registry of algorithm names for `rllib train --run=contrib/<alg_name>`"""


def _import_random_agent():
    from ray.rllib.contrib.random_agent.random_agent import RandomAgent

    return RandomAgent, RandomAgent.get_default_config()


def _import_alphazero():
    from ray.rllib.algorithms.alpha_zero.alpha_zero import (
        AlphaZeroTrainer,
        DEFAULT_CONFIG,
    )

    return AlphaZeroTrainer, DEFAULT_CONFIG


def _import_maddpg():
    from ray.rllib.algorithms.maddpg import maddpg

    return maddpg.MADDPGTrainer, maddpg.DEFAULT_CONFIG


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomAgent": _import_random_agent,
    # Deprecated: Use `MADDPG` and `AlphaZero`, instead.
    "contrib/MADDPG": _import_maddpg,
    "contrib/AlphaZero": _import_alphazero,
}
