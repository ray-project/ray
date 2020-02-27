"""Registry of algorithm names for `rllib train --run=<alg_name>`"""


def _import_random_agent():
    from ray.rllib.contrib.random_agent.random_agent import RandomAgent
    return RandomAgent


def _import_maddpg():
    from ray.rllib.contrib import maddpg
    return maddpg.MADDPGTrainer


def _import_alphazero():
    from ray.rllib.contrib.alpha_zero.core.alpha_zero_trainer import\
        AlphaZeroTrainer
    return AlphaZeroTrainer


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomAgent": _import_random_agent,
    "contrib/MADDPG": _import_maddpg,
    "contrib/AlphaZero": _import_alphazero,
}
