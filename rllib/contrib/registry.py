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


def _import_bandit_lints():
    from ray.rllib.contrib.bandits.agents.lin_ts import LinTSTrainer
    return LinTSTrainer


def _import_bandit_linucb():
    from ray.rllib.contrib.bandits.agents.lin_ucb import LinUCBTrainer
    return LinUCBTrainer


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomAgent": _import_random_agent,
    "contrib/MADDPG": _import_maddpg,
    "contrib/AlphaZero": _import_alphazero,
    "contrib/LinTS": _import_bandit_lints,
    "contrib/LinUCB": _import_bandit_linucb
}
