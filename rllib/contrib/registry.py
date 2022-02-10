"""Registry of algorithm names for `rllib train --run=contrib/<alg_name>`"""

from ray.rllib.utils.deprecation import Deprecated


def _import_random_agent():
    from ray.rllib.contrib.random_agent.random_agent import RandomAgent

    return RandomAgent, RandomAgent.get_default_config()


def _import_maddpg():
    from ray.rllib.contrib import maddpg

    return maddpg.MADDPGTrainer, maddpg.DEFAULT_CONFIG


def _import_alphazero():
    from ray.rllib.contrib.alpha_zero.core.alpha_zero_trainer import (
        AlphaZeroTrainer,
        DEFAULT_CONFIG,
    )

    return AlphaZeroTrainer, DEFAULT_CONFIG


def _import_bandit_lints():
    from ray.rllib.agents.bandit.bandit import BanditLinTSTrainer

    @Deprecated(old="contrib/LinTS", new="BanditLinTS", error=True)
    class _DeprecatedBandit(BanditLinTSTrainer):
        pass

    return _DeprecatedBandit, BanditLinTSTrainer.get_default_config()


def _import_bandit_linucb():
    from ray.rllib.agents.bandit.bandit import BanditLinUCBTrainer

    @Deprecated(old="contrib/LinUCB", new="BanditLinUCB", error=True)
    class _DeprecatedBandit(BanditLinUCBTrainer):
        pass

    return _DeprecatedBandit, BanditLinUCBTrainer.get_default_config()


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomAgent": _import_random_agent,
    "contrib/MADDPG": _import_maddpg,
    "contrib/AlphaZero": _import_alphazero,
    # Deprecated: Use BanditLin[TS|UCB], instead.
    "contrib/LinTS": _import_bandit_lints,
    "contrib/LinUCB": _import_bandit_linucb,
}
