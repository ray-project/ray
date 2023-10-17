import logging

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.util import log_once


def rllib_contrib_warning(algo_str):
    if log_once(f"{algo_str}_contrib"):
        logging.getLogger(__name__).warning(
            "{} has/have been moved to `rllib_contrib` and will no longer be maintained"
            " by the RLlib team. You can still use it/them normally inside RLlib util "
            "Ray 2.8, but from Ray 2.9 on, all `rllib_contrib` algorithms will no "
            "longer be part of the core repo, and will therefore have to be installed "
            "separately with pinned dependencies for e.g. ray[rllib] and other "
            "packages! See "
            "https://github.com/ray-project/ray/tree/master/rllib_contrib#rllib-contrib"
            " for more information on the RLlib contrib effort.".format(algo_str)
        )


__all__ = [
    "Algorithm",
    "AlgorithmConfig",
]
