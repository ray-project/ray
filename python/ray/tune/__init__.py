from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.tune.error import TuneError
from ray.tune.tune import run_experiments
from ray.tune.experiment import Experiment
from ray.tune.registry import register_env, register_trainable
from ray.tune.trainable import Trainable
from ray.tune.suggest import grid_search, function, sample_from


def _setup_logger():
    logger = logging.getLogger("ray.tune")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        ))
    logger.addHandler(handler)
    logger.propagate = False


_setup_logger()

__all__ = [
    "Trainable",
    "TuneError",
    "grid_search",
    "register_env",
    "register_trainable",
    "run_experiments",
    "Experiment",
    "function",
    "sample_from",
]
