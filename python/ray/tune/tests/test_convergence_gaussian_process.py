import numpy as np

import ray
from ray import tune
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.suggest import ConcurrencyLimiter
import unittest


def loss(config, reporter):
    x = config.get("x")
    reporter(loss=x**2)  # A simple function to optimize


class ConvergenceTest(unittest.TestCase):
    """Test convergence in gaussian process."""

    def test_convergence_gaussian_process(self):
        np.random.seed(0)
        ray.init(local_mode=True, num_cpus=1, num_gpus=1)

        space = {
            "x": (0, 20)  # This is the space of parameters to explore
        }

        resources_per_trial = {"cpu": 1, "gpu": 0}

        # Following bayesian optimization
        gp = BayesOptSearch(
            space, metric="loss", mode="min", random_search_steps=10)
        gp.repeat_float_precision = 5
        gp = ConcurrencyLimiter(gp, 1)

        # Execution of the BO.
        analysis = tune.run(
            loss,
            # stop=EarlyStopping("loss", mode="min", patience=5),
            search_alg=gp,
            config={},
            num_samples=100,  # Number of iterations
            resources_per_trial=resources_per_trial,
            raise_on_failed_trial=False,
            fail_fast=True,
            verbose=1)
        assert len(analysis.trials) == 41

        ray.shutdown()
