import ray
from ray import tune
from ray.tune.suggest.bayesopt import BayesOptSearch
from ray.tune.stopper import EarlyStopping
from ray.tune.schedulers import AsyncHyperBandScheduler
import unittest


def loss(config, reporter):
    x = config.get("x")
    reporter(loss=x**2)  # A simple function to optimize


class ConvergenceTest(unittest.TestCase):
    """Test convergence in gaussian process."""
    def test_convergence_gaussian_process(self):
        ray.init()

        space = {
            "x": (0, 20)  # This is the space of parameters to explore
        }

        resources_per_trial = {
            "cpu": 1,
            "gpu": 0
        }

        # Following bayesian optimization
        gp = BayesOptSearch(
            space,
            metric="loss",
            mode="min",
            random_search_steps=10
        )

        # Execution of the BO.
        tune.run(
            loss,
            stop=EarlyStopping("loss", mode="min", patience=5),
            search_alg=gp,
            config={},
            num_samples=100,  # Number of iterations
            resources_per_trial=resources_per_trial,
            raise_on_failed_trial=False,
            verbose=0
        )

        ray.shutdown()
