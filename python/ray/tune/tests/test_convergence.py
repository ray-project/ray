import math
import numpy as np

import ray
from ray import tune
from ray.tune.stopper import ExperimentPlateauStopper
from ray.tune.suggest import ConcurrencyLimiter
import unittest


def loss(config, reporter):
    x = config.get("x")
    reporter(loss=x**2)  # A simple function to optimize


class ConvergenceTest(unittest.TestCase):
    """Test convergence in gaussian process."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(local_mode=False, num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _testConvergence(self, searcher, top=3, patience=20):
        # This is the space of parameters to explore
        space = {"x": tune.uniform(0, 20)}

        resources_per_trial = {"cpu": 1, "gpu": 0}

        analysis = tune.run(
            loss,
            metric="loss",
            mode="min",
            stop=ExperimentPlateauStopper(
                metric="loss", top=top, patience=patience),
            search_alg=searcher,
            config=space,
            num_samples=100,  # Number of iterations
            resources_per_trial=resources_per_trial,
            raise_on_failed_trial=False,
            fail_fast=True,
            reuse_actors=True,
            verbose=1)
        print(f"Num trials: {len(analysis.trials)}. "
              f"Best result: {analysis.best_config['x']}")

        return analysis

    def testConvergenceAx(self):
        from ray.tune.suggest.ax import AxSearch

        np.random.seed(0)

        searcher = AxSearch()
        analysis = self._testConvergence(searcher, patience=10)

        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-5)

    def testConvergenceBayesOpt(self):
        from ray.tune.suggest.bayesopt import BayesOptSearch

        np.random.seed(0)

        # Following bayesian optimization
        searcher = BayesOptSearch(random_search_steps=10)
        searcher.repeat_float_precision = 5
        searcher = ConcurrencyLimiter(searcher, 1)

        analysis = self._testConvergence(searcher, patience=100)

        assert len(analysis.trials) < 50
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-5)

    def testConvergenceDragonfly(self):
        from ray.tune.suggest.dragonfly import DragonflySearch

        np.random.seed(0)
        searcher = DragonflySearch(domain="euclidean", optimizer="bandit")
        analysis = self._testConvergence(searcher)

        assert len(analysis.trials) < 100
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-5)

    def testConvergenceHEBO(self):
        from ray.tune.suggest.hebo import HEBOSearch

        np.random.seed(0)
        searcher = HEBOSearch()
        analysis = self._testConvergence(searcher)

        assert len(analysis.trials) < 100
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-2)

    def testConvergenceHyperopt(self):
        from ray.tune.suggest.hyperopt import HyperOptSearch

        np.random.seed(0)
        searcher = HyperOptSearch(random_state_seed=1234)
        analysis = self._testConvergence(searcher, patience=50, top=5)

        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-2)

    def testConvergenceNevergrad(self):
        from ray.tune.suggest.nevergrad import NevergradSearch
        import nevergrad as ng

        np.random.seed(0)
        searcher = NevergradSearch(optimizer=ng.optimizers.PSO)
        analysis = self._testConvergence(searcher, patience=50, top=5)

        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-3)

    def testConvergenceOptuna(self):
        from ray.tune.suggest.optuna import OptunaSearch

        np.random.seed(1)
        searcher = OptunaSearch()
        analysis = self._testConvergence(
            searcher,
            top=5,
        )

        # This assertion is much weaker than in the BO case, but TPE
        # don't converge too close. It is still unlikely to get to this
        # tolerance with random search (~0.01% chance)
        assert len(analysis.trials) < 100
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-2)

    def testConvergenceSkOpt(self):
        from ray.tune.suggest.skopt import SkOptSearch

        np.random.seed(0)
        searcher = SkOptSearch()
        analysis = self._testConvergence(searcher)

        assert len(analysis.trials) < 100
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-3)

    def testConvergenceZoopt(self):
        from ray.tune.suggest.zoopt import ZOOptSearch

        np.random.seed(0)
        searcher = ZOOptSearch(budget=100)
        analysis = self._testConvergence(searcher)

        assert len(analysis.trials) < 100
        assert math.isclose(analysis.best_config["x"], 0, abs_tol=1e-3)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
