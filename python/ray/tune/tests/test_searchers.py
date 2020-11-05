import unittest

import numpy as np

from ray import tune


def _invalid_objective(config):
    # DragonFly uses `point`
    metric = "point" if "point" in config else "report"

    if config[metric] > 4:
        tune.report(float("inf"))
    elif config[metric] > 3:
        tune.report(float("-inf"))
    elif config[metric] > 2:
        tune.report(np.nan)
    else:
        tune.report(float(config[metric]) or 0.1)


class InvalidValuesTest(unittest.TestCase):
    def setUp(self):
        self.config = {"report": tune.uniform(0.0, 5.0)}

    def tearDown(self):
        pass

    def testBayesOpt(self):
        from ray.tune.suggest.bayesopt import BayesOptSearch

        out = tune.run(
            _invalid_objective,
            search_alg=BayesOptSearch(),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testBOHB(self):
        from ray.tune.suggest.bohb import TuneBOHB

        out = tune.run(
            _invalid_objective,
            search_alg=TuneBOHB(),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testDragonfly(self):
        from ray.tune.suggest.dragonfly import DragonflySearch

        out = tune.run(
            _invalid_objective,
            search_alg=DragonflySearch(domain="euclidean", optimizer="random"),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["point"], 2.0)

    def testHyperopt(self):
        from ray.tune.suggest.hyperopt import HyperOptSearch

        out = tune.run(
            _invalid_objective,
            search_alg=HyperOptSearch(),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testNevergrad(self):
        from ray.tune.suggest.nevergrad import NevergradSearch
        import nevergrad as ng

        out = tune.run(
            _invalid_objective,
            search_alg=NevergradSearch(optimizer=ng.optimizers.OnePlusOne),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testOptuna(self):
        from ray.tune.suggest.optuna import OptunaSearch

        out = tune.run(
            _invalid_objective,
            search_alg=OptunaSearch(),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testSkopt(self):
        from ray.tune.suggest.skopt import SkOptSearch

        out = tune.run(
            _invalid_objective,
            search_alg=SkOptSearch(),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=100,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)

    def testZOOpt(self):
        from ray.tune.suggest.zoopt import ZOOptSearch
        np.random.seed(1234)

        out = tune.run(
            _invalid_objective,
            search_alg=ZOOptSearch(budget=8, parallel_num=8),
            config=self.config,
            metric="_metric",
            mode="max",
            num_samples=8,
            reuse_actors=True)

        best_trial = out.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
