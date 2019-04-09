"""This test checks that GeneticSearch is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.automl import GeneticSearch
from ray.tune.automl import ContinuousSpace, DiscreteSpace, SearchSpace


def michalewicz_function(config, reporter):
    """f(x) = -sum{sin(xi) * [sin(i*xi^2 / pi)]^(2m)}"""
    import numpy as np
    x = np.array(
        [config['x1'], config['x2'], config['x3'], config['x4'], config['x5']])
    sin_x = np.sin(x)
    z = (np.arange(1, 6) / np.pi * (x * x))
    sin_z = np.power(np.sin(z), 20)  # let m = 20
    y = np.dot(sin_x, sin_z)

    # Negate y since we want to minimize y value
    reporter(timesteps_total=1, neg_mean_loss=-y)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    space = SearchSpace({
        ContinuousSpace('x1', 0, 4, 100),
        ContinuousSpace('x2', -2, 2, 100),
        ContinuousSpace('x3', 1, 5, 100),
        ContinuousSpace('x4', -3, 3, 100),
        DiscreteSpace('x5', [-1, 0, 1, 2, 3]),
    })

    config = {"stop": {"training_iteration": 100}}
    algo = GeneticSearch(
        space,
        reward_attr="neg_mean_loss",
        max_generation=2 if args.smoke_test else 10,
        population_size=10 if args.smoke_test else 50)
    scheduler = AsyncHyperBandScheduler(reward_attr="neg_mean_loss")
    run(michalewicz_function,
        name="my_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
