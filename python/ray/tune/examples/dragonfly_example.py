"""This test checks that Dragonfly is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.dragonfly import DragonflySearch

if __name__ == "__main__":
    import argparse
    from dragonfly.opt.gp_bandit import EuclideanGPBandit
    from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
    from dragonfly import load_config

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    config = {
        "num_samples": 10 if args.smoke_test else 50,
        "config": {
            "iterations": 100,
        },
        "stop": {
            "timesteps_total": 100
        },
    }

    domain_vars = [{'name': 'x', 'type': 'float', 'min': 0, 'max': 1, 'dim': 3}]
    domain_constraints = [
        {'name': 'quadrant', 'constraint': 'np.linalg.norm(x[0:2]) <= 0.5'},
    ]

    config_params = {'domain': domain_vars, 'domain_constraints': domain_constraints}
    config = load_config(config_params)

    func_caller = EuclideanFunctionCaller(None, config.domain)
    optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)
    algo = DragonflySearch(
        optimizer, max_concurrent=4, metric="mean_loss", mode="min")
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    run(func_caller.func,
        name="skopt_exp_with_warmstart",
        search_alg=algo,
        scheduler=scheduler,
        **config)
