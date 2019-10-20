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
    from dragonfly.exd.worker_manager import SyntheticWorkerManager
    from dragonfly.utils.euclidean_synthetic_functions import get_syn_func_caller

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
    func_caller = get_syn_func_caller('hartmann6', 
        noise_type='gauss', noise_scale=0.1)
    worker_manager = SyntheticWorkerManager(1, time_distro='const')
    optimizer = EuclideanGPBandit(func_caller, worker_manager)
    algo = DragonflySearch(optimizer,
        max_concurrent=4,
        metric="mean_loss",
        mode="min")
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    run(func_caller.func,
        name="skopt_exp_with_warmstart",
        search_alg=algo,
        scheduler=scheduler,
        **config)
