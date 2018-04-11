from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments, register_trainable
from ray.tune.hpo_scheduler import HyperOptScheduler


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    reporter(
        timesteps_total=1,
        episode_reward_mean=-(
            (config["height"] - 14)**2 + abs(config["width"] - 3)))
    time.sleep(0.2)


if __name__ == '__main__':
    import argparse
    from hyperopt import hp

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(redirect_output=True)

    register_trainable("exp", easy_objective)

    space = {
        'width': hp.uniform('width', 0, 20),
        'height': hp.uniform('height', -100, 100),
    }

    config = {
        "my_exp": {
            "run": "exp",
            "repeat": 5 if args.smoke_test else 1000,
            "stop": {
                "training_iteration": 1
            },
            "config": {
                "space": space
            }
        }
    }
    hpo_sched = HyperOptScheduler()

    run_experiments(config, verbose=False, scheduler=hpo_sched)
