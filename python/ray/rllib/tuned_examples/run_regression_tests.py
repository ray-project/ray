#!/usr/bin/env python
# This script runs all the integration tests for RLlib.
# TODO(ekl) add large-scale tests on different envs here.

import glob
import yaml

import ray
from ray.tune import run_experiments

if __name__ == '__main__':
    experiments = {}

    for test in glob.glob("regression_tests/*.yaml"):
        config = yaml.load(open(test).read())
        experiments.update(config)

    print("== Test config ==")
    print(yaml.dump(experiments))

    ray.init()
    trials = run_experiments(experiments)

    num_failures = 0
    for t in trials:
        if (t.last_result["episode_reward_mean"] <
                t.stopping_criterion["episode_reward_mean"]):
            num_failures += 1

    if num_failures:
        raise Exception("{} trials did not converge".format(num_failures))
