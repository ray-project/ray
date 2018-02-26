#!/usr/bin/env python
# This script runs all the fast integration tests for RLlib.
# It should finish within a few minutes.

import glob
import os
import sys
import yaml

import ray
from ray.tune import run_experiments


if __name__ == '__main__':
    experiments = {}
    fast = len(sys.argv) > 1 and sys.argv[1] == "--fast"

    for test in glob.glob("smoke_test/*.yaml"):
        config = yaml.load(open(test).read())
        if fast:
            for trial in config.values():
                trial["stop"]["training_iteration"] = 1
        experiments.update(config)

    print("== Test config ==")
    print(yaml.dump(experiments))

    ray.init()
    trials = run_experiments(experiments)

    num_failures = 0
    for t in trials:
        if (t.last_result.episode_reward_mean <
                t.stopping_criterion["episode_reward_mean"]):
            num_failures += 1

    if num_failures:
        raise Exception(
            "{} trials did not converge".format(num_failures))
