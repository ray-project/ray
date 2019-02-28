#!/usr/bin/env python
# Runs one or more regression tests. Retries tests up to 3 times.
#
# Example usage:
#   ./run_regression_tests.sh regression-tests/cartpole-es.yaml

import yaml
import sys

import ray
from ray.tune import run_experiments

if __name__ == "__main__":

    ray.init()

    for test in sys.argv[1:]:
        experiments = yaml.load(open(test).read())

        print("== Test config ==")
        print(yaml.dump(experiments))

        for i in range(3):
            trials = run_experiments(experiments, resume=False)

            num_failures = 0
            for t in trials:
                if (t.last_result["episode_reward_mean"] <
                        t.stopping_criterion["episode_reward_mean"]):
                    num_failures += 1

            if not num_failures:
                print("Regression test PASSED")
                sys.exit(0)

            print("Regression test flaked, retry", i)

        print("Regression test FAILED")
        sys.exit(1)
