#!/usr/bin/env python
# Runs one or more regression tests. Retries tests up to 3 times.
#
# Example usage:
# $ python run_regression_tests.py regression-tests/cartpole-es-[tf|torch].yaml
#
# When using in BAZEL (with py_test), e.g. see in ray/rllib/BUILD:
# py_test(
#     name = "run_regression_tests",
#     main = "tests/run_regression_tests.py",
#     tags = ["learning_tests"],
#     size = "medium",  # 5min timeout
#     srcs = ["tests/run_regression_tests.py"],
#     data = glob(["tuned_examples/regression_tests/*.yaml"]),
#     # Pass `BAZEL` option and the path to look for yaml regression files.
#     args = ["BAZEL", "tuned_examples/regression_tests"]
# )

import argparse
import os
from pathlib import Path
import sys
import yaml

import ray
from ray.tune import run_experiments
from ray.rllib import _register_all

parser = argparse.ArgumentParser()
parser.add_argument(
    "--torch",
    action="store_true",
    help="Runs all tests with PyTorch enabled.")
parser.add_argument(
    "--yaml-dir",
    type=str,
    help="The directory in which to find all yamls to test.")

if __name__ == "__main__":
    args = parser.parse_args()

    # Bazel regression test mode: Get path to look for yaml files from argv[2].
    # Get the path or single file to use.
    rllib_dir = Path(__file__).parent.parent
    print("rllib dir={}".format(rllib_dir))

    if not os.path.isdir(os.path.join(rllib_dir, args.yaml_dir)):
        raise ValueError("yaml-dir ({}) not found!".format(args.yaml_dir))

    yaml_files = rllib_dir.rglob(args.yaml_dir + "/*.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True)

    print("Will run the following regression tests:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

    # Loop through all collected files.
    for yaml_file in yaml_files:
        experiments = yaml.load(open(yaml_file).read())
        assert len(experiments) == 1,\
            "Error, can only run a single experiment per yaml file!"

        print("== Test config ==")
        print(yaml.dump(experiments))

        # Add torch option to exp configs.
        for exp in experiments.values():
            if args.torch:
                exp["config"]["framework"] = "torch"

        # Try running each test 3 times and make sure it reaches the given
        # reward.
        passed = False
        for i in range(3):
            try:
                ray.init(num_cpus=5)
                trials = run_experiments(experiments, resume=False, verbose=1)
            finally:
                ray.shutdown()
                _register_all()

            for t in trials:
                if (t.last_result["episode_reward_mean"] >=
                        t.stopping_criterion["episode_reward_mean"]):
                    passed = True
                    break

            if passed:
                print("Regression test PASSED")
                break
            else:
                print("Regression test FAILED on attempt {}", i + 1)

        if not passed:
            print("Overall regression FAILED: Exiting with Error.")
            sys.exit(1)
