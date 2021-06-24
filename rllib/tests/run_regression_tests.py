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
from ray.rllib.utils.deprecation import deprecation_warning

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["jax", "tf2", "tf", "tfe", "torch"],
    default="tf",
    help="The deep learning framework to use.")
parser.add_argument(
    "--yaml-dir",
    type=str,
    help="The directory in which to find all yamls to test.")
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.")

# Obsoleted arg, use --framework=torch instead.
parser.add_argument(
    "--torch",
    action="store_true",
    help="Runs all tests with PyTorch enabled.")

if __name__ == "__main__":
    args = parser.parse_args()

    # Bazel regression test mode: Get path to look for yaml files.
    # Get the path or single file to use.
    rllib_dir = Path(__file__).parent.parent
    print("rllib dir={}".format(rllib_dir))

    abs_yaml_path = os.path.join(rllib_dir, args.yaml_dir)
    # Single file given.
    if os.path.isfile(abs_yaml_path):
        yaml_files = [abs_yaml_path]
    # Given path/file does not exist.
    elif not os.path.isdir(abs_yaml_path):
        raise ValueError("yaml-dir ({}) not found!".format(args.yaml_dir))
    # Path given -> Get all yaml files in there via rglob.
    else:
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

        # Add torch option to exp configs.
        for exp in experiments.values():
            exp["config"]["framework"] = args.framework
            if args.torch:
                deprecation_warning(old="--torch", new="--framework=torch")
                exp["config"]["framework"] = "torch"
                args.framework = "torch"

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiments))

        # Try running each test 3 times and make sure it reaches the given
        # reward.
        passed = False
        for i in range(3):
            try:
                ray.init(num_cpus=5, local_mode=args.local_mode)
                trials = run_experiments(experiments, resume=False, verbose=2)
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
                print("Regression test FAILED on attempt {}".format(i + 1))

        if not passed:
            print("Overall regression FAILED: Exiting with Error.")
            sys.exit(1)
