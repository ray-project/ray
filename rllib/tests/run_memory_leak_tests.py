#!/usr/bin/env python
# Runs one or more memory leak tests.
#
# Example usage:
# $ python run_memory_leak_tests.py memory-leak-test-ppo.yaml
#
# When using in BAZEL (with py_test), e.g. see in ray/rllib/BUILD:
# py_test(
#     name = "memory_leak_ppo",
#     main = "tests/test_memory_leak.py",
#     tags = ["memory_leak_tests"],
#     size = "medium",  # 5min timeout
#     srcs = ["tests/test_memory_leak.py"],
#     data = glob(["tuned_examples/ppo/*.yaml"]),
#     # Pass `BAZEL` option and the path to look for yaml files.
#     args = ["BAZEL", "tuned_examples/ppo/memory-leak-test-ppo.yaml"]
# )

import argparse
import numpy as np
import os
from pathlib import Path
import sys
import yaml

import ray
from ray.tune import run_experiments
from ray.rllib import _register_all

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
parser.add_argument(
    "--check-iters",
    type=int,
    default=10,
    help="The number of past iters to search through for memory leaks.")

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

    print("Will run the following memory-leak tests:")
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

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiments))

        # Try running each test 3 times and make sure it reaches the given
        # reward.
        leaking = True
        try:
            ray.init(num_cpus=5, local_mode=args.local_mode)
            available_memory = ray.cluster_resources()["memory"]
            trials = run_experiments(experiments, resume=False, verbose=2)
        finally:
            ray.shutdown()
            _register_all()

        # How many Mb are we expected to have used during the run?
        mb_usage_threshold = 500
        std_mem_percent_threshold = 0.5

        for trial in trials:
            # Simple check: Compare 3rd entry with last one.
            mem_series = trial.metric_n_steps["perf/ram_util_percent"]["10"]
            mem_series = list(mem_series)[-args.check_iters:]
            std_dev_mem = np.std(mem_series)
            total_used = (
                mem_series[-1] - mem_series[0]) / 100 * available_memory / 1e6
            print(f"trial {trial}")
            print(f" -> mem consumption % stddev "
                  f"over last 10 iters={std_dev_mem} "
                  f"(must be smaller {std_mem_percent_threshold})")
            print(f" -> total mem used "
                  f"over last 10 iters={total_used} "
                  f"(must be smaller {mb_usage_threshold})")
            if std_dev_mem < std_mem_percent_threshold and \
                    total_used < mb_usage_threshold:
                leaking = False
                break

        if not leaking:
            print("Memory leak test PASSED")
            break
        else:
            print("Memory leak test FAILED. Exiting with Error.")
            sys.exit(1)
