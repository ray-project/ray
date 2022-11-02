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
import os
from pathlib import Path
import sys
import yaml

import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.debug.memory import check_memory_leaks

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    required=False,
    choices=["jax", "tf2", "tf", "torch", None],
    default=None,
    help="The deep learning framework to use.",
)
parser.add_argument(
    "--yaml-dir",
    required=True,
    type=str,
    help="The directory in which to find all yamls to test.",
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.",
)
parser.add_argument(
    "--to-check",
    nargs="+",
    default=["env", "policy", "rollout_worker"],
    help="List of 'env', 'policy', 'rollout_worker', 'model'.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Bazel regression test mode: Get path to look for yaml files.
    # Get the path or single file to use.
    rllib_dir = Path(__file__).parent.parent.parent
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
            map(lambda path: str(path.absolute()), yaml_files), reverse=True
        )

    print("Will run the following memory-leak tests:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

    # Loop through all collected files.
    for yaml_file in yaml_files:
        experiments = yaml.safe_load(open(yaml_file).read())
        assert (
            len(experiments) == 1
        ), "Error, can only run a single experiment per yaml file!"

        experiment = list(experiments.values())[0]

        # Add framework option to exp configs.
        if args.framework:
            experiment["config"]["framework"] = args.framework
        # Create env on local_worker for memory leak testing just the env.
        experiment["config"]["create_env_on_driver"] = True
        # Always run with eager-tracing when framework=tf2 if not in local-mode.
        if args.framework == "tf2" and not args.local_mode:
            experiment["config"]["eager_tracing"] = True
        # experiment["config"]["callbacks"] = MemoryTrackingCallbacks

        # Move "env" specifier into config.
        experiment["config"]["env"] = experiment["env"]
        experiment.pop("env", None)

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiment))

        # Construct the trainer instance based on the given config.
        leaking = True
        try:
            ray.init(num_cpus=5, local_mode=args.local_mode)
            trainer = get_algorithm_class(experiment["run"])(experiment["config"])
            results = check_memory_leaks(
                trainer,
                to_check=set(args.to_check),
            )
            if not results:
                leaking = False
        finally:
            ray.shutdown()

        if not leaking:
            print("Memory leak test PASSED")
        else:
            print("Memory leak test FAILED. Exiting with Error.")
            sys.exit(1)
