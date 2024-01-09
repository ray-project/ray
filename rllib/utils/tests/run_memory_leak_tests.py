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
from ray.rllib.common import SupportedFileType
from ray.rllib.train import load_experiments_from_file
from ray.rllib.utils.debug.memory import check_memory_leaks
from ray.rllib.utils.deprecation import deprecation_warning
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    required=False,
    choices=["jax", "tf2", "tf", "torch", None],
    default=None,
    help="The deep learning framework to use.",
)
parser.add_argument(
    "--dir",
    type=str,
    required=True,
    help="The directory or file in which to find all tests.",
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.",
)
parser.add_argument(
    "--to-check",
    nargs="+",
    default=["env", "policy", "rollout_worker", "learner"],
    help="List of 'env', 'policy', 'rollout_worker', 'model', 'learner'.",
)

# Obsoleted arg, use --dir instead.
parser.add_argument("--yaml-dir", type=str, default="")


if __name__ == "__main__":
    args = parser.parse_args()

    if args.yaml_dir != "":
        deprecation_warning(old="--yaml-dir", new="--dir", error=True)

    # Bazel regression test mode: Get path to look for yaml files.
    # Get the path or single file to use.
    rllib_dir = Path(__file__).parent.parent.parent
    print("rllib dir={}".format(rllib_dir))

    abs_path = os.path.join(rllib_dir, args.dir)
    # Single file given.
    if os.path.isfile(abs_path):
        files = [abs_path]
    # Path given -> Get all py/yaml files in there via rglob.
    elif os.path.isdir(abs_path):
        files = []
        for type_ in ["yaml", "yml", "py"]:
            files += list(rllib_dir.rglob(args.dir + f"/*.{type_}"))
        files = sorted(map(lambda path: str(path.absolute()), files), reverse=True)
    # Given path/file does not exist.
    else:
        raise ValueError(f"--dir ({args.dir}) not found!")

    print("Will run the following memory-leak tests:")
    for file in files:
        print("->", file)

    # Loop through all collected files.
    for file in files:
        # For python files, need to make sure, we only deliver the module name into the
        # `load_experiments_from_file` function (everything from "/ray/rllib" on).
        if file.endswith(".py"):
            if file.endswith("__init__.py"):  # weird CI learning test (BAZEL) case
                continue
            experiments = load_experiments_from_file(file, SupportedFileType.python)
        else:
            experiments = load_experiments_from_file(file, SupportedFileType.yaml)

        assert (
            len(experiments) == 1
        ), "Error, can only run a single experiment per yaml file!"

        experiment = list(experiments.values())[0]

        # Add framework option to exp configs.
        if args.framework:
            experiment["config"]["framework"] = args.framework
        # Create env on local_worker for memory leak testing just the env.
        experiment["config"]["create_env_on_driver"] = True
        # experiment["config"]["callbacks"] = MemoryTrackingCallbacks

        # Move "env" specifier into config.
        experiment["config"]["env"] = experiment["env"]
        experiment.pop("env", None)

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiment))

        # Construct the Algorithm instance based on the given config.
        leaking = True
        try:
            ray.init(num_cpus=5, local_mode=args.local_mode)
            if isinstance(experiment["run"], str):
                algo_cls = get_trainable_cls(experiment["run"])
            else:
                algo_cls = get_trainable_cls(experiment["run"].__name__)
            algo = algo_cls(experiment["config"])
            results = check_memory_leaks(algo, to_check=set(args.to_check))
            if not results:
                leaking = False
        finally:
            ray.shutdown()

        if not leaking:
            print("Memory leak test PASSED")
        else:
            print("Memory leak test FAILED. Exiting with Error.")
            sys.exit(1)
