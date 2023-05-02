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
from ray.rllib.common import SupportedFileType
from ray.rllib.train import load_experiments_from_file
from ray.rllib.utils.deprecation import deprecation_warning

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["jax", "tf2", "tf", "torch"],
    default="tf",
    help="The deep learning framework to use.",
)
parser.add_argument(
    "--dir",
    type=str,
    required=True,
    help="The directory or file in which to find all tests.",
)
parser.add_argument("--num-cpus", type=int, default=None)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.",
)
parser.add_argument(
    "--override-mean-reward",
    type=float,
    default=0.0,
    help=(
        "Override "
        "the mean reward specified by the yaml file in the stopping criteria. This "
        "is particularly useful for timed tests."
    ),
)

# Obsoleted arg, use --dir instead.
parser.add_argument("--yaml-dir", type=str, default="")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.yaml_dir != "":
        deprecation_warning(old="--yaml-dir", new="--dir", error=False)
        args.dir = args.yaml_dir

    # Bazel regression test mode: Get path to look for yaml files.
    # Get the path or single file to use.
    rllib_dir = Path(__file__).parent.parent
    print(f"rllib dir={rllib_dir}")

    abs_path = os.path.join(rllib_dir, args.dir)
    # Single file given.
    if os.path.isfile(abs_path):
        files = [abs_path]
    # Path given -> Get all yaml files in there via rglob.
    elif os.path.isdir(abs_path):
        files = []
        for type_ in ["yaml", "yml", "py"]:
            files += list(rllib_dir.rglob(args.dir + f"/*.{type_}"))
        files = sorted(map(lambda path: str(path.absolute()), files), reverse=True)
    # Given path/file does not exist.
    else:
        raise ValueError(f"--dir ({args.dir}) not found!")

    print("Will run the following regression tests:")
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
        ), "Error, can only run a single experiment per file!"

        exp = list(experiments.values())[0]
        exp["config"]["framework"] = args.framework

        # Override the mean reward if specified. This is used by the ray ci
        # for overriding the episode reward mean for tf2 tests for off policy
        # long learning tests such as sac and ddpg on the pendulum environment.
        if args.override_mean_reward != 0.0:
            exp["stop"][
                "sampler_results/episode_reward_mean"
            ] = args.override_mean_reward

        # QMIX does not support tf yet -> skip.
        if exp["run"] == "QMIX" and args.framework != "torch":
            print(f"Skipping framework='{args.framework}' for QMIX.")
            continue

        # Always run with eager-tracing when framework=tf2 if not in local-mode.
        # Ignore this if the yaml explicitly tells us to disable eager tracing
        if (
            args.framework == "tf2"
            and not args.local_mode
            and not exp["config"].get("eager_tracing") is False
        ):

            exp["config"]["eager_tracing"] = True

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiments))

        # Try running each test 3 times and make sure it reaches the given
        # reward.
        passed = False
        for i in range(3):
            # Try starting a new ray cluster.
            try:
                ray.init(num_cpus=args.num_cpus, local_mode=args.local_mode)
            # Allow running this script on existing cluster as well.
            except ConnectionError:
                ray.init()
            else:
                try:
                    trials = run_experiments(experiments, resume=False, verbose=2)
                finally:
                    ray.shutdown()
                    _register_all()

            for t in trials:
                # If we have evaluation workers, use their rewards.
                # This is useful for offline learning tests, where
                # we evaluate against an actual environment.
                check_eval = exp["config"].get("evaluation_interval", None) is not None
                reward_mean = (
                    t.last_result["evaluation"]["sampler_results"][
                        "episode_reward_mean"
                    ]
                    if check_eval
                    else (
                        # Some algos don't store sampler results under `sampler_results`
                        # e.g. ARS. Need to keep this logic around for now.
                        t.last_result["sampler_results"]["episode_reward_mean"]
                        if "sampler_results" in t.last_result
                        else t.last_result["episode_reward_mean"]
                    )
                )

                # If we are using evaluation workers, we may have
                # a stopping criterion under the "evaluation/" scope. If
                # not, use `episode_reward_mean`.
                if check_eval:
                    min_reward = t.stopping_criterion.get(
                        "evaluation/sampler_results/episode_reward_mean",
                        t.stopping_criterion.get("sampler_results/episode_reward_mean"),
                    )
                # Otherwise, expect `episode_reward_mean` to be set.
                else:
                    min_reward = t.stopping_criterion.get(
                        "sampler_results/episode_reward_mean"
                    )

                # If min reward not defined, always pass.
                if min_reward is None or reward_mean >= min_reward:
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
