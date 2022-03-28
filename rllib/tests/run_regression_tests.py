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
    help="The deep learning framework to use.",
)
parser.add_argument(
    "--yaml-dir",
    type=str,
    required=True,
    help="The directory in which to find all yamls to test.",
)
parser.add_argument("--num-cpus", type=int, default=6)
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
# Obsoleted arg, use --framework=torch instead.
parser.add_argument(
    "--torch", action="store_true", help="Runs all tests with PyTorch enabled."
)

if __name__ == "__main__":
    args = parser.parse_args()

    # Error if deprecated --torch option used.
    if args.torch:
        deprecation_warning(old="--torch", new="--framework=torch", error=True)

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
            map(lambda path: str(path.absolute()), yaml_files), reverse=True
        )

    print("Will run the following regression tests:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

    # Loop through all collected files.
    for yaml_file in yaml_files:
        experiments = yaml.safe_load(open(yaml_file).read())
        assert (
            len(experiments) == 1
        ), "Error, can only run a single experiment per yaml file!"

        exp = list(experiments.values())[0]
        exp["config"]["framework"] = args.framework

        # Override the mean reward if specified. This is used by the ray ci
        # for overriding the episode reward mean for tf2 tests for off policy
        # long learning tests such as sac and ddpg on the pendulum environment.
        if args.override_mean_reward != 0.0:
            exp["stop"]["episode_reward_mean"] = args.override_mean_reward

        # QMIX does not support tf yet -> skip.
        if exp["run"] == "QMIX" and args.framework != "torch":
            print(f"Skipping framework='{args.framework}' for QMIX.")
            continue

        # Always run with eager-tracing when framework=tf2 if not in local-mode.
        if args.framework in ["tf2", "tfe"] and not args.local_mode:
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
                    t.last_result["evaluation"]["episode_reward_mean"]
                    if check_eval
                    else t.last_result["episode_reward_mean"]
                )

                # If we are using evaluation workers, we may have
                # a stopping criterion under the "evaluation/" scope. If
                # not, use `episode_reward_mean`.
                if check_eval:
                    min_reward = t.stopping_criterion.get(
                        "evaluation/episode_reward_mean",
                        t.stopping_criterion.get("episode_reward_mean"),
                    )
                # Otherwise, expect `episode_reward_mean` to be set.
                else:
                    min_reward = t.stopping_criterion.get("episode_reward_mean")

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
