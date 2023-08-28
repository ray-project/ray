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
import re
import yaml

import ray
from ray import air
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib import _register_all
from ray.rllib.common import SupportedFileType
from ray.rllib.train import load_experiments_from_file
from ray.rllib.utils.deprecation import deprecation_warning
from ray.tune import run_experiments

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    type=str,
    choices=["torch", "tf2", "tf"],
    default=None,
    help="The deep learning framework to use. If not provided, try using the one "
    "specified in the file, otherwise, use RLlib's default: `torch`.",
)
parser.add_argument(
    "--dir",
    type=str,
    required=True,
    help="The directory or file in which to find all tests.",
)
parser.add_argument(
    "--env",
    type=str,
    default=None,
    help="An optional env override setting. If not provided, try using the one "
    "specified in the file.",
)
parser.add_argument("--num-cpus", type=int, default=None)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.",
)
parser.add_argument(
    "--num-samples",
    type=int,
    default=1,
    help="The number of seeds/samples to run with the given experiment config.",
)
parser.add_argument(
    "--override-mean-reward",
    type=float,
    default=0.0,
    help=(
        "Override the mean reward specified by the yaml file in the stopping criteria. "
        "This is particularly useful for timed tests."
    ),
)
parser.add_argument(
    "--verbose",
    type=int,
    default=2,
    help="The verbosity level for the main `tune.run_experiments()` call.",
)
parser.add_argument(
    "--wandb-key",
    type=str,
    default=None,
    help="The WandB API key to use for uploading results.",
)
parser.add_argument(
    "--wandb-project",
    type=str,
    default=None,
    help="The WandB project name to use.",
)
parser.add_argument(
    "--wandb-run-name",
    type=str,
    default=None,
    help="The WandB run name to use.",
)
# parser.add_argument(
#    "--wandb-from-checkpoint",
#    type=str,
#    default=None,
#    help=(
#        "The WandB checkpoint location (e.g. `[team name]/[project name]/checkpoint_"
#        "[run name]:v[version]`) from which to resume an experiment."
#    ),
# )
parser.add_argument(
    "--checkpoint-freq",
    type=int,
    default=0,
    help=(
        "The frequency (in training iterations) with which to create checkpoints. "
        "Note that if --wandb-key is provided, these checkpoints will automatically "
        "be uploaded to WandB."
    ),
)

# Obsoleted arg, use --dir instead.
parser.add_argument("--yaml-dir", type=str, default="")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.yaml_dir != "":
        deprecation_warning(old="--yaml-dir", new="--dir", error=True)

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
        config_is_python = False
        # For python files, need to make sure, we only deliver the module name into the
        # `load_experiments_from_file` function (everything from "/ray/rllib" on).
        if file.endswith(".py"):
            if file.endswith("__init__.py"):  # weird CI learning test (BAZEL) case
                continue
            experiments = load_experiments_from_file(file, SupportedFileType.python)
            config_is_python = True
        else:
            experiments = load_experiments_from_file(file, SupportedFileType.yaml)

        assert (
            len(experiments) == 1
        ), "Error, can only run a single experiment per file!"

        exp = list(experiments.values())[0]
        exp_name = list(experiments.keys())[0]

        # Set the number of samples to run.
        exp["num_samples"] = args.num_samples

        # Make sure there is a config and a stopping criterium.
        exp["config"] = exp.get("config", {})
        exp["stop"] = exp.get("stop", {})

        # Override framework setting with the command line one, if provided.
        # Otherwise, will use framework setting in file (or default: torch).
        if args.framework is not None:
            exp["config"]["framework"] = args.framework
        # Override env setting if given on command line.
        if args.env is not None:
            exp["config"]["env"] = args.env
        else:
            exp["config"]["env"] = exp["env"]

        # Override the mean reward if specified. This is used by the ray ci
        # for overriding the episode reward mean for tf2 tests for off policy
        # long learning tests such as sac and ddpg on the pendulum environment.
        if args.override_mean_reward != 0.0:
            exp["stop"][
                "sampler_results/episode_reward_mean"
            ] = args.override_mean_reward

        # Checkpoint settings.
        exp["checkpoint_config"] = air.CheckpointConfig(
            checkpoint_frequency=args.checkpoint_freq,
            checkpoint_at_end=args.checkpoint_freq > 0,
        )

        # QMIX does not support tf yet -> skip.
        if exp["run"] == "QMIX" and args.framework != "torch":
            print(f"Skipping framework='{args.framework}' for QMIX.")
            continue

        # Always run with eager-tracing when framework=tf2, if not in local-mode
        # and unless the yaml explicitly tells us to disable eager tracing.
        if (
            (args.framework == "tf2" or exp["config"].get("framework") == "tf2")
            and not args.local_mode
            # Note: This check will always fail for python configs, b/c normally,
            # algorithm configs have `self.eager_tracing=False` by default.
            # Thus, you'd have to set `eager_tracing` to True explicitly in your python
            # config to make sure we are indeed using eager tracing.
            and exp["config"].get("eager_tracing") is not False
        ):
            exp["config"]["eager_tracing"] = True

        # Print out the actual config (not for py files as yaml.dump weirdly fails).
        if not config_is_python:
            print("== Test config ==")
            print(yaml.dump(experiments))

        callbacks = None
        if args.wandb_key is not None:
            project = args.wandb_project or (
                exp["run"].lower()
                + "-"
                + re.sub("\\W+", "-", exp["config"]["env"].lower())
                if config_is_python
                else list(experiments.keys())[0]
            )
            callbacks = [
                WandbLoggerCallback(
                    api_key=args.wandb_key,
                    project=project,
                    upload_checkpoints=True,
                    **({"name": args.wandb_run_name} if args.wandb_run_name else {}),
                )
            ]

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
                    trials = run_experiments(
                        experiments,
                        resume=False,
                        verbose=args.verbose,
                        callbacks=callbacks,
                    )
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
