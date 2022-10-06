#!/usr/bin/env python

import argparse
import os
from pathlib import Path
import yaml

import ray
from ray.tune.experiment.config_parser import _make_parser
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.resources import resources_to_json
from ray.tune.tune import run_experiments
from ray.tune.schedulers import create_scheduler
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch

# Try to import both backends for flag checking/warnings.
tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

EXAMPLE_USAGE = """
Training example via RLlib CLI:
    rllib train --run DQN --env CartPole-v0

Grid search example via RLlib CLI:
    rllib train -f tuned_examples/cartpole-ppo-grid-search-example.yaml

Grid search example via executable:
    ./train.py -f tuned_examples/cartpole-ppo-grid-search-example.yaml

Note that -f overrides all other trial-specific command-line options.
"""


def create_parser(parser_creator=None):
    parser = _make_parser(
        parser_creator=parser_creator,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Train a reinforcement learning agent.",
        epilog=EXAMPLE_USAGE,
    )

    # See also the base parser definition in ray/tune/experiment/__config_parser.py
    parser.add_argument(
        "--ray-address",
        default=None,
        type=str,
        help="Connect to an existing Ray cluster at this address instead "
        "of starting a new one.",
    )
    parser.add_argument(
        "--ray-ui", action="store_true", help="Whether to enable the Ray web UI."
    )
    # Deprecated: Use --ray-ui, instead.
    parser.add_argument(
        "--no-ray-ui",
        action="store_true",
        help="Deprecated! Ray UI is disabled by default now. "
        "Use `--ray-ui` to enable.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Run ray in local mode for easier debugging.",
    )
    parser.add_argument(
        "--ray-num-cpus",
        default=None,
        type=int,
        help="--num-cpus to use if starting a new cluster.",
    )
    parser.add_argument(
        "--ray-num-gpus",
        default=None,
        type=int,
        help="--num-gpus to use if starting a new cluster.",
    )
    parser.add_argument(
        "--ray-num-nodes",
        default=None,
        type=int,
        help="Emulate multiple cluster nodes for debugging.",
    )
    parser.add_argument(
        "--ray-object-store-memory",
        default=None,
        type=int,
        help="--object-store-memory to use if starting a new cluster.",
    )
    parser.add_argument(
        "--experiment-name",
        default="default",
        type=str,
        help="Name of the subdirectory under `local_dir` to put results in.",
    )
    parser.add_argument(
        "--local-dir",
        default=DEFAULT_RESULTS_DIR,
        type=str,
        help="Local dir to save training results to. Defaults to '{}'.".format(
            DEFAULT_RESULTS_DIR
        ),
    )
    parser.add_argument(
        "--upload-dir",
        default="",
        type=str,
        help="Optional URI to sync training results to (e.g. s3://bucket).",
    )
    # This will override any framework setting found in a yaml file.
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "tfe", "torch"],
        default=None,
        help="The DL framework specifier.",
    )
    parser.add_argument(
        "-v", action="store_true", help="Whether to use INFO level logging."
    )
    parser.add_argument(
        "-vv", action="store_true", help="Whether to use DEBUG level logging."
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Whether to attempt to resume previous Tune experiments.",
    )
    parser.add_argument(
        "--trace",
        action="store_true",
        help="Whether to attempt to enable tracing for eager mode.",
    )
    parser.add_argument(
        "--env", default=None, type=str, help="The gym environment to use."
    )
    parser.add_argument(
        "-f",
        "--config-file",
        default=None,
        type=str,
        help="If specified, use config options from this file. Note that this "
        "overrides any trial-specific options set via flags above.",
    )

    # Obsolete: Use --framework=torch|tf2|tfe instead!
    parser.add_argument(
        "--torch",
        action="store_true",
        help="Whether to use PyTorch (instead of tf) as the DL framework.",
    )
    parser.add_argument(
        "--eager",
        action="store_true",
        help="Whether to attempt to enable TF eager execution.",
    )

    return parser


def run(args, parser):
    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.safe_load(f)
    else:
        # Note: keep this in sync with tune/experiment/__config_parser.py
        experiments = {
            args.experiment_name: {  # i.e. log to ~/ray_results/default
                "run": args.run,
                "checkpoint_config": {
                    "checkpoint_frequency": args.checkpoint_freq,
                    "checkpoint_at_end": args.checkpoint_at_end,
                    "num_to_keep": args.keep_checkpoints_num,
                    "checkpoint_score_attribute": args.checkpoint_score_attr,
                },
                "local_dir": args.local_dir,
                "resources_per_trial": (
                    args.resources_per_trial
                    and resources_to_json(args.resources_per_trial)
                ),
                "stop": args.stop,
                "config": dict(args.config, env=args.env),
                "restore": args.restore,
                "num_samples": args.num_samples,
                "sync_config": {
                    "upload_dir": args.upload_dir,
                },
            }
        }

    # Ray UI.
    if args.no_ray_ui:
        deprecation_warning(old="--no-ray-ui", new="--ray-ui", error=False)
        args.ray_ui = False

    verbose = 1
    for exp in experiments.values():
        # Bazel makes it hard to find files specified in `args` (and `data`).
        # Look for them here.
        # NOTE: Some of our yaml files don't have a `config` section.
        input_ = exp.get("config", {}).get("input")

        if input_ and input_ != "sampler":
            # This script runs in the ray/rllib dir.
            rllib_dir = Path(__file__).parent

            def patch_path(path):
                if isinstance(path, list):
                    return [patch_path(i) for i in path]
                elif isinstance(path, dict):
                    return {patch_path(k): patch_path(v) for k, v in path.items()}
                elif isinstance(path, str):
                    if os.path.exists(path):
                        return path
                    else:
                        abs_path = str(rllib_dir.absolute().joinpath(path))
                        return abs_path if os.path.exists(abs_path) else path
                else:
                    return path

            exp["config"]["input"] = patch_path(input_)

        if not exp.get("run"):
            parser.error("the following arguments are required: --run")
        if not exp.get("env") and not exp.get("config", {}).get("env"):
            parser.error("the following arguments are required: --env")

        if args.torch:
            deprecation_warning("--torch", "--framework=torch")
            exp["config"]["framework"] = "torch"
        elif args.eager:
            deprecation_warning("--eager", "--framework=[tf2|tfe]")
            exp["config"]["framework"] = "tfe"
        elif args.framework is not None:
            exp["config"]["framework"] = args.framework

        if args.trace:
            if exp["config"]["framework"] not in ["tf2", "tfe"]:
                raise ValueError("Must enable --eager to enable tracing.")
            exp["config"]["eager_tracing"] = True

        if args.v:
            exp["config"]["log_level"] = "INFO"
            verbose = 3  # Print details on trial result
        if args.vv:
            exp["config"]["log_level"] = "DEBUG"
            verbose = 3  # Print details on trial result

    if args.ray_num_nodes:
        # Import this only here so that train.py also works with
        # older versions (and user doesn't use `--ray-num-nodes`).
        from ray.cluster_utils import Cluster

        cluster = Cluster()
        for _ in range(args.ray_num_nodes):
            cluster.add_node(
                num_cpus=args.ray_num_cpus or 1,
                num_gpus=args.ray_num_gpus or 0,
                object_store_memory=args.ray_object_store_memory,
            )
        ray.init(address=cluster.address)
    else:
        ray.init(
            include_dashboard=args.ray_ui,
            address=args.ray_address,
            object_store_memory=args.ray_object_store_memory,
            num_cpus=args.ray_num_cpus,
            num_gpus=args.ray_num_gpus,
            local_mode=args.local_mode,
        )

    run_experiments(
        experiments,
        scheduler=create_scheduler(args.scheduler, **args.scheduler_config),
        resume=args.resume,
        verbose=verbose,
        concurrent=True,
    )

    ray.shutdown()


def main():
    parser = create_parser()
    args = parser.parse_args()
    run(args, parser)


if __name__ == "__main__":
    main()
