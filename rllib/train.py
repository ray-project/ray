#!/usr/bin/env python

import json
import os
from pathlib import Path
import yaml
import typer

import ray
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.resources import resources_to_json, json_to_resources
from ray.tune.tune import run_experiments
from ray.tune.schedulers import create_scheduler
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch

# Try to import both backends for flag checking/warnings.
tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def run(
    run: str = typer.Option(...),
    env: str = typer.Option(None),
    config: str = typer.Option("{}"),
    config_file: str = typer.Option(None),
    stop: str = typer.Option("{}"),
    experiment_name: str = typer.Option("default"),
    v: bool = typer.Option(False),
    vv: bool = typer.Option(False),
    resume: bool = typer.Option(False),
    num_samples: int = typer.Option(1),
    checkpoint_freq: int = typer.Option(0),
    checkpoint_at_end: bool = typer.Option(False),
    local_dir: str = typer.Option(DEFAULT_RESULTS_DIR),
    local_mode: bool = typer.Option(False),
    restore: str = typer.Option(None),
    framework: str = typer.Option(None),
    resources_per_trial: str = typer.Option(None),
    sync_on_checkpoint: bool = typer.Option(False),
    keep_checkpoints_num: int = typer.Option(None),
    checkpoint_score_attr: str = typer.Option("training_iteration"),
    export_formats: str = typer.Option(None),
    max_failures: int = typer.Option(3),
    scheduler: str = typer.Option("FIFO"),
    scheduler_config: str = typer.Option("{}"),
    ray_address: str = typer.Option(None),
    ray_ui: bool = typer.Option(False),
    ray_num_cpus: int = typer.Option(None),
    ray_num_gpus: int = typer.Option(None),
    ray_num_nodes: int = typer.Option(None),
    ray_object_store_memory: int = typer.Option(None),
    upload_dir: str = typer.Option(""),
    trace: bool = typer.Option(False),
    torch: bool = typer.Option(False),  # deprecated
    eager: bool = typer.Option(False),  # deprecated
):
    # FIXME: sync_on_checkpoint, export_formats, max_failures not used anywhere.
    stop = json.loads(stop)
    config = json.loads(config)
    scheduler_config = json.loads(scheduler_config)
    resources_per_trial = json_to_resources(resources_per_trial)

    if config_file:
        with open(config_file) as f:
            experiments = yaml.safe_load(f)
    else:
        # Note: keep this in sync with tune/experiment/__config_parser.py
        experiments = {
            experiment_name: {  # i.e. log to ~/ray_results/default
                "run": run,
                "checkpoint_freq": checkpoint_freq,
                "checkpoint_at_end": checkpoint_at_end,
                "keep_checkpoints_num": keep_checkpoints_num,
                "checkpoint_score_attr": checkpoint_score_attr,
                "local_dir": local_dir,
                "resources_per_trial": (resources_to_json(resources_per_trial)),
                "stop": stop,
                "config": dict(config, env=env),
                "restore": restore,
                "num_samples": num_samples,
                "sync_config": {
                    "upload_dir": upload_dir,
                },
            }
        }

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

        if not exp.get("env") and not exp.get("config", {}).get("env"):
            raise ValueError(
                "You either need to provide an --env argument or pass"
                "an `env` key with a valid environment to your `config`"
                "argument."
            )
        if torch:
            deprecation_warning("--torch", "--framework=torch")
            exp["config"]["framework"] = "torch"
        elif eager:
            deprecation_warning("--eager", "--framework=[tf2|tfe]")
            exp["config"]["framework"] = "tfe"
        elif framework is not None:
            exp["config"]["framework"] = framework

        if trace:
            if exp["config"]["framework"] not in ["tf2", "tfe"]:
                raise ValueError("Must enable --eager to enable tracing.")
            exp["config"]["eager_tracing"] = True

        if v:
            exp["config"]["log_level"] = "INFO"
            verbose = 3  # Print details on trial result
        if vv:
            exp["config"]["log_level"] = "DEBUG"
            verbose = 3  # Print details on trial result

    if ray_num_nodes:
        # Import this only here so that train.py also works with
        # older versions (and user doesn't use `--ray-num-nodes`).
        from ray.cluster_utils import Cluster

        cluster = Cluster()
        for _ in range(ray_num_nodes):
            cluster.add_node(
                num_cpus=ray_num_cpus or 1,
                num_gpus=ray_num_gpus or 0,
                object_store_memory=ray_object_store_memory,
            )
        ray.init(address=cluster.address)
    else:
        ray.init(
            include_dashboard=ray_ui,
            address=ray_address,
            object_store_memory=ray_object_store_memory,
            num_cpus=ray_num_cpus,
            num_gpus=ray_num_gpus,
            local_mode=local_mode,
        )

    run_experiments(
        experiments,
        scheduler=create_scheduler(scheduler, **scheduler_config),
        resume=resume,
        verbose=verbose,
        concurrent=True,
    )

    ray.shutdown()


def main():
    """Run the CLI."""
    typer.run(run)


if __name__ == "__main__":
    main()
