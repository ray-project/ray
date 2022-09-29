#!/usr/bin/env python

import json
import os
from pathlib import Path
import yaml
import typer
from typing import Optional

import ray
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.resources import resources_to_json, json_to_resources
from ray.tune.tune import run_experiments
from ray.tune.schedulers import create_scheduler
from ray.rllib.utils.framework import try_import_tf, try_import_torch

# Try to import both backends for flag checking/warnings.
tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _patch_path(path: str):
    """
    Patch a path to be relative to the current working directory.

    Args:
        path: relative input path.

    Returns: Patched path.
    """
    # This script runs in the ray/rllib dir.
    rllib_dir = Path(__file__).parent
    if isinstance(path, list):
        return [_patch_path(i) for i in path]
    elif isinstance(path, dict):
        return {_patch_path(k): _patch_path(v) for k, v in path.items()}
    elif isinstance(path, str):
        if os.path.exists(path):
            return path
        else:
            abs_path = str(rllib_dir.absolute().joinpath(path))
            return abs_path if os.path.exists(abs_path) else path
    else:
        return path


def load_experiments_from_file(config_file: str):
    with open(config_file) as f:
        experiments = yaml.safe_load(f)
    return experiments


def load_single_experiment_from_config(
    run: str,
    env: str,
    config: str,
    stop: str,
    experiment_name: str,
    num_samples: int,
    checkpoint_freq: int,
    checkpoint_at_end: bool,
    local_dir: str,
    restore: str,
    resources_per_trial: str,
    keep_checkpoints_num: int,
    checkpoint_score_attr: str,
    upload_dir: str
):
    config = json.loads(config)
    resources_per_trial = json_to_resources(resources_per_trial)

    experiment = {
        experiment_name: {  # i.e. log to ~/ray_results/default
            "run": run,
            "checkpoint_freq": checkpoint_freq,
            "checkpoint_at_end": checkpoint_at_end,
            "keep_checkpoints_num": keep_checkpoints_num,
            "checkpoint_score_attr": checkpoint_score_attr,
            "local_dir": local_dir,
            "resources_per_trial": (
                resources_per_trial and resources_to_json(resources_per_trial)
            ),
            "stop": json.loads(stop),
            "config": dict(config, env=env),
            "restore": restore,
            "num_samples": num_samples,
            "sync_config": {
                "upload_dir": upload_dir,
            },
        }
    }
    return experiment


def override_experiments_with_config(experiments, v, vv, framework, trace):
    verbose = 1
    for exp in experiments.values():
        # Bazel makes it hard to find files specified in `args` (and `data`).
        # Look for them here.
        # NOTE: Some of our yaml files don't have a `config` section.
        input_ = exp.get("config", {}).get("input")
        if input_ and input_ != "sampler":
            exp["config"]["input"] = _patch_path(input_)

        if not exp.get("env") and not exp.get("config", {}).get("env"):
            raise ValueError(
                "You either need to provide an --env argument or pass"
                "an `env` key with a valid environment to your `config`"
                "argument."
            )
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

    return experiments, verbose


def init_ray_from_config(
    ray_num_nodes,
    ray_num_cpus,
    ray_num_gpus,
    ray_object_store_memory,
    ray_ui,
    ray_address,
    local_mode,
):
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


def run(
    # Config-based arguments.
    run: str = typer.Option(...),
    env: str = typer.Option(None),
    config: str = typer.Option("{}"),
    stop: str = typer.Option("{}"),
    experiment_name: str = typer.Option("default"),
    num_samples: int = typer.Option(1),
    checkpoint_freq: int = typer.Option(0),
    checkpoint_at_end: bool = typer.Option(False),
    local_dir: str = typer.Option(DEFAULT_RESULTS_DIR),
    restore: str = typer.Option(None),
    resources_per_trial: str = typer.Option(None),
    keep_checkpoints_num: int = typer.Option(None),
    checkpoint_score_attr: str = typer.Option("training_iteration"),
    upload_dir: str = typer.Option(""),
    # File-based arguments.
    config_file: str = typer.Option(None),
    # Additional config arguments used for overriding.
    v: bool = typer.Option(False),
    vv: bool = typer.Option(False),
    framework: str = typer.Option(None),
    trace: bool = typer.Option(False),
    # Ray cluster options.
    local_mode: bool = typer.Option(False),
    ray_address: str = typer.Option(None),
    ray_ui: bool = typer.Option(False),
    ray_num_cpus: int = typer.Option(None),
    ray_num_gpus: int = typer.Option(None),
    ray_num_nodes: int = typer.Option(None),
    ray_object_store_memory: int = typer.Option(None),
    # Ray scheduling options.
    resume: bool = typer.Option(False),
    scheduler: str = typer.Option("FIFO"),
    scheduler_config: str = typer.Option("{}"),
):
    # Either load experiments from file, or create a single experiment from
    # the command line arguments passed in.
    if config_file:
        experiments = load_experiments_from_file(config_file)
    else:
        experiments = load_single_experiment_from_config(
            run=run,
            env=env,
            config=config,
            stop=stop,
            experiment_name=experiment_name,
            num_samples=num_samples,
            checkpoint_freq=checkpoint_freq,
            checkpoint_at_end=checkpoint_at_end,
            local_dir=local_dir,
            restore=restore,
            resources_per_trial=resources_per_trial,
            keep_checkpoints_num=keep_checkpoints_num,
            checkpoint_score_attr=checkpoint_score_attr,
            upload_dir=upload_dir,
        )

    # Override experiment data with command line arguments.
    experiments, verbose = override_experiments_with_config(
        experiments=experiments,
        v=v,
        vv=vv,
        framework=framework,
        trace=trace,
    )

    # Initialize the Ray cluster with the specified options.
    init_ray_from_config(
        ray_num_nodes=ray_num_nodes,
        ray_num_cpus=ray_num_cpus,
        ray_num_gpus=ray_num_gpus,
        ray_object_store_memory=ray_object_store_memory,
        ray_ui=ray_ui,
        ray_address=ray_address,
        local_mode=local_mode,
    )

    # Run the Tune experiment and return the trials.
    scheduler_config = json.loads(scheduler_config)
    trials = run_experiments(
        experiments,
        scheduler=create_scheduler(scheduler, **scheduler_config),
        resume=resume,
        verbose=verbose,
        concurrent=True,
    )
    ray.shutdown()
    return trials


def main():
    """Run the CLI."""
    typer.run(run)


if __name__ == "__main__":
    main()
