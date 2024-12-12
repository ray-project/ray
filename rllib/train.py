#!/usr/bin/env python
import importlib
import json
import os
from pathlib import Path
import re
import sys
import typer
from typing import Optional
import uuid
import yaml

import ray
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.common import CLIArguments as cli
from ray.rllib.common import FrameworkEnum, SupportedFileType
from ray.rllib.common import _download_example_file, _get_file_type
from ray.train.constants import _DEPRECATED_VALUE
from ray.tune.resources import resources_to_json, json_to_resources
from ray.tune.tune import run_experiments
from ray.tune.schedulers import create_scheduler
from ray.util.annotations import DeveloperAPI, PublicAPI


def _import_backends():
    """Try to import both backends for flag checking/warnings."""
    tf1, tf, tfv = try_import_tf()
    torch, _ = try_import_torch()


# Create the "train" Typer app
train_app = typer.Typer()


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


@PublicAPI(stability="beta")
def load_experiments_from_file(
    config_file: str,
    file_type: SupportedFileType,
    stop: Optional[str] = None,
    checkpoint_config: Optional[dict] = None,
) -> dict:
    """Load experiments from a file. Supports YAML and Python files.

    If you want to use a Python file, it has to have a 'config' variable
    that is an AlgorithmConfig object and - optionally - a `stop` variable defining
    the stop criteria.

    Args:
        config_file: The yaml or python file to be used as experiment definition.
            Must only contain exactly one experiment.
        file_type: One value of the `SupportedFileType` enum (yaml or python).
        stop: An optional stop json string, only used if file_type is python.
            If None (and file_type is python), will try to extract stop information
            from a defined `stop` variable in the python file, otherwise, will use {}.
        checkpoint_config: An optional checkpoint config to add to the returned
            experiments dict.

    Returns:
        The experiments dict ready to be passed into `tune.run_experiments()`.
    """

    # Yaml file.
    if file_type == SupportedFileType.yaml:
        with open(config_file) as f:
            experiments = yaml.safe_load(f)
            if stop is not None and stop != "{}":
                raise ValueError("`stop` criteria only supported for python files.")
        # Make sure yaml experiments are always old API stack.
        for experiment in experiments.values():
            experiment["config"]["enable_rl_module_and_learner"] = False
            experiment["config"]["enable_env_runner_and_connector_v2"] = False
    # Python file case (ensured by file type enum)
    else:
        module_name = os.path.basename(config_file).replace(".py", "")
        spec = importlib.util.spec_from_file_location(module_name, config_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if not hasattr(module, "config"):
            raise ValueError(
                "Your Python file must contain a 'config' variable "
                "that is an AlgorithmConfig object."
            )
        algo_config = getattr(module, "config")
        if stop is None:
            stop = getattr(module, "stop", {})
        else:
            stop = json.loads(stop)

        # Note: we do this gymnastics to support the old format that
        # "_run_rllib_experiments" expects. Ideally, we'd just build the config and
        # run the algo.
        config = algo_config.to_dict()
        experiments = {
            f"default_{uuid.uuid4().hex}": {
                "run": algo_config.algo_class,
                "env": config.get("env"),
                "config": config,
                "stop": stop,
            }
        }

    for key, val in experiments.items():
        experiments[key]["checkpoint_config"] = checkpoint_config or {}

    return experiments


@DeveloperAPI
@train_app.command()
def file(
    # File-based arguments.
    config_file: str = cli.ConfigFile,
    # stopping conditions
    stop: Optional[str] = cli.Stop,
    # Environment override.
    env: Optional[str] = cli.Env,
    # Checkpointing
    checkpoint_freq: int = cli.CheckpointFreq,
    checkpoint_at_end: bool = cli.CheckpointAtEnd,
    keep_checkpoints_num: int = cli.KeepCheckpointsNum,
    checkpoint_score_attr: str = cli.CheckpointScoreAttr,
    # Additional config arguments used for overriding.
    v: bool = cli.V,
    vv: bool = cli.VV,
    framework: FrameworkEnum = cli.Framework,
    trace: bool = cli.Trace,
    # WandB options.
    wandb_key: Optional[str] = cli.WandBKey,
    wandb_project: Optional[str] = cli.WandBProject,
    wandb_run_name: Optional[str] = cli.WandBRunName,
    # Ray cluster options.
    local_mode: bool = cli.LocalMode,
    ray_address: Optional[str] = cli.RayAddress,
    ray_ui: bool = cli.RayUi,
    ray_num_cpus: Optional[int] = cli.RayNumCpus,
    ray_num_gpus: Optional[int] = cli.RayNumGpus,
    ray_num_nodes: Optional[int] = cli.RayNumNodes,
    ray_object_store_memory: Optional[int] = cli.RayObjectStoreMemory,
    # Ray scheduling options.
    resume: bool = cli.Resume,
    scheduler: Optional[str] = cli.Scheduler,
    scheduler_config: str = cli.SchedulerConfig,
):
    """Train a reinforcement learning agent from file.
    The file argument is required to run this command.\n\n

    Grid search example with the RLlib CLI:\n
      rllib train file tuned_examples/ppo/cartpole-ppo.yaml\n\n

    You can also run an example from a URL with the file content:\n
      rllib train file https://raw.githubusercontent.com/ray-project/ray/\
      master/rllib/tuned_examples/ppo/cartpole-ppo.yaml
    """
    # Attempt to download the file if it's not found locally.
    config_file, temp_file = _download_example_file(
        example_file=config_file, base_url=None
    )

    _import_backends()
    framework = framework.value if framework else None

    checkpoint_config = {
        "checkpoint_frequency": checkpoint_freq,
        "checkpoint_at_end": checkpoint_at_end,
        "num_to_keep": keep_checkpoints_num,
        "checkpoint_score_attribute": checkpoint_score_attr,
    }

    file_type = _get_file_type(config_file)

    experiments = load_experiments_from_file(
        config_file, file_type, stop, checkpoint_config
    )
    exp_name = list(experiments.keys())[0]
    experiment = experiments[exp_name]
    algo = experiment["run"]

    # Override the env from the config by the value given on the command line.
    if env is not None:
        experiment["env"] = env

    # WandB logging support.
    callbacks = None
    if wandb_key is not None:
        project = wandb_project or (
            algo.lower() + "-" + re.sub("\\W+", "-", experiment["env"].lower())
            if file_type == SupportedFileType.python
            else exp_name
        )
        callbacks = [
            WandbLoggerCallback(
                api_key=wandb_key,
                project=project,
                **({"name": wandb_run_name} if wandb_run_name is not None else {}),
            )
        ]

    # if we had to download the config file, remove the temp file.
    if temp_file:
        temp_file.close()

    _run_rllib_experiments(
        experiments=experiments,
        v=v,
        vv=vv,
        framework=framework,
        trace=trace,
        ray_num_nodes=ray_num_nodes,
        ray_num_cpus=ray_num_cpus,
        ray_num_gpus=ray_num_gpus,
        ray_object_store_memory=ray_object_store_memory,
        ray_ui=ray_ui,
        ray_address=ray_address,
        local_mode=local_mode,
        resume=resume,
        scheduler=scheduler,
        scheduler_config=scheduler_config,
        algo=algo,
        callbacks=callbacks,
    )


@DeveloperAPI
@train_app.callback(invoke_without_command=True)
def run(
    # Context object for subcommands
    ctx: typer.Context,
    # Config-based arguments.
    algo: str = cli.Algo,
    env: str = cli.Env,
    config: str = cli.Config,
    stop: str = cli.Stop,
    experiment_name: str = cli.ExperimentName,
    num_samples: int = cli.NumSamples,
    checkpoint_freq: int = cli.CheckpointFreq,
    checkpoint_at_end: bool = cli.CheckpointAtEnd,
    storage_path: str = cli.StoragePath,
    restore: str = cli.Restore,
    resources_per_trial: str = cli.ResourcesPerTrial,
    keep_checkpoints_num: int = cli.KeepCheckpointsNum,
    checkpoint_score_attr: str = cli.CheckpointScoreAttr,
    # Additional config arguments used for overriding.
    v: bool = cli.V,
    vv: bool = cli.VV,
    framework: FrameworkEnum = cli.Framework,
    trace: bool = cli.Trace,
    # Ray cluster options.
    local_mode: bool = cli.LocalMode,
    ray_address: str = cli.RayAddress,
    ray_ui: bool = cli.RayUi,
    ray_num_cpus: int = cli.RayNumCpus,
    ray_num_gpus: int = cli.RayNumGpus,
    ray_num_nodes: int = cli.RayNumNodes,
    ray_object_store_memory: int = cli.RayObjectStoreMemory,
    # Ray scheduling options.
    resume: bool = cli.Resume,
    scheduler: str = cli.Scheduler,
    scheduler_config: str = cli.SchedulerConfig,
    # TODO(arturn): [Deprecated] Remove in 2.11.
    local_dir: str = cli.LocalDir,
    upload_dir: str = cli.UploadDir,
):
    """Train a reinforcement learning agent from command line options.
    The options --env and --algo are required to run this command.

    Training example via RLlib CLI:\n
        rllib train --algo DQN --env CartPole-v1\n\n
    """

    # If no subcommand is specified, simply run the following lines as the
    # "rllib train" main command.
    if ctx.invoked_subcommand is None:
        # we only check for backends when actually running the command. otherwise the
        # start-up time is too slow.
        _import_backends()

        framework = framework.value if framework else None

        config = json.loads(config)
        resources_per_trial = json_to_resources(resources_per_trial)

        if local_dir != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "`local_dir` is deprecated. Please use `storage_path` instead."
            )
        if upload_dir != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "`upload_dir` is deprecated. Please use `storage_path` instead."
            )

        # Load a single experiment from configuration
        experiments = {
            experiment_name: {  # i.e. log to ~/ray_results/default
                "run": algo,
                "checkpoint_config": {
                    "checkpoint_frequency": checkpoint_freq,
                    "checkpoint_at_end": checkpoint_at_end,
                    "num_to_keep": keep_checkpoints_num,
                    "checkpoint_score_attribute": checkpoint_score_attr,
                },
                "storage_path": storage_path,
                "resources_per_trial": (
                    resources_per_trial and resources_to_json(resources_per_trial)
                ),
                "stop": json.loads(stop),
                "config": dict(config, env=env),
                "restore": restore,
                "num_samples": num_samples,
            }
        }

        _run_rllib_experiments(
            experiments=experiments,
            v=v,
            vv=vv,
            framework=framework,
            trace=trace,
            ray_num_nodes=ray_num_nodes,
            ray_num_cpus=ray_num_cpus,
            ray_num_gpus=ray_num_gpus,
            ray_object_store_memory=ray_object_store_memory,
            ray_ui=ray_ui,
            ray_address=ray_address,
            local_mode=local_mode,
            resume=resume,
            scheduler=scheduler,
            scheduler_config=scheduler_config,
            algo=algo,
        )


def _run_rllib_experiments(
    experiments: dict,
    v: cli.V,
    vv: cli.VV,
    framework: str,
    trace: cli.Trace,
    ray_num_nodes: cli.RayNumNodes,
    ray_num_cpus: cli.RayNumCpus,
    ray_num_gpus: cli.RayNumGpus,
    ray_object_store_memory: cli.RayObjectStoreMemory,
    ray_ui: cli.RayUi,
    ray_address: cli.RayAddress,
    local_mode: cli.LocalMode,
    resume: cli.Resume,
    scheduler: cli.Scheduler,
    scheduler_config: cli.SchedulerConfig,
    algo: cli.Algo,
    callbacks=None,
):
    """Main training function for the RLlib CLI, whether you've loaded your
    experiments from a config file or from command line options."""

    # Override experiment data with command line arguments.
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
                "You either need to provide an --env argument (e.g. 'CartPole-v1') "
                "or pass an `env` key with a valid environment to your `config`"
                "argument."
            )
        elif framework is not None:
            exp["config"]["framework"] = framework
        if trace:
            if exp["config"]["framework"] not in ["tf2"]:
                raise ValueError("Must enable framework=tf2 to enable eager tracing.")
            exp["config"]["eager_tracing"] = True
        if v:
            exp["config"]["log_level"] = "INFO"
            verbose = 3  # Print details on trial result
        if vv:
            exp["config"]["log_level"] = "DEBUG"
            verbose = 3  # Print details on trial result

    # Initialize the Ray cluster with the specified options.
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

    # Run the Tune experiment and return the trials.
    scheduler_config = json.loads(scheduler_config)
    trials = run_experiments(
        experiments,
        scheduler=create_scheduler(scheduler, **scheduler_config),
        resume=resume,
        verbose=verbose,
        concurrent=True,
        callbacks=callbacks,
    )
    ray.shutdown()

    checkpoints = []
    for trial in trials:
        if trial.checkpoint:
            checkpoints.append(trial.checkpoint)

    if checkpoints:
        from rich import print
        from rich.panel import Panel

        print("\nYour training finished.")

        print("Best available checkpoint for each trial:")
        for cp in checkpoints:
            print(f"  {cp.path}")

        print(
            "\nYou can now evaluate your trained algorithm from any "
            "checkpoint, e.g. by running:"
        )
        print(Panel(f"[green]  rllib evaluate {checkpoints[0].path} --algo {algo}"))


@DeveloperAPI
def main():
    """Run the CLI."""
    train_app()


if __name__ == "__main__":
    main()
