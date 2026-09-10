"""Ray Train launcher: runs an experiment on a TorchTrainer.

The per-worker body is adapter-driven. Final metrics are collected from rank 0's
returned value via a shared file — NOT ``trainer.fit().metrics``, which in Ray
Train v2 binds to the last *checkpointed* report (a mid-run checkpoint carrying
only ``{"step": N}`` would otherwise shadow the final full metrics).
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

import ray
import ray.train
from ray.train.torch import TorchTrainer

from core.experiment_config import ExperimentConfig
from core.registry import get_adapter_cls
from core.train_context import RayTrainContext

logger = logging.getLogger(__name__)

# Shared cluster storage (visible to all nodes). Follows train_benchmark.py's
# constant pattern rather than reading ANYSCALE_ARTIFACT_STORAGE.
STORAGE_PATH = "/mnt/cluster_storage/train_benchmark"


def train_fn_per_worker(train_loop_config: Dict[str, Any]) -> None:
    """Ray Train entrypoint: run the adapter; rank 0 writes its final metrics."""
    cfg: ExperimentConfig = train_loop_config["cfg"]
    metrics_path: str = train_loop_config["metrics_path"]
    ctx = RayTrainContext()
    metrics = get_adapter_cls(cfg.framework)(cfg, ctx).run()

    # Always write (even an empty dict): the driver must never be able to pick
    # up a previous run's file, and a visibly empty result beats a stale one.
    if ctx.world_rank == 0:
        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump(metrics or {}, f)


def run_with_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    run_config_kwargs = {}
    # Experiment-declared env vars (if any) land in each worker process at launch
    # (before torch/CUDA init). Anything cluster-wide should be set on the cluster.
    # The harness code itself reaches workers via the job-level working_dir set in
    # runner.main() (workers inherit it), NOT via a per-worker runtime_env.
    if cfg.env_vars:
        run_config_kwargs["worker_runtime_env"] = {"env_vars": dict(cfg.env_vars)}

    # Unique per run, so a driver can never read a previous run's (or another
    # experiment's) metrics from the shared fixed-path storage.
    run_name = f"{cfg.name}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}"
    metrics_path = f"{STORAGE_PATH}/{run_name}_metrics.json"

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"cfg": cfg, "metrics_path": metrics_path},
        # cfg.scaling mirrors ray.train.ScalingConfig field-for-field.
        scaling_config=ray.train.ScalingConfig(
            num_workers=cfg.scaling.num_workers,
            use_gpu=cfg.scaling.use_gpu,
            resources_per_worker=cfg.scaling.resources_per_worker,
            accelerator_type=cfg.scaling.accelerator_type,
        ),
        run_config=ray.train.RunConfig(
            storage_path=STORAGE_PATH,
            name=run_name,
            failure_config=ray.train.FailureConfig(max_failures=cfg.max_failures),
            **run_config_kwargs,
        ),
    )
    trainer.fit()

    try:
        with open(metrics_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise RuntimeError(
            f"Training finished but rank 0 never wrote metrics to "
            f"{metrics_path}. The run produced no usable benchmark result."
        )
