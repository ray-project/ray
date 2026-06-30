"""Ray Train launcher: runs an experiment on a TorchTrainer.

The per-worker body is adapter-driven. Final metrics are collected from rank 0's
returned value via a shared file — NOT ``trainer.fit().metrics``, which in Ray
Train v2 binds to the last *checkpointed* report (a mid-run checkpoint carrying
only ``{"step": N}`` would otherwise shadow the final full metrics).
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict

import ray.train
from ray.train.torch import TorchTrainer

from core.experiment_config import ExperimentConfig
from core.registry import get_adapter_cls
from core.train_context import RayTrainContext

logger = logging.getLogger(__name__)

# Shared cluster storage (visible to all nodes). Follows train_benchmark.py's
# constant pattern rather than reading ANYSCALE_ARTIFACT_STORAGE.
STORAGE_PATH = "/mnt/cluster_storage/train_benchmark"
METRICS_OUTPUT_PATH = "/mnt/cluster_storage/train_benchmark_metrics.json"


def train_fn_per_worker(train_loop_config: Dict[str, Any]) -> None:
    """Ray Train entrypoint: run the adapter; rank 0 writes its final metrics."""
    cfg: ExperimentConfig = train_loop_config["cfg"]
    ctx = RayTrainContext()
    metrics = get_adapter_cls(cfg.adapter)(cfg, ctx).run()

    if ctx.world_rank == 0 and metrics:
        with open(METRICS_OUTPUT_PATH, "w") as f:
            json.dump(metrics, f)


def run_with_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    run_config_kwargs = {}
    # Experiment-declared env vars (if any) land in each worker process at launch
    # (before torch/CUDA init). Anything cluster-wide should be set on the cluster.
    if cfg.env_vars:
        run_config_kwargs["worker_runtime_env"] = {"env_vars": dict(cfg.env_vars)}

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"cfg": cfg},
        # cfg.scaling mirrors ray.train.ScalingConfig field-for-field.
        scaling_config=ray.train.ScalingConfig(
            num_workers=cfg.scaling.num_workers,
            use_gpu=cfg.scaling.use_gpu,
            resources_per_worker=cfg.scaling.resources_per_worker,
            accelerator_type=cfg.scaling.accelerator_type,
        ),
        run_config=ray.train.RunConfig(
            storage_path=STORAGE_PATH,
            name=f"{cfg.name}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}",
            failure_config=ray.train.FailureConfig(max_failures=cfg.max_failures),
            **run_config_kwargs,
        ),
    )
    trainer.fit()

    with open(METRICS_OUTPUT_PATH, "r") as f:
        return json.load(f)
