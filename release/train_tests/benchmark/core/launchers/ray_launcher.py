"""Ray Train launcher: runs an experiment on a TorchTrainer.

Mirrors the legacy train_benchmark.py wiring (ScalingConfig / RunConfig /
storage_path) but the per-worker body is adapter-driven and the final metrics
come back through a shared file written by rank 0.
"""

import json
import logging
import os
from typing import Any, Dict

from core.experiment_config import ExperimentConfig
from core.registry import get_adapter_cls
from core.train_context import RayTrainContext, default_metrics_path

logger = logging.getLogger(__name__)


def _train_fn_per_worker(train_loop_config: Dict[str, Any]) -> None:
    cfg: ExperimentConfig = train_loop_config["cfg"]
    metrics_path: str = train_loop_config["metrics_path"]

    ctx = RayTrainContext()
    adapter = get_adapter_cls(cfg.model.adapter)(cfg, ctx)
    metrics = adapter.run()

    if ctx.world_rank == 0 and metrics:
        with open(metrics_path, "w") as f:
            json.dump(metrics, f)


def run_with_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    import ray.train
    from ray.train.torch import TorchTrainer
    from ray.train.v2._internal.util import date_str

    metrics_path = default_metrics_path(cfg.name)

    storage_root = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "/mnt/cluster_storage")
    trainer = TorchTrainer(
        train_loop_per_worker=_train_fn_per_worker,
        train_loop_config={"cfg": cfg, "metrics_path": metrics_path},
        scaling_config=ray.train.ScalingConfig(
            num_workers=cfg.num_workers, use_gpu=cfg.use_gpu
        ),
        run_config=ray.train.RunConfig(
            storage_path=f"{storage_root}/train_benchmark/",
            name=f"{cfg.name}-{date_str(include_ms=True)}",
            failure_config=ray.train.FailureConfig(max_failures=cfg.max_failures),
        ),
    )
    trainer.fit()

    with open(metrics_path, "r") as f:
        return json.load(f)
