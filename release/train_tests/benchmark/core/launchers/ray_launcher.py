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


# Env vars the benchmark always wants on workers, unless overridden per
# experiment. expandable_segments must be set before the CUDA caching allocator
# initializes, which is why it goes through runtime_env (worker process launch)
# rather than os.environ inside the train fn — by then NCCL has already init'd
# CUDA and the setting is ignored.
_DEFAULT_WORKER_ENV_VARS: Dict[str, str] = {
    "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
}

# Driver env vars worth forwarding to workers if the driver has them set
# (auth, cache location, CUDA toolkit path for DeepSpeed's import-time probe).
_FORWARDED_ENV_KEYS = (
    "HF_TOKEN",
    "HUGGING_FACE_HUB_TOKEN",
    "HF_HOME",
    "CUDA_HOME",
)


def _collect_worker_env_vars(cfg: ExperimentConfig) -> Dict[str, str]:
    env_vars = dict(_DEFAULT_WORKER_ENV_VARS)

    # Point workers at the shared HF cache so they reuse a warm cache (populated
    # by `--prepare-data`) instead of each cold-fetching from the Hub.
    from data.text_dataset import shared_hf_cache

    cache = shared_hf_cache()
    if cache:
        env_vars["HF_HOME"] = cache

    for key in _FORWARDED_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            env_vars[key] = value
    # Experiment-declared env wins over defaults and forwarded values.
    env_vars.update(cfg.env_vars or {})
    return env_vars


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

    env_vars = _collect_worker_env_vars(cfg)
    logger.info(f"Worker env vars: {env_vars}")

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
            # Per-worker runtime env: lands in each worker process at launch,
            # before torch/CUDA init — so PYTORCH_CUDA_ALLOC_CONF actually takes
            # effect (setting it inside the train fn is too late; NCCL has
            # already initialized the CUDA allocator by then).
            worker_runtime_env={"env_vars": env_vars},
        ),
    )
    trainer.fit()

    with open(metrics_path, "r") as f:
        return json.load(f)
