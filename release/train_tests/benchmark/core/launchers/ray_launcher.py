"""Ray Train launcher: runs an experiment on a TorchTrainer.

The per-worker body is adapter-driven; final metrics come back natively through
Ray Train's Result (``ray.train.report`` -> ``result.metrics``) rather than a
side file. Storage is the cluster's via ``RunConfig.storage_path``.
"""

import logging
from typing import Any, Dict

from core.experiment_config import ExperimentConfig
from core.registry import get_adapter_cls
from core.train_context import RayTrainContext

logger = logging.getLogger(__name__)


def train_fn_per_worker(train_loop_config: Dict[str, Any]) -> None:
    """Ray Train entrypoint: run the adapter on each worker.

    Metrics are surfaced via ``ray.train.report`` inside the adapter, so the
    driver reads them off ``trainer.fit().metrics`` — no shared file needed.
    """
    cfg: ExperimentConfig = train_loop_config["cfg"]
    ctx = RayTrainContext()
    adapter = get_adapter_cls(cfg.adapter)(cfg, ctx)
    adapter.run()


def run_with_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    import os
    from datetime import datetime

    import ray.train
    from ray.train.torch import TorchTrainer

    # Unique run name. Use a stdlib timestamp rather than Ray's private
    # ray.train.v2._internal.util.date_str (no stability contract).
    run_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    storage_root = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "/mnt/cluster_storage")
    run_config_kwargs = {}
    # Experiment-declared env vars (if any) land in each worker process at
    # launch — before torch/CUDA init, which matters for e.g.
    # PYTORCH_CUDA_ALLOC_CONF. Anything cluster-wide should be set on the cluster.
    if cfg.env_vars:
        run_config_kwargs["worker_runtime_env"] = {"env_vars": dict(cfg.env_vars)}

    scaling = cfg.scaling
    scaling_kwargs = {"num_workers": scaling.num_workers, "use_gpu": scaling.use_gpu}
    if scaling.resources_per_worker:
        scaling_kwargs["resources_per_worker"] = scaling.resources_per_worker
    if scaling.accelerator_type:
        scaling_kwargs["accelerator_type"] = scaling.accelerator_type

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"cfg": cfg},
        scaling_config=ray.train.ScalingConfig(**scaling_kwargs),
        run_config=ray.train.RunConfig(
            storage_path=f"{storage_root}/train_benchmark/",
            name=f"{cfg.name}-{run_suffix}",
            failure_config=ray.train.FailureConfig(max_failures=cfg.max_failures),
            **run_config_kwargs,
        ),
    )
    result = trainer.fit()
    return dict(result.metrics) if result.metrics else {}
