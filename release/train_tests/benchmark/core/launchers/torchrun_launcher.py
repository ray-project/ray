"""Torchrun launcher: the parity baseline from the proposal.

Run under ``torchrun`` so the same adapter code path executes without Ray Train
in the loop, isolating Ray's orchestration overhead from raw framework compute:

    torchrun --nproc_per_node=8 core/runner.py \
        --experiment experiments/qwen3_06b_deepspeed.yaml \
        --launcher torchrun

This function runs *inside* each torchrun-spawned process; it initializes the
process group, runs the adapter, and lets rank 0 persist metrics.
"""

import json
import logging
import os
from typing import Any, Dict

from core.experiment_config import ExperimentConfig
from core.registry import get_adapter_cls
from core.train_context import TorchrunContext, default_metrics_path

logger = logging.getLogger(__name__)


def run_with_torchrun(cfg: ExperimentConfig) -> Dict[str, Any]:
    import torch.distributed as dist

    backend = "nccl" if os.environ.get("LOCAL_RANK") and _cuda_available() else "gloo"
    if not dist.is_initialized():
        dist.init_process_group(backend=backend)

    ctx = TorchrunContext(cfg.name)
    adapter = get_adapter_cls(cfg.model.adapter)(cfg, ctx)
    metrics = adapter.run()

    dist.barrier()
    dist.destroy_process_group()

    if ctx.world_rank == 0:
        metrics_path = default_metrics_path(cfg.name)
        if os.path.isfile(metrics_path):
            with open(metrics_path, "r") as f:
                return json.load(f)
        return metrics or {}
    return {}


def _cuda_available() -> bool:
    import torch

    return torch.cuda.is_available()
