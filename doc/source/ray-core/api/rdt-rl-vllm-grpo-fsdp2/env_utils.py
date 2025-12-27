"""Utility functions for environment setup and FSDP initialization."""

import torch
import torch.distributed as dist
import torch.nn as nn
from torch.distributed.device_mesh import init_device_mesh
from torch.distributed.fsdp import fully_shard, MixedPrecisionPolicy

from settings import WORLD_SIZE_PER_MODEL

from dataclasses import dataclass
from torch.distributed.device_mesh import DeviceMesh


@dataclass
class FSDPState:
    """State container for FSDP2-managed modules."""

    mesh: DeviceMesh
    module: nn.Module


def check_cuda_availability() -> None:
    if not torch.cuda.is_available() or torch.cuda.device_count() == 0:
        raise RuntimeError("CUDA is required for this GRPO demo")

    min_major = min(
        torch.cuda.get_device_properties(idx).major
        for idx in range(torch.cuda.device_count())
    )
    if min_major < 8:
        raise RuntimeError(
            "vLLM requires GPUs with compute capability >= 8.0 (Ampere)."
        )


def apply_fsdp2(module: nn.Module) -> FSDPState:
    """Applies FSDP2 to a PyTorch module for distributed training.

    FSDP2 shards model parameters across GPUs to reduce memory usage per device.
    This function initializes the device mesh, sets up mixed precision training,
    and applies FSDP2's composable fully_shard per-layer for better efficiency.
    """
    if not dist.is_initialized():
        raise RuntimeError(
            "torch.distributed must be initialized before building FSDP state. "
            "Call initialize_distributed() first."
        )

    mesh = init_device_mesh("cuda", (WORLD_SIZE_PER_MODEL,), mesh_dim_names=("fsdp",))

    # Use float16 to match vLLM's dtype and the learner model's dtype.
    # This ensures weight sync doesn't require dtype conversion.
    mp_policy = MixedPrecisionPolicy(
        param_dtype=torch.float16,
        reduce_dtype=torch.float16,
    )

    # Apply FSDP2 per-layer for better memory efficiency and communication overlap.
    # This assumes the module has a 'layers' attribute (e.g., transformer layers).
    if hasattr(module, "layers"):
        for layer in module.layers:
            fully_shard(
                layer,
                mesh=mesh,
                mp_policy=mp_policy,
                reshard_after_forward=True,
            )

    return FSDPState(mesh=mesh, module=module)
