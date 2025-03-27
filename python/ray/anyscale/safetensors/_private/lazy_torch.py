"""Import alias for torch that makes all usages of the module *lazy*.

`torch` is a very heavyweight module (takes ~0.5s to import) and we want to avoid
importing it in spawned subprocesses as it impacts forking performance.

This module should therefore be used as a drop-in replacement for `torch` everywhere
in the codebase. Example:

# Before:
import torch
torch.dtype

# After:
from ray.anyscale.safetensors._private.lazy_torch import torch
torch.dtype
"""
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import torch
else:

    class LazyTorch:
        def __getattr__(self, attr: str) -> Any:
            import torch

            return getattr(torch, attr)

    torch = LazyTorch()
