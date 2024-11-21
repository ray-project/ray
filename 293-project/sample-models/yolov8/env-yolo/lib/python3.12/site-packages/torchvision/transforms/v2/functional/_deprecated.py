import warnings
from typing import Any, List

import torch

from torchvision.transforms import functional as _F


@torch.jit.unused
def to_tensor(inpt: Any) -> torch.Tensor:
    """[DEPREACTED] Use to_image() and to_dtype() instead."""
    warnings.warn(
        "The function `to_tensor(...)` is deprecated and will be removed in a future release. "
        "Instead, please use `to_image(...)` followed by `to_dtype(..., dtype=torch.float32, scale=True)`."
    )
    return _F.to_tensor(inpt)


def get_image_size(inpt: torch.Tensor) -> List[int]:
    warnings.warn(
        "The function `get_image_size(...)` is deprecated and will be removed in a future release. "
        "Instead, please use `get_size(...)` which returns `[h, w]` instead of `[w, h]`."
    )
    return _F.get_image_size(inpt)
