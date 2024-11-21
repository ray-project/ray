from typing import Any, Dict, Optional, Union

import numpy as np
import PIL.Image
import torch

from torchvision import tv_tensors
from torchvision.transforms.v2 import functional as F, Transform

from torchvision.transforms.v2._utils import is_pure_tensor


class PILToTensor(Transform):
    """Convert a PIL Image to a tensor of the same type - this does not scale values.

    This transform does not support torchscript.

    Converts a PIL Image (H x W x C) to a Tensor of shape (C x H x W).
    """

    _transformed_types = (PIL.Image.Image,)

    def _transform(self, inpt: PIL.Image.Image, params: Dict[str, Any]) -> torch.Tensor:
        return F.pil_to_tensor(inpt)


class ToImage(Transform):
    """Convert a tensor, ndarray, or PIL Image to :class:`~torchvision.tv_tensors.Image`
    ; this does not scale values.

    This transform does not support torchscript.
    """

    _transformed_types = (is_pure_tensor, PIL.Image.Image, np.ndarray)

    def _transform(
        self, inpt: Union[torch.Tensor, PIL.Image.Image, np.ndarray], params: Dict[str, Any]
    ) -> tv_tensors.Image:
        return F.to_image(inpt)


class ToPILImage(Transform):
    """Convert a tensor or an ndarray to PIL Image

    This transform does not support torchscript.

    Converts a torch.*Tensor of shape C x H x W or a numpy ndarray of shape
    H x W x C to a PIL Image while adjusting the value range depending on the ``mode``.

    Args:
        mode (`PIL.Image mode`_): color space and pixel depth of input data (optional).
            If ``mode`` is ``None`` (default) there are some assumptions made about the input data:

            - If the input has 4 channels, the ``mode`` is assumed to be ``RGBA``.
            - If the input has 3 channels, the ``mode`` is assumed to be ``RGB``.
            - If the input has 2 channels, the ``mode`` is assumed to be ``LA``.
            - If the input has 1 channel, the ``mode`` is determined by the data type (i.e ``int``, ``float``,
              ``short``).

    .. _PIL.Image mode: https://pillow.readthedocs.io/en/latest/handbook/concepts.html#concept-modes
    """

    _transformed_types = (is_pure_tensor, tv_tensors.Image, np.ndarray)

    def __init__(self, mode: Optional[str] = None) -> None:
        super().__init__()
        self.mode = mode

    def _transform(
        self, inpt: Union[torch.Tensor, PIL.Image.Image, np.ndarray], params: Dict[str, Any]
    ) -> PIL.Image.Image:
        return F.to_pil_image(inpt, mode=self.mode)


class ToPureTensor(Transform):
    """Convert all TVTensors to pure tensors, removing associated metadata (if any).

    This doesn't scale or change the values, only the type.
    """

    _transformed_types = (tv_tensors.TVTensor,)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> torch.Tensor:
        return inpt.as_subclass(torch.Tensor)
