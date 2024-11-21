import warnings
from typing import Any, Dict, Union

import numpy as np
import PIL.Image
import torch
from torchvision.transforms import functional as _F

from torchvision.transforms.v2 import Transform


class ToTensor(Transform):
    """[DEPRECATED] Use ``v2.Compose([v2.ToImage(), v2.ToDtype(torch.float32, scale=True)])`` instead.

    Convert a PIL Image or ndarray to tensor and scale the values accordingly.

    .. warning::
        :class:`v2.ToTensor` is deprecated and will be removed in a future release.
        Please use instead ``v2.Compose([v2.ToImage(), v2.ToDtype(torch.float32, scale=True)])``.
        Output is equivalent up to float precision.

    This transform does not support torchscript.


    Converts a PIL Image or numpy.ndarray (H x W x C) in the range
    [0, 255] to a torch.FloatTensor of shape (C x H x W) in the range [0.0, 1.0]
    if the PIL Image belongs to one of the modes (L, LA, P, I, F, RGB, YCbCr, RGBA, CMYK, 1)
    or if the numpy.ndarray has dtype = np.uint8

    In the other cases, tensors are returned without scaling.

    .. note::
        Because the input image is scaled to [0.0, 1.0], this transformation should not be used when
        transforming target image masks. See the `references`_ for implementing the transforms for image masks.

    .. _references: https://github.com/pytorch/vision/tree/main/references/segmentation
    """

    _transformed_types = (PIL.Image.Image, np.ndarray)

    def __init__(self) -> None:
        warnings.warn(
            "The transform `ToTensor()` is deprecated and will be removed in a future release. "
            "Instead, please use `v2.Compose([v2.ToImage(), v2.ToDtype(torch.float32, scale=True)])`."
            "Output is equivalent up to float precision."
        )
        super().__init__()

    def _transform(self, inpt: Union[PIL.Image.Image, np.ndarray], params: Dict[str, Any]) -> torch.Tensor:
        return _F.to_tensor(inpt)
