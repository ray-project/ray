from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

import torch
from torch.utils._pytree import tree_flatten

from ._tv_tensor import TVTensor


class BoundingBoxFormat(Enum):
    """Coordinate format of a bounding box.

    Available formats are

    * ``XYXY``
    * ``XYWH``
    * ``CXCYWH``
    """

    XYXY = "XYXY"
    XYWH = "XYWH"
    CXCYWH = "CXCYWH"


class BoundingBoxes(TVTensor):
    """:class:`torch.Tensor` subclass for bounding boxes with shape ``[N, 4]``.

    .. note::
        There should be only one :class:`~torchvision.tv_tensors.BoundingBoxes`
        instance per sample e.g. ``{"img": img, "bbox": BoundingBoxes(...)}``,
        although one :class:`~torchvision.tv_tensors.BoundingBoxes` object can
        contain multiple bounding boxes.

    Args:
        data: Any data that can be turned into a tensor with :func:`torch.as_tensor`.
        format (BoundingBoxFormat, str): Format of the bounding box.
        canvas_size (two-tuple of ints): Height and width of the corresponding image or video.
        dtype (torch.dtype, optional): Desired data type of the bounding box. If omitted, will be inferred from
            ``data``.
        device (torch.device, optional): Desired device of the bounding box. If omitted and ``data`` is a
            :class:`torch.Tensor`, the device is taken from it. Otherwise, the bounding box is constructed on the CPU.
        requires_grad (bool, optional): Whether autograd should record operations on the bounding box. If omitted and
            ``data`` is a :class:`torch.Tensor`, the value is taken from it. Otherwise, defaults to ``False``.
    """

    format: BoundingBoxFormat
    canvas_size: Tuple[int, int]

    @classmethod
    def _wrap(cls, tensor: torch.Tensor, *, format: Union[BoundingBoxFormat, str], canvas_size: Tuple[int, int], check_dims: bool = True) -> BoundingBoxes:  # type: ignore[override]
        if check_dims:
            if tensor.ndim == 1:
                tensor = tensor.unsqueeze(0)
            elif tensor.ndim != 2:
                raise ValueError(f"Expected a 1D or 2D tensor, got {tensor.ndim}D")
        if isinstance(format, str):
            format = BoundingBoxFormat[format.upper()]
        bounding_boxes = tensor.as_subclass(cls)
        bounding_boxes.format = format
        bounding_boxes.canvas_size = canvas_size
        return bounding_boxes

    def __new__(
        cls,
        data: Any,
        *,
        format: Union[BoundingBoxFormat, str],
        canvas_size: Tuple[int, int],
        dtype: Optional[torch.dtype] = None,
        device: Optional[Union[torch.device, str, int]] = None,
        requires_grad: Optional[bool] = None,
    ) -> BoundingBoxes:
        tensor = cls._to_tensor(data, dtype=dtype, device=device, requires_grad=requires_grad)
        return cls._wrap(tensor, format=format, canvas_size=canvas_size)

    @classmethod
    def _wrap_output(
        cls,
        output: torch.Tensor,
        args: Sequence[Any] = (),
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> BoundingBoxes:
        # If there are BoundingBoxes instances in the output, their metadata got lost when we called
        # super().__torch_function__. We need to restore the metadata somehow, so we choose to take
        # the metadata from the first bbox in the parameters.
        # This should be what we want in most cases. When it's not, it's probably a mis-use anyway, e.g.
        # something like some_xyxy_bbox + some_xywh_bbox; we don't guard against those cases.
        flat_params, _ = tree_flatten(args + (tuple(kwargs.values()) if kwargs else ()))  # type: ignore[operator]
        first_bbox_from_args = next(x for x in flat_params if isinstance(x, BoundingBoxes))
        format, canvas_size = first_bbox_from_args.format, first_bbox_from_args.canvas_size

        if isinstance(output, torch.Tensor) and not isinstance(output, BoundingBoxes):
            output = BoundingBoxes._wrap(output, format=format, canvas_size=canvas_size, check_dims=False)
        elif isinstance(output, (tuple, list)):
            output = type(output)(
                BoundingBoxes._wrap(part, format=format, canvas_size=canvas_size, check_dims=False) for part in output
            )
        return output

    def __repr__(self, *, tensor_contents: Any = None) -> str:  # type: ignore[override]
        return self._make_repr(format=self.format, canvas_size=self.canvas_size)
