from typing import List, Optional, Tuple

import PIL.Image
import torch
from torchvision import tv_tensors
from torchvision.transforms import _functional_pil as _FP
from torchvision.tv_tensors import BoundingBoxFormat

from torchvision.utils import _log_api_usage_once

from ._utils import _get_kernel, _register_kernel_internal, is_pure_tensor


def get_dimensions(inpt: torch.Tensor) -> List[int]:
    if torch.jit.is_scripting():
        return get_dimensions_image(inpt)

    _log_api_usage_once(get_dimensions)

    kernel = _get_kernel(get_dimensions, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(get_dimensions, torch.Tensor)
@_register_kernel_internal(get_dimensions, tv_tensors.Image, tv_tensor_wrapper=False)
def get_dimensions_image(image: torch.Tensor) -> List[int]:
    chw = list(image.shape[-3:])
    ndims = len(chw)
    if ndims == 3:
        return chw
    elif ndims == 2:
        chw.insert(0, 1)
        return chw
    else:
        raise TypeError(f"Input tensor should have at least two dimensions, but got {ndims}")


_get_dimensions_image_pil = _register_kernel_internal(get_dimensions, PIL.Image.Image)(_FP.get_dimensions)


@_register_kernel_internal(get_dimensions, tv_tensors.Video, tv_tensor_wrapper=False)
def get_dimensions_video(video: torch.Tensor) -> List[int]:
    return get_dimensions_image(video)


def get_num_channels(inpt: torch.Tensor) -> int:
    if torch.jit.is_scripting():
        return get_num_channels_image(inpt)

    _log_api_usage_once(get_num_channels)

    kernel = _get_kernel(get_num_channels, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(get_num_channels, torch.Tensor)
@_register_kernel_internal(get_num_channels, tv_tensors.Image, tv_tensor_wrapper=False)
def get_num_channels_image(image: torch.Tensor) -> int:
    chw = image.shape[-3:]
    ndims = len(chw)
    if ndims == 3:
        return chw[0]
    elif ndims == 2:
        return 1
    else:
        raise TypeError(f"Input tensor should have at least two dimensions, but got {ndims}")


_get_num_channels_image_pil = _register_kernel_internal(get_num_channels, PIL.Image.Image)(_FP.get_image_num_channels)


@_register_kernel_internal(get_num_channels, tv_tensors.Video, tv_tensor_wrapper=False)
def get_num_channels_video(video: torch.Tensor) -> int:
    return get_num_channels_image(video)


# We changed the names to ensure it can be used not only for images but also videos. Thus, we just alias it without
# deprecating the old names.
get_image_num_channels = get_num_channels


def get_size(inpt: torch.Tensor) -> List[int]:
    if torch.jit.is_scripting():
        return get_size_image(inpt)

    _log_api_usage_once(get_size)

    kernel = _get_kernel(get_size, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(get_size, torch.Tensor)
@_register_kernel_internal(get_size, tv_tensors.Image, tv_tensor_wrapper=False)
def get_size_image(image: torch.Tensor) -> List[int]:
    hw = list(image.shape[-2:])
    ndims = len(hw)
    if ndims == 2:
        return hw
    else:
        raise TypeError(f"Input tensor should have at least two dimensions, but got {ndims}")


@_register_kernel_internal(get_size, PIL.Image.Image)
def _get_size_image_pil(image: PIL.Image.Image) -> List[int]:
    width, height = _FP.get_image_size(image)
    return [height, width]


@_register_kernel_internal(get_size, tv_tensors.Video, tv_tensor_wrapper=False)
def get_size_video(video: torch.Tensor) -> List[int]:
    return get_size_image(video)


@_register_kernel_internal(get_size, tv_tensors.Mask, tv_tensor_wrapper=False)
def get_size_mask(mask: torch.Tensor) -> List[int]:
    return get_size_image(mask)


@_register_kernel_internal(get_size, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def get_size_bounding_boxes(bounding_box: tv_tensors.BoundingBoxes) -> List[int]:
    return list(bounding_box.canvas_size)


def get_num_frames(inpt: torch.Tensor) -> int:
    if torch.jit.is_scripting():
        return get_num_frames_video(inpt)

    _log_api_usage_once(get_num_frames)

    kernel = _get_kernel(get_num_frames, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(get_num_frames, torch.Tensor)
@_register_kernel_internal(get_num_frames, tv_tensors.Video, tv_tensor_wrapper=False)
def get_num_frames_video(video: torch.Tensor) -> int:
    return video.shape[-4]


def _xywh_to_xyxy(xywh: torch.Tensor, inplace: bool) -> torch.Tensor:
    xyxy = xywh if inplace else xywh.clone()
    xyxy[..., 2:] += xyxy[..., :2]
    return xyxy


def _xyxy_to_xywh(xyxy: torch.Tensor, inplace: bool) -> torch.Tensor:
    xywh = xyxy if inplace else xyxy.clone()
    xywh[..., 2:] -= xywh[..., :2]
    return xywh


def _cxcywh_to_xyxy(cxcywh: torch.Tensor, inplace: bool) -> torch.Tensor:
    if not inplace:
        cxcywh = cxcywh.clone()

    # Trick to do fast division by 2 and ceil, without casting. It produces the same result as
    # `torchvision.ops._box_convert._box_cxcywh_to_xyxy`.
    half_wh = cxcywh[..., 2:].div(-2, rounding_mode=None if cxcywh.is_floating_point() else "floor").abs_()
    # (cx - width / 2) = x1, same for y1
    cxcywh[..., :2].sub_(half_wh)
    # (x1 + width) = x2, same for y2
    cxcywh[..., 2:].add_(cxcywh[..., :2])

    return cxcywh


def _xyxy_to_cxcywh(xyxy: torch.Tensor, inplace: bool) -> torch.Tensor:
    if not inplace:
        xyxy = xyxy.clone()

    # (x2 - x1) = width, same for height
    xyxy[..., 2:].sub_(xyxy[..., :2])
    # (x1 * 2 + width) / 2 = x1 + width / 2 = x1 + (x2-x1)/2 = (x1 + x2)/2 = cx, same for cy
    xyxy[..., :2].mul_(2).add_(xyxy[..., 2:]).div_(2, rounding_mode=None if xyxy.is_floating_point() else "floor")

    return xyxy


def _convert_bounding_box_format(
    bounding_boxes: torch.Tensor, old_format: BoundingBoxFormat, new_format: BoundingBoxFormat, inplace: bool = False
) -> torch.Tensor:

    if new_format == old_format:
        return bounding_boxes

    # TODO: Add _xywh_to_cxcywh and _cxcywh_to_xywh to improve performance
    if old_format == BoundingBoxFormat.XYWH:
        bounding_boxes = _xywh_to_xyxy(bounding_boxes, inplace)
    elif old_format == BoundingBoxFormat.CXCYWH:
        bounding_boxes = _cxcywh_to_xyxy(bounding_boxes, inplace)

    if new_format == BoundingBoxFormat.XYWH:
        bounding_boxes = _xyxy_to_xywh(bounding_boxes, inplace)
    elif new_format == BoundingBoxFormat.CXCYWH:
        bounding_boxes = _xyxy_to_cxcywh(bounding_boxes, inplace)

    return bounding_boxes


def convert_bounding_box_format(
    inpt: torch.Tensor,
    old_format: Optional[BoundingBoxFormat] = None,
    new_format: Optional[BoundingBoxFormat] = None,
    inplace: bool = False,
) -> torch.Tensor:
    """See :func:`~torchvision.transforms.v2.ConvertBoundingBoxFormat` for details."""
    # This being a kernel / functional hybrid, we need an option to pass `old_format` explicitly for pure tensor
    # inputs as well as extract it from `tv_tensors.BoundingBoxes` inputs. However, putting a default value on
    # `old_format` means we also need to put one on `new_format` to have syntactically correct Python. Here we mimic the
    # default error that would be thrown if `new_format` had no default value.
    if new_format is None:
        raise TypeError("convert_bounding_box_format() missing 1 required argument: 'new_format'")

    if not torch.jit.is_scripting():
        _log_api_usage_once(convert_bounding_box_format)

    if isinstance(old_format, str):
        old_format = BoundingBoxFormat[old_format.upper()]
    if isinstance(new_format, str):
        new_format = BoundingBoxFormat[new_format.upper()]

    if torch.jit.is_scripting() or is_pure_tensor(inpt):
        if old_format is None:
            raise ValueError("For pure tensor inputs, `old_format` has to be passed.")
        return _convert_bounding_box_format(inpt, old_format=old_format, new_format=new_format, inplace=inplace)
    elif isinstance(inpt, tv_tensors.BoundingBoxes):
        if old_format is not None:
            raise ValueError("For bounding box tv_tensor inputs, `old_format` must not be passed.")
        output = _convert_bounding_box_format(
            inpt.as_subclass(torch.Tensor), old_format=inpt.format, new_format=new_format, inplace=inplace
        )
        return tv_tensors.wrap(output, like=inpt, format=new_format)
    else:
        raise TypeError(
            f"Input can either be a plain tensor or a bounding box tv_tensor, but got {type(inpt)} instead."
        )


def _clamp_bounding_boxes(
    bounding_boxes: torch.Tensor, format: BoundingBoxFormat, canvas_size: Tuple[int, int]
) -> torch.Tensor:
    # TODO: Investigate if it makes sense from a performance perspective to have an implementation for every
    #  BoundingBoxFormat instead of converting back and forth
    in_dtype = bounding_boxes.dtype
    bounding_boxes = bounding_boxes.clone() if bounding_boxes.is_floating_point() else bounding_boxes.float()
    xyxy_boxes = convert_bounding_box_format(
        bounding_boxes, old_format=format, new_format=tv_tensors.BoundingBoxFormat.XYXY, inplace=True
    )
    xyxy_boxes[..., 0::2].clamp_(min=0, max=canvas_size[1])
    xyxy_boxes[..., 1::2].clamp_(min=0, max=canvas_size[0])
    out_boxes = convert_bounding_box_format(
        xyxy_boxes, old_format=BoundingBoxFormat.XYXY, new_format=format, inplace=True
    )
    return out_boxes.to(in_dtype)


def clamp_bounding_boxes(
    inpt: torch.Tensor,
    format: Optional[BoundingBoxFormat] = None,
    canvas_size: Optional[Tuple[int, int]] = None,
) -> torch.Tensor:
    """See :func:`~torchvision.transforms.v2.ClampBoundingBoxes` for details."""
    if not torch.jit.is_scripting():
        _log_api_usage_once(clamp_bounding_boxes)

    if torch.jit.is_scripting() or is_pure_tensor(inpt):

        if format is None or canvas_size is None:
            raise ValueError("For pure tensor inputs, `format` and `canvas_size` have to be passed.")
        return _clamp_bounding_boxes(inpt, format=format, canvas_size=canvas_size)
    elif isinstance(inpt, tv_tensors.BoundingBoxes):
        if format is not None or canvas_size is not None:
            raise ValueError("For bounding box tv_tensor inputs, `format` and `canvas_size` must not be passed.")
        output = _clamp_bounding_boxes(inpt.as_subclass(torch.Tensor), format=inpt.format, canvas_size=inpt.canvas_size)
        return tv_tensors.wrap(output, like=inpt)
    else:
        raise TypeError(
            f"Input can either be a plain tensor or a bounding box tv_tensor, but got {type(inpt)} instead."
        )
