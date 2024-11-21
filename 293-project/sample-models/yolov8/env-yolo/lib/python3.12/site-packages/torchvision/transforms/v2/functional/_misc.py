import math
from typing import List, Optional, Tuple

import PIL.Image
import torch
from torch.nn.functional import conv2d, pad as torch_pad

from torchvision import tv_tensors
from torchvision.transforms._functional_tensor import _max_value
from torchvision.transforms.functional import pil_to_tensor, to_pil_image

from torchvision.utils import _log_api_usage_once

from ._meta import _convert_bounding_box_format

from ._utils import _get_kernel, _register_kernel_internal, is_pure_tensor


def normalize(
    inpt: torch.Tensor,
    mean: List[float],
    std: List[float],
    inplace: bool = False,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.Normalize` for details."""
    if torch.jit.is_scripting():
        return normalize_image(inpt, mean=mean, std=std, inplace=inplace)

    _log_api_usage_once(normalize)

    kernel = _get_kernel(normalize, type(inpt))
    return kernel(inpt, mean=mean, std=std, inplace=inplace)


@_register_kernel_internal(normalize, torch.Tensor)
@_register_kernel_internal(normalize, tv_tensors.Image)
def normalize_image(image: torch.Tensor, mean: List[float], std: List[float], inplace: bool = False) -> torch.Tensor:
    if not image.is_floating_point():
        raise TypeError(f"Input tensor should be a float tensor. Got {image.dtype}.")

    if image.ndim < 3:
        raise ValueError(f"Expected tensor to be a tensor image of size (..., C, H, W). Got {image.shape}.")

    if isinstance(std, (tuple, list)):
        divzero = not all(std)
    elif isinstance(std, (int, float)):
        divzero = std == 0
    else:
        divzero = False
    if divzero:
        raise ValueError("std evaluated to zero, leading to division by zero.")

    dtype = image.dtype
    device = image.device
    mean = torch.as_tensor(mean, dtype=dtype, device=device)
    std = torch.as_tensor(std, dtype=dtype, device=device)
    if mean.ndim == 1:
        mean = mean.view(-1, 1, 1)
    if std.ndim == 1:
        std = std.view(-1, 1, 1)

    if inplace:
        image = image.sub_(mean)
    else:
        image = image.sub(mean)

    return image.div_(std)


@_register_kernel_internal(normalize, tv_tensors.Video)
def normalize_video(video: torch.Tensor, mean: List[float], std: List[float], inplace: bool = False) -> torch.Tensor:
    return normalize_image(video, mean, std, inplace=inplace)


def gaussian_blur(inpt: torch.Tensor, kernel_size: List[int], sigma: Optional[List[float]] = None) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.GaussianBlur` for details."""
    if torch.jit.is_scripting():
        return gaussian_blur_image(inpt, kernel_size=kernel_size, sigma=sigma)

    _log_api_usage_once(gaussian_blur)

    kernel = _get_kernel(gaussian_blur, type(inpt))
    return kernel(inpt, kernel_size=kernel_size, sigma=sigma)


def _get_gaussian_kernel1d(kernel_size: int, sigma: float, dtype: torch.dtype, device: torch.device) -> torch.Tensor:
    lim = (kernel_size - 1) / (2.0 * math.sqrt(2.0))
    x = torch.linspace(-lim, lim, steps=kernel_size, dtype=dtype, device=device)
    kernel1d = torch.softmax(x.div(sigma).pow(2).neg(), dim=0)
    return kernel1d


def _get_gaussian_kernel2d(
    kernel_size: List[int], sigma: List[float], dtype: torch.dtype, device: torch.device
) -> torch.Tensor:
    kernel1d_x = _get_gaussian_kernel1d(kernel_size[0], sigma[0], dtype, device)
    kernel1d_y = _get_gaussian_kernel1d(kernel_size[1], sigma[1], dtype, device)
    kernel2d = kernel1d_y.unsqueeze(-1) * kernel1d_x
    return kernel2d


@_register_kernel_internal(gaussian_blur, torch.Tensor)
@_register_kernel_internal(gaussian_blur, tv_tensors.Image)
def gaussian_blur_image(
    image: torch.Tensor, kernel_size: List[int], sigma: Optional[List[float]] = None
) -> torch.Tensor:
    # TODO: consider deprecating integers from sigma on the future
    if isinstance(kernel_size, int):
        kernel_size = [kernel_size, kernel_size]
    elif len(kernel_size) != 2:
        raise ValueError(f"If kernel_size is a sequence its length should be 2. Got {len(kernel_size)}")
    for ksize in kernel_size:
        if ksize % 2 == 0 or ksize < 0:
            raise ValueError(f"kernel_size should have odd and positive integers. Got {kernel_size}")

    if sigma is None:
        sigma = [ksize * 0.15 + 0.35 for ksize in kernel_size]
    else:
        if isinstance(sigma, (list, tuple)):
            length = len(sigma)
            if length == 1:
                s = sigma[0]
                sigma = [s, s]
            elif length != 2:
                raise ValueError(f"If sigma is a sequence, its length should be 2. Got {length}")
        elif isinstance(sigma, (int, float)):
            s = float(sigma)
            sigma = [s, s]
        else:
            raise TypeError(f"sigma should be either float or sequence of floats. Got {type(sigma)}")
    for s in sigma:
        if s <= 0.0:
            raise ValueError(f"sigma should have positive values. Got {sigma}")

    if image.numel() == 0:
        return image

    dtype = image.dtype
    shape = image.shape
    ndim = image.ndim
    if ndim == 3:
        image = image.unsqueeze(dim=0)
    elif ndim > 4:
        image = image.reshape((-1,) + shape[-3:])

    fp = torch.is_floating_point(image)
    kernel = _get_gaussian_kernel2d(kernel_size, sigma, dtype=dtype if fp else torch.float32, device=image.device)
    kernel = kernel.expand(shape[-3], 1, kernel.shape[0], kernel.shape[1])

    output = image if fp else image.to(dtype=torch.float32)

    # padding = (left, right, top, bottom)
    padding = [kernel_size[0] // 2, kernel_size[0] // 2, kernel_size[1] // 2, kernel_size[1] // 2]
    output = torch_pad(output, padding, mode="reflect")
    output = conv2d(output, kernel, groups=shape[-3])

    if ndim == 3:
        output = output.squeeze(dim=0)
    elif ndim > 4:
        output = output.reshape(shape)

    if not fp:
        output = output.round_().to(dtype=dtype)

    return output


@_register_kernel_internal(gaussian_blur, PIL.Image.Image)
def _gaussian_blur_image_pil(
    image: PIL.Image.Image, kernel_size: List[int], sigma: Optional[List[float]] = None
) -> PIL.Image.Image:
    t_img = pil_to_tensor(image)
    output = gaussian_blur_image(t_img, kernel_size=kernel_size, sigma=sigma)
    return to_pil_image(output, mode=image.mode)


@_register_kernel_internal(gaussian_blur, tv_tensors.Video)
def gaussian_blur_video(
    video: torch.Tensor, kernel_size: List[int], sigma: Optional[List[float]] = None
) -> torch.Tensor:
    return gaussian_blur_image(video, kernel_size, sigma)


def gaussian_noise(inpt: torch.Tensor, mean: float = 0.0, sigma: float = 0.1, clip: bool = True) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.GaussianNoise`"""
    if torch.jit.is_scripting():
        return gaussian_noise_image(inpt, mean=mean, sigma=sigma)

    _log_api_usage_once(gaussian_noise)

    kernel = _get_kernel(gaussian_noise, type(inpt))
    return kernel(inpt, mean=mean, sigma=sigma, clip=clip)


@_register_kernel_internal(gaussian_noise, torch.Tensor)
@_register_kernel_internal(gaussian_noise, tv_tensors.Image)
def gaussian_noise_image(image: torch.Tensor, mean: float = 0.0, sigma: float = 0.1, clip: bool = True) -> torch.Tensor:
    if not image.is_floating_point():
        raise ValueError(f"Input tensor is expected to be in float dtype, got dtype={image.dtype}")
    if sigma < 0:
        raise ValueError(f"sigma shouldn't be negative. Got {sigma}")

    noise = mean + torch.randn_like(image) * sigma
    out = image + noise
    if clip:
        out = torch.clamp(out, 0, 1)
    return out


@_register_kernel_internal(gaussian_noise, tv_tensors.Video)
def gaussian_noise_video(video: torch.Tensor, mean: float = 0.0, sigma: float = 0.1, clip: bool = True) -> torch.Tensor:
    return gaussian_noise_image(video, mean=mean, sigma=sigma, clip=clip)


@_register_kernel_internal(gaussian_noise, PIL.Image.Image)
def _gaussian_noise_pil(
    video: torch.Tensor, mean: float = 0.0, sigma: float = 0.1, clip: bool = True
) -> PIL.Image.Image:
    raise ValueError("Gaussian Noise is not implemented for PIL images.")


def to_dtype(inpt: torch.Tensor, dtype: torch.dtype = torch.float, scale: bool = False) -> torch.Tensor:
    """See :func:`~torchvision.transforms.v2.ToDtype` for details."""
    if torch.jit.is_scripting():
        return to_dtype_image(inpt, dtype=dtype, scale=scale)

    _log_api_usage_once(to_dtype)

    kernel = _get_kernel(to_dtype, type(inpt))
    return kernel(inpt, dtype=dtype, scale=scale)


def _num_value_bits(dtype: torch.dtype) -> int:
    if dtype == torch.uint8:
        return 8
    elif dtype == torch.int8:
        return 7
    elif dtype == torch.int16:
        return 15
    elif dtype == torch.uint16:
        return 16
    elif dtype == torch.int32:
        return 31
    elif dtype == torch.int64:
        return 63
    else:
        raise TypeError(f"Number of value bits is only defined for integer dtypes, but got {dtype}.")


@_register_kernel_internal(to_dtype, torch.Tensor)
@_register_kernel_internal(to_dtype, tv_tensors.Image)
def to_dtype_image(image: torch.Tensor, dtype: torch.dtype = torch.float, scale: bool = False) -> torch.Tensor:

    if image.dtype == dtype:
        return image
    elif not scale:
        return image.to(dtype)

    float_input = image.is_floating_point()
    if torch.jit.is_scripting():
        # TODO: remove this branch as soon as `dtype.is_floating_point` is supported by JIT
        float_output = torch.tensor(0, dtype=dtype).is_floating_point()
    else:
        float_output = dtype.is_floating_point

    if float_input:
        # float to float
        if float_output:
            return image.to(dtype)

        # float to int
        if (image.dtype == torch.float32 and dtype in (torch.int32, torch.int64)) or (
            image.dtype == torch.float64 and dtype == torch.int64
        ):
            raise RuntimeError(f"The conversion from {image.dtype} to {dtype} cannot be performed safely.")

        # For data in the range `[0.0, 1.0]`, just multiplying by the maximum value of the integer range and converting
        # to the integer dtype  is not sufficient. For example, `torch.rand(...).mul(255).to(torch.uint8)` will only
        # be `255` if the input is exactly `1.0`. See https://github.com/pytorch/vision/pull/2078#issuecomment-612045321
        # for a detailed analysis.
        # To mitigate this, we could round before we convert to the integer dtype, but this is an extra operation.
        # Instead, we can also multiply by the maximum value plus something close to `1`. See
        # https://github.com/pytorch/vision/pull/2078#issuecomment-613524965 for details.
        eps = 1e-3
        max_value = float(_max_value(dtype))
        # We need to scale first since the conversion would otherwise turn the input range `[0.0, 1.0]` into the
        # discrete set `{0, 1}`.
        return image.mul(max_value + 1.0 - eps).to(dtype)
    else:
        # int to float
        if float_output:
            return image.to(dtype).mul_(1.0 / _max_value(image.dtype))

        # int to int
        num_value_bits_input = _num_value_bits(image.dtype)
        num_value_bits_output = _num_value_bits(dtype)

        # TODO: Remove if/else inner blocks once uint16 dtype supports bitwise shift operations.
        shift_by = abs(num_value_bits_input - num_value_bits_output)
        if num_value_bits_input > num_value_bits_output:
            if image.dtype == torch.uint16:
                return (image / 2 ** (shift_by)).to(dtype)
            else:
                return image.bitwise_right_shift(shift_by).to(dtype)
        else:
            if dtype == torch.uint16:
                return image.to(dtype) * 2 ** (shift_by)
            else:
                return image.to(dtype).bitwise_left_shift_(shift_by)


# We encourage users to use to_dtype() instead but we keep this for BC
def convert_image_dtype(image: torch.Tensor, dtype: torch.dtype = torch.float32) -> torch.Tensor:
    """[DEPRECATED] Use to_dtype() instead."""
    return to_dtype_image(image, dtype=dtype, scale=True)


@_register_kernel_internal(to_dtype, tv_tensors.Video)
def to_dtype_video(video: torch.Tensor, dtype: torch.dtype = torch.float, scale: bool = False) -> torch.Tensor:
    return to_dtype_image(video, dtype, scale=scale)


@_register_kernel_internal(to_dtype, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
@_register_kernel_internal(to_dtype, tv_tensors.Mask, tv_tensor_wrapper=False)
def _to_dtype_tensor_dispatch(inpt: torch.Tensor, dtype: torch.dtype, scale: bool = False) -> torch.Tensor:
    # We don't need to unwrap and rewrap here, since TVTensor.to() preserves the type
    return inpt.to(dtype)


def sanitize_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: Optional[tv_tensors.BoundingBoxFormat] = None,
    canvas_size: Optional[Tuple[int, int]] = None,
    min_size: float = 1.0,
    min_area: float = 1.0,
) -> Tuple[torch.Tensor, torch.Tensor]:
    """Remove degenerate/invalid bounding boxes and return the corresponding indexing mask.

    This removes bounding boxes that:

    - are below a given ``min_size`` or ``min_area``: by default this also removes degenerate boxes that have e.g. X2 <= X1.
    - have any coordinate outside of their corresponding image. You may want to
      call :func:`~torchvision.transforms.v2.functional.clamp_bounding_boxes` first to avoid undesired removals.

    It is recommended to call it at the end of a pipeline, before passing the
    input to the models. It is critical to call this transform if
    :class:`~torchvision.transforms.v2.RandomIoUCrop` was called.
    If you want to be extra careful, you may call it after all transforms that
    may modify bounding boxes but once at the end should be enough in most
    cases.

    Args:
        bounding_boxes (Tensor or :class:`~torchvision.tv_tensors.BoundingBoxes`): The bounding boxes to be sanitized.
        format (str or :class:`~torchvision.tv_tensors.BoundingBoxFormat`, optional): The format of the bounding boxes.
            Must be left to none if ``bounding_boxes`` is a :class:`~torchvision.tv_tensors.BoundingBoxes` object.
        canvas_size (tuple of int, optional): The canvas_size of the bounding boxes
            (size of the corresponding image/video).
            Must be left to none if ``bounding_boxes`` is a :class:`~torchvision.tv_tensors.BoundingBoxes` object.
        min_size (float, optional) The size below which bounding boxes are removed. Default is 1.
        min_area (float, optional) The area below which bounding boxes are removed. Default is 1.

    Returns:
        out (tuple of Tensors): The subset of valid bounding boxes, and the corresponding indexing mask.
        The mask can then be used to subset other tensors (e.g. labels) that are associated with the bounding boxes.
    """
    if torch.jit.is_scripting() or is_pure_tensor(bounding_boxes):
        if format is None or canvas_size is None:
            raise ValueError(
                "format and canvas_size cannot be None if bounding_boxes is a pure tensor. "
                f"Got format={format} and canvas_size={canvas_size}."
                "Set those to appropriate values or pass bounding_boxes as a tv_tensors.BoundingBoxes object."
            )
        if isinstance(format, str):
            format = tv_tensors.BoundingBoxFormat[format.upper()]
        valid = _get_sanitize_bounding_boxes_mask(
            bounding_boxes, format=format, canvas_size=canvas_size, min_size=min_size, min_area=min_area
        )
        bounding_boxes = bounding_boxes[valid]
    else:
        if not isinstance(bounding_boxes, tv_tensors.BoundingBoxes):
            raise ValueError("bounding_boxes must be a tv_tensors.BoundingBoxes instance or a pure tensor.")
        if format is not None or canvas_size is not None:
            raise ValueError(
                "format and canvas_size must be None when bounding_boxes is a tv_tensors.BoundingBoxes instance. "
                f"Got format={format} and canvas_size={canvas_size}. "
                "Leave those to None or pass bounding_boxes as a pure tensor."
            )
        valid = _get_sanitize_bounding_boxes_mask(
            bounding_boxes,
            format=bounding_boxes.format,
            canvas_size=bounding_boxes.canvas_size,
            min_size=min_size,
            min_area=min_area,
        )
        bounding_boxes = tv_tensors.wrap(bounding_boxes[valid], like=bounding_boxes)

    return bounding_boxes, valid


def _get_sanitize_bounding_boxes_mask(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    min_size: float = 1.0,
    min_area: float = 1.0,
) -> torch.Tensor:

    bounding_boxes = _convert_bounding_box_format(
        bounding_boxes, new_format=tv_tensors.BoundingBoxFormat.XYXY, old_format=format
    )

    image_h, image_w = canvas_size
    ws, hs = bounding_boxes[:, 2] - bounding_boxes[:, 0], bounding_boxes[:, 3] - bounding_boxes[:, 1]
    valid = (ws >= min_size) & (hs >= min_size) & (bounding_boxes >= 0).all(dim=-1) & (ws * hs >= min_area)
    # TODO: Do we really need to check for out of bounds here? All
    # transforms should be clamping anyway, so this should never happen?
    image_h, image_w = canvas_size
    valid &= (bounding_boxes[:, 0] <= image_w) & (bounding_boxes[:, 2] <= image_w)
    valid &= (bounding_boxes[:, 1] <= image_h) & (bounding_boxes[:, 3] <= image_h)
    return valid
