import math
import numbers
import warnings
from typing import Any, List, Optional, Sequence, Tuple, Union

import PIL.Image
import torch
from torch.nn.functional import grid_sample, interpolate, pad as torch_pad

from torchvision import tv_tensors
from torchvision.transforms import _functional_pil as _FP
from torchvision.transforms._functional_tensor import _pad_symmetric
from torchvision.transforms.functional import (
    _compute_resized_output_size as __compute_resized_output_size,
    _get_perspective_coeffs,
    _interpolation_modes_from_int,
    InterpolationMode,
    pil_modes_mapping,
    pil_to_tensor,
    to_pil_image,
)

from torchvision.utils import _log_api_usage_once

from ._meta import _get_size_image_pil, clamp_bounding_boxes, convert_bounding_box_format

from ._utils import _FillTypeJIT, _get_kernel, _register_five_ten_crop_kernel_internal, _register_kernel_internal


def _check_interpolation(interpolation: Union[InterpolationMode, int]) -> InterpolationMode:
    if isinstance(interpolation, int):
        interpolation = _interpolation_modes_from_int(interpolation)
    elif not isinstance(interpolation, InterpolationMode):
        raise ValueError(
            f"Argument interpolation should be an `InterpolationMode` or a corresponding Pillow integer constant, "
            f"but got {interpolation}."
        )
    return interpolation


def horizontal_flip(inpt: torch.Tensor) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomHorizontalFlip` for details."""
    if torch.jit.is_scripting():
        return horizontal_flip_image(inpt)

    _log_api_usage_once(horizontal_flip)

    kernel = _get_kernel(horizontal_flip, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(horizontal_flip, torch.Tensor)
@_register_kernel_internal(horizontal_flip, tv_tensors.Image)
def horizontal_flip_image(image: torch.Tensor) -> torch.Tensor:
    return image.flip(-1)


@_register_kernel_internal(horizontal_flip, PIL.Image.Image)
def _horizontal_flip_image_pil(image: PIL.Image.Image) -> PIL.Image.Image:
    return _FP.hflip(image)


@_register_kernel_internal(horizontal_flip, tv_tensors.Mask)
def horizontal_flip_mask(mask: torch.Tensor) -> torch.Tensor:
    return horizontal_flip_image(mask)


def horizontal_flip_bounding_boxes(
    bounding_boxes: torch.Tensor, format: tv_tensors.BoundingBoxFormat, canvas_size: Tuple[int, int]
) -> torch.Tensor:
    shape = bounding_boxes.shape

    bounding_boxes = bounding_boxes.clone().reshape(-1, 4)

    if format == tv_tensors.BoundingBoxFormat.XYXY:
        bounding_boxes[:, [2, 0]] = bounding_boxes[:, [0, 2]].sub_(canvas_size[1]).neg_()
    elif format == tv_tensors.BoundingBoxFormat.XYWH:
        bounding_boxes[:, 0].add_(bounding_boxes[:, 2]).sub_(canvas_size[1]).neg_()
    else:  # format == tv_tensors.BoundingBoxFormat.CXCYWH:
        bounding_boxes[:, 0].sub_(canvas_size[1]).neg_()

    return bounding_boxes.reshape(shape)


@_register_kernel_internal(horizontal_flip, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _horizontal_flip_bounding_boxes_dispatch(inpt: tv_tensors.BoundingBoxes) -> tv_tensors.BoundingBoxes:
    output = horizontal_flip_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, canvas_size=inpt.canvas_size
    )
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(horizontal_flip, tv_tensors.Video)
def horizontal_flip_video(video: torch.Tensor) -> torch.Tensor:
    return horizontal_flip_image(video)


def vertical_flip(inpt: torch.Tensor) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomVerticalFlip` for details."""
    if torch.jit.is_scripting():
        return vertical_flip_image(inpt)

    _log_api_usage_once(vertical_flip)

    kernel = _get_kernel(vertical_flip, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(vertical_flip, torch.Tensor)
@_register_kernel_internal(vertical_flip, tv_tensors.Image)
def vertical_flip_image(image: torch.Tensor) -> torch.Tensor:
    return image.flip(-2)


@_register_kernel_internal(vertical_flip, PIL.Image.Image)
def _vertical_flip_image_pil(image: PIL.Image.Image) -> PIL.Image.Image:
    return _FP.vflip(image)


@_register_kernel_internal(vertical_flip, tv_tensors.Mask)
def vertical_flip_mask(mask: torch.Tensor) -> torch.Tensor:
    return vertical_flip_image(mask)


def vertical_flip_bounding_boxes(
    bounding_boxes: torch.Tensor, format: tv_tensors.BoundingBoxFormat, canvas_size: Tuple[int, int]
) -> torch.Tensor:
    shape = bounding_boxes.shape

    bounding_boxes = bounding_boxes.clone().reshape(-1, 4)

    if format == tv_tensors.BoundingBoxFormat.XYXY:
        bounding_boxes[:, [1, 3]] = bounding_boxes[:, [3, 1]].sub_(canvas_size[0]).neg_()
    elif format == tv_tensors.BoundingBoxFormat.XYWH:
        bounding_boxes[:, 1].add_(bounding_boxes[:, 3]).sub_(canvas_size[0]).neg_()
    else:  # format == tv_tensors.BoundingBoxFormat.CXCYWH:
        bounding_boxes[:, 1].sub_(canvas_size[0]).neg_()

    return bounding_boxes.reshape(shape)


@_register_kernel_internal(vertical_flip, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _vertical_flip_bounding_boxes_dispatch(inpt: tv_tensors.BoundingBoxes) -> tv_tensors.BoundingBoxes:
    output = vertical_flip_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, canvas_size=inpt.canvas_size
    )
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(vertical_flip, tv_tensors.Video)
def vertical_flip_video(video: torch.Tensor) -> torch.Tensor:
    return vertical_flip_image(video)


# We changed the names to align them with the transforms, i.e. `RandomHorizontalFlip`. Still, `hflip` and `vflip` are
# prevalent and well understood. Thus, we just alias them without deprecating the old names.
hflip = horizontal_flip
vflip = vertical_flip


def _compute_resized_output_size(
    canvas_size: Tuple[int, int], size: Optional[List[int]], max_size: Optional[int] = None
) -> List[int]:
    if isinstance(size, int):
        size = [size]
    elif max_size is not None and size is not None and len(size) != 1:
        raise ValueError(
            "max_size should only be passed if size is None or specifies the length of the smaller edge, "
            "i.e. size should be an int or a sequence of length 1 in torchscript mode."
        )
    return __compute_resized_output_size(canvas_size, size=size, max_size=max_size, allow_size_none=True)


def resize(
    inpt: torch.Tensor,
    size: Optional[List[int]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    max_size: Optional[int] = None,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.Resize` for details."""
    if torch.jit.is_scripting():
        return resize_image(inpt, size=size, interpolation=interpolation, max_size=max_size, antialias=antialias)

    _log_api_usage_once(resize)

    kernel = _get_kernel(resize, type(inpt))
    return kernel(inpt, size=size, interpolation=interpolation, max_size=max_size, antialias=antialias)


# This is an internal helper method for resize_image. We should put it here instead of keeping it
# inside resize_image due to torchscript.
# uint8 dtype support for bilinear and bicubic is limited to cpu and
# according to our benchmarks on eager, non-AVX CPUs should still prefer u8->f32->interpolate->u8 path for bilinear
def _do_native_uint8_resize_on_cpu(interpolation: InterpolationMode) -> bool:
    if interpolation == InterpolationMode.BILINEAR:
        if torch.compiler.is_compiling():
            return True
        else:
            return "AVX2" in torch.backends.cpu.get_cpu_capability()

    return interpolation == InterpolationMode.BICUBIC


@_register_kernel_internal(resize, torch.Tensor)
@_register_kernel_internal(resize, tv_tensors.Image)
def resize_image(
    image: torch.Tensor,
    size: Optional[List[int]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    max_size: Optional[int] = None,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    interpolation = _check_interpolation(interpolation)
    antialias = False if antialias is None else antialias
    align_corners: Optional[bool] = None
    if interpolation == InterpolationMode.BILINEAR or interpolation == InterpolationMode.BICUBIC:
        align_corners = False
    else:
        # The default of antialias is True from 0.17, so we don't warn or
        # error if other interpolation modes are used. This is documented.
        antialias = False

    shape = image.shape
    numel = image.numel()
    num_channels, old_height, old_width = shape[-3:]
    new_height, new_width = _compute_resized_output_size((old_height, old_width), size=size, max_size=max_size)

    if (new_height, new_width) == (old_height, old_width):
        return image
    elif numel > 0:
        dtype = image.dtype
        acceptable_dtypes = [torch.float32, torch.float64]
        if interpolation == InterpolationMode.NEAREST or interpolation == InterpolationMode.NEAREST_EXACT:
            # uint8 dtype can be included for cpu and cuda input if nearest mode
            acceptable_dtypes.append(torch.uint8)
        elif image.device.type == "cpu":
            if _do_native_uint8_resize_on_cpu(interpolation):
                acceptable_dtypes.append(torch.uint8)

        image = image.reshape(-1, num_channels, old_height, old_width)
        strides = image.stride()
        if image.is_contiguous(memory_format=torch.channels_last) and image.shape[0] == 1 and numel != strides[0]:
            # There is a weird behaviour in torch core where the output tensor of `interpolate()` can be allocated as
            # contiguous even though the input is un-ambiguously channels_last (https://github.com/pytorch/pytorch/issues/68430).
            # In particular this happens for the typical torchvision use-case of single CHW images where we fake the batch dim
            # to become 1CHW. Below, we restride those tensors to trick torch core into properly allocating the output as
            # channels_last, thus preserving the memory format of the input. This is not just for format consistency:
            # for uint8 bilinear images, this also avoids an extra copy (re-packing) of the output and saves time.
            # TODO: when https://github.com/pytorch/pytorch/issues/68430 is fixed (possibly by https://github.com/pytorch/pytorch/pull/100373),
            # we should be able to remove this hack.
            new_strides = list(strides)
            new_strides[0] = numel
            image = image.as_strided((1, num_channels, old_height, old_width), new_strides)

        need_cast = dtype not in acceptable_dtypes
        if need_cast:
            image = image.to(dtype=torch.float32)

        image = interpolate(
            image,
            size=[new_height, new_width],
            mode=interpolation.value,
            align_corners=align_corners,
            antialias=antialias,
        )

        if need_cast:
            if interpolation == InterpolationMode.BICUBIC and dtype == torch.uint8:
                # This path is hit on non-AVX archs, or on GPU.
                image = image.clamp_(min=0, max=255)
            if dtype in (torch.uint8, torch.int8, torch.int16, torch.int32, torch.int64):
                image = image.round_()
            image = image.to(dtype=dtype)

    return image.reshape(shape[:-3] + (num_channels, new_height, new_width))


def _resize_image_pil(
    image: PIL.Image.Image,
    size: Union[Sequence[int], int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    max_size: Optional[int] = None,
) -> PIL.Image.Image:
    old_height, old_width = image.height, image.width
    new_height, new_width = _compute_resized_output_size(
        (old_height, old_width),
        size=size,  # type: ignore[arg-type]
        max_size=max_size,
    )

    interpolation = _check_interpolation(interpolation)

    if (new_height, new_width) == (old_height, old_width):
        return image

    return image.resize((new_width, new_height), resample=pil_modes_mapping[interpolation])


@_register_kernel_internal(resize, PIL.Image.Image)
def __resize_image_pil_dispatch(
    image: PIL.Image.Image,
    size: Union[Sequence[int], int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    max_size: Optional[int] = None,
    antialias: Optional[bool] = True,
) -> PIL.Image.Image:
    if antialias is False:
        warnings.warn("Anti-alias option is always applied for PIL Image input. Argument antialias is ignored.")
    return _resize_image_pil(image, size=size, interpolation=interpolation, max_size=max_size)


def resize_mask(mask: torch.Tensor, size: Optional[List[int]], max_size: Optional[int] = None) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = resize_image(mask, size=size, interpolation=InterpolationMode.NEAREST, max_size=max_size)

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(resize, tv_tensors.Mask, tv_tensor_wrapper=False)
def _resize_mask_dispatch(
    inpt: tv_tensors.Mask, size: List[int], max_size: Optional[int] = None, **kwargs: Any
) -> tv_tensors.Mask:
    output = resize_mask(inpt.as_subclass(torch.Tensor), size, max_size=max_size)
    return tv_tensors.wrap(output, like=inpt)


def resize_bounding_boxes(
    bounding_boxes: torch.Tensor,
    canvas_size: Tuple[int, int],
    size: Optional[List[int]],
    max_size: Optional[int] = None,
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    old_height, old_width = canvas_size
    new_height, new_width = _compute_resized_output_size(canvas_size, size=size, max_size=max_size)

    if (new_height, new_width) == (old_height, old_width):
        return bounding_boxes, canvas_size

    w_ratio = new_width / old_width
    h_ratio = new_height / old_height
    ratios = torch.tensor([w_ratio, h_ratio, w_ratio, h_ratio], device=bounding_boxes.device)
    return (
        bounding_boxes.mul(ratios).to(bounding_boxes.dtype),
        (new_height, new_width),
    )


@_register_kernel_internal(resize, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _resize_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, size: Optional[List[int]], max_size: Optional[int] = None, **kwargs: Any
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = resize_bounding_boxes(
        inpt.as_subclass(torch.Tensor), inpt.canvas_size, size, max_size=max_size
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


@_register_kernel_internal(resize, tv_tensors.Video)
def resize_video(
    video: torch.Tensor,
    size: Optional[List[int]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    max_size: Optional[int] = None,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    return resize_image(video, size=size, interpolation=interpolation, max_size=max_size, antialias=antialias)


def affine(
    inpt: torch.Tensor,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomAffine` for details."""
    if torch.jit.is_scripting():
        return affine_image(
            inpt,
            angle=angle,
            translate=translate,
            scale=scale,
            shear=shear,
            interpolation=interpolation,
            fill=fill,
            center=center,
        )

    _log_api_usage_once(affine)

    kernel = _get_kernel(affine, type(inpt))
    return kernel(
        inpt,
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        interpolation=interpolation,
        fill=fill,
        center=center,
    )


def _affine_parse_args(
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    interpolation: InterpolationMode = InterpolationMode.NEAREST,
    center: Optional[List[float]] = None,
) -> Tuple[float, List[float], List[float], Optional[List[float]]]:
    if not isinstance(angle, (int, float)):
        raise TypeError("Argument angle should be int or float")

    if not isinstance(translate, (list, tuple)):
        raise TypeError("Argument translate should be a sequence")

    if len(translate) != 2:
        raise ValueError("Argument translate should be a sequence of length 2")

    if scale <= 0.0:
        raise ValueError("Argument scale should be positive")

    if not isinstance(shear, (numbers.Number, (list, tuple))):
        raise TypeError("Shear should be either a single value or a sequence of two values")

    if not isinstance(interpolation, InterpolationMode):
        raise TypeError("Argument interpolation should be a InterpolationMode")

    if isinstance(angle, int):
        angle = float(angle)

    if isinstance(translate, tuple):
        translate = list(translate)

    if isinstance(shear, numbers.Number):
        shear = [shear, 0.0]

    if isinstance(shear, tuple):
        shear = list(shear)

    if len(shear) == 1:
        shear = [shear[0], shear[0]]

    if len(shear) != 2:
        raise ValueError(f"Shear should be a sequence containing two values. Got {shear}")

    if center is not None:
        if not isinstance(center, (list, tuple)):
            raise TypeError("Argument center should be a sequence")
        else:
            center = [float(c) for c in center]

    return angle, translate, shear, center


def _get_inverse_affine_matrix(
    center: List[float], angle: float, translate: List[float], scale: float, shear: List[float], inverted: bool = True
) -> List[float]:
    # Helper method to compute inverse matrix for affine transformation

    # Pillow requires inverse affine transformation matrix:
    # Affine matrix is : M = T * C * RotateScaleShear * C^-1
    #
    # where T is translation matrix: [1, 0, tx | 0, 1, ty | 0, 0, 1]
    #       C is translation matrix to keep center: [1, 0, cx | 0, 1, cy | 0, 0, 1]
    #       RotateScaleShear is rotation with scale and shear matrix
    #
    #       RotateScaleShear(a, s, (sx, sy)) =
    #       = R(a) * S(s) * SHy(sy) * SHx(sx)
    #       = [ s*cos(a - sy)/cos(sy), s*(-cos(a - sy)*tan(sx)/cos(sy) - sin(a)), 0 ]
    #         [ s*sin(a - sy)/cos(sy), s*(-sin(a - sy)*tan(sx)/cos(sy) + cos(a)), 0 ]
    #         [ 0                    , 0                                      , 1 ]
    # where R is a rotation matrix, S is a scaling matrix, and SHx and SHy are the shears:
    # SHx(s) = [1, -tan(s)] and SHy(s) = [1      , 0]
    #          [0, 1      ]              [-tan(s), 1]
    #
    # Thus, the inverse is M^-1 = C * RotateScaleShear^-1 * C^-1 * T^-1

    rot = math.radians(angle)
    sx = math.radians(shear[0])
    sy = math.radians(shear[1])

    cx, cy = center
    tx, ty = translate

    # Cached results
    cos_sy = math.cos(sy)
    tan_sx = math.tan(sx)
    rot_minus_sy = rot - sy
    cx_plus_tx = cx + tx
    cy_plus_ty = cy + ty

    # Rotate Scale Shear (RSS) without scaling
    a = math.cos(rot_minus_sy) / cos_sy
    b = -(a * tan_sx + math.sin(rot))
    c = math.sin(rot_minus_sy) / cos_sy
    d = math.cos(rot) - c * tan_sx

    if inverted:
        # Inverted rotation matrix with scale and shear
        # det([[a, b], [c, d]]) == 1, since det(rotation) = 1 and det(shear) = 1
        matrix = [d / scale, -b / scale, 0.0, -c / scale, a / scale, 0.0]
        # Apply inverse of translation and of center translation: RSS^-1 * C^-1 * T^-1
        # and then apply center translation: C * RSS^-1 * C^-1 * T^-1
        matrix[2] += cx - matrix[0] * cx_plus_tx - matrix[1] * cy_plus_ty
        matrix[5] += cy - matrix[3] * cx_plus_tx - matrix[4] * cy_plus_ty
    else:
        matrix = [a * scale, b * scale, 0.0, c * scale, d * scale, 0.0]
        # Apply inverse of center translation: RSS * C^-1
        # and then apply translation and center : T * C * RSS * C^-1
        matrix[2] += cx_plus_tx - matrix[0] * cx - matrix[1] * cy
        matrix[5] += cy_plus_ty - matrix[3] * cx - matrix[4] * cy

    return matrix


def _compute_affine_output_size(matrix: List[float], w: int, h: int) -> Tuple[int, int]:
    if torch.compiler.is_compiling() and not torch.jit.is_scripting():
        return _compute_affine_output_size_python(matrix, w, h)
    else:
        return _compute_affine_output_size_tensor(matrix, w, h)


def _compute_affine_output_size_tensor(matrix: List[float], w: int, h: int) -> Tuple[int, int]:
    # Inspired of PIL implementation:
    # https://github.com/python-pillow/Pillow/blob/11de3318867e4398057373ee9f12dcb33db7335c/src/PIL/Image.py#L2054

    # pts are Top-Left, Top-Right, Bottom-Left, Bottom-Right points.
    # Points are shifted due to affine matrix torch convention about
    # the center point. Center is (0, 0) for image center pivot point (w * 0.5, h * 0.5)
    half_w = 0.5 * w
    half_h = 0.5 * h
    pts = torch.tensor(
        [
            [-half_w, -half_h, 1.0],
            [-half_w, half_h, 1.0],
            [half_w, half_h, 1.0],
            [half_w, -half_h, 1.0],
        ]
    )
    theta = torch.tensor(matrix, dtype=torch.float).view(2, 3)
    new_pts = torch.matmul(pts, theta.T)
    min_vals, max_vals = new_pts.aminmax(dim=0)

    # shift points to [0, w] and [0, h] interval to match PIL results
    halfs = torch.tensor((half_w, half_h))
    min_vals.add_(halfs)
    max_vals.add_(halfs)

    # Truncate precision to 1e-4 to avoid ceil of Xe-15 to 1.0
    tol = 1e-4
    inv_tol = 1.0 / tol
    cmax = max_vals.mul_(inv_tol).trunc_().mul_(tol).ceil_()
    cmin = min_vals.mul_(inv_tol).trunc_().mul_(tol).floor_()
    size = cmax.sub_(cmin)
    return int(size[0]), int(size[1])  # w, h


def _compute_affine_output_size_python(matrix: List[float], w: int, h: int) -> Tuple[int, int]:
    # Mostly copied from PIL implementation:
    # The only difference is with transformed points as input matrix has zero translation part here and
    # PIL has a centered translation part.
    # https://github.com/python-pillow/Pillow/blob/11de3318867e4398057373ee9f12dcb33db7335c/src/PIL/Image.py#L2054

    a, b, c, d, e, f = matrix
    xx = []
    yy = []

    half_w = 0.5 * w
    half_h = 0.5 * h
    for x, y in ((-half_w, -half_h), (half_w, -half_h), (half_w, half_h), (-half_w, half_h)):
        nx = a * x + b * y + c
        ny = d * x + e * y + f
        xx.append(nx + half_w)
        yy.append(ny + half_h)

    nw = math.ceil(max(xx)) - math.floor(min(xx))
    nh = math.ceil(max(yy)) - math.floor(min(yy))
    return int(nw), int(nh)  # w, h


def _apply_grid_transform(img: torch.Tensor, grid: torch.Tensor, mode: str, fill: _FillTypeJIT) -> torch.Tensor:
    input_shape = img.shape
    output_height, output_width = grid.shape[1], grid.shape[2]
    num_channels, input_height, input_width = input_shape[-3:]
    output_shape = input_shape[:-3] + (num_channels, output_height, output_width)

    if img.numel() == 0:
        return img.reshape(output_shape)

    img = img.reshape(-1, num_channels, input_height, input_width)
    squashed_batch_size = img.shape[0]

    # We are using context knowledge that grid should have float dtype
    fp = img.dtype == grid.dtype
    float_img = img if fp else img.to(grid.dtype)

    if squashed_batch_size > 1:
        # Apply same grid to a batch of images
        grid = grid.expand(squashed_batch_size, -1, -1, -1)

    # Append a dummy mask for customized fill colors, should be faster than grid_sample() twice
    if fill is not None:
        mask = torch.ones(
            (squashed_batch_size, 1, input_height, input_width), dtype=float_img.dtype, device=float_img.device
        )
        float_img = torch.cat((float_img, mask), dim=1)

    float_img = grid_sample(float_img, grid, mode=mode, padding_mode="zeros", align_corners=False)

    # Fill with required color
    if fill is not None:
        float_img, mask = torch.tensor_split(float_img, indices=(-1,), dim=-3)
        mask = mask.expand_as(float_img)
        fill_list = fill if isinstance(fill, (tuple, list)) else [float(fill)]  # type: ignore[arg-type]
        fill_img = torch.tensor(fill_list, dtype=float_img.dtype, device=float_img.device).view(1, -1, 1, 1)
        if mode == "nearest":
            float_img = torch.where(mask < 0.5, fill_img.expand_as(float_img), float_img)
        else:  # 'bilinear'
            # The following is mathematically equivalent to:
            # img * mask + (1.0 - mask) * fill = img * mask - fill * mask + fill = mask * (img - fill) + fill
            float_img = float_img.sub_(fill_img).mul_(mask).add_(fill_img)

    img = float_img.round_().to(img.dtype) if not fp else float_img

    return img.reshape(output_shape)


def _assert_grid_transform_inputs(
    image: torch.Tensor,
    matrix: Optional[List[float]],
    interpolation: str,
    fill: _FillTypeJIT,
    supported_interpolation_modes: List[str],
    coeffs: Optional[List[float]] = None,
) -> None:
    if matrix is not None:
        if not isinstance(matrix, list):
            raise TypeError("Argument matrix should be a list")
        elif len(matrix) != 6:
            raise ValueError("Argument matrix should have 6 float values")

    if coeffs is not None and len(coeffs) != 8:
        raise ValueError("Argument coeffs should have 8 float values")

    if fill is not None:
        if isinstance(fill, (tuple, list)):
            length = len(fill)
            num_channels = image.shape[-3]
            if length > 1 and length != num_channels:
                raise ValueError(
                    "The number of elements in 'fill' cannot broadcast to match the number of "
                    f"channels of the image ({length} != {num_channels})"
                )
        elif not isinstance(fill, (int, float)):
            raise ValueError("Argument fill should be either int, float, tuple or list")

    if interpolation not in supported_interpolation_modes:
        raise ValueError(f"Interpolation mode '{interpolation}' is unsupported with Tensor input")


def _affine_grid(
    theta: torch.Tensor,
    w: int,
    h: int,
    ow: int,
    oh: int,
) -> torch.Tensor:
    # https://github.com/pytorch/pytorch/blob/74b65c32be68b15dc7c9e8bb62459efbfbde33d8/aten/src/ATen/native/
    # AffineGridGenerator.cpp#L18
    # Difference with AffineGridGenerator is that:
    # 1) we normalize grid values after applying theta
    # 2) we can normalize by other image size, such that it covers "extend" option like in PIL.Image.rotate
    dtype = theta.dtype
    device = theta.device

    base_grid = torch.empty(1, oh, ow, 3, dtype=dtype, device=device)
    x_grid = torch.linspace((1.0 - ow) * 0.5, (ow - 1.0) * 0.5, steps=ow, device=device)
    base_grid[..., 0].copy_(x_grid)
    y_grid = torch.linspace((1.0 - oh) * 0.5, (oh - 1.0) * 0.5, steps=oh, device=device).unsqueeze_(-1)
    base_grid[..., 1].copy_(y_grid)
    base_grid[..., 2].fill_(1)

    rescaled_theta = theta.transpose(1, 2).div_(torch.tensor([0.5 * w, 0.5 * h], dtype=dtype, device=device))
    output_grid = base_grid.view(1, oh * ow, 3).bmm(rescaled_theta)
    return output_grid.view(1, oh, ow, 2)


@_register_kernel_internal(affine, torch.Tensor)
@_register_kernel_internal(affine, tv_tensors.Image)
def affine_image(
    image: torch.Tensor,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
) -> torch.Tensor:
    interpolation = _check_interpolation(interpolation)

    angle, translate, shear, center = _affine_parse_args(angle, translate, scale, shear, interpolation, center)

    height, width = image.shape[-2:]

    center_f = [0.0, 0.0]
    if center is not None:
        # Center values should be in pixel coordinates but translated such that (0, 0) corresponds to image center.
        center_f = [(c - s * 0.5) for c, s in zip(center, [width, height])]

    translate_f = [float(t) for t in translate]
    matrix = _get_inverse_affine_matrix(center_f, angle, translate_f, scale, shear)

    _assert_grid_transform_inputs(image, matrix, interpolation.value, fill, ["nearest", "bilinear"])

    dtype = image.dtype if torch.is_floating_point(image) else torch.float32
    theta = torch.tensor(matrix, dtype=dtype, device=image.device).reshape(1, 2, 3)
    grid = _affine_grid(theta, w=width, h=height, ow=width, oh=height)
    return _apply_grid_transform(image, grid, interpolation.value, fill=fill)


@_register_kernel_internal(affine, PIL.Image.Image)
def _affine_image_pil(
    image: PIL.Image.Image,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
) -> PIL.Image.Image:
    interpolation = _check_interpolation(interpolation)
    angle, translate, shear, center = _affine_parse_args(angle, translate, scale, shear, interpolation, center)

    # center = (img_size[0] * 0.5 + 0.5, img_size[1] * 0.5 + 0.5)
    # it is visually better to estimate the center without 0.5 offset
    # otherwise image rotated by 90 degrees is shifted vs output image of torch.rot90 or F_t.affine
    if center is None:
        height, width = _get_size_image_pil(image)
        center = [width * 0.5, height * 0.5]
    matrix = _get_inverse_affine_matrix(center, angle, translate, scale, shear)

    return _FP.affine(image, matrix, interpolation=pil_modes_mapping[interpolation], fill=fill)


def _affine_bounding_boxes_with_expand(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    center: Optional[List[float]] = None,
    expand: bool = False,
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    if bounding_boxes.numel() == 0:
        return bounding_boxes, canvas_size

    original_shape = bounding_boxes.shape
    original_dtype = bounding_boxes.dtype
    bounding_boxes = bounding_boxes.clone() if bounding_boxes.is_floating_point() else bounding_boxes.float()
    dtype = bounding_boxes.dtype
    device = bounding_boxes.device
    bounding_boxes = (
        convert_bounding_box_format(
            bounding_boxes, old_format=format, new_format=tv_tensors.BoundingBoxFormat.XYXY, inplace=True
        )
    ).reshape(-1, 4)

    angle, translate, shear, center = _affine_parse_args(
        angle, translate, scale, shear, InterpolationMode.NEAREST, center
    )

    if center is None:
        height, width = canvas_size
        center = [width * 0.5, height * 0.5]

    affine_vector = _get_inverse_affine_matrix(center, angle, translate, scale, shear, inverted=False)
    transposed_affine_matrix = (
        torch.tensor(
            affine_vector,
            dtype=dtype,
            device=device,
        )
        .reshape(2, 3)
        .T
    )
    # 1) Let's transform bboxes into a tensor of 4 points (top-left, top-right, bottom-left, bottom-right corners).
    # Tensor of points has shape (N * 4, 3), where N is the number of bboxes
    # Single point structure is similar to
    # [(xmin, ymin, 1), (xmax, ymin, 1), (xmax, ymax, 1), (xmin, ymax, 1)]
    points = bounding_boxes[:, [[0, 1], [2, 1], [2, 3], [0, 3]]].reshape(-1, 2)
    points = torch.cat([points, torch.ones(points.shape[0], 1, device=device, dtype=dtype)], dim=-1)
    # 2) Now let's transform the points using affine matrix
    transformed_points = torch.matmul(points, transposed_affine_matrix)
    # 3) Reshape transformed points to [N boxes, 4 points, x/y coords]
    # and compute bounding box from 4 transformed points:
    transformed_points = transformed_points.reshape(-1, 4, 2)
    out_bbox_mins, out_bbox_maxs = torch.aminmax(transformed_points, dim=1)
    out_bboxes = torch.cat([out_bbox_mins, out_bbox_maxs], dim=1)

    if expand:
        # Compute minimum point for transformed image frame:
        # Points are Top-Left, Top-Right, Bottom-Left, Bottom-Right points.
        height, width = canvas_size
        points = torch.tensor(
            [
                [0.0, 0.0, 1.0],
                [0.0, float(height), 1.0],
                [float(width), float(height), 1.0],
                [float(width), 0.0, 1.0],
            ],
            dtype=dtype,
            device=device,
        )
        new_points = torch.matmul(points, transposed_affine_matrix)
        tr = torch.amin(new_points, dim=0, keepdim=True)
        # Translate bounding boxes
        out_bboxes.sub_(tr.repeat((1, 2)))
        # Estimate meta-data for image with inverted=True
        affine_vector = _get_inverse_affine_matrix(center, angle, translate, scale, shear)
        new_width, new_height = _compute_affine_output_size(affine_vector, width, height)
        canvas_size = (new_height, new_width)

    out_bboxes = clamp_bounding_boxes(out_bboxes, format=tv_tensors.BoundingBoxFormat.XYXY, canvas_size=canvas_size)
    out_bboxes = convert_bounding_box_format(
        out_bboxes, old_format=tv_tensors.BoundingBoxFormat.XYXY, new_format=format, inplace=True
    ).reshape(original_shape)

    out_bboxes = out_bboxes.to(original_dtype)
    return out_bboxes, canvas_size


def affine_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    center: Optional[List[float]] = None,
) -> torch.Tensor:
    out_box, _ = _affine_bounding_boxes_with_expand(
        bounding_boxes,
        format=format,
        canvas_size=canvas_size,
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        center=center,
        expand=False,
    )
    return out_box


@_register_kernel_internal(affine, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _affine_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    center: Optional[List[float]] = None,
    **kwargs,
) -> tv_tensors.BoundingBoxes:
    output = affine_bounding_boxes(
        inpt.as_subclass(torch.Tensor),
        format=inpt.format,
        canvas_size=inpt.canvas_size,
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        center=center,
    )
    return tv_tensors.wrap(output, like=inpt)


def affine_mask(
    mask: torch.Tensor,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = affine_image(
        mask,
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        interpolation=InterpolationMode.NEAREST,
        fill=fill,
        center=center,
    )

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(affine, tv_tensors.Mask, tv_tensor_wrapper=False)
def _affine_mask_dispatch(
    inpt: tv_tensors.Mask,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
    **kwargs,
) -> tv_tensors.Mask:
    output = affine_mask(
        inpt.as_subclass(torch.Tensor),
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        fill=fill,
        center=center,
    )
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(affine, tv_tensors.Video)
def affine_video(
    video: torch.Tensor,
    angle: Union[int, float],
    translate: List[float],
    scale: float,
    shear: List[float],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    fill: _FillTypeJIT = None,
    center: Optional[List[float]] = None,
) -> torch.Tensor:
    return affine_image(
        video,
        angle=angle,
        translate=translate,
        scale=scale,
        shear=shear,
        interpolation=interpolation,
        fill=fill,
        center=center,
    )


def rotate(
    inpt: torch.Tensor,
    angle: float,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomRotation` for details."""
    if torch.jit.is_scripting():
        return rotate_image(inpt, angle=angle, interpolation=interpolation, expand=expand, fill=fill, center=center)

    _log_api_usage_once(rotate)

    kernel = _get_kernel(rotate, type(inpt))
    return kernel(inpt, angle=angle, interpolation=interpolation, expand=expand, fill=fill, center=center)


@_register_kernel_internal(rotate, torch.Tensor)
@_register_kernel_internal(rotate, tv_tensors.Image)
def rotate_image(
    image: torch.Tensor,
    angle: float,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    angle = angle % 360  # shift angle to [0, 360) range

    # fast path: transpose without affine transform
    if center is None:
        if angle == 0:
            return image.clone()
        if angle == 180:
            return torch.rot90(image, k=2, dims=(-2, -1))

        if expand or image.shape[-1] == image.shape[-2]:
            if angle == 90:
                return torch.rot90(image, k=1, dims=(-2, -1))
            if angle == 270:
                return torch.rot90(image, k=3, dims=(-2, -1))

    interpolation = _check_interpolation(interpolation)

    input_height, input_width = image.shape[-2:]

    center_f = [0.0, 0.0]
    if center is not None:
        # Center values should be in pixel coordinates but translated such that (0, 0) corresponds to image center.
        center_f = [(c - s * 0.5) for c, s in zip(center, [input_width, input_height])]

    # due to current incoherence of rotation angle direction between affine and rotate implementations
    # we need to set -angle.
    matrix = _get_inverse_affine_matrix(center_f, -angle, [0.0, 0.0], 1.0, [0.0, 0.0])

    _assert_grid_transform_inputs(image, matrix, interpolation.value, fill, ["nearest", "bilinear"])

    output_width, output_height = (
        _compute_affine_output_size(matrix, input_width, input_height) if expand else (input_width, input_height)
    )
    dtype = image.dtype if torch.is_floating_point(image) else torch.float32
    theta = torch.tensor(matrix, dtype=dtype, device=image.device).reshape(1, 2, 3)
    grid = _affine_grid(theta, w=input_width, h=input_height, ow=output_width, oh=output_height)
    return _apply_grid_transform(image, grid, interpolation.value, fill=fill)


@_register_kernel_internal(rotate, PIL.Image.Image)
def _rotate_image_pil(
    image: PIL.Image.Image,
    angle: float,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
) -> PIL.Image.Image:
    interpolation = _check_interpolation(interpolation)

    return _FP.rotate(
        image, angle, interpolation=pil_modes_mapping[interpolation], expand=expand, fill=fill, center=center
    )


def rotate_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    angle: float,
    expand: bool = False,
    center: Optional[List[float]] = None,
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    return _affine_bounding_boxes_with_expand(
        bounding_boxes,
        format=format,
        canvas_size=canvas_size,
        angle=-angle,
        translate=[0.0, 0.0],
        scale=1.0,
        shear=[0.0, 0.0],
        center=center,
        expand=expand,
    )


@_register_kernel_internal(rotate, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _rotate_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, angle: float, expand: bool = False, center: Optional[List[float]] = None, **kwargs
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = rotate_bounding_boxes(
        inpt.as_subclass(torch.Tensor),
        format=inpt.format,
        canvas_size=inpt.canvas_size,
        angle=angle,
        expand=expand,
        center=center,
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


def rotate_mask(
    mask: torch.Tensor,
    angle: float,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = rotate_image(
        mask,
        angle=angle,
        expand=expand,
        interpolation=InterpolationMode.NEAREST,
        fill=fill,
        center=center,
    )

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(rotate, tv_tensors.Mask, tv_tensor_wrapper=False)
def _rotate_mask_dispatch(
    inpt: tv_tensors.Mask,
    angle: float,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
    **kwargs,
) -> tv_tensors.Mask:
    output = rotate_mask(inpt.as_subclass(torch.Tensor), angle=angle, expand=expand, fill=fill, center=center)
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(rotate, tv_tensors.Video)
def rotate_video(
    video: torch.Tensor,
    angle: float,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
    expand: bool = False,
    center: Optional[List[float]] = None,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    return rotate_image(video, angle, interpolation=interpolation, expand=expand, fill=fill, center=center)


def pad(
    inpt: torch.Tensor,
    padding: List[int],
    fill: Optional[Union[int, float, List[float]]] = None,
    padding_mode: str = "constant",
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.Pad` for details."""
    if torch.jit.is_scripting():
        return pad_image(inpt, padding=padding, fill=fill, padding_mode=padding_mode)

    _log_api_usage_once(pad)

    kernel = _get_kernel(pad, type(inpt))
    return kernel(inpt, padding=padding, fill=fill, padding_mode=padding_mode)


def _parse_pad_padding(padding: Union[int, List[int]]) -> List[int]:
    if isinstance(padding, int):
        pad_left = pad_right = pad_top = pad_bottom = padding
    elif isinstance(padding, (tuple, list)):
        if len(padding) == 1:
            pad_left = pad_right = pad_top = pad_bottom = padding[0]
        elif len(padding) == 2:
            pad_left = pad_right = padding[0]
            pad_top = pad_bottom = padding[1]
        elif len(padding) == 4:
            pad_left = padding[0]
            pad_top = padding[1]
            pad_right = padding[2]
            pad_bottom = padding[3]
        else:
            raise ValueError(
                f"Padding must be an int or a 1, 2, or 4 element tuple, not a {len(padding)} element tuple"
            )
    else:
        raise TypeError(f"`padding` should be an integer or tuple or list of integers, but got {padding}")

    return [pad_left, pad_right, pad_top, pad_bottom]


@_register_kernel_internal(pad, torch.Tensor)
@_register_kernel_internal(pad, tv_tensors.Image)
def pad_image(
    image: torch.Tensor,
    padding: List[int],
    fill: Optional[Union[int, float, List[float]]] = None,
    padding_mode: str = "constant",
) -> torch.Tensor:
    # Be aware that while `padding` has order `[left, top, right, bottom]`, `torch_padding` uses
    # `[left, right, top, bottom]`. This stems from the fact that we align our API with PIL, but need to use `torch_pad`
    # internally.
    torch_padding = _parse_pad_padding(padding)

    if padding_mode not in ("constant", "edge", "reflect", "symmetric"):
        raise ValueError(
            f"`padding_mode` should be either `'constant'`, `'edge'`, `'reflect'` or `'symmetric'`, "
            f"but got `'{padding_mode}'`."
        )

    if fill is None:
        fill = 0

    if isinstance(fill, (int, float)):
        return _pad_with_scalar_fill(image, torch_padding, fill=fill, padding_mode=padding_mode)
    elif len(fill) == 1:
        return _pad_with_scalar_fill(image, torch_padding, fill=fill[0], padding_mode=padding_mode)
    else:
        return _pad_with_vector_fill(image, torch_padding, fill=fill, padding_mode=padding_mode)


def _pad_with_scalar_fill(
    image: torch.Tensor,
    torch_padding: List[int],
    fill: Union[int, float],
    padding_mode: str,
) -> torch.Tensor:
    shape = image.shape
    num_channels, height, width = shape[-3:]

    batch_size = 1
    for s in shape[:-3]:
        batch_size *= s

    image = image.reshape(batch_size, num_channels, height, width)

    if padding_mode == "edge":
        # Similar to the padding order, `torch_pad`'s PIL's padding modes don't have the same names. Thus, we map
        # the PIL name for the padding mode, which we are also using for our API, to the corresponding `torch_pad`
        # name.
        padding_mode = "replicate"

    if padding_mode == "constant":
        image = torch_pad(image, torch_padding, mode=padding_mode, value=float(fill))
    elif padding_mode in ("reflect", "replicate"):
        # `torch_pad` only supports `"reflect"` or `"replicate"` padding for floating point inputs.
        # TODO: See https://github.com/pytorch/pytorch/issues/40763
        dtype = image.dtype
        if not image.is_floating_point():
            needs_cast = True
            image = image.to(torch.float32)
        else:
            needs_cast = False

        image = torch_pad(image, torch_padding, mode=padding_mode)

        if needs_cast:
            image = image.to(dtype)
    else:  # padding_mode == "symmetric"
        image = _pad_symmetric(image, torch_padding)

    new_height, new_width = image.shape[-2:]

    return image.reshape(shape[:-3] + (num_channels, new_height, new_width))


# TODO: This should be removed once torch_pad supports non-scalar padding values
def _pad_with_vector_fill(
    image: torch.Tensor,
    torch_padding: List[int],
    fill: List[float],
    padding_mode: str,
) -> torch.Tensor:
    if padding_mode != "constant":
        raise ValueError(f"Padding mode '{padding_mode}' is not supported if fill is not scalar")

    output = _pad_with_scalar_fill(image, torch_padding, fill=0, padding_mode="constant")
    left, right, top, bottom = torch_padding

    # We are creating the tensor in the autodetected dtype first and convert to the right one after to avoid an implicit
    # float -> int conversion. That happens for example for the valid input of a uint8 image with floating point fill
    # value.
    fill = torch.tensor(fill, device=image.device).to(dtype=image.dtype).reshape(-1, 1, 1)

    if top > 0:
        output[..., :top, :] = fill
    if left > 0:
        output[..., :, :left] = fill
    if bottom > 0:
        output[..., -bottom:, :] = fill
    if right > 0:
        output[..., :, -right:] = fill
    return output


_pad_image_pil = _register_kernel_internal(pad, PIL.Image.Image)(_FP.pad)


@_register_kernel_internal(pad, tv_tensors.Mask)
def pad_mask(
    mask: torch.Tensor,
    padding: List[int],
    fill: Optional[Union[int, float, List[float]]] = None,
    padding_mode: str = "constant",
) -> torch.Tensor:
    if fill is None:
        fill = 0

    if isinstance(fill, (tuple, list)):
        raise ValueError("Non-scalar fill value is not supported")

    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = pad_image(mask, padding=padding, fill=fill, padding_mode=padding_mode)

    if needs_squeeze:
        output = output.squeeze(0)

    return output


def pad_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    padding: List[int],
    padding_mode: str = "constant",
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    if padding_mode not in ["constant"]:
        # TODO: add support of other padding modes
        raise ValueError(f"Padding mode '{padding_mode}' is not supported with bounding boxes")

    left, right, top, bottom = _parse_pad_padding(padding)

    if format == tv_tensors.BoundingBoxFormat.XYXY:
        pad = [left, top, left, top]
    else:
        pad = [left, top, 0, 0]
    bounding_boxes = bounding_boxes + torch.tensor(pad, dtype=bounding_boxes.dtype, device=bounding_boxes.device)

    height, width = canvas_size
    height += top + bottom
    width += left + right
    canvas_size = (height, width)

    return clamp_bounding_boxes(bounding_boxes, format=format, canvas_size=canvas_size), canvas_size


@_register_kernel_internal(pad, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _pad_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, padding: List[int], padding_mode: str = "constant", **kwargs
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = pad_bounding_boxes(
        inpt.as_subclass(torch.Tensor),
        format=inpt.format,
        canvas_size=inpt.canvas_size,
        padding=padding,
        padding_mode=padding_mode,
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


@_register_kernel_internal(pad, tv_tensors.Video)
def pad_video(
    video: torch.Tensor,
    padding: List[int],
    fill: Optional[Union[int, float, List[float]]] = None,
    padding_mode: str = "constant",
) -> torch.Tensor:
    return pad_image(video, padding, fill=fill, padding_mode=padding_mode)


def crop(inpt: torch.Tensor, top: int, left: int, height: int, width: int) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomCrop` for details."""
    if torch.jit.is_scripting():
        return crop_image(inpt, top=top, left=left, height=height, width=width)

    _log_api_usage_once(crop)

    kernel = _get_kernel(crop, type(inpt))
    return kernel(inpt, top=top, left=left, height=height, width=width)


@_register_kernel_internal(crop, torch.Tensor)
@_register_kernel_internal(crop, tv_tensors.Image)
def crop_image(image: torch.Tensor, top: int, left: int, height: int, width: int) -> torch.Tensor:
    h, w = image.shape[-2:]

    right = left + width
    bottom = top + height

    if left < 0 or top < 0 or right > w or bottom > h:
        image = image[..., max(top, 0) : bottom, max(left, 0) : right]
        torch_padding = [
            max(min(right, 0) - left, 0),
            max(right - max(w, left), 0),
            max(min(bottom, 0) - top, 0),
            max(bottom - max(h, top), 0),
        ]
        return _pad_with_scalar_fill(image, torch_padding, fill=0, padding_mode="constant")
    return image[..., top:bottom, left:right]


_crop_image_pil = _FP.crop
_register_kernel_internal(crop, PIL.Image.Image)(_crop_image_pil)


def crop_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    top: int,
    left: int,
    height: int,
    width: int,
) -> Tuple[torch.Tensor, Tuple[int, int]]:

    # Crop or implicit pad if left and/or top have negative values:
    if format == tv_tensors.BoundingBoxFormat.XYXY:
        sub = [left, top, left, top]
    else:
        sub = [left, top, 0, 0]

    bounding_boxes = bounding_boxes - torch.tensor(sub, dtype=bounding_boxes.dtype, device=bounding_boxes.device)
    canvas_size = (height, width)

    return clamp_bounding_boxes(bounding_boxes, format=format, canvas_size=canvas_size), canvas_size


@_register_kernel_internal(crop, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _crop_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, top: int, left: int, height: int, width: int
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = crop_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, top=top, left=left, height=height, width=width
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


@_register_kernel_internal(crop, tv_tensors.Mask)
def crop_mask(mask: torch.Tensor, top: int, left: int, height: int, width: int) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = crop_image(mask, top, left, height, width)

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(crop, tv_tensors.Video)
def crop_video(video: torch.Tensor, top: int, left: int, height: int, width: int) -> torch.Tensor:
    return crop_image(video, top, left, height, width)


def perspective(
    inpt: torch.Tensor,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomPerspective` for details."""
    if torch.jit.is_scripting():
        return perspective_image(
            inpt,
            startpoints=startpoints,
            endpoints=endpoints,
            interpolation=interpolation,
            fill=fill,
            coefficients=coefficients,
        )

    _log_api_usage_once(perspective)

    kernel = _get_kernel(perspective, type(inpt))
    return kernel(
        inpt,
        startpoints=startpoints,
        endpoints=endpoints,
        interpolation=interpolation,
        fill=fill,
        coefficients=coefficients,
    )


def _perspective_grid(coeffs: List[float], ow: int, oh: int, dtype: torch.dtype, device: torch.device) -> torch.Tensor:
    # https://github.com/python-pillow/Pillow/blob/4634eafe3c695a014267eefdce830b4a825beed7/
    # src/libImaging/Geometry.c#L394

    #
    # x_out = (coeffs[0] * x + coeffs[1] * y + coeffs[2]) / (coeffs[6] * x + coeffs[7] * y + 1)
    # y_out = (coeffs[3] * x + coeffs[4] * y + coeffs[5]) / (coeffs[6] * x + coeffs[7] * y + 1)
    #
    theta1 = torch.tensor(
        [[[coeffs[0], coeffs[1], coeffs[2]], [coeffs[3], coeffs[4], coeffs[5]]]], dtype=dtype, device=device
    )
    theta2 = torch.tensor([[[coeffs[6], coeffs[7], 1.0], [coeffs[6], coeffs[7], 1.0]]], dtype=dtype, device=device)

    d = 0.5
    base_grid = torch.empty(1, oh, ow, 3, dtype=dtype, device=device)
    x_grid = torch.linspace(d, ow + d - 1.0, steps=ow, device=device, dtype=dtype)
    base_grid[..., 0].copy_(x_grid)
    y_grid = torch.linspace(d, oh + d - 1.0, steps=oh, device=device, dtype=dtype).unsqueeze_(-1)
    base_grid[..., 1].copy_(y_grid)
    base_grid[..., 2].fill_(1)

    rescaled_theta1 = theta1.transpose(1, 2).div_(torch.tensor([0.5 * ow, 0.5 * oh], dtype=dtype, device=device))
    shape = (1, oh * ow, 3)
    output_grid1 = base_grid.view(shape).bmm(rescaled_theta1)
    output_grid2 = base_grid.view(shape).bmm(theta2.transpose(1, 2))

    output_grid = output_grid1.div_(output_grid2).sub_(1.0)
    return output_grid.view(1, oh, ow, 2)


def _perspective_coefficients(
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    coefficients: Optional[List[float]],
) -> List[float]:
    if coefficients is not None:
        if startpoints is not None and endpoints is not None:
            raise ValueError("The startpoints/endpoints and the coefficients shouldn't be defined concurrently.")
        elif len(coefficients) != 8:
            raise ValueError("Argument coefficients should have 8 float values")
        return coefficients
    elif startpoints is not None and endpoints is not None:
        return _get_perspective_coeffs(startpoints, endpoints)
    else:
        raise ValueError("Either the startpoints/endpoints or the coefficients must have non `None` values.")


@_register_kernel_internal(perspective, torch.Tensor)
@_register_kernel_internal(perspective, tv_tensors.Image)
def perspective_image(
    image: torch.Tensor,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
) -> torch.Tensor:
    perspective_coeffs = _perspective_coefficients(startpoints, endpoints, coefficients)
    interpolation = _check_interpolation(interpolation)

    _assert_grid_transform_inputs(
        image,
        matrix=None,
        interpolation=interpolation.value,
        fill=fill,
        supported_interpolation_modes=["nearest", "bilinear"],
        coeffs=perspective_coeffs,
    )

    oh, ow = image.shape[-2:]
    dtype = image.dtype if torch.is_floating_point(image) else torch.float32
    grid = _perspective_grid(perspective_coeffs, ow=ow, oh=oh, dtype=dtype, device=image.device)
    return _apply_grid_transform(image, grid, interpolation.value, fill=fill)


@_register_kernel_internal(perspective, PIL.Image.Image)
def _perspective_image_pil(
    image: PIL.Image.Image,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
) -> PIL.Image.Image:
    perspective_coeffs = _perspective_coefficients(startpoints, endpoints, coefficients)
    interpolation = _check_interpolation(interpolation)
    return _FP.perspective(image, perspective_coeffs, interpolation=pil_modes_mapping[interpolation], fill=fill)


def perspective_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    coefficients: Optional[List[float]] = None,
) -> torch.Tensor:
    if bounding_boxes.numel() == 0:
        return bounding_boxes

    perspective_coeffs = _perspective_coefficients(startpoints, endpoints, coefficients)

    original_shape = bounding_boxes.shape
    # TODO: first cast to float if bbox is int64 before convert_bounding_box_format
    bounding_boxes = (
        convert_bounding_box_format(bounding_boxes, old_format=format, new_format=tv_tensors.BoundingBoxFormat.XYXY)
    ).reshape(-1, 4)

    dtype = bounding_boxes.dtype if torch.is_floating_point(bounding_boxes) else torch.float32
    device = bounding_boxes.device

    # perspective_coeffs are computed as endpoint -> start point
    # We have to invert perspective_coeffs for bboxes:
    # (x, y) - end point and (x_out, y_out) - start point
    #   x_out = (coeffs[0] * x + coeffs[1] * y + coeffs[2]) / (coeffs[6] * x + coeffs[7] * y + 1)
    #   y_out = (coeffs[3] * x + coeffs[4] * y + coeffs[5]) / (coeffs[6] * x + coeffs[7] * y + 1)
    # and we would like to get:
    # x = (inv_coeffs[0] * x_out + inv_coeffs[1] * y_out + inv_coeffs[2])
    #       / (inv_coeffs[6] * x_out + inv_coeffs[7] * y_out + 1)
    # y = (inv_coeffs[3] * x_out + inv_coeffs[4] * y_out + inv_coeffs[5])
    #       / (inv_coeffs[6] * x_out + inv_coeffs[7] * y_out + 1)
    # and compute inv_coeffs in terms of coeffs

    denom = perspective_coeffs[0] * perspective_coeffs[4] - perspective_coeffs[1] * perspective_coeffs[3]
    if denom == 0:
        raise RuntimeError(
            f"Provided perspective_coeffs {perspective_coeffs} can not be inverted to transform bounding boxes. "
            f"Denominator is zero, denom={denom}"
        )

    inv_coeffs = [
        (perspective_coeffs[4] - perspective_coeffs[5] * perspective_coeffs[7]) / denom,
        (-perspective_coeffs[1] + perspective_coeffs[2] * perspective_coeffs[7]) / denom,
        (perspective_coeffs[1] * perspective_coeffs[5] - perspective_coeffs[2] * perspective_coeffs[4]) / denom,
        (-perspective_coeffs[3] + perspective_coeffs[5] * perspective_coeffs[6]) / denom,
        (perspective_coeffs[0] - perspective_coeffs[2] * perspective_coeffs[6]) / denom,
        (-perspective_coeffs[0] * perspective_coeffs[5] + perspective_coeffs[2] * perspective_coeffs[3]) / denom,
        (-perspective_coeffs[4] * perspective_coeffs[6] + perspective_coeffs[3] * perspective_coeffs[7]) / denom,
        (-perspective_coeffs[0] * perspective_coeffs[7] + perspective_coeffs[1] * perspective_coeffs[6]) / denom,
    ]

    theta1 = torch.tensor(
        [[inv_coeffs[0], inv_coeffs[1], inv_coeffs[2]], [inv_coeffs[3], inv_coeffs[4], inv_coeffs[5]]],
        dtype=dtype,
        device=device,
    )

    theta2 = torch.tensor(
        [[inv_coeffs[6], inv_coeffs[7], 1.0], [inv_coeffs[6], inv_coeffs[7], 1.0]], dtype=dtype, device=device
    )

    # 1) Let's transform bboxes into a tensor of 4 points (top-left, top-right, bottom-left, bottom-right corners).
    # Tensor of points has shape (N * 4, 3), where N is the number of bboxes
    # Single point structure is similar to
    # [(xmin, ymin, 1), (xmax, ymin, 1), (xmax, ymax, 1), (xmin, ymax, 1)]
    points = bounding_boxes[:, [[0, 1], [2, 1], [2, 3], [0, 3]]].reshape(-1, 2)
    points = torch.cat([points, torch.ones(points.shape[0], 1, device=points.device)], dim=-1)
    # 2) Now let's transform the points using perspective matrices
    #   x_out = (coeffs[0] * x + coeffs[1] * y + coeffs[2]) / (coeffs[6] * x + coeffs[7] * y + 1)
    #   y_out = (coeffs[3] * x + coeffs[4] * y + coeffs[5]) / (coeffs[6] * x + coeffs[7] * y + 1)

    numer_points = torch.matmul(points, theta1.T)
    denom_points = torch.matmul(points, theta2.T)
    transformed_points = numer_points.div_(denom_points)

    # 3) Reshape transformed points to [N boxes, 4 points, x/y coords]
    # and compute bounding box from 4 transformed points:
    transformed_points = transformed_points.reshape(-1, 4, 2)
    out_bbox_mins, out_bbox_maxs = torch.aminmax(transformed_points, dim=1)

    out_bboxes = clamp_bounding_boxes(
        torch.cat([out_bbox_mins, out_bbox_maxs], dim=1).to(bounding_boxes.dtype),
        format=tv_tensors.BoundingBoxFormat.XYXY,
        canvas_size=canvas_size,
    )

    # out_bboxes should be of shape [N boxes, 4]

    return convert_bounding_box_format(
        out_bboxes, old_format=tv_tensors.BoundingBoxFormat.XYXY, new_format=format, inplace=True
    ).reshape(original_shape)


@_register_kernel_internal(perspective, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _perspective_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    coefficients: Optional[List[float]] = None,
    **kwargs,
) -> tv_tensors.BoundingBoxes:
    output = perspective_bounding_boxes(
        inpt.as_subclass(torch.Tensor),
        format=inpt.format,
        canvas_size=inpt.canvas_size,
        startpoints=startpoints,
        endpoints=endpoints,
        coefficients=coefficients,
    )
    return tv_tensors.wrap(output, like=inpt)


def perspective_mask(
    mask: torch.Tensor,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = perspective_image(
        mask, startpoints, endpoints, interpolation=InterpolationMode.NEAREST, fill=fill, coefficients=coefficients
    )

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(perspective, tv_tensors.Mask, tv_tensor_wrapper=False)
def _perspective_mask_dispatch(
    inpt: tv_tensors.Mask,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
    **kwargs,
) -> tv_tensors.Mask:
    output = perspective_mask(
        inpt.as_subclass(torch.Tensor),
        startpoints=startpoints,
        endpoints=endpoints,
        fill=fill,
        coefficients=coefficients,
    )
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(perspective, tv_tensors.Video)
def perspective_video(
    video: torch.Tensor,
    startpoints: Optional[List[List[int]]],
    endpoints: Optional[List[List[int]]],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
    coefficients: Optional[List[float]] = None,
) -> torch.Tensor:
    return perspective_image(
        video, startpoints, endpoints, interpolation=interpolation, fill=fill, coefficients=coefficients
    )


def elastic(
    inpt: torch.Tensor,
    displacement: torch.Tensor,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.ElasticTransform` for details."""
    if torch.jit.is_scripting():
        return elastic_image(inpt, displacement=displacement, interpolation=interpolation, fill=fill)

    _log_api_usage_once(elastic)

    kernel = _get_kernel(elastic, type(inpt))
    return kernel(inpt, displacement=displacement, interpolation=interpolation, fill=fill)


elastic_transform = elastic


@_register_kernel_internal(elastic, torch.Tensor)
@_register_kernel_internal(elastic, tv_tensors.Image)
def elastic_image(
    image: torch.Tensor,
    displacement: torch.Tensor,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    if not isinstance(displacement, torch.Tensor):
        raise TypeError("Argument displacement should be a Tensor")

    interpolation = _check_interpolation(interpolation)

    height, width = image.shape[-2:]
    device = image.device
    dtype = image.dtype if torch.is_floating_point(image) else torch.float32

    # Patch: elastic transform should support (cpu,f16) input
    is_cpu_half = device.type == "cpu" and dtype == torch.float16
    if is_cpu_half:
        image = image.to(torch.float32)
        dtype = torch.float32

    # We are aware that if input image dtype is uint8 and displacement is float64 then
    # displacement will be cast to float32 and all computations will be done with float32
    # We can fix this later if needed

    expected_shape = (1, height, width, 2)
    if expected_shape != displacement.shape:
        raise ValueError(f"Argument displacement shape should be {expected_shape}, but given {displacement.shape}")

    grid = _create_identity_grid((height, width), device=device, dtype=dtype).add_(
        displacement.to(dtype=dtype, device=device)
    )
    output = _apply_grid_transform(image, grid, interpolation.value, fill=fill)

    if is_cpu_half:
        output = output.to(torch.float16)

    return output


@_register_kernel_internal(elastic, PIL.Image.Image)
def _elastic_image_pil(
    image: PIL.Image.Image,
    displacement: torch.Tensor,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
) -> PIL.Image.Image:
    t_img = pil_to_tensor(image)
    output = elastic_image(t_img, displacement, interpolation=interpolation, fill=fill)
    return to_pil_image(output, mode=image.mode)


def _create_identity_grid(size: Tuple[int, int], device: torch.device, dtype: torch.dtype) -> torch.Tensor:
    sy, sx = size
    base_grid = torch.empty(1, sy, sx, 2, device=device, dtype=dtype)
    x_grid = torch.linspace((-sx + 1) / sx, (sx - 1) / sx, sx, device=device, dtype=dtype)
    base_grid[..., 0].copy_(x_grid)

    y_grid = torch.linspace((-sy + 1) / sy, (sy - 1) / sy, sy, device=device, dtype=dtype).unsqueeze_(-1)
    base_grid[..., 1].copy_(y_grid)

    return base_grid


def elastic_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    displacement: torch.Tensor,
) -> torch.Tensor:
    expected_shape = (1, canvas_size[0], canvas_size[1], 2)
    if not isinstance(displacement, torch.Tensor):
        raise TypeError("Argument displacement should be a Tensor")
    elif displacement.shape != expected_shape:
        raise ValueError(f"Argument displacement shape should be {expected_shape}, but given {displacement.shape}")

    if bounding_boxes.numel() == 0:
        return bounding_boxes

    # TODO: add in docstring about approximation we are doing for grid inversion
    device = bounding_boxes.device
    dtype = bounding_boxes.dtype if torch.is_floating_point(bounding_boxes) else torch.float32

    if displacement.dtype != dtype or displacement.device != device:
        displacement = displacement.to(dtype=dtype, device=device)

    original_shape = bounding_boxes.shape
    # TODO: first cast to float if bbox is int64 before convert_bounding_box_format
    bounding_boxes = (
        convert_bounding_box_format(bounding_boxes, old_format=format, new_format=tv_tensors.BoundingBoxFormat.XYXY)
    ).reshape(-1, 4)

    id_grid = _create_identity_grid(canvas_size, device=device, dtype=dtype)
    # We construct an approximation of inverse grid as inv_grid = id_grid - displacement
    # This is not an exact inverse of the grid
    inv_grid = id_grid.sub_(displacement)

    # Get points from bboxes
    points = bounding_boxes[:, [[0, 1], [2, 1], [2, 3], [0, 3]]].reshape(-1, 2)
    if points.is_floating_point():
        points = points.ceil_()
    index_xy = points.to(dtype=torch.long)
    index_x, index_y = index_xy[:, 0], index_xy[:, 1]

    # Transform points:
    t_size = torch.tensor(canvas_size[::-1], device=displacement.device, dtype=displacement.dtype)
    transformed_points = inv_grid[0, index_y, index_x, :].add_(1).mul_(0.5 * t_size).sub_(0.5)

    transformed_points = transformed_points.reshape(-1, 4, 2)
    out_bbox_mins, out_bbox_maxs = torch.aminmax(transformed_points, dim=1)
    out_bboxes = clamp_bounding_boxes(
        torch.cat([out_bbox_mins, out_bbox_maxs], dim=1).to(bounding_boxes.dtype),
        format=tv_tensors.BoundingBoxFormat.XYXY,
        canvas_size=canvas_size,
    )

    return convert_bounding_box_format(
        out_bboxes, old_format=tv_tensors.BoundingBoxFormat.XYXY, new_format=format, inplace=True
    ).reshape(original_shape)


@_register_kernel_internal(elastic, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _elastic_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, displacement: torch.Tensor, **kwargs
) -> tv_tensors.BoundingBoxes:
    output = elastic_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, canvas_size=inpt.canvas_size, displacement=displacement
    )
    return tv_tensors.wrap(output, like=inpt)


def elastic_mask(
    mask: torch.Tensor,
    displacement: torch.Tensor,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = elastic_image(mask, displacement=displacement, interpolation=InterpolationMode.NEAREST, fill=fill)

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(elastic, tv_tensors.Mask, tv_tensor_wrapper=False)
def _elastic_mask_dispatch(
    inpt: tv_tensors.Mask, displacement: torch.Tensor, fill: _FillTypeJIT = None, **kwargs
) -> tv_tensors.Mask:
    output = elastic_mask(inpt.as_subclass(torch.Tensor), displacement=displacement, fill=fill)
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(elastic, tv_tensors.Video)
def elastic_video(
    video: torch.Tensor,
    displacement: torch.Tensor,
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    fill: _FillTypeJIT = None,
) -> torch.Tensor:
    return elastic_image(video, displacement, interpolation=interpolation, fill=fill)


def center_crop(inpt: torch.Tensor, output_size: List[int]) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomCrop` for details."""
    if torch.jit.is_scripting():
        return center_crop_image(inpt, output_size=output_size)

    _log_api_usage_once(center_crop)

    kernel = _get_kernel(center_crop, type(inpt))
    return kernel(inpt, output_size=output_size)


def _center_crop_parse_output_size(output_size: List[int]) -> List[int]:
    if isinstance(output_size, numbers.Number):
        s = int(output_size)
        return [s, s]
    elif isinstance(output_size, (tuple, list)) and len(output_size) == 1:
        return [output_size[0], output_size[0]]
    else:
        return list(output_size)


def _center_crop_compute_padding(crop_height: int, crop_width: int, image_height: int, image_width: int) -> List[int]:
    return [
        (crop_width - image_width) // 2 if crop_width > image_width else 0,
        (crop_height - image_height) // 2 if crop_height > image_height else 0,
        (crop_width - image_width + 1) // 2 if crop_width > image_width else 0,
        (crop_height - image_height + 1) // 2 if crop_height > image_height else 0,
    ]


def _center_crop_compute_crop_anchor(
    crop_height: int, crop_width: int, image_height: int, image_width: int
) -> Tuple[int, int]:
    crop_top = int(round((image_height - crop_height) / 2.0))
    crop_left = int(round((image_width - crop_width) / 2.0))
    return crop_top, crop_left


@_register_kernel_internal(center_crop, torch.Tensor)
@_register_kernel_internal(center_crop, tv_tensors.Image)
def center_crop_image(image: torch.Tensor, output_size: List[int]) -> torch.Tensor:
    crop_height, crop_width = _center_crop_parse_output_size(output_size)
    shape = image.shape
    if image.numel() == 0:
        return image.reshape(shape[:-2] + (crop_height, crop_width))
    image_height, image_width = shape[-2:]

    if crop_height > image_height or crop_width > image_width:
        padding_ltrb = _center_crop_compute_padding(crop_height, crop_width, image_height, image_width)
        image = torch_pad(image, _parse_pad_padding(padding_ltrb), value=0.0)

        image_height, image_width = image.shape[-2:]
        if crop_width == image_width and crop_height == image_height:
            return image

    crop_top, crop_left = _center_crop_compute_crop_anchor(crop_height, crop_width, image_height, image_width)
    return image[..., crop_top : (crop_top + crop_height), crop_left : (crop_left + crop_width)]


@_register_kernel_internal(center_crop, PIL.Image.Image)
def _center_crop_image_pil(image: PIL.Image.Image, output_size: List[int]) -> PIL.Image.Image:
    crop_height, crop_width = _center_crop_parse_output_size(output_size)
    image_height, image_width = _get_size_image_pil(image)

    if crop_height > image_height or crop_width > image_width:
        padding_ltrb = _center_crop_compute_padding(crop_height, crop_width, image_height, image_width)
        image = _pad_image_pil(image, padding_ltrb, fill=0)

        image_height, image_width = _get_size_image_pil(image)
        if crop_width == image_width and crop_height == image_height:
            return image

    crop_top, crop_left = _center_crop_compute_crop_anchor(crop_height, crop_width, image_height, image_width)
    return _crop_image_pil(image, crop_top, crop_left, crop_height, crop_width)


def center_crop_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    canvas_size: Tuple[int, int],
    output_size: List[int],
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    crop_height, crop_width = _center_crop_parse_output_size(output_size)
    crop_top, crop_left = _center_crop_compute_crop_anchor(crop_height, crop_width, *canvas_size)
    return crop_bounding_boxes(
        bounding_boxes, format, top=crop_top, left=crop_left, height=crop_height, width=crop_width
    )


@_register_kernel_internal(center_crop, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _center_crop_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, output_size: List[int]
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = center_crop_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, canvas_size=inpt.canvas_size, output_size=output_size
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


@_register_kernel_internal(center_crop, tv_tensors.Mask)
def center_crop_mask(mask: torch.Tensor, output_size: List[int]) -> torch.Tensor:
    if mask.ndim < 3:
        mask = mask.unsqueeze(0)
        needs_squeeze = True
    else:
        needs_squeeze = False

    output = center_crop_image(image=mask, output_size=output_size)

    if needs_squeeze:
        output = output.squeeze(0)

    return output


@_register_kernel_internal(center_crop, tv_tensors.Video)
def center_crop_video(video: torch.Tensor, output_size: List[int]) -> torch.Tensor:
    return center_crop_image(video, output_size)


def resized_crop(
    inpt: torch.Tensor,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomResizedCrop` for details."""
    if torch.jit.is_scripting():
        return resized_crop_image(
            inpt,
            top=top,
            left=left,
            height=height,
            width=width,
            size=size,
            interpolation=interpolation,
            antialias=antialias,
        )

    _log_api_usage_once(resized_crop)

    kernel = _get_kernel(resized_crop, type(inpt))
    return kernel(
        inpt,
        top=top,
        left=left,
        height=height,
        width=width,
        size=size,
        interpolation=interpolation,
        antialias=antialias,
    )


@_register_kernel_internal(resized_crop, torch.Tensor)
@_register_kernel_internal(resized_crop, tv_tensors.Image)
def resized_crop_image(
    image: torch.Tensor,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    image = crop_image(image, top, left, height, width)
    return resize_image(image, size, interpolation=interpolation, antialias=antialias)


def _resized_crop_image_pil(
    image: PIL.Image.Image,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
) -> PIL.Image.Image:
    image = _crop_image_pil(image, top, left, height, width)
    return _resize_image_pil(image, size, interpolation=interpolation)


@_register_kernel_internal(resized_crop, PIL.Image.Image)
def _resized_crop_image_pil_dispatch(
    image: PIL.Image.Image,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    antialias: Optional[bool] = True,
) -> PIL.Image.Image:
    if antialias is False:
        warnings.warn("Anti-alias option is always applied for PIL Image input. Argument antialias is ignored.")
    return _resized_crop_image_pil(
        image,
        top=top,
        left=left,
        height=height,
        width=width,
        size=size,
        interpolation=interpolation,
    )


def resized_crop_bounding_boxes(
    bounding_boxes: torch.Tensor,
    format: tv_tensors.BoundingBoxFormat,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
) -> Tuple[torch.Tensor, Tuple[int, int]]:
    bounding_boxes, canvas_size = crop_bounding_boxes(bounding_boxes, format, top, left, height, width)
    return resize_bounding_boxes(bounding_boxes, canvas_size=canvas_size, size=size)


@_register_kernel_internal(resized_crop, tv_tensors.BoundingBoxes, tv_tensor_wrapper=False)
def _resized_crop_bounding_boxes_dispatch(
    inpt: tv_tensors.BoundingBoxes, top: int, left: int, height: int, width: int, size: List[int], **kwargs
) -> tv_tensors.BoundingBoxes:
    output, canvas_size = resized_crop_bounding_boxes(
        inpt.as_subclass(torch.Tensor), format=inpt.format, top=top, left=left, height=height, width=width, size=size
    )
    return tv_tensors.wrap(output, like=inpt, canvas_size=canvas_size)


def resized_crop_mask(
    mask: torch.Tensor,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
) -> torch.Tensor:
    mask = crop_mask(mask, top, left, height, width)
    return resize_mask(mask, size)


@_register_kernel_internal(resized_crop, tv_tensors.Mask, tv_tensor_wrapper=False)
def _resized_crop_mask_dispatch(
    inpt: tv_tensors.Mask, top: int, left: int, height: int, width: int, size: List[int], **kwargs
) -> tv_tensors.Mask:
    output = resized_crop_mask(
        inpt.as_subclass(torch.Tensor), top=top, left=left, height=height, width=width, size=size
    )
    return tv_tensors.wrap(output, like=inpt)


@_register_kernel_internal(resized_crop, tv_tensors.Video)
def resized_crop_video(
    video: torch.Tensor,
    top: int,
    left: int,
    height: int,
    width: int,
    size: List[int],
    interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
    antialias: Optional[bool] = True,
) -> torch.Tensor:
    return resized_crop_image(
        video, top, left, height, width, antialias=antialias, size=size, interpolation=interpolation
    )


def five_crop(
    inpt: torch.Tensor, size: List[int]
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """See :class:`~torchvision.transforms.v2.FiveCrop` for details."""
    if torch.jit.is_scripting():
        return five_crop_image(inpt, size=size)

    _log_api_usage_once(five_crop)

    kernel = _get_kernel(five_crop, type(inpt))
    return kernel(inpt, size=size)


def _parse_five_crop_size(size: List[int]) -> List[int]:
    if isinstance(size, numbers.Number):
        s = int(size)
        size = [s, s]
    elif isinstance(size, (tuple, list)) and len(size) == 1:
        s = size[0]
        size = [s, s]

    if len(size) != 2:
        raise ValueError("Please provide only two dimensions (h, w) for size.")

    return size


@_register_five_ten_crop_kernel_internal(five_crop, torch.Tensor)
@_register_five_ten_crop_kernel_internal(five_crop, tv_tensors.Image)
def five_crop_image(
    image: torch.Tensor, size: List[int]
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    crop_height, crop_width = _parse_five_crop_size(size)
    image_height, image_width = image.shape[-2:]

    if crop_width > image_width or crop_height > image_height:
        raise ValueError(f"Requested crop size {size} is bigger than input size {(image_height, image_width)}")

    tl = crop_image(image, 0, 0, crop_height, crop_width)
    tr = crop_image(image, 0, image_width - crop_width, crop_height, crop_width)
    bl = crop_image(image, image_height - crop_height, 0, crop_height, crop_width)
    br = crop_image(image, image_height - crop_height, image_width - crop_width, crop_height, crop_width)
    center = center_crop_image(image, [crop_height, crop_width])

    return tl, tr, bl, br, center


@_register_five_ten_crop_kernel_internal(five_crop, PIL.Image.Image)
def _five_crop_image_pil(
    image: PIL.Image.Image, size: List[int]
) -> Tuple[PIL.Image.Image, PIL.Image.Image, PIL.Image.Image, PIL.Image.Image, PIL.Image.Image]:
    crop_height, crop_width = _parse_five_crop_size(size)
    image_height, image_width = _get_size_image_pil(image)

    if crop_width > image_width or crop_height > image_height:
        raise ValueError(f"Requested crop size {size} is bigger than input size {(image_height, image_width)}")

    tl = _crop_image_pil(image, 0, 0, crop_height, crop_width)
    tr = _crop_image_pil(image, 0, image_width - crop_width, crop_height, crop_width)
    bl = _crop_image_pil(image, image_height - crop_height, 0, crop_height, crop_width)
    br = _crop_image_pil(image, image_height - crop_height, image_width - crop_width, crop_height, crop_width)
    center = _center_crop_image_pil(image, [crop_height, crop_width])

    return tl, tr, bl, br, center


@_register_five_ten_crop_kernel_internal(five_crop, tv_tensors.Video)
def five_crop_video(
    video: torch.Tensor, size: List[int]
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    return five_crop_image(video, size)


def ten_crop(
    inpt: torch.Tensor, size: List[int], vertical_flip: bool = False
) -> Tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """See :class:`~torchvision.transforms.v2.TenCrop` for details."""
    if torch.jit.is_scripting():
        return ten_crop_image(inpt, size=size, vertical_flip=vertical_flip)

    _log_api_usage_once(ten_crop)

    kernel = _get_kernel(ten_crop, type(inpt))
    return kernel(inpt, size=size, vertical_flip=vertical_flip)


@_register_five_ten_crop_kernel_internal(ten_crop, torch.Tensor)
@_register_five_ten_crop_kernel_internal(ten_crop, tv_tensors.Image)
def ten_crop_image(
    image: torch.Tensor, size: List[int], vertical_flip: bool = False
) -> Tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    non_flipped = five_crop_image(image, size)

    if vertical_flip:
        image = vertical_flip_image(image)
    else:
        image = horizontal_flip_image(image)

    flipped = five_crop_image(image, size)

    return non_flipped + flipped


@_register_five_ten_crop_kernel_internal(ten_crop, PIL.Image.Image)
def _ten_crop_image_pil(
    image: PIL.Image.Image, size: List[int], vertical_flip: bool = False
) -> Tuple[
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
    PIL.Image.Image,
]:
    non_flipped = _five_crop_image_pil(image, size)

    if vertical_flip:
        image = _vertical_flip_image_pil(image)
    else:
        image = _horizontal_flip_image_pil(image)

    flipped = _five_crop_image_pil(image, size)

    return non_flipped + flipped


@_register_five_ten_crop_kernel_internal(ten_crop, tv_tensors.Video)
def ten_crop_video(
    video: torch.Tensor, size: List[int], vertical_flip: bool = False
) -> Tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    return ten_crop_image(video, size, vertical_flip=vertical_flip)
