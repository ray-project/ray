from typing import List

import PIL.Image
import torch
from torch.nn.functional import conv2d
from torchvision import tv_tensors
from torchvision.transforms import _functional_pil as _FP
from torchvision.transforms._functional_tensor import _max_value

from torchvision.utils import _log_api_usage_once

from ._misc import _num_value_bits, to_dtype_image
from ._type_conversion import pil_to_tensor, to_pil_image
from ._utils import _get_kernel, _register_kernel_internal


def rgb_to_grayscale(inpt: torch.Tensor, num_output_channels: int = 1) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.Grayscale` for details."""
    if torch.jit.is_scripting():
        return rgb_to_grayscale_image(inpt, num_output_channels=num_output_channels)

    _log_api_usage_once(rgb_to_grayscale)

    kernel = _get_kernel(rgb_to_grayscale, type(inpt))
    return kernel(inpt, num_output_channels=num_output_channels)


# `to_grayscale` actually predates `rgb_to_grayscale` in v1, but only handles PIL images. Since `rgb_to_grayscale` is a
# superset in terms of functionality and has the same signature, we alias here to avoid disruption.
to_grayscale = rgb_to_grayscale


def _rgb_to_grayscale_image(
    image: torch.Tensor, num_output_channels: int = 1, preserve_dtype: bool = True
) -> torch.Tensor:
    # TODO: Maybe move the validation that num_output_channels is 1 or 3 to this function instead of callers.
    if image.shape[-3] == 1 and num_output_channels == 1:
        return image.clone()
    if image.shape[-3] == 1 and num_output_channels == 3:
        s = [1] * len(image.shape)
        s[-3] = 3
        return image.repeat(s)
    r, g, b = image.unbind(dim=-3)
    l_img = r.mul(0.2989).add_(g, alpha=0.587).add_(b, alpha=0.114)
    l_img = l_img.unsqueeze(dim=-3)
    if preserve_dtype:
        l_img = l_img.to(image.dtype)
    if num_output_channels == 3:
        l_img = l_img.expand(image.shape)
    return l_img


@_register_kernel_internal(rgb_to_grayscale, torch.Tensor)
@_register_kernel_internal(rgb_to_grayscale, tv_tensors.Image)
def rgb_to_grayscale_image(image: torch.Tensor, num_output_channels: int = 1) -> torch.Tensor:
    if num_output_channels not in (1, 3):
        raise ValueError(f"num_output_channels must be 1 or 3, got {num_output_channels}.")
    return _rgb_to_grayscale_image(image, num_output_channels=num_output_channels, preserve_dtype=True)


@_register_kernel_internal(rgb_to_grayscale, PIL.Image.Image)
def _rgb_to_grayscale_image_pil(image: PIL.Image.Image, num_output_channels: int = 1) -> PIL.Image.Image:
    if num_output_channels not in (1, 3):
        raise ValueError(f"num_output_channels must be 1 or 3, got {num_output_channels}.")
    return _FP.to_grayscale(image, num_output_channels=num_output_channels)


def grayscale_to_rgb(inpt: torch.Tensor) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RGB` for details."""
    if torch.jit.is_scripting():
        return grayscale_to_rgb_image(inpt)

    _log_api_usage_once(grayscale_to_rgb)

    kernel = _get_kernel(grayscale_to_rgb, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(grayscale_to_rgb, torch.Tensor)
@_register_kernel_internal(grayscale_to_rgb, tv_tensors.Image)
def grayscale_to_rgb_image(image: torch.Tensor) -> torch.Tensor:
    if image.shape[-3] >= 3:
        # Image already has RGB channels. We don't need to do anything.
        return image
    # rgb_to_grayscale can be used to add channels so we reuse that function.
    return _rgb_to_grayscale_image(image, num_output_channels=3, preserve_dtype=True)


@_register_kernel_internal(grayscale_to_rgb, PIL.Image.Image)
def grayscale_to_rgb_image_pil(image: PIL.Image.Image) -> PIL.Image.Image:
    return image.convert(mode="RGB")


def _blend(image1: torch.Tensor, image2: torch.Tensor, ratio: float) -> torch.Tensor:
    ratio = float(ratio)
    fp = image1.is_floating_point()
    bound = _max_value(image1.dtype)
    output = image1.mul(ratio).add_(image2, alpha=(1.0 - ratio)).clamp_(0, bound)
    return output if fp else output.to(image1.dtype)


def adjust_brightness(inpt: torch.Tensor, brightness_factor: float) -> torch.Tensor:
    """Adjust brightness."""

    if torch.jit.is_scripting():
        return adjust_brightness_image(inpt, brightness_factor=brightness_factor)

    _log_api_usage_once(adjust_brightness)

    kernel = _get_kernel(adjust_brightness, type(inpt))
    return kernel(inpt, brightness_factor=brightness_factor)


@_register_kernel_internal(adjust_brightness, torch.Tensor)
@_register_kernel_internal(adjust_brightness, tv_tensors.Image)
def adjust_brightness_image(image: torch.Tensor, brightness_factor: float) -> torch.Tensor:
    if brightness_factor < 0:
        raise ValueError(f"brightness_factor ({brightness_factor}) is not non-negative.")

    c = image.shape[-3]
    if c not in [1, 3]:
        raise TypeError(f"Input image tensor permitted channel values are 1 or 3, but found {c}")

    fp = image.is_floating_point()
    bound = _max_value(image.dtype)
    output = image.mul(brightness_factor).clamp_(0, bound)
    return output if fp else output.to(image.dtype)


@_register_kernel_internal(adjust_brightness, PIL.Image.Image)
def _adjust_brightness_image_pil(image: PIL.Image.Image, brightness_factor: float) -> PIL.Image.Image:
    return _FP.adjust_brightness(image, brightness_factor=brightness_factor)


@_register_kernel_internal(adjust_brightness, tv_tensors.Video)
def adjust_brightness_video(video: torch.Tensor, brightness_factor: float) -> torch.Tensor:
    return adjust_brightness_image(video, brightness_factor=brightness_factor)


def adjust_saturation(inpt: torch.Tensor, saturation_factor: float) -> torch.Tensor:
    """Adjust saturation."""
    if torch.jit.is_scripting():
        return adjust_saturation_image(inpt, saturation_factor=saturation_factor)

    _log_api_usage_once(adjust_saturation)

    kernel = _get_kernel(adjust_saturation, type(inpt))
    return kernel(inpt, saturation_factor=saturation_factor)


@_register_kernel_internal(adjust_saturation, torch.Tensor)
@_register_kernel_internal(adjust_saturation, tv_tensors.Image)
def adjust_saturation_image(image: torch.Tensor, saturation_factor: float) -> torch.Tensor:
    if saturation_factor < 0:
        raise ValueError(f"saturation_factor ({saturation_factor}) is not non-negative.")

    c = image.shape[-3]
    if c not in [1, 3]:
        raise TypeError(f"Input image tensor permitted channel values are 1 or 3, but found {c}")

    if c == 1:  # Match PIL behaviour
        return image

    grayscale_image = _rgb_to_grayscale_image(image, num_output_channels=1, preserve_dtype=False)
    if not image.is_floating_point():
        grayscale_image = grayscale_image.floor_()

    return _blend(image, grayscale_image, saturation_factor)


_adjust_saturation_image_pil = _register_kernel_internal(adjust_saturation, PIL.Image.Image)(_FP.adjust_saturation)


@_register_kernel_internal(adjust_saturation, tv_tensors.Video)
def adjust_saturation_video(video: torch.Tensor, saturation_factor: float) -> torch.Tensor:
    return adjust_saturation_image(video, saturation_factor=saturation_factor)


def adjust_contrast(inpt: torch.Tensor, contrast_factor: float) -> torch.Tensor:
    """See :class:`~torchvision.transforms.RandomAutocontrast`"""
    if torch.jit.is_scripting():
        return adjust_contrast_image(inpt, contrast_factor=contrast_factor)

    _log_api_usage_once(adjust_contrast)

    kernel = _get_kernel(adjust_contrast, type(inpt))
    return kernel(inpt, contrast_factor=contrast_factor)


@_register_kernel_internal(adjust_contrast, torch.Tensor)
@_register_kernel_internal(adjust_contrast, tv_tensors.Image)
def adjust_contrast_image(image: torch.Tensor, contrast_factor: float) -> torch.Tensor:
    if contrast_factor < 0:
        raise ValueError(f"contrast_factor ({contrast_factor}) is not non-negative.")

    c = image.shape[-3]
    if c not in [1, 3]:
        raise TypeError(f"Input image tensor permitted channel values are 1 or 3, but found {c}")
    fp = image.is_floating_point()
    if c == 3:
        grayscale_image = _rgb_to_grayscale_image(image, num_output_channels=1, preserve_dtype=False)
        if not fp:
            grayscale_image = grayscale_image.floor_()
    else:
        grayscale_image = image if fp else image.to(torch.float32)
    mean = torch.mean(grayscale_image, dim=(-3, -2, -1), keepdim=True)
    return _blend(image, mean, contrast_factor)


_adjust_contrast_image_pil = _register_kernel_internal(adjust_contrast, PIL.Image.Image)(_FP.adjust_contrast)


@_register_kernel_internal(adjust_contrast, tv_tensors.Video)
def adjust_contrast_video(video: torch.Tensor, contrast_factor: float) -> torch.Tensor:
    return adjust_contrast_image(video, contrast_factor=contrast_factor)


def adjust_sharpness(inpt: torch.Tensor, sharpness_factor: float) -> torch.Tensor:
    """See :class:`~torchvision.transforms.RandomAdjustSharpness`"""
    if torch.jit.is_scripting():
        return adjust_sharpness_image(inpt, sharpness_factor=sharpness_factor)

    _log_api_usage_once(adjust_sharpness)

    kernel = _get_kernel(adjust_sharpness, type(inpt))
    return kernel(inpt, sharpness_factor=sharpness_factor)


@_register_kernel_internal(adjust_sharpness, torch.Tensor)
@_register_kernel_internal(adjust_sharpness, tv_tensors.Image)
def adjust_sharpness_image(image: torch.Tensor, sharpness_factor: float) -> torch.Tensor:
    num_channels, height, width = image.shape[-3:]
    if num_channels not in (1, 3):
        raise TypeError(f"Input image tensor can have 1 or 3 channels, but found {num_channels}")

    if sharpness_factor < 0:
        raise ValueError(f"sharpness_factor ({sharpness_factor}) is not non-negative.")

    if image.numel() == 0 or height <= 2 or width <= 2:
        return image

    bound = _max_value(image.dtype)
    fp = image.is_floating_point()
    shape = image.shape

    if image.ndim > 4:
        image = image.reshape(-1, num_channels, height, width)
        needs_unsquash = True
    else:
        needs_unsquash = False

    # The following is a normalized 3x3 kernel with 1s in the edges and a 5 in the middle.
    kernel_dtype = image.dtype if fp else torch.float32
    a, b = 1.0 / 13.0, 5.0 / 13.0
    kernel = torch.tensor([[a, a, a], [a, b, a], [a, a, a]], dtype=kernel_dtype, device=image.device)
    kernel = kernel.expand(num_channels, 1, 3, 3)

    # We copy and cast at the same time to avoid modifications on the original data
    output = image.to(dtype=kernel_dtype, copy=True)
    blurred_degenerate = conv2d(output, kernel, groups=num_channels)
    if not fp:
        # it is better to round before cast
        blurred_degenerate = blurred_degenerate.round_()

    # Create a view on the underlying output while pointing at the same data. We do this to avoid indexing twice.
    view = output[..., 1:-1, 1:-1]

    # We speed up blending by minimizing flops and doing in-place. The 2 blend options are mathematically equivalent:
    # x+(1-r)*(y-x) = x + (1-r)*y - (1-r)*x = x*r + y*(1-r)
    view.add_(blurred_degenerate.sub_(view), alpha=(1.0 - sharpness_factor))

    # The actual data of output have been modified by the above. We only need to clamp and cast now.
    output = output.clamp_(0, bound)
    if not fp:
        output = output.to(image.dtype)

    if needs_unsquash:
        output = output.reshape(shape)

    return output


_adjust_sharpness_image_pil = _register_kernel_internal(adjust_sharpness, PIL.Image.Image)(_FP.adjust_sharpness)


@_register_kernel_internal(adjust_sharpness, tv_tensors.Video)
def adjust_sharpness_video(video: torch.Tensor, sharpness_factor: float) -> torch.Tensor:
    return adjust_sharpness_image(video, sharpness_factor=sharpness_factor)


def adjust_hue(inpt: torch.Tensor, hue_factor: float) -> torch.Tensor:
    """Adjust hue"""
    if torch.jit.is_scripting():
        return adjust_hue_image(inpt, hue_factor=hue_factor)

    _log_api_usage_once(adjust_hue)

    kernel = _get_kernel(adjust_hue, type(inpt))
    return kernel(inpt, hue_factor=hue_factor)


def _rgb_to_hsv(image: torch.Tensor) -> torch.Tensor:
    r, g, _ = image.unbind(dim=-3)

    # Implementation is based on
    # https://github.com/python-pillow/Pillow/blob/4174d4267616897df3746d315d5a2d0f82c656ee/src/libImaging/Convert.c#L330
    minc, maxc = torch.aminmax(image, dim=-3)

    # The algorithm erases S and H channel where `maxc = minc`. This avoids NaN
    # from happening in the results, because
    #   + S channel has division by `maxc`, which is zero only if `maxc = minc`
    #   + H channel has division by `(maxc - minc)`.
    #
    # Instead of overwriting NaN afterwards, we just prevent it from occurring so
    # we don't need to deal with it in case we save the NaN in a buffer in
    # backprop, if it is ever supported, but it doesn't hurt to do so.
    eqc = maxc == minc

    channels_range = maxc - minc
    # Since `eqc => channels_range = 0`, replacing denominator with 1 when `eqc` is fine.
    ones = torch.ones_like(maxc)
    s = channels_range / torch.where(eqc, ones, maxc)
    # Note that `eqc => maxc = minc = r = g = b`. So the following calculation
    # of `h` would reduce to `bc - gc + 2 + rc - bc + 4 + rc - bc = 6` so it
    # would not matter what values `rc`, `gc`, and `bc` have here, and thus
    # replacing denominator with 1 when `eqc` is fine.
    channels_range_divisor = torch.where(eqc, ones, channels_range).unsqueeze_(dim=-3)
    rc, gc, bc = ((maxc.unsqueeze(dim=-3) - image) / channels_range_divisor).unbind(dim=-3)

    mask_maxc_neq_r = maxc != r
    mask_maxc_eq_g = maxc == g

    hg = rc.add(2.0).sub_(bc).mul_(mask_maxc_eq_g & mask_maxc_neq_r)
    hr = bc.sub_(gc).mul_(~mask_maxc_neq_r)
    hb = gc.add_(4.0).sub_(rc).mul_(mask_maxc_neq_r.logical_and_(mask_maxc_eq_g.logical_not_()))

    h = hr.add_(hg).add_(hb)
    h = h.mul_(1.0 / 6.0).add_(1.0).fmod_(1.0)
    return torch.stack((h, s, maxc), dim=-3)


def _hsv_to_rgb(img: torch.Tensor) -> torch.Tensor:
    h, s, v = img.unbind(dim=-3)
    h6 = h.mul(6)
    i = torch.floor(h6)
    f = h6.sub_(i)
    i = i.to(dtype=torch.int32)

    sxf = s * f
    one_minus_s = 1.0 - s
    q = (1.0 - sxf).mul_(v).clamp_(0.0, 1.0)
    t = sxf.add_(one_minus_s).mul_(v).clamp_(0.0, 1.0)
    p = one_minus_s.mul_(v).clamp_(0.0, 1.0)
    i.remainder_(6)

    vpqt = torch.stack((v, p, q, t), dim=-3)

    # vpqt -> rgb mapping based on i
    select = torch.tensor([[0, 2, 1, 1, 3, 0], [3, 0, 0, 2, 1, 1], [1, 1, 3, 0, 0, 2]], dtype=torch.long)
    select = select.to(device=img.device, non_blocking=True)

    select = select[:, i]
    if select.ndim > 3:
        # if input.shape is (B, ..., C, H, W) then
        # select.shape is (C, B, ...,  H, W)
        # thus we move C axis to get (B, ..., C, H, W)
        select = select.moveaxis(0, -3)

    return vpqt.gather(-3, select)


@_register_kernel_internal(adjust_hue, torch.Tensor)
@_register_kernel_internal(adjust_hue, tv_tensors.Image)
def adjust_hue_image(image: torch.Tensor, hue_factor: float) -> torch.Tensor:
    if not (-0.5 <= hue_factor <= 0.5):
        raise ValueError(f"hue_factor ({hue_factor}) is not in [-0.5, 0.5].")

    c = image.shape[-3]
    if c not in [1, 3]:
        raise TypeError(f"Input image tensor permitted channel values are 1 or 3, but found {c}")

    if c == 1:  # Match PIL behaviour
        return image

    if image.numel() == 0:
        # exit earlier on empty images
        return image

    orig_dtype = image.dtype
    image = to_dtype_image(image, torch.float32, scale=True)

    image = _rgb_to_hsv(image)
    h, s, v = image.unbind(dim=-3)
    h.add_(hue_factor).remainder_(1.0)
    image = torch.stack((h, s, v), dim=-3)
    image_hue_adj = _hsv_to_rgb(image)

    return to_dtype_image(image_hue_adj, orig_dtype, scale=True)


_adjust_hue_image_pil = _register_kernel_internal(adjust_hue, PIL.Image.Image)(_FP.adjust_hue)


@_register_kernel_internal(adjust_hue, tv_tensors.Video)
def adjust_hue_video(video: torch.Tensor, hue_factor: float) -> torch.Tensor:
    return adjust_hue_image(video, hue_factor=hue_factor)


def adjust_gamma(inpt: torch.Tensor, gamma: float, gain: float = 1) -> torch.Tensor:
    """Adjust gamma."""
    if torch.jit.is_scripting():
        return adjust_gamma_image(inpt, gamma=gamma, gain=gain)

    _log_api_usage_once(adjust_gamma)

    kernel = _get_kernel(adjust_gamma, type(inpt))
    return kernel(inpt, gamma=gamma, gain=gain)


@_register_kernel_internal(adjust_gamma, torch.Tensor)
@_register_kernel_internal(adjust_gamma, tv_tensors.Image)
def adjust_gamma_image(image: torch.Tensor, gamma: float, gain: float = 1.0) -> torch.Tensor:
    if gamma < 0:
        raise ValueError("Gamma should be a non-negative real number")

    # The input image is either assumed to be at [0, 1] scale (if float) or is converted to that scale (if integer).
    # Since the gamma is non-negative, the output remains at [0, 1] scale.
    if not torch.is_floating_point(image):
        output = to_dtype_image(image, torch.float32, scale=True).pow_(gamma)
    else:
        output = image.pow(gamma)

    if gain != 1.0:
        # The clamp operation is needed only if multiplication is performed. It's only when gain != 1, that the scale
        # of the output can go beyond [0, 1].
        output = output.mul_(gain).clamp_(0.0, 1.0)

    return to_dtype_image(output, image.dtype, scale=True)


_adjust_gamma_image_pil = _register_kernel_internal(adjust_gamma, PIL.Image.Image)(_FP.adjust_gamma)


@_register_kernel_internal(adjust_gamma, tv_tensors.Video)
def adjust_gamma_video(video: torch.Tensor, gamma: float, gain: float = 1) -> torch.Tensor:
    return adjust_gamma_image(video, gamma=gamma, gain=gain)


def posterize(inpt: torch.Tensor, bits: int) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomPosterize` for details."""
    if torch.jit.is_scripting():
        return posterize_image(inpt, bits=bits)

    _log_api_usage_once(posterize)

    kernel = _get_kernel(posterize, type(inpt))
    return kernel(inpt, bits=bits)


@_register_kernel_internal(posterize, torch.Tensor)
@_register_kernel_internal(posterize, tv_tensors.Image)
def posterize_image(image: torch.Tensor, bits: int) -> torch.Tensor:
    if image.is_floating_point():
        levels = 1 << bits
        return image.mul(levels).floor_().clamp_(0, levels - 1).mul_(1.0 / levels)
    else:
        num_value_bits = _num_value_bits(image.dtype)
        if bits >= num_value_bits:
            return image

        mask = ((1 << bits) - 1) << (num_value_bits - bits)
        return image & mask


_posterize_image_pil = _register_kernel_internal(posterize, PIL.Image.Image)(_FP.posterize)


@_register_kernel_internal(posterize, tv_tensors.Video)
def posterize_video(video: torch.Tensor, bits: int) -> torch.Tensor:
    return posterize_image(video, bits=bits)


def solarize(inpt: torch.Tensor, threshold: float) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomSolarize` for details."""
    if torch.jit.is_scripting():
        return solarize_image(inpt, threshold=threshold)

    _log_api_usage_once(solarize)

    kernel = _get_kernel(solarize, type(inpt))
    return kernel(inpt, threshold=threshold)


@_register_kernel_internal(solarize, torch.Tensor)
@_register_kernel_internal(solarize, tv_tensors.Image)
def solarize_image(image: torch.Tensor, threshold: float) -> torch.Tensor:
    if threshold > _max_value(image.dtype):
        raise TypeError(f"Threshold should be less or equal the maximum value of the dtype, but got {threshold}")

    return torch.where(image >= threshold, invert_image(image), image)


_solarize_image_pil = _register_kernel_internal(solarize, PIL.Image.Image)(_FP.solarize)


@_register_kernel_internal(solarize, tv_tensors.Video)
def solarize_video(video: torch.Tensor, threshold: float) -> torch.Tensor:
    return solarize_image(video, threshold=threshold)


def autocontrast(inpt: torch.Tensor) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomAutocontrast` for details."""
    if torch.jit.is_scripting():
        return autocontrast_image(inpt)

    _log_api_usage_once(autocontrast)

    kernel = _get_kernel(autocontrast, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(autocontrast, torch.Tensor)
@_register_kernel_internal(autocontrast, tv_tensors.Image)
def autocontrast_image(image: torch.Tensor) -> torch.Tensor:
    c = image.shape[-3]
    if c not in [1, 3]:
        raise TypeError(f"Input image tensor permitted channel values are 1 or 3, but found {c}")

    if image.numel() == 0:
        # exit earlier on empty images
        return image

    bound = _max_value(image.dtype)
    fp = image.is_floating_point()
    float_image = image if fp else image.to(torch.float32)

    minimum = float_image.amin(dim=(-2, -1), keepdim=True)
    maximum = float_image.amax(dim=(-2, -1), keepdim=True)

    eq_idxs = maximum == minimum
    inv_scale = maximum.sub_(minimum).mul_(1.0 / bound)
    minimum[eq_idxs] = 0.0
    inv_scale[eq_idxs] = 1.0

    if fp:
        diff = float_image.sub(minimum)
    else:
        diff = float_image.sub_(minimum)

    return diff.div_(inv_scale).clamp_(0, bound).to(image.dtype)


_autocontrast_image_pil = _register_kernel_internal(autocontrast, PIL.Image.Image)(_FP.autocontrast)


@_register_kernel_internal(autocontrast, tv_tensors.Video)
def autocontrast_video(video: torch.Tensor) -> torch.Tensor:
    return autocontrast_image(video)


def equalize(inpt: torch.Tensor) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.RandomEqualize` for details."""
    if torch.jit.is_scripting():
        return equalize_image(inpt)

    _log_api_usage_once(equalize)

    kernel = _get_kernel(equalize, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(equalize, torch.Tensor)
@_register_kernel_internal(equalize, tv_tensors.Image)
def equalize_image(image: torch.Tensor) -> torch.Tensor:
    if image.numel() == 0:
        return image

    # 1. The algorithm below can easily be extended to support arbitrary integer dtypes. However, the histogram that
    #    would be needed to computed will have at least `torch.iinfo(dtype).max + 1` values. That is perfectly fine for
    #    `torch.int8`, `torch.uint8`, and `torch.int16`, at least questionable for `torch.int32` and completely
    #    unfeasible for `torch.int64`.
    # 2. Floating point inputs need to be binned for this algorithm. Apart from converting them to an integer dtype, we
    #    could also use PyTorch's builtin histogram functionality. However, that has its own set of issues: in addition
    #    to being slow in general, PyTorch's implementation also doesn't support batches. In total, that makes it slower
    #    and more complicated to implement than a simple conversion and a fast histogram implementation for integers.
    # Since we need to convert in most cases anyway and out of the acceptable dtypes mentioned in 1. `torch.uint8` is
    # by far the most common, we choose it as base.
    output_dtype = image.dtype
    image = to_dtype_image(image, torch.uint8, scale=True)

    # The histogram is computed by using the flattened image as index. For example, a pixel value of 127 in the image
    # corresponds to adding 1 to index 127 in the histogram.
    batch_shape = image.shape[:-2]
    flat_image = image.flatten(start_dim=-2).to(torch.long)
    hist = flat_image.new_zeros(batch_shape + (256,), dtype=torch.int32)
    hist.scatter_add_(dim=-1, index=flat_image, src=hist.new_ones(1).expand_as(flat_image))
    cum_hist = hist.cumsum(dim=-1)

    # The simplest form of lookup-table (LUT) that also achieves histogram equalization is
    # `lut = cum_hist / flat_image.shape[-1] * 255`
    # However, PIL uses a more elaborate scheme:
    # https://github.com/python-pillow/Pillow/blob/eb59cb61d5239ee69cbbf12709a0c6fd7314e6d7/src/PIL/ImageOps.py#L368-L385
    # `lut = ((cum_hist + num_non_max_pixels // (2 * 255)) // num_non_max_pixels) * 255`

    # The last non-zero element in the histogram is the first element in the cumulative histogram with the maximum
    # value. Thus, the "max" in `num_non_max_pixels` does not refer to 255 as the maximum value of uint8 images, but
    # rather the maximum value in the image, which might be or not be 255.
    index = cum_hist.argmax(dim=-1)
    num_non_max_pixels = flat_image.shape[-1] - hist.gather(dim=-1, index=index.unsqueeze_(-1))

    # This is performance optimization that saves us one multiplication later. With this, the LUT computation simplifies
    # to `lut = (cum_hist + step // 2) // step` and thus saving the final multiplication by 255 while keeping the
    # division count the same. PIL uses the variable name `step` for this, so we keep that for easier comparison.
    step = num_non_max_pixels.div_(255, rounding_mode="floor")

    # Although it looks like we could return early if we find `step == 0` like PIL does, that is unfortunately not as
    # easy due to our support for batched images. We can only return early if `(step == 0).all()` holds. If it doesn't,
    # we have to go through the computation below anyway. Since `step == 0` is an edge case anyway, it makes no sense to
    # pay the runtime cost for checking it every time.
    valid_equalization = step.ne(0).unsqueeze_(-1)

    # `lut[k]` is computed with `cum_hist[k-1]` with `lut[0] == (step // 2) // step == 0`. Thus, we perform the
    # computation only for `lut[1:]` with `cum_hist[:-1]` and add `lut[0] == 0` afterwards.
    cum_hist = cum_hist[..., :-1]
    (
        cum_hist.add_(step // 2)
        # We need the `clamp_`(min=1) call here to avoid zero division since they fail for integer dtypes. This has no
        # effect on the returned result of this kernel since images inside the batch with `step == 0` are returned as is
        # instead of equalized version.
        .div_(step.clamp_(min=1), rounding_mode="floor")
        # We need the `clamp_` call here since PILs LUT computation scheme can produce values outside the valid value
        # range of uint8 images
        .clamp_(0, 255)
    )
    lut = cum_hist.to(torch.uint8)
    lut = torch.cat([lut.new_zeros(1).expand(batch_shape + (1,)), lut], dim=-1)
    equalized_image = lut.gather(dim=-1, index=flat_image).view_as(image)

    output = torch.where(valid_equalization, equalized_image, image)
    return to_dtype_image(output, output_dtype, scale=True)


_equalize_image_pil = _register_kernel_internal(equalize, PIL.Image.Image)(_FP.equalize)


@_register_kernel_internal(equalize, tv_tensors.Video)
def equalize_video(video: torch.Tensor) -> torch.Tensor:
    return equalize_image(video)


def invert(inpt: torch.Tensor) -> torch.Tensor:
    """See :func:`~torchvision.transforms.v2.RandomInvert`."""
    if torch.jit.is_scripting():
        return invert_image(inpt)

    _log_api_usage_once(invert)

    kernel = _get_kernel(invert, type(inpt))
    return kernel(inpt)


@_register_kernel_internal(invert, torch.Tensor)
@_register_kernel_internal(invert, tv_tensors.Image)
def invert_image(image: torch.Tensor) -> torch.Tensor:
    if image.is_floating_point():
        return 1.0 - image
    elif image.dtype == torch.uint8:
        return image.bitwise_not()
    else:  # signed integer dtypes
        # We can't use `Tensor.bitwise_not` here, since we want to retain the leading zero bit that encodes the sign
        return image.bitwise_xor((1 << _num_value_bits(image.dtype)) - 1)


_invert_image_pil = _register_kernel_internal(invert, PIL.Image.Image)(_FP.invert)


@_register_kernel_internal(invert, tv_tensors.Video)
def invert_video(video: torch.Tensor) -> torch.Tensor:
    return invert_image(video)


def permute_channels(inpt: torch.Tensor, permutation: List[int]) -> torch.Tensor:
    """Permute the channels of the input according to the given permutation.

    This function supports plain :class:`~torch.Tensor`'s, :class:`PIL.Image.Image`'s, and
    :class:`torchvision.tv_tensors.Image` and :class:`torchvision.tv_tensors.Video`.

    Example:
        >>> rgb_image = torch.rand(3, 256, 256)
        >>> bgr_image = F.permute_channels(rgb_image, permutation=[2, 1, 0])

    Args:
        permutation (List[int]): Valid permutation of the input channel indices. The index of the element determines the
            channel index in the input and the value determines the channel index in the output. For example,
            ``permutation=[2, 0 , 1]``

            - takes ``ìnpt[..., 0, :, :]`` and puts it at ``output[..., 2, :, :]``,
            - takes ``ìnpt[..., 1, :, :]`` and puts it at ``output[..., 0, :, :]``, and
            - takes ``ìnpt[..., 2, :, :]`` and puts it at ``output[..., 1, :, :]``.

    Raises:
        ValueError: If ``len(permutation)`` doesn't match the number of channels in the input.
    """
    if torch.jit.is_scripting():
        return permute_channels_image(inpt, permutation=permutation)

    _log_api_usage_once(permute_channels)

    kernel = _get_kernel(permute_channels, type(inpt))
    return kernel(inpt, permutation=permutation)


@_register_kernel_internal(permute_channels, torch.Tensor)
@_register_kernel_internal(permute_channels, tv_tensors.Image)
def permute_channels_image(image: torch.Tensor, permutation: List[int]) -> torch.Tensor:
    shape = image.shape
    num_channels, height, width = shape[-3:]

    if len(permutation) != num_channels:
        raise ValueError(
            f"Length of permutation does not match number of channels: " f"{len(permutation)} != {num_channels}"
        )

    if image.numel() == 0:
        return image

    image = image.reshape(-1, num_channels, height, width)
    image = image[:, permutation, :, :]
    return image.reshape(shape)


@_register_kernel_internal(permute_channels, PIL.Image.Image)
def _permute_channels_image_pil(image: PIL.Image.Image, permutation: List[int]) -> PIL.Image.Image:
    return to_pil_image(permute_channels_image(pil_to_tensor(image), permutation=permutation))


@_register_kernel_internal(permute_channels, tv_tensors.Video)
def permute_channels_video(video: torch.Tensor, permutation: List[int]) -> torch.Tensor:
    return permute_channels_image(video, permutation=permutation)
