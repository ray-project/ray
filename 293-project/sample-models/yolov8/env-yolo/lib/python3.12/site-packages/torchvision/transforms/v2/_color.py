import collections.abc
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import torch
from torchvision import transforms as _transforms
from torchvision.transforms.v2 import functional as F, Transform

from ._transform import _RandomApplyTransform
from ._utils import query_chw


class Grayscale(Transform):
    """Convert images or videos to grayscale.

    If the input is a :class:`torch.Tensor`, it is expected
    to have [..., 3 or 1, H, W] shape, where ... means an arbitrary number of leading dimensions

    Args:
        num_output_channels (int): (1 or 3) number of channels desired for output image
    """

    _v1_transform_cls = _transforms.Grayscale

    def __init__(self, num_output_channels: int = 1):
        super().__init__()
        self.num_output_channels = num_output_channels

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.rgb_to_grayscale, inpt, num_output_channels=self.num_output_channels)


class RandomGrayscale(_RandomApplyTransform):
    """Randomly convert image or videos to grayscale with a probability of p (default 0.1).

    If the input is a :class:`torch.Tensor`, it is expected to have [..., 3 or 1, H, W] shape,
    where ... means an arbitrary number of leading dimensions

    The output has the same number of channels as the input.

    Args:
        p (float): probability that image should be converted to grayscale.
    """

    _v1_transform_cls = _transforms.RandomGrayscale

    def __init__(self, p: float = 0.1) -> None:
        super().__init__(p=p)

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        num_input_channels, *_ = query_chw(flat_inputs)
        return dict(num_input_channels=num_input_channels)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.rgb_to_grayscale, inpt, num_output_channels=params["num_input_channels"])


class RGB(Transform):
    """Convert images or videos to RGB (if they are already not RGB).

    If the input is a :class:`torch.Tensor`, it is expected
    to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions
    """

    def __init__(self):
        super().__init__()

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.grayscale_to_rgb, inpt)


class ColorJitter(Transform):
    """Randomly change the brightness, contrast, saturation and hue of an image or video.

    If the input is a :class:`torch.Tensor`, it is expected
    to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions.
    If img is PIL Image, mode "1", "I", "F" and modes with transparency (alpha channel) are not supported.

    Args:
        brightness (float or tuple of float (min, max)): How much to jitter brightness.
            brightness_factor is chosen uniformly from [max(0, 1 - brightness), 1 + brightness]
            or the given [min, max]. Should be non negative numbers.
        contrast (float or tuple of float (min, max)): How much to jitter contrast.
            contrast_factor is chosen uniformly from [max(0, 1 - contrast), 1 + contrast]
            or the given [min, max]. Should be non-negative numbers.
        saturation (float or tuple of float (min, max)): How much to jitter saturation.
            saturation_factor is chosen uniformly from [max(0, 1 - saturation), 1 + saturation]
            or the given [min, max]. Should be non negative numbers.
        hue (float or tuple of float (min, max)): How much to jitter hue.
            hue_factor is chosen uniformly from [-hue, hue] or the given [min, max].
            Should have 0<= hue <= 0.5 or -0.5 <= min <= max <= 0.5.
            To jitter hue, the pixel values of the input image has to be non-negative for conversion to HSV space;
            thus it does not work if you normalize your image to an interval with negative values,
            or use an interpolation that generates negative values before using this function.
    """

    _v1_transform_cls = _transforms.ColorJitter

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        return {attr: value or 0 for attr, value in super()._extract_params_for_v1_transform().items()}

    def __init__(
        self,
        brightness: Optional[Union[float, Sequence[float]]] = None,
        contrast: Optional[Union[float, Sequence[float]]] = None,
        saturation: Optional[Union[float, Sequence[float]]] = None,
        hue: Optional[Union[float, Sequence[float]]] = None,
    ) -> None:
        super().__init__()
        self.brightness = self._check_input(brightness, "brightness")
        self.contrast = self._check_input(contrast, "contrast")
        self.saturation = self._check_input(saturation, "saturation")
        self.hue = self._check_input(hue, "hue", center=0, bound=(-0.5, 0.5), clip_first_on_zero=False)

    def _check_input(
        self,
        value: Optional[Union[float, Sequence[float]]],
        name: str,
        center: float = 1.0,
        bound: Tuple[float, float] = (0, float("inf")),
        clip_first_on_zero: bool = True,
    ) -> Optional[Tuple[float, float]]:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            if value < 0:
                raise ValueError(f"If {name} is a single number, it must be non negative.")
            value = [center - value, center + value]
            if clip_first_on_zero:
                value[0] = max(value[0], 0.0)
        elif isinstance(value, collections.abc.Sequence) and len(value) == 2:
            value = [float(v) for v in value]
        else:
            raise TypeError(f"{name}={value} should be a single number or a sequence with length 2.")

        if not bound[0] <= value[0] <= value[1] <= bound[1]:
            raise ValueError(f"{name} values should be between {bound}, but got {value}.")

        return None if value[0] == value[1] == center else (float(value[0]), float(value[1]))

    @staticmethod
    def _generate_value(left: float, right: float) -> float:
        return torch.empty(1).uniform_(left, right).item()

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        fn_idx = torch.randperm(4)

        b = None if self.brightness is None else self._generate_value(self.brightness[0], self.brightness[1])
        c = None if self.contrast is None else self._generate_value(self.contrast[0], self.contrast[1])
        s = None if self.saturation is None else self._generate_value(self.saturation[0], self.saturation[1])
        h = None if self.hue is None else self._generate_value(self.hue[0], self.hue[1])

        return dict(fn_idx=fn_idx, brightness_factor=b, contrast_factor=c, saturation_factor=s, hue_factor=h)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        output = inpt
        brightness_factor = params["brightness_factor"]
        contrast_factor = params["contrast_factor"]
        saturation_factor = params["saturation_factor"]
        hue_factor = params["hue_factor"]
        for fn_id in params["fn_idx"]:
            if fn_id == 0 and brightness_factor is not None:
                output = self._call_kernel(F.adjust_brightness, output, brightness_factor=brightness_factor)
            elif fn_id == 1 and contrast_factor is not None:
                output = self._call_kernel(F.adjust_contrast, output, contrast_factor=contrast_factor)
            elif fn_id == 2 and saturation_factor is not None:
                output = self._call_kernel(F.adjust_saturation, output, saturation_factor=saturation_factor)
            elif fn_id == 3 and hue_factor is not None:
                output = self._call_kernel(F.adjust_hue, output, hue_factor=hue_factor)
        return output


class RandomChannelPermutation(Transform):
    """Randomly permute the channels of an image or video"""

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        num_channels, *_ = query_chw(flat_inputs)
        return dict(permutation=torch.randperm(num_channels))

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.permute_channels, inpt, params["permutation"])


class RandomPhotometricDistort(Transform):
    """Randomly distorts the image or video as used in `SSD: Single Shot
    MultiBox Detector <https://arxiv.org/abs/1512.02325>`_.

    This transform relies on :class:`~torchvision.transforms.v2.ColorJitter`
    under the hood to adjust the contrast, saturation, hue, brightness, and also
    randomly permutes channels.

    Args:
        brightness (tuple of float (min, max), optional): How much to jitter brightness.
            brightness_factor is chosen uniformly from [min, max]. Should be non negative numbers.
        contrast (tuple of float (min, max), optional): How much to jitter contrast.
            contrast_factor is chosen uniformly from [min, max]. Should be non-negative numbers.
        saturation (tuple of float (min, max), optional): How much to jitter saturation.
            saturation_factor is chosen uniformly from [min, max]. Should be non negative numbers.
        hue (tuple of float (min, max), optional): How much to jitter hue.
            hue_factor is chosen uniformly from [min, max].  Should have -0.5 <= min <= max <= 0.5.
            To jitter hue, the pixel values of the input image has to be non-negative for conversion to HSV space;
            thus it does not work if you normalize your image to an interval with negative values,
            or use an interpolation that generates negative values before using this function.
        p (float, optional) probability each distortion operation (contrast, saturation, ...) to be applied.
            Default is 0.5.
    """

    def __init__(
        self,
        brightness: Tuple[float, float] = (0.875, 1.125),
        contrast: Tuple[float, float] = (0.5, 1.5),
        saturation: Tuple[float, float] = (0.5, 1.5),
        hue: Tuple[float, float] = (-0.05, 0.05),
        p: float = 0.5,
    ):
        super().__init__()
        self.brightness = brightness
        self.contrast = contrast
        self.hue = hue
        self.saturation = saturation
        self.p = p

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        num_channels, *_ = query_chw(flat_inputs)
        params: Dict[str, Any] = {
            key: ColorJitter._generate_value(range[0], range[1]) if torch.rand(1) < self.p else None
            for key, range in [
                ("brightness_factor", self.brightness),
                ("contrast_factor", self.contrast),
                ("saturation_factor", self.saturation),
                ("hue_factor", self.hue),
            ]
        }
        params["contrast_before"] = bool(torch.rand(()) < 0.5)
        params["channel_permutation"] = torch.randperm(num_channels) if torch.rand(1) < self.p else None
        return params

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        if params["brightness_factor"] is not None:
            inpt = self._call_kernel(F.adjust_brightness, inpt, brightness_factor=params["brightness_factor"])
        if params["contrast_factor"] is not None and params["contrast_before"]:
            inpt = self._call_kernel(F.adjust_contrast, inpt, contrast_factor=params["contrast_factor"])
        if params["saturation_factor"] is not None:
            inpt = self._call_kernel(F.adjust_saturation, inpt, saturation_factor=params["saturation_factor"])
        if params["hue_factor"] is not None:
            inpt = self._call_kernel(F.adjust_hue, inpt, hue_factor=params["hue_factor"])
        if params["contrast_factor"] is not None and not params["contrast_before"]:
            inpt = self._call_kernel(F.adjust_contrast, inpt, contrast_factor=params["contrast_factor"])
        if params["channel_permutation"] is not None:
            inpt = self._call_kernel(F.permute_channels, inpt, permutation=params["channel_permutation"])
        return inpt


class RandomEqualize(_RandomApplyTransform):
    """Equalize the histogram of the given image or video with a given probability.

    If the input is a :class:`torch.Tensor`, it is expected
    to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions.
    If img is PIL Image, it is expected to be in mode "P", "L" or "RGB".

    Args:
        p (float): probability of the image being equalized. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomEqualize

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.equalize, inpt)


class RandomInvert(_RandomApplyTransform):
    """Inverts the colors of the given image or video with a given probability.

    If img is a Tensor, it is expected to be in [..., 1 or 3, H, W] format,
    where ... means it can have an arbitrary number of leading dimensions.
    If img is PIL Image, it is expected to be in mode "L" or "RGB".

    Args:
        p (float): probability of the image being color inverted. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomInvert

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.invert, inpt)


class RandomPosterize(_RandomApplyTransform):
    """Posterize the image or video with a given probability by reducing the
    number of bits for each color channel.

    If the input is a :class:`torch.Tensor`, it should be of type torch.uint8,
    and it is expected to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions.
    If img is PIL Image, it is expected to be in mode "L" or "RGB".

    Args:
        bits (int): number of bits to keep for each channel (0-8)
        p (float): probability of the image being posterized. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomPosterize

    def __init__(self, bits: int, p: float = 0.5) -> None:
        super().__init__(p=p)
        self.bits = bits

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.posterize, inpt, bits=self.bits)


class RandomSolarize(_RandomApplyTransform):
    """Solarize the image or video with a given probability by inverting all pixel
    values above a threshold.

    If img is a Tensor, it is expected to be in [..., 1 or 3, H, W] format,
    where ... means it can have an arbitrary number of leading dimensions.
    If img is PIL Image, it is expected to be in mode "L" or "RGB".

    Args:
        threshold (float): all pixels equal or above this value are inverted.
        p (float): probability of the image being solarized. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomSolarize

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        params = super()._extract_params_for_v1_transform()
        params["threshold"] = float(params["threshold"])
        return params

    def __init__(self, threshold: float, p: float = 0.5) -> None:
        super().__init__(p=p)
        self.threshold = threshold

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.solarize, inpt, threshold=self.threshold)


class RandomAutocontrast(_RandomApplyTransform):
    """Autocontrast the pixels of the given image or video with a given probability.

    If the input is a :class:`torch.Tensor`, it is expected
    to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions.
    If img is PIL Image, it is expected to be in mode "L" or "RGB".

    Args:
        p (float): probability of the image being autocontrasted. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomAutocontrast

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.autocontrast, inpt)


class RandomAdjustSharpness(_RandomApplyTransform):
    """Adjust the sharpness of the image or video with a given probability.

    If the input is a :class:`torch.Tensor`,
    it is expected to have [..., 1 or 3, H, W] shape, where ... means an arbitrary number of leading dimensions.

    Args:
        sharpness_factor (float):  How much to adjust the sharpness. Can be
            any non-negative number. 0 gives a blurred image, 1 gives the
            original image while 2 increases the sharpness by a factor of 2.
        p (float): probability of the image being sharpened. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomAdjustSharpness

    def __init__(self, sharpness_factor: float, p: float = 0.5) -> None:
        super().__init__(p=p)
        self.sharpness_factor = sharpness_factor

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.adjust_sharpness, inpt, sharpness_factor=self.sharpness_factor)
