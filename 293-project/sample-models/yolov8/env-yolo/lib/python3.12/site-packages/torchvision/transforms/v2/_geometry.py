import math
import numbers
import warnings
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Type, Union

import PIL.Image
import torch

from torchvision import transforms as _transforms, tv_tensors
from torchvision.ops.boxes import box_iou
from torchvision.transforms.functional import _get_perspective_coeffs
from torchvision.transforms.v2 import functional as F, InterpolationMode, Transform
from torchvision.transforms.v2.functional._utils import _FillType

from ._transform import _RandomApplyTransform
from ._utils import (
    _check_padding_arg,
    _check_padding_mode_arg,
    _check_sequence_input,
    _get_fill,
    _setup_angle,
    _setup_fill_arg,
    _setup_number_or_seq,
    _setup_size,
    get_bounding_boxes,
    has_all,
    has_any,
    is_pure_tensor,
    query_size,
)


class RandomHorizontalFlip(_RandomApplyTransform):
    """Horizontally flip the input with a given probability.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        p (float, optional): probability of the input being flipped. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomHorizontalFlip

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.horizontal_flip, inpt)


class RandomVerticalFlip(_RandomApplyTransform):
    """Vertically flip the input with a given probability.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        p (float, optional): probability of the input being flipped. Default value is 0.5
    """

    _v1_transform_cls = _transforms.RandomVerticalFlip

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.vertical_flip, inpt)


class Resize(Transform):
    """Resize the input to the given size.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        size (sequence, int, or None): Desired
            output size.

            - If size is a sequence like (h, w), output size will be matched to this.
            - If size is an int, smaller edge of the image will be matched to this
              number.  i.e, if height > width, then image will be rescaled to
              (size * height / width, size).
            - If size is None, the output shape is determined by the ``max_size``
              parameter.

            .. note::
                In torchscript mode size as single int is not supported, use a sequence of length 1: ``[size, ]``.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.NEAREST_EXACT``,
            ``InterpolationMode.BILINEAR`` and ``InterpolationMode.BICUBIC`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        max_size (int, optional): The maximum allowed for the longer edge of
            the resized image.

            - If ``size`` is an int: if the longer edge of the image is greater
              than ``max_size`` after being resized according to ``size``,
              ``size`` will be overruled so that the longer edge is equal to
              ``max_size``. As a result, the smaller edge may be shorter than
              ``size``. This is only supported if ``size`` is an int (or a
              sequence of length 1 in torchscript mode).
            - If ``size`` is None: the longer edge of the image will be matched
              to max_size.  i.e, if height > width, then image will be rescaled
              to (max_size, max_size * width / height).

            This should be left to ``None`` (default) when ``size`` is a
            sequence.

        antialias (bool, optional): Whether to apply antialiasing.
            It only affects **tensors** with bilinear or bicubic modes and it is
            ignored otherwise: on PIL images, antialiasing is always applied on
            bilinear or bicubic modes; on other modes (for PIL images and
            tensors), antialiasing makes no sense and this parameter is ignored.
            Possible values are:

            - ``True`` (default): will apply antialiasing for bilinear or bicubic modes.
              Other mode aren't affected. This is probably what you want to use.
            - ``False``: will not apply antialiasing for tensors on any mode. PIL
              images are still antialiased on bilinear or bicubic modes, because
              PIL doesn't support no antialias.
            - ``None``: equivalent to ``False`` for tensors and ``True`` for
              PIL images. This value exists for legacy reasons and you probably
              don't want to use it unless you really know what you are doing.

            The default value changed from ``None`` to ``True`` in
            v0.17, for the PIL and Tensor backends to be consistent.
    """

    _v1_transform_cls = _transforms.Resize

    def __init__(
        self,
        size: Union[int, Sequence[int], None],
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        max_size: Optional[int] = None,
        antialias: Optional[bool] = True,
    ) -> None:
        super().__init__()

        if isinstance(size, int):
            size = [size]
        elif isinstance(size, Sequence) and len(size) in {1, 2}:
            size = list(size)
        elif size is None:
            if not isinstance(max_size, int):
                raise ValueError(f"max_size must be an integer when size is None, but got {max_size} instead.")
        else:
            raise ValueError(
                f"size can be an integer, a sequence of one or two integers, or None, but got {size} instead."
            )
        self.size = size

        self.interpolation = interpolation
        self.max_size = max_size
        self.antialias = antialias

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(
            F.resize,
            inpt,
            self.size,
            interpolation=self.interpolation,
            max_size=self.max_size,
            antialias=self.antialias,
        )


class CenterCrop(Transform):
    """Crop the input at the center.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    If image size is smaller than output size along any edge, image is padded with 0 and then center cropped.

    Args:
        size (sequence or int): Desired output size of the crop. If size is an
            int instead of sequence like (h, w), a square crop (size, size) is
            made. If provided a sequence of length 1, it will be interpreted as (size[0], size[0]).
    """

    _v1_transform_cls = _transforms.CenterCrop

    def __init__(self, size: Union[int, Sequence[int]]):
        super().__init__()
        self.size = _setup_size(size, error_msg="Please provide only two dimensions (h, w) for size.")

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.center_crop, inpt, output_size=self.size)


class RandomResizedCrop(Transform):
    """Crop a random portion of the input and resize it to a given size.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    A crop of the original input is made: the crop has a random area (H * W)
    and a random aspect ratio. This crop is finally resized to the given
    size. This is popularly used to train the Inception networks.

    Args:
        size (int or sequence): expected output size of the crop, for each edge. If size is an
            int instead of sequence like (h, w), a square output size ``(size, size)`` is
            made. If provided a sequence of length 1, it will be interpreted as (size[0], size[0]).

            .. note::
                In torchscript mode size as single int is not supported, use a sequence of length 1: ``[size, ]``.
        scale (tuple of float, optional): Specifies the lower and upper bounds for the random area of the crop,
            before resizing. The scale is defined with respect to the area of the original image.
        ratio (tuple of float, optional): lower and upper bounds for the random aspect ratio of the crop, before
            resizing.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.NEAREST_EXACT``,
            ``InterpolationMode.BILINEAR`` and ``InterpolationMode.BICUBIC`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        antialias (bool, optional): Whether to apply antialiasing.
            It only affects **tensors** with bilinear or bicubic modes and it is
            ignored otherwise: on PIL images, antialiasing is always applied on
            bilinear or bicubic modes; on other modes (for PIL images and
            tensors), antialiasing makes no sense and this parameter is ignored.
            Possible values are:

            - ``True`` (default): will apply antialiasing for bilinear or bicubic modes.
              Other mode aren't affected. This is probably what you want to use.
            - ``False``: will not apply antialiasing for tensors on any mode. PIL
              images are still antialiased on bilinear or bicubic modes, because
              PIL doesn't support no antialias.
            - ``None``: equivalent to ``False`` for tensors and ``True`` for
              PIL images. This value exists for legacy reasons and you probably
              don't want to use it unless you really know what you are doing.

            The default value changed from ``None`` to ``True`` in
            v0.17, for the PIL and Tensor backends to be consistent.
    """

    _v1_transform_cls = _transforms.RandomResizedCrop

    def __init__(
        self,
        size: Union[int, Sequence[int]],
        scale: Tuple[float, float] = (0.08, 1.0),
        ratio: Tuple[float, float] = (3.0 / 4.0, 4.0 / 3.0),
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        antialias: Optional[bool] = True,
    ) -> None:
        super().__init__()
        self.size = _setup_size(size, error_msg="Please provide only two dimensions (h, w) for size.")

        if not isinstance(scale, Sequence):
            raise TypeError("Scale should be a sequence")
        if not isinstance(ratio, Sequence):
            raise TypeError("Ratio should be a sequence")
        if (scale[0] > scale[1]) or (ratio[0] > ratio[1]):
            warnings.warn("Scale and ratio should be of kind (min, max)")

        self.scale = scale
        self.ratio = ratio
        self.interpolation = interpolation
        self.antialias = antialias

        self._log_ratio = torch.log(torch.tensor(self.ratio))

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        height, width = query_size(flat_inputs)
        area = height * width

        log_ratio = self._log_ratio
        for _ in range(10):
            target_area = area * torch.empty(1).uniform_(self.scale[0], self.scale[1]).item()
            aspect_ratio = torch.exp(
                torch.empty(1).uniform_(
                    log_ratio[0],  # type: ignore[arg-type]
                    log_ratio[1],  # type: ignore[arg-type]
                )
            ).item()

            w = int(round(math.sqrt(target_area * aspect_ratio)))
            h = int(round(math.sqrt(target_area / aspect_ratio)))

            if 0 < w <= width and 0 < h <= height:
                i = torch.randint(0, height - h + 1, size=(1,)).item()
                j = torch.randint(0, width - w + 1, size=(1,)).item()
                break
        else:
            # Fallback to central crop
            in_ratio = float(width) / float(height)
            if in_ratio < min(self.ratio):
                w = width
                h = int(round(w / min(self.ratio)))
            elif in_ratio > max(self.ratio):
                h = height
                w = int(round(h * max(self.ratio)))
            else:  # whole image
                w = width
                h = height
            i = (height - h) // 2
            j = (width - w) // 2

        return dict(top=i, left=j, height=h, width=w)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(
            F.resized_crop, inpt, **params, size=self.size, interpolation=self.interpolation, antialias=self.antialias
        )


class FiveCrop(Transform):
    """Crop the image or video into four corners and the central crop.

    If the input is a :class:`torch.Tensor` or a :class:`~torchvision.tv_tensors.Image` or a
    :class:`~torchvision.tv_tensors.Video` it can have arbitrary number of leading batch dimensions.
    For example, the image can have ``[..., C, H, W]`` shape.

    .. Note::
         This transform returns a tuple of images and there may be a mismatch in the number of
         inputs and targets your Dataset returns. See below for an example of how to deal with
         this.

    Args:
         size (sequence or int): Desired output size of the crop. If size is an ``int``
            instead of sequence like (h, w), a square crop of size (size, size) is made.
            If provided a sequence of length 1, it will be interpreted as (size[0], size[0]).

    Example:
        >>> class BatchMultiCrop(transforms.Transform):
        ...     def forward(self, sample: Tuple[Tuple[Union[tv_tensors.Image, tv_tensors.Video], ...], int]):
        ...         images_or_videos, labels = sample
        ...         batch_size = len(images_or_videos)
        ...         image_or_video = images_or_videos[0]
        ...         images_or_videos = tv_tensors.wrap(torch.stack(images_or_videos), like=image_or_video)
        ...         labels = torch.full((batch_size,), label, device=images_or_videos.device)
        ...         return images_or_videos, labels
        ...
        >>> image = tv_tensors.Image(torch.rand(3, 256, 256))
        >>> label = 3
        >>> transform = transforms.Compose([transforms.FiveCrop(224), BatchMultiCrop()])
        >>> images, labels = transform(image, label)
        >>> images.shape
        torch.Size([5, 3, 224, 224])
        >>> labels
        tensor([3, 3, 3, 3, 3])
    """

    _v1_transform_cls = _transforms.FiveCrop

    def __init__(self, size: Union[int, Sequence[int]]) -> None:
        super().__init__()
        self.size = _setup_size(size, error_msg="Please provide only two dimensions (h, w) for size.")

    def _call_kernel(self, functional: Callable, inpt: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(inpt, (tv_tensors.BoundingBoxes, tv_tensors.Mask)):
            warnings.warn(
                f"{type(self).__name__}() is currently passing through inputs of type "
                f"tv_tensors.{type(inpt).__name__}. This will likely change in the future."
            )
        return super()._call_kernel(functional, inpt, *args, **kwargs)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.five_crop, inpt, self.size)

    def _check_inputs(self, flat_inputs: List[Any]) -> None:
        if has_any(flat_inputs, tv_tensors.BoundingBoxes, tv_tensors.Mask):
            raise TypeError(f"BoundingBoxes'es and Mask's are not supported by {type(self).__name__}()")


class TenCrop(Transform):
    """Crop the image or video into four corners and the central crop plus the flipped version of
    these (horizontal flipping is used by default).

    If the input is a :class:`torch.Tensor` or a :class:`~torchvision.tv_tensors.Image` or a
    :class:`~torchvision.tv_tensors.Video` it can have arbitrary number of leading batch dimensions.
    For example, the image can have ``[..., C, H, W]`` shape.

    See :class:`~torchvision.transforms.v2.FiveCrop` for an example.

    .. Note::
         This transform returns a tuple of images and there may be a mismatch in the number of
         inputs and targets your Dataset returns. See below for an example of how to deal with
         this.

    Args:
        size (sequence or int): Desired output size of the crop. If size is an
            int instead of sequence like (h, w), a square crop (size, size) is
            made. If provided a sequence of length 1, it will be interpreted as (size[0], size[0]).
        vertical_flip (bool, optional): Use vertical flipping instead of horizontal
    """

    _v1_transform_cls = _transforms.TenCrop

    def __init__(self, size: Union[int, Sequence[int]], vertical_flip: bool = False) -> None:
        super().__init__()
        self.size = _setup_size(size, error_msg="Please provide only two dimensions (h, w) for size.")
        self.vertical_flip = vertical_flip

    def _call_kernel(self, functional: Callable, inpt: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(inpt, (tv_tensors.BoundingBoxes, tv_tensors.Mask)):
            warnings.warn(
                f"{type(self).__name__}() is currently passing through inputs of type "
                f"tv_tensors.{type(inpt).__name__}. This will likely change in the future."
            )
        return super()._call_kernel(functional, inpt, *args, **kwargs)

    def _check_inputs(self, flat_inputs: List[Any]) -> None:
        if has_any(flat_inputs, tv_tensors.BoundingBoxes, tv_tensors.Mask):
            raise TypeError(f"BoundingBoxes'es and Mask's are not supported by {type(self).__name__}()")

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.ten_crop, inpt, self.size, vertical_flip=self.vertical_flip)


class Pad(Transform):
    """Pad the input on all sides with the given "pad" value.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        padding (int or sequence): Padding on each border. If a single int is provided this
            is used to pad all borders. If sequence of length 2 is provided this is the padding
            on left/right and top/bottom respectively. If a sequence of length 4 is provided
            this is the padding for the left, top, right and bottom borders respectively.

            .. note::
                In torchscript mode padding as single int is not supported, use a sequence of
                length 1: ``[padding, ]``.
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
        padding_mode (str, optional): Type of padding. Should be: constant, edge, reflect or symmetric.
            Default is "constant".

            - constant: pads with a constant value, this value is specified with fill

            - edge: pads with the last value at the edge of the image.

            - reflect: pads with reflection of image without repeating the last value on the edge.
              For example, padding [1, 2, 3, 4] with 2 elements on both sides in reflect mode
              will result in [3, 2, 1, 2, 3, 4, 3, 2]

            - symmetric: pads with reflection of image repeating the last value on the edge.
              For example, padding [1, 2, 3, 4] with 2 elements on both sides in symmetric mode
              will result in [2, 1, 1, 2, 3, 4, 4, 3]
    """

    _v1_transform_cls = _transforms.Pad

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        params = super()._extract_params_for_v1_transform()

        if not (params["fill"] is None or isinstance(params["fill"], (int, float))):
            raise ValueError(f"{type(self).__name__}() can only be scripted for a scalar `fill`, but got {self.fill}.")

        return params

    def __init__(
        self,
        padding: Union[int, Sequence[int]],
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
        padding_mode: Literal["constant", "edge", "reflect", "symmetric"] = "constant",
    ) -> None:
        super().__init__()

        _check_padding_arg(padding)
        _check_padding_mode_arg(padding_mode)

        # This cast does Sequence[int] -> List[int] and is required to make mypy happy
        if not isinstance(padding, int):
            padding = list(padding)
        self.padding = padding
        self.fill = fill
        self._fill = _setup_fill_arg(fill)
        self.padding_mode = padding_mode

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(F.pad, inpt, padding=self.padding, fill=fill, padding_mode=self.padding_mode)  # type: ignore[arg-type]


class RandomZoomOut(_RandomApplyTransform):
    """ "Zoom out" transformation from
    `"SSD: Single Shot MultiBox Detector" <https://arxiv.org/abs/1512.02325>`_.

    This transformation randomly pads images, videos, bounding boxes and masks creating a zoom out effect.
    Output spatial size is randomly sampled from original size up to a maximum size configured
    with ``side_range`` parameter:

    .. code-block:: python

        r = uniform_sample(side_range[0], side_range[1])
        output_width = input_width * r
        output_height = input_height * r

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
        side_range (sequence of floats, optional): tuple of two floats defines minimum and maximum factors to
            scale the input size.
        p (float, optional): probability that the zoom operation will be performed.
    """

    def __init__(
        self,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
        side_range: Sequence[float] = (1.0, 4.0),
        p: float = 0.5,
    ) -> None:
        super().__init__(p=p)

        self.fill = fill
        self._fill = _setup_fill_arg(fill)

        _check_sequence_input(side_range, "side_range", req_sizes=(2,))

        self.side_range = side_range
        if side_range[0] < 1.0 or side_range[0] > side_range[1]:
            raise ValueError(f"Invalid side range provided {side_range}.")

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        orig_h, orig_w = query_size(flat_inputs)

        r = self.side_range[0] + torch.rand(1) * (self.side_range[1] - self.side_range[0])
        canvas_width = int(orig_w * r)
        canvas_height = int(orig_h * r)

        r = torch.rand(2)
        left = int((canvas_width - orig_w) * r[0])
        top = int((canvas_height - orig_h) * r[1])
        right = canvas_width - (left + orig_w)
        bottom = canvas_height - (top + orig_h)
        padding = [left, top, right, bottom]

        return dict(padding=padding)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(F.pad, inpt, **params, fill=fill)


class RandomRotation(Transform):
    """Rotate the input by angle.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        degrees (sequence or number): Range of degrees to select from.
            If degrees is a number instead of sequence like (min, max), the range of degrees
            will be (-degrees, +degrees).
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.NEAREST``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.BILINEAR`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        expand (bool, optional): Optional expansion flag.
            If true, expands the output to make it large enough to hold the entire rotated image.
            If false or omitted, make the output image the same size as the input image.
            Note that the expand flag assumes rotation around the center (see note below) and no translation.
        center (sequence, optional): Optional center of rotation, (x, y). Origin is the upper left corner.
            Default is the center of the image.

            .. note::

                In theory, setting ``center`` has no effect if ``expand=True``, since the image center will become the
                center of rotation. In practice however, due to numerical precision, this can lead to off-by-one
                differences of the resulting image size compared to using the image center in the first place. Thus, when
                setting ``expand=True``, it's best to leave ``center=None`` (default).
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.

    .. _filters: https://pillow.readthedocs.io/en/latest/handbook/concepts.html#filters

    """

    _v1_transform_cls = _transforms.RandomRotation

    def __init__(
        self,
        degrees: Union[numbers.Number, Sequence],
        interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
        expand: bool = False,
        center: Optional[List[float]] = None,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
    ) -> None:
        super().__init__()
        self.degrees = _setup_angle(degrees, name="degrees", req_sizes=(2,))
        self.interpolation = interpolation
        self.expand = expand

        self.fill = fill
        self._fill = _setup_fill_arg(fill)

        if center is not None:
            _check_sequence_input(center, "center", req_sizes=(2,))

        self.center = center

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        angle = torch.empty(1).uniform_(self.degrees[0], self.degrees[1]).item()
        return dict(angle=angle)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(
            F.rotate,
            inpt,
            **params,
            interpolation=self.interpolation,
            expand=self.expand,
            center=self.center,
            fill=fill,
        )


class RandomAffine(Transform):
    """Random affine transformation the input keeping center invariant.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        degrees (sequence or number): Range of degrees to select from.
            If degrees is a number instead of sequence like (min, max), the range of degrees
            will be (-degrees, +degrees). Set to 0 to deactivate rotations.
        translate (tuple, optional): tuple of maximum absolute fraction for horizontal
            and vertical translations. For example translate=(a, b), then horizontal shift
            is randomly sampled in the range -img_width * a < dx < img_width * a and vertical shift is
            randomly sampled in the range -img_height * b < dy < img_height * b. Will not translate by default.
        scale (tuple, optional): scaling factor interval, e.g (a, b), then scale is
            randomly sampled from the range a <= scale <= b. Will keep original scale by default.
        shear (sequence or number, optional): Range of degrees to select from.
            If shear is a number, a shear parallel to the x-axis in the range (-shear, +shear)
            will be applied. Else if shear is a sequence of 2 values a shear parallel to the x-axis in the
            range (shear[0], shear[1]) will be applied. Else if shear is a sequence of 4 values,
            an x-axis shear in (shear[0], shear[1]) and y-axis shear in (shear[2], shear[3]) will be applied.
            Will not apply shear by default.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.NEAREST``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.BILINEAR`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
        center (sequence, optional): Optional center of rotation, (x, y). Origin is the upper left corner.
            Default is the center of the image.

    .. _filters: https://pillow.readthedocs.io/en/latest/handbook/concepts.html#filters

    """

    _v1_transform_cls = _transforms.RandomAffine

    def __init__(
        self,
        degrees: Union[numbers.Number, Sequence],
        translate: Optional[Sequence[float]] = None,
        scale: Optional[Sequence[float]] = None,
        shear: Optional[Union[int, float, Sequence[float]]] = None,
        interpolation: Union[InterpolationMode, int] = InterpolationMode.NEAREST,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
        center: Optional[List[float]] = None,
    ) -> None:
        super().__init__()
        self.degrees = _setup_angle(degrees, name="degrees", req_sizes=(2,))
        if translate is not None:
            _check_sequence_input(translate, "translate", req_sizes=(2,))
            for t in translate:
                if not (0.0 <= t <= 1.0):
                    raise ValueError("translation values should be between 0 and 1")
        self.translate = translate
        if scale is not None:
            _check_sequence_input(scale, "scale", req_sizes=(2,))
            for s in scale:
                if s <= 0:
                    raise ValueError("scale values should be positive")
        self.scale = scale

        if shear is not None:
            self.shear = _setup_angle(shear, name="shear", req_sizes=(2, 4))
        else:
            self.shear = shear

        self.interpolation = interpolation
        self.fill = fill
        self._fill = _setup_fill_arg(fill)

        if center is not None:
            _check_sequence_input(center, "center", req_sizes=(2,))

        self.center = center

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        height, width = query_size(flat_inputs)

        angle = torch.empty(1).uniform_(self.degrees[0], self.degrees[1]).item()
        if self.translate is not None:
            max_dx = float(self.translate[0] * width)
            max_dy = float(self.translate[1] * height)
            tx = int(round(torch.empty(1).uniform_(-max_dx, max_dx).item()))
            ty = int(round(torch.empty(1).uniform_(-max_dy, max_dy).item()))
            translate = (tx, ty)
        else:
            translate = (0, 0)

        if self.scale is not None:
            scale = torch.empty(1).uniform_(self.scale[0], self.scale[1]).item()
        else:
            scale = 1.0

        shear_x = shear_y = 0.0
        if self.shear is not None:
            shear_x = torch.empty(1).uniform_(self.shear[0], self.shear[1]).item()
            if len(self.shear) == 4:
                shear_y = torch.empty(1).uniform_(self.shear[2], self.shear[3]).item()

        shear = (shear_x, shear_y)
        return dict(angle=angle, translate=translate, scale=scale, shear=shear)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(
            F.affine,
            inpt,
            **params,
            interpolation=self.interpolation,
            fill=fill,
            center=self.center,
        )


class RandomCrop(Transform):
    """Crop the input at a random location.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        size (sequence or int): Desired output size of the crop. If size is an
            int instead of sequence like (h, w), a square crop (size, size) is
            made. If provided a sequence of length 1, it will be interpreted as (size[0], size[0]).
        padding (int or sequence, optional): Optional padding on each border
            of the image. Default is None. If a single int is provided this
            is used to pad all borders. If sequence of length 2 is provided this is the padding
            on left/right and top/bottom respectively. If a sequence of length 4 is provided
            this is the padding for the left, top, right and bottom borders respectively.

            .. note::
                In torchscript mode padding as single int is not supported, use a sequence of
                length 1: ``[padding, ]``.
        pad_if_needed (boolean, optional): It will pad the image if smaller than the
            desired size to avoid raising an exception. Since cropping is done
            after padding, the padding seems to be done at a random offset.
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
        padding_mode (str, optional): Type of padding. Should be: constant, edge, reflect or symmetric.
            Default is constant.

            - constant: pads with a constant value, this value is specified with fill

            - edge: pads with the last value at the edge of the image.

            - reflect: pads with reflection of image without repeating the last value on the edge.
              For example, padding [1, 2, 3, 4] with 2 elements on both sides in reflect mode
              will result in [3, 2, 1, 2, 3, 4, 3, 2]

            - symmetric: pads with reflection of image repeating the last value on the edge.
              For example, padding [1, 2, 3, 4] with 2 elements on both sides in symmetric mode
              will result in [2, 1, 1, 2, 3, 4, 4, 3]
    """

    _v1_transform_cls = _transforms.RandomCrop

    def _extract_params_for_v1_transform(self) -> Dict[str, Any]:
        params = super()._extract_params_for_v1_transform()

        if not (params["fill"] is None or isinstance(params["fill"], (int, float))):
            raise ValueError(f"{type(self).__name__}() can only be scripted for a scalar `fill`, but got {self.fill}.")

        padding = self.padding
        if padding is not None:
            pad_left, pad_right, pad_top, pad_bottom = padding
            padding = [pad_left, pad_top, pad_right, pad_bottom]
        params["padding"] = padding

        return params

    def __init__(
        self,
        size: Union[int, Sequence[int]],
        padding: Optional[Union[int, Sequence[int]]] = None,
        pad_if_needed: bool = False,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
        padding_mode: Literal["constant", "edge", "reflect", "symmetric"] = "constant",
    ) -> None:
        super().__init__()

        self.size = _setup_size(size, error_msg="Please provide only two dimensions (h, w) for size.")

        if pad_if_needed or padding is not None:
            if padding is not None:
                _check_padding_arg(padding)
            _check_padding_mode_arg(padding_mode)

        self.padding = F._geometry._parse_pad_padding(padding) if padding else None  # type: ignore[arg-type]
        self.pad_if_needed = pad_if_needed
        self.fill = fill
        self._fill = _setup_fill_arg(fill)
        self.padding_mode = padding_mode

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        padded_height, padded_width = query_size(flat_inputs)

        if self.padding is not None:
            pad_left, pad_right, pad_top, pad_bottom = self.padding
            padded_height += pad_top + pad_bottom
            padded_width += pad_left + pad_right
        else:
            pad_left = pad_right = pad_top = pad_bottom = 0

        cropped_height, cropped_width = self.size

        if self.pad_if_needed:
            if padded_height < cropped_height:
                diff = cropped_height - padded_height

                pad_top += diff
                pad_bottom += diff
                padded_height += 2 * diff

            if padded_width < cropped_width:
                diff = cropped_width - padded_width

                pad_left += diff
                pad_right += diff
                padded_width += 2 * diff

        if padded_height < cropped_height or padded_width < cropped_width:
            raise ValueError(
                f"Required crop size {(cropped_height, cropped_width)} is larger than "
                f"{'padded ' if self.padding is not None else ''}input image size {(padded_height, padded_width)}."
            )

        # We need a different order here than we have in self.padding since this padding will be parsed again in `F.pad`
        padding = [pad_left, pad_top, pad_right, pad_bottom]
        needs_pad = any(padding)

        needs_vert_crop, top = (
            (True, int(torch.randint(0, padded_height - cropped_height + 1, size=())))
            if padded_height > cropped_height
            else (False, 0)
        )
        needs_horz_crop, left = (
            (True, int(torch.randint(0, padded_width - cropped_width + 1, size=())))
            if padded_width > cropped_width
            else (False, 0)
        )

        return dict(
            needs_crop=needs_vert_crop or needs_horz_crop,
            top=top,
            left=left,
            height=cropped_height,
            width=cropped_width,
            needs_pad=needs_pad,
            padding=padding,
        )

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        if params["needs_pad"]:
            fill = _get_fill(self._fill, type(inpt))
            inpt = self._call_kernel(F.pad, inpt, padding=params["padding"], fill=fill, padding_mode=self.padding_mode)

        if params["needs_crop"]:
            inpt = self._call_kernel(
                F.crop, inpt, top=params["top"], left=params["left"], height=params["height"], width=params["width"]
            )

        return inpt


class RandomPerspective(_RandomApplyTransform):
    """Perform a random perspective transformation of the input with a given probability.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        distortion_scale (float, optional): argument to control the degree of distortion and ranges from 0 to 1.
            Default is 0.5.
        p (float, optional): probability of the input being transformed. Default is 0.5.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.BILINEAR`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
    """

    _v1_transform_cls = _transforms.RandomPerspective

    def __init__(
        self,
        distortion_scale: float = 0.5,
        p: float = 0.5,
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
    ) -> None:
        super().__init__(p=p)

        if not (0 <= distortion_scale <= 1):
            raise ValueError("Argument distortion_scale value should be between 0 and 1")

        self.distortion_scale = distortion_scale
        self.interpolation = interpolation
        self.fill = fill
        self._fill = _setup_fill_arg(fill)

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        height, width = query_size(flat_inputs)

        distortion_scale = self.distortion_scale

        half_height = height // 2
        half_width = width // 2
        bound_height = int(distortion_scale * half_height) + 1
        bound_width = int(distortion_scale * half_width) + 1
        topleft = [
            int(torch.randint(0, bound_width, size=(1,))),
            int(torch.randint(0, bound_height, size=(1,))),
        ]
        topright = [
            int(torch.randint(width - bound_width, width, size=(1,))),
            int(torch.randint(0, bound_height, size=(1,))),
        ]
        botright = [
            int(torch.randint(width - bound_width, width, size=(1,))),
            int(torch.randint(height - bound_height, height, size=(1,))),
        ]
        botleft = [
            int(torch.randint(0, bound_width, size=(1,))),
            int(torch.randint(height - bound_height, height, size=(1,))),
        ]
        startpoints = [[0, 0], [width - 1, 0], [width - 1, height - 1], [0, height - 1]]
        endpoints = [topleft, topright, botright, botleft]
        perspective_coeffs = _get_perspective_coeffs(startpoints, endpoints)
        return dict(coefficients=perspective_coeffs)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(
            F.perspective,
            inpt,
            startpoints=None,
            endpoints=None,
            fill=fill,
            interpolation=self.interpolation,
            **params,
        )


class ElasticTransform(Transform):
    """Transform the input with elastic transformations.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Given alpha and sigma, it will generate displacement
    vectors for all pixels based on random offsets. Alpha controls the strength
    and sigma controls the smoothness of the displacements.
    The displacements are added to an identity grid and the resulting grid is
    used to transform the input.

    .. note::
        Implementation to transform bounding boxes is approximative (not exact).
        We construct an approximation of the inverse grid as ``inverse_grid = identity - displacement``.
        This is not an exact inverse of the grid used to transform images, i.e. ``grid = identity + displacement``.
        Our assumption is that ``displacement * displacement`` is small and can be ignored.
        Large displacements would lead to large errors in the approximation.

    Applications:
        Randomly transforms the morphology of objects in images and produces a
        see-through-water-like effect.

    Args:
        alpha (float or sequence of floats, optional): Magnitude of displacements. Default is 50.0.
        sigma (float or sequence of floats, optional): Smoothness of displacements. Default is 5.0.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.BILINEAR`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        fill (number or tuple or dict, optional): Pixel fill value used when the  ``padding_mode`` is constant.
            Default is 0. If a tuple of length 3, it is used to fill R, G, B channels respectively.
            Fill value can be also a dictionary mapping data type to the fill value, e.g.
            ``fill={tv_tensors.Image: 127, tv_tensors.Mask: 0}`` where ``Image`` will be filled with 127 and
            ``Mask`` will be filled with 0.
    """

    _v1_transform_cls = _transforms.ElasticTransform

    def __init__(
        self,
        alpha: Union[float, Sequence[float]] = 50.0,
        sigma: Union[float, Sequence[float]] = 5.0,
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        fill: Union[_FillType, Dict[Union[Type, str], _FillType]] = 0,
    ) -> None:
        super().__init__()
        self.alpha = _setup_number_or_seq(alpha, "alpha")
        self.sigma = _setup_number_or_seq(sigma, "sigma")

        self.interpolation = interpolation
        self.fill = fill
        self._fill = _setup_fill_arg(fill)

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        size = list(query_size(flat_inputs))

        dx = torch.rand([1, 1] + size) * 2 - 1
        if self.sigma[0] > 0.0:
            kx = int(8 * self.sigma[0] + 1)
            # if kernel size is even we have to make it odd
            if kx % 2 == 0:
                kx += 1
            dx = self._call_kernel(F.gaussian_blur, dx, [kx, kx], list(self.sigma))
        dx = dx * self.alpha[0] / size[0]

        dy = torch.rand([1, 1] + size) * 2 - 1
        if self.sigma[1] > 0.0:
            ky = int(8 * self.sigma[1] + 1)
            # if kernel size is even we have to make it odd
            if ky % 2 == 0:
                ky += 1
            dy = self._call_kernel(F.gaussian_blur, dy, [ky, ky], list(self.sigma))
        dy = dy * self.alpha[1] / size[1]
        displacement = torch.concat([dx, dy], 1).permute([0, 2, 3, 1])  # 1 x H x W x 2
        return dict(displacement=displacement)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        fill = _get_fill(self._fill, type(inpt))
        return self._call_kernel(
            F.elastic,
            inpt,
            **params,
            fill=fill,
            interpolation=self.interpolation,
        )


class RandomIoUCrop(Transform):
    """Random IoU crop transformation from
    `"SSD: Single Shot MultiBox Detector" <https://arxiv.org/abs/1512.02325>`_.

    This transformation requires an image or video data and ``tv_tensors.BoundingBoxes`` in the input.

    .. warning::
        In order to properly remove the bounding boxes below the IoU threshold, `RandomIoUCrop`
        must be followed by :class:`~torchvision.transforms.v2.SanitizeBoundingBoxes`, either immediately
        after or later in the transforms pipeline.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        min_scale (float, optional): Minimum factors to scale the input size.
        max_scale (float, optional): Maximum factors to scale the input size.
        min_aspect_ratio (float, optional): Minimum aspect ratio for the cropped image or video.
        max_aspect_ratio (float, optional): Maximum aspect ratio for the cropped image or video.
        sampler_options (list of float, optional): List of minimal IoU (Jaccard) overlap between all the boxes and
            a cropped image or video. Default, ``None`` which corresponds to ``[0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]``
        trials (int, optional): Number of trials to find a crop for a given value of minimal IoU (Jaccard) overlap.
            Default, 40.
    """

    def __init__(
        self,
        min_scale: float = 0.3,
        max_scale: float = 1.0,
        min_aspect_ratio: float = 0.5,
        max_aspect_ratio: float = 2.0,
        sampler_options: Optional[List[float]] = None,
        trials: int = 40,
    ):
        super().__init__()
        # Configuration similar to https://github.com/weiliu89/caffe/blob/ssd/examples/ssd/ssd_coco.py#L89-L174
        self.min_scale = min_scale
        self.max_scale = max_scale
        self.min_aspect_ratio = min_aspect_ratio
        self.max_aspect_ratio = max_aspect_ratio
        if sampler_options is None:
            sampler_options = [0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]
        self.options = sampler_options
        self.trials = trials

    def _check_inputs(self, flat_inputs: List[Any]) -> None:
        if not (
            has_all(flat_inputs, tv_tensors.BoundingBoxes)
            and has_any(flat_inputs, PIL.Image.Image, tv_tensors.Image, is_pure_tensor)
        ):
            raise TypeError(
                f"{type(self).__name__}() requires input sample to contain tensor or PIL images "
                "and bounding boxes. Sample can also contain masks."
            )

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        orig_h, orig_w = query_size(flat_inputs)
        bboxes = get_bounding_boxes(flat_inputs)

        while True:
            # sample an option
            idx = int(torch.randint(low=0, high=len(self.options), size=(1,)))
            min_jaccard_overlap = self.options[idx]
            if min_jaccard_overlap >= 1.0:  # a value larger than 1 encodes the leave as-is option
                return dict()

            for _ in range(self.trials):
                # check the aspect ratio limitations
                r = self.min_scale + (self.max_scale - self.min_scale) * torch.rand(2)
                new_w = int(orig_w * r[0])
                new_h = int(orig_h * r[1])
                aspect_ratio = new_w / new_h
                if not (self.min_aspect_ratio <= aspect_ratio <= self.max_aspect_ratio):
                    continue

                # check for 0 area crops
                r = torch.rand(2)
                left = int((orig_w - new_w) * r[0])
                top = int((orig_h - new_h) * r[1])
                right = left + new_w
                bottom = top + new_h
                if left == right or top == bottom:
                    continue

                # check for any valid boxes with centers within the crop area
                xyxy_bboxes = F.convert_bounding_box_format(
                    bboxes.as_subclass(torch.Tensor),
                    bboxes.format,
                    tv_tensors.BoundingBoxFormat.XYXY,
                )
                cx = 0.5 * (xyxy_bboxes[..., 0] + xyxy_bboxes[..., 2])
                cy = 0.5 * (xyxy_bboxes[..., 1] + xyxy_bboxes[..., 3])
                is_within_crop_area = (left < cx) & (cx < right) & (top < cy) & (cy < bottom)
                if not is_within_crop_area.any():
                    continue

                # check at least 1 box with jaccard limitations
                xyxy_bboxes = xyxy_bboxes[is_within_crop_area]
                ious = box_iou(
                    xyxy_bboxes,
                    torch.tensor([[left, top, right, bottom]], dtype=xyxy_bboxes.dtype, device=xyxy_bboxes.device),
                )
                if ious.max() < min_jaccard_overlap:
                    continue

                return dict(top=top, left=left, height=new_h, width=new_w, is_within_crop_area=is_within_crop_area)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:

        if len(params) < 1:
            return inpt

        output = self._call_kernel(
            F.crop, inpt, top=params["top"], left=params["left"], height=params["height"], width=params["width"]
        )

        if isinstance(output, tv_tensors.BoundingBoxes):
            # We "mark" the invalid boxes as degenreate, and they can be
            # removed by a later call to SanitizeBoundingBoxes()
            output[~params["is_within_crop_area"]] = 0

        return output


class ScaleJitter(Transform):
    """Perform Large Scale Jitter on the input according to
    `"Simple Copy-Paste is a Strong Data Augmentation Method for Instance Segmentation" <https://arxiv.org/abs/2012.07177>`_.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        target_size (tuple of int): Target size. This parameter defines base scale for jittering,
            e.g. ``min(target_size[0] / width, target_size[1] / height)``.
        scale_range (tuple of float, optional): Minimum and maximum of the scale range. Default, ``(0.1, 2.0)``.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.NEAREST_EXACT``,
            ``InterpolationMode.BILINEAR`` and ``InterpolationMode.BICUBIC`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        antialias (bool, optional): Whether to apply antialiasing.
            It only affects **tensors** with bilinear or bicubic modes and it is
            ignored otherwise: on PIL images, antialiasing is always applied on
            bilinear or bicubic modes; on other modes (for PIL images and
            tensors), antialiasing makes no sense and this parameter is ignored.
            Possible values are:

            - ``True`` (default): will apply antialiasing for bilinear or bicubic modes.
              Other mode aren't affected. This is probably what you want to use.
            - ``False``: will not apply antialiasing for tensors on any mode. PIL
              images are still antialiased on bilinear or bicubic modes, because
              PIL doesn't support no antialias.
            - ``None``: equivalent to ``False`` for tensors and ``True`` for
              PIL images. This value exists for legacy reasons and you probably
              don't want to use it unless you really know what you are doing.

            The default value changed from ``None`` to ``True`` in
            v0.17, for the PIL and Tensor backends to be consistent.
    """

    def __init__(
        self,
        target_size: Tuple[int, int],
        scale_range: Tuple[float, float] = (0.1, 2.0),
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        antialias: Optional[bool] = True,
    ):
        super().__init__()
        self.target_size = target_size
        self.scale_range = scale_range
        self.interpolation = interpolation
        self.antialias = antialias

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        orig_height, orig_width = query_size(flat_inputs)

        scale = self.scale_range[0] + torch.rand(1) * (self.scale_range[1] - self.scale_range[0])
        r = min(self.target_size[1] / orig_height, self.target_size[0] / orig_width) * scale
        new_width = int(orig_width * r)
        new_height = int(orig_height * r)

        return dict(size=(new_height, new_width))

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(
            F.resize, inpt, size=params["size"], interpolation=self.interpolation, antialias=self.antialias
        )


class RandomShortestSize(Transform):
    """Randomly resize the input.

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        min_size (int or sequence of int): Minimum spatial size. Single integer value or a sequence of integer values.
        max_size (int, optional): Maximum spatial size. Default, None.
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.NEAREST_EXACT``,
            ``InterpolationMode.BILINEAR`` and ``InterpolationMode.BICUBIC`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        antialias (bool, optional): Whether to apply antialiasing.
            It only affects **tensors** with bilinear or bicubic modes and it is
            ignored otherwise: on PIL images, antialiasing is always applied on
            bilinear or bicubic modes; on other modes (for PIL images and
            tensors), antialiasing makes no sense and this parameter is ignored.
            Possible values are:

            - ``True`` (default): will apply antialiasing for bilinear or bicubic modes.
              Other mode aren't affected. This is probably what you want to use.
            - ``False``: will not apply antialiasing for tensors on any mode. PIL
              images are still antialiased on bilinear or bicubic modes, because
              PIL doesn't support no antialias.
            - ``None``: equivalent to ``False`` for tensors and ``True`` for
              PIL images. This value exists for legacy reasons and you probably
              don't want to use it unless you really know what you are doing.

            The default value changed from ``None`` to ``True`` in
            v0.17, for the PIL and Tensor backends to be consistent.
    """

    def __init__(
        self,
        min_size: Union[List[int], Tuple[int], int],
        max_size: Optional[int] = None,
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        antialias: Optional[bool] = True,
    ):
        super().__init__()
        self.min_size = [min_size] if isinstance(min_size, int) else list(min_size)
        self.max_size = max_size
        self.interpolation = interpolation
        self.antialias = antialias

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        orig_height, orig_width = query_size(flat_inputs)

        min_size = self.min_size[int(torch.randint(len(self.min_size), ()))]
        r = min_size / min(orig_height, orig_width)
        if self.max_size is not None:
            r = min(r, self.max_size / max(orig_height, orig_width))

        new_width = int(orig_width * r)
        new_height = int(orig_height * r)

        return dict(size=(new_height, new_width))

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(
            F.resize, inpt, size=params["size"], interpolation=self.interpolation, antialias=self.antialias
        )


class RandomResize(Transform):
    """Randomly resize the input.

    This transformation can be used together with ``RandomCrop`` as data augmentations to train
    models on image segmentation task.

    Output spatial size is randomly sampled from the interval ``[min_size, max_size]``:

    .. code-block:: python

        size = uniform_sample(min_size, max_size)
        output_width = size
        output_height = size

    If the input is a :class:`torch.Tensor` or a ``TVTensor`` (e.g. :class:`~torchvision.tv_tensors.Image`,
    :class:`~torchvision.tv_tensors.Video`, :class:`~torchvision.tv_tensors.BoundingBoxes` etc.)
    it can have arbitrary number of leading batch dimensions. For example,
    the image can have ``[..., C, H, W]`` shape. A bounding box can have ``[..., 4]`` shape.

    Args:
        min_size (int): Minimum output size for random sampling
        max_size (int): Maximum output size for random sampling
        interpolation (InterpolationMode, optional): Desired interpolation enum defined by
            :class:`torchvision.transforms.InterpolationMode`. Default is ``InterpolationMode.BILINEAR``.
            If input is Tensor, only ``InterpolationMode.NEAREST``, ``InterpolationMode.NEAREST_EXACT``,
            ``InterpolationMode.BILINEAR`` and ``InterpolationMode.BICUBIC`` are supported.
            The corresponding Pillow integer constants, e.g. ``PIL.Image.BILINEAR`` are accepted as well.
        antialias (bool, optional): Whether to apply antialiasing.
            It only affects **tensors** with bilinear or bicubic modes and it is
            ignored otherwise: on PIL images, antialiasing is always applied on
            bilinear or bicubic modes; on other modes (for PIL images and
            tensors), antialiasing makes no sense and this parameter is ignored.
            Possible values are:

            - ``True`` (default): will apply antialiasing for bilinear or bicubic modes.
              Other mode aren't affected. This is probably what you want to use.
            - ``False``: will not apply antialiasing for tensors on any mode. PIL
              images are still antialiased on bilinear or bicubic modes, because
              PIL doesn't support no antialias.
            - ``None``: equivalent to ``False`` for tensors and ``True`` for
              PIL images. This value exists for legacy reasons and you probably
              don't want to use it unless you really know what you are doing.

            The default value changed from ``None`` to ``True`` in
            v0.17, for the PIL and Tensor backends to be consistent.
    """

    def __init__(
        self,
        min_size: int,
        max_size: int,
        interpolation: Union[InterpolationMode, int] = InterpolationMode.BILINEAR,
        antialias: Optional[bool] = True,
    ) -> None:
        super().__init__()
        self.min_size = min_size
        self.max_size = max_size
        self.interpolation = interpolation
        self.antialias = antialias

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        size = int(torch.randint(self.min_size, self.max_size, ()))
        return dict(size=[size])

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(
            F.resize, inpt, params["size"], interpolation=self.interpolation, antialias=self.antialias
        )
