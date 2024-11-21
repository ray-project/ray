import warnings
from typing import Any, Callable, Dict, List, Optional, Sequence, Type, Union

import PIL.Image

import torch
from torch.utils._pytree import tree_flatten, tree_unflatten

from torchvision import transforms as _transforms, tv_tensors
from torchvision.transforms.v2 import functional as F, Transform

from ._utils import _parse_labels_getter, _setup_number_or_seq, _setup_size, get_bounding_boxes, has_any, is_pure_tensor


# TODO: do we want/need to expose this?
class Identity(Transform):
    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return inpt


class Lambda(Transform):
    """Apply a user-defined function as a transform.

    This transform does not support torchscript.

    Args:
        lambd (function): Lambda/function to be used for transform.
    """

    _transformed_types = (object,)

    def __init__(self, lambd: Callable[[Any], Any], *types: Type):
        super().__init__()
        self.lambd = lambd
        self.types = types or self._transformed_types

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        if isinstance(inpt, self.types):
            return self.lambd(inpt)
        else:
            return inpt

    def extra_repr(self) -> str:
        extras = []
        name = getattr(self.lambd, "__name__", None)
        if name:
            extras.append(name)
        extras.append(f"types={[type.__name__ for type in self.types]}")
        return ", ".join(extras)


class LinearTransformation(Transform):
    """Transform a tensor image or video with a square transformation matrix and a mean_vector computed offline.

    This transform does not support PIL Image.
    Given transformation_matrix and mean_vector, will flatten the torch.*Tensor and
    subtract mean_vector from it which is then followed by computing the dot
    product with the transformation matrix and then reshaping the tensor to its
    original shape.

    Applications:
        whitening transformation: Suppose X is a column vector zero-centered data.
        Then compute the data covariance matrix [D x D] with torch.mm(X.t(), X),
        perform SVD on this matrix and pass it as transformation_matrix.

    Args:
        transformation_matrix (Tensor): tensor [D x D], D = C x H x W
        mean_vector (Tensor): tensor [D], D = C x H x W
    """

    _v1_transform_cls = _transforms.LinearTransformation

    _transformed_types = (is_pure_tensor, tv_tensors.Image, tv_tensors.Video)

    def __init__(self, transformation_matrix: torch.Tensor, mean_vector: torch.Tensor):
        super().__init__()
        if transformation_matrix.size(0) != transformation_matrix.size(1):
            raise ValueError(
                "transformation_matrix should be square. Got "
                f"{tuple(transformation_matrix.size())} rectangular matrix."
            )

        if mean_vector.size(0) != transformation_matrix.size(0):
            raise ValueError(
                f"mean_vector should have the same length {mean_vector.size(0)}"
                f" as any one of the dimensions of the transformation_matrix [{tuple(transformation_matrix.size())}]"
            )

        if transformation_matrix.device != mean_vector.device:
            raise ValueError(
                f"Input tensors should be on the same device. Got {transformation_matrix.device} and {mean_vector.device}"
            )

        if transformation_matrix.dtype != mean_vector.dtype:
            raise ValueError(
                f"Input tensors should have the same dtype. Got {transformation_matrix.dtype} and {mean_vector.dtype}"
            )

        self.transformation_matrix = transformation_matrix
        self.mean_vector = mean_vector

    def _check_inputs(self, sample: Any) -> Any:
        if has_any(sample, PIL.Image.Image):
            raise TypeError(f"{type(self).__name__}() does not support PIL images.")

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        shape = inpt.shape
        n = shape[-3] * shape[-2] * shape[-1]
        if n != self.transformation_matrix.shape[0]:
            raise ValueError(
                "Input tensor and transformation matrix have incompatible shape."
                + f"[{shape[-3]} x {shape[-2]} x {shape[-1]}] != "
                + f"{self.transformation_matrix.shape[0]}"
            )

        if inpt.device.type != self.mean_vector.device.type:
            raise ValueError(
                "Input tensor should be on the same device as transformation matrix and mean vector. "
                f"Got {inpt.device} vs {self.mean_vector.device}"
            )

        flat_inpt = inpt.reshape(-1, n) - self.mean_vector

        transformation_matrix = self.transformation_matrix.to(flat_inpt.dtype)
        output = torch.mm(flat_inpt, transformation_matrix)
        output = output.reshape(shape)

        if isinstance(inpt, (tv_tensors.Image, tv_tensors.Video)):
            output = tv_tensors.wrap(output, like=inpt)
        return output


class Normalize(Transform):
    """Normalize a tensor image or video with mean and standard deviation.

    This transform does not support PIL Image.
    Given mean: ``(mean[1],...,mean[n])`` and std: ``(std[1],..,std[n])`` for ``n``
    channels, this transform will normalize each channel of the input
    ``torch.*Tensor`` i.e.,
    ``output[channel] = (input[channel] - mean[channel]) / std[channel]``

    .. note::
        This transform acts out of place, i.e., it does not mutate the input tensor.

    Args:
        mean (sequence): Sequence of means for each channel.
        std (sequence): Sequence of standard deviations for each channel.
        inplace(bool,optional): Bool to make this operation in-place.

    """

    _v1_transform_cls = _transforms.Normalize

    def __init__(self, mean: Sequence[float], std: Sequence[float], inplace: bool = False):
        super().__init__()
        self.mean = list(mean)
        self.std = list(std)
        self.inplace = inplace

    def _check_inputs(self, sample: Any) -> Any:
        if has_any(sample, PIL.Image.Image):
            raise TypeError(f"{type(self).__name__}() does not support PIL images.")

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.normalize, inpt, mean=self.mean, std=self.std, inplace=self.inplace)


class GaussianBlur(Transform):
    """Blurs image with randomly chosen Gaussian blur kernel.

    The convolution will be using reflection padding corresponding to the kernel size, to maintain the input shape.

    If the input is a Tensor, it is expected
    to have [..., C, H, W] shape, where ... means an arbitrary number of leading dimensions.

    Args:
        kernel_size (int or sequence): Size of the Gaussian kernel.
        sigma (float or tuple of float (min, max)): Standard deviation to be used for
            creating kernel to perform blurring. If float, sigma is fixed. If it is tuple
            of float (min, max), sigma is chosen uniformly at random to lie in the
            given range.
    """

    _v1_transform_cls = _transforms.GaussianBlur

    def __init__(
        self, kernel_size: Union[int, Sequence[int]], sigma: Union[int, float, Sequence[float]] = (0.1, 2.0)
    ) -> None:
        super().__init__()
        self.kernel_size = _setup_size(kernel_size, "Kernel size should be a tuple/list of two integers")
        for ks in self.kernel_size:
            if ks <= 0 or ks % 2 == 0:
                raise ValueError("Kernel size value should be an odd and positive number.")

        self.sigma = _setup_number_or_seq(sigma, "sigma")

        if not 0.0 < self.sigma[0] <= self.sigma[1]:
            raise ValueError(f"sigma values should be positive and of the form (min, max). Got {self.sigma}")

    def _get_params(self, flat_inputs: List[Any]) -> Dict[str, Any]:
        sigma = torch.empty(1).uniform_(self.sigma[0], self.sigma[1]).item()
        return dict(sigma=[sigma, sigma])

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.gaussian_blur, inpt, self.kernel_size, **params)


class GaussianNoise(Transform):
    """Add gaussian noise to images or videos.

    The input tensor is expected to be in [..., 1 or 3, H, W] format,
    where ... means it can have an arbitrary number of leading dimensions.
    Each image or frame in a batch will be transformed independently i.e. the
    noise added to each image will be different.

    The input tensor is also expected to be of float dtype in ``[0, 1]``.
    This transform does not support PIL images.

    Args:
        mean (float): Mean of the sampled normal distribution. Default is 0.
        sigma (float): Standard deviation of the sampled normal distribution. Default is 0.1.
        clip (bool, optional): Whether to clip the values in ``[0, 1]`` after adding noise. Default is True.
    """

    def __init__(self, mean: float = 0.0, sigma: float = 0.1, clip=True) -> None:
        super().__init__()
        self.mean = mean
        self.sigma = sigma
        self.clip = clip

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.gaussian_noise, inpt, mean=self.mean, sigma=self.sigma, clip=self.clip)


class ToDtype(Transform):
    """Converts the input to a specific dtype, optionally scaling the values for images or videos.

    .. note::
        ``ToDtype(dtype, scale=True)`` is the recommended replacement for ``ConvertImageDtype(dtype)``.

    Args:
        dtype (``torch.dtype`` or dict of ``TVTensor`` -> ``torch.dtype``): The dtype to convert to.
            If a ``torch.dtype`` is passed, e.g. ``torch.float32``, only images and videos will be converted
            to that dtype: this is for compatibility with :class:`~torchvision.transforms.v2.ConvertImageDtype`.
            A dict can be passed to specify per-tv_tensor conversions, e.g.
            ``dtype={tv_tensors.Image: torch.float32, tv_tensors.Mask: torch.int64, "others":None}``. The "others"
            key can be used as a catch-all for any other tv_tensor type, and ``None`` means no conversion.
        scale (bool, optional): Whether to scale the values for images or videos. See :ref:`range_and_dtype`.
            Default: ``False``.
    """

    _transformed_types = (torch.Tensor,)

    def __init__(
        self, dtype: Union[torch.dtype, Dict[Union[Type, str], Optional[torch.dtype]]], scale: bool = False
    ) -> None:
        super().__init__()

        if not isinstance(dtype, (dict, torch.dtype)):
            raise ValueError(f"dtype must be a dict or a torch.dtype, got {type(dtype)} instead")

        if (
            isinstance(dtype, dict)
            and torch.Tensor in dtype
            and any(cls in dtype for cls in [tv_tensors.Image, tv_tensors.Video])
        ):
            warnings.warn(
                "Got `dtype` values for `torch.Tensor` and either `tv_tensors.Image` or `tv_tensors.Video`. "
                "Note that a plain `torch.Tensor` will *not* be transformed by this (or any other transformation) "
                "in case a `tv_tensors.Image` or `tv_tensors.Video` is present in the input."
            )
        self.dtype = dtype
        self.scale = scale

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        if isinstance(self.dtype, torch.dtype):
            # For consistency / BC with ConvertImageDtype, we only care about images or videos when dtype
            # is a simple torch.dtype
            if not is_pure_tensor(inpt) and not isinstance(inpt, (tv_tensors.Image, tv_tensors.Video)):
                return inpt

            dtype: Optional[torch.dtype] = self.dtype
        elif type(inpt) in self.dtype:
            dtype = self.dtype[type(inpt)]
        elif "others" in self.dtype:
            dtype = self.dtype["others"]
        else:
            raise ValueError(
                f"No dtype was specified for type {type(inpt)}. "
                "If you only need to convert the dtype of images or videos, you can just pass e.g. dtype=torch.float32. "
                "If you're passing a dict as dtype, "
                'you can use "others" as a catch-all key '
                'e.g. dtype={tv_tensors.Mask: torch.int64, "others": None} to pass-through the rest of the inputs.'
            )

        supports_scaling = is_pure_tensor(inpt) or isinstance(inpt, (tv_tensors.Image, tv_tensors.Video))
        if dtype is None:
            if self.scale and supports_scaling:
                warnings.warn(
                    "scale was set to True but no dtype was specified for images or videos: no scaling will be done."
                )
            return inpt

        return self._call_kernel(F.to_dtype, inpt, dtype=dtype, scale=self.scale)


class ConvertImageDtype(Transform):
    """[DEPRECATED] Use ``v2.ToDtype(dtype, scale=True)`` instead.

    Convert input image to the given ``dtype`` and scale the values accordingly.

    .. warning::
        Consider using ``ToDtype(dtype, scale=True)`` instead. See :class:`~torchvision.transforms.v2.ToDtype`.

    This function does not support PIL Image.

    Args:
        dtype (torch.dtype): Desired data type of the output

    .. note::

        When converting from a smaller to a larger integer ``dtype`` the maximum values are **not** mapped exactly.
        If converted back and forth, this mismatch has no effect.

    Raises:
        RuntimeError: When trying to cast :class:`torch.float32` to :class:`torch.int32` or :class:`torch.int64` as
            well as for trying to cast :class:`torch.float64` to :class:`torch.int64`. These conversions might lead to
            overflow errors since the floating point ``dtype`` cannot store consecutive integers over the whole range
            of the integer ``dtype``.
    """

    _v1_transform_cls = _transforms.ConvertImageDtype

    def __init__(self, dtype: torch.dtype = torch.float32) -> None:
        super().__init__()
        self.dtype = dtype

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        return self._call_kernel(F.to_dtype, inpt, dtype=self.dtype, scale=True)


class SanitizeBoundingBoxes(Transform):
    """Remove degenerate/invalid bounding boxes and their corresponding labels and masks.

    This transform removes bounding boxes and their associated labels/masks that:

    - are below a given ``min_size`` or ``min_area``: by default this also removes degenerate boxes that have e.g. X2 <= X1.
    - have any coordinate outside of their corresponding image. You may want to
      call :class:`~torchvision.transforms.v2.ClampBoundingBoxes` first to avoid undesired removals.

    It can also sanitize other tensors like the "iscrowd" or "area" properties from COCO
    (see ``labels_getter`` parameter).

    It is recommended to call it at the end of a pipeline, before passing the
    input to the models. It is critical to call this transform if
    :class:`~torchvision.transforms.v2.RandomIoUCrop` was called.
    If you want to be extra careful, you may call it after all transforms that
    may modify bounding boxes but once at the end should be enough in most
    cases.

    Args:
        min_size (float, optional): The size below which bounding boxes are removed. Default is 1.
        min_area (float, optional): The area below which bounding boxes are removed. Default is 1.
        labels_getter (callable or str or None, optional): indicates how to identify the labels in the input
            (or anything else that needs to be sanitized along with the bounding boxes).
            By default, this will try to find a "labels" key in the input (case-insensitive), if
            the input is a dict or it is a tuple whose second element is a dict.
            This heuristic should work well with a lot of datasets, including the built-in torchvision datasets.

            It can also be a callable that takes the same input as the transform, and returns either:

            - A single tensor (the labels)
            - A tuple/list of tensors, each of which will be subject to the same sanitization as the bounding boxes.
              This is useful to sanitize multiple tensors like the labels, and the "iscrowd" or "area" properties
              from COCO.

            If ``labels_getter`` is None then only bounding boxes are sanitized.
    """

    def __init__(
        self,
        min_size: float = 1.0,
        min_area: float = 1.0,
        labels_getter: Union[Callable[[Any], Any], str, None] = "default",
    ) -> None:
        super().__init__()

        if min_size < 1:
            raise ValueError(f"min_size must be >= 1, got {min_size}.")
        self.min_size = min_size

        if min_area < 1:
            raise ValueError(f"min_area must be >= 1, got {min_area}.")
        self.min_area = min_area

        self.labels_getter = labels_getter
        self._labels_getter = _parse_labels_getter(labels_getter)

    def forward(self, *inputs: Any) -> Any:
        inputs = inputs if len(inputs) > 1 else inputs[0]

        labels = self._labels_getter(inputs)
        if labels is not None:
            msg = "The labels in the input to forward() must be a tensor or None, got {type} instead."
            if isinstance(labels, torch.Tensor):
                labels = (labels,)
            elif isinstance(labels, (tuple, list)):
                for entry in labels:
                    if not isinstance(entry, torch.Tensor):
                        # TODO: we don't need to enforce tensors, just that entries are indexable as t[bool_mask]
                        raise ValueError(msg.format(type=type(entry)))
            else:
                raise ValueError(msg.format(type=type(labels)))

        flat_inputs, spec = tree_flatten(inputs)
        boxes = get_bounding_boxes(flat_inputs)

        if labels is not None:
            for label in labels:
                if boxes.shape[0] != label.shape[0]:
                    raise ValueError(
                        f"Number of boxes (shape={boxes.shape}) and must match the number of labels."
                        f"Found labels with shape={label.shape})."
                    )

        valid = F._misc._get_sanitize_bounding_boxes_mask(
            boxes,
            format=boxes.format,
            canvas_size=boxes.canvas_size,
            min_size=self.min_size,
            min_area=self.min_area,
        )

        params = dict(valid=valid, labels=labels)
        flat_outputs = [self._transform(inpt, params) for inpt in flat_inputs]

        return tree_unflatten(flat_outputs, spec)

    def _transform(self, inpt: Any, params: Dict[str, Any]) -> Any:
        is_label = params["labels"] is not None and any(inpt is label for label in params["labels"])
        is_bounding_boxes_or_mask = isinstance(inpt, (tv_tensors.BoundingBoxes, tv_tensors.Mask))

        if not (is_label or is_bounding_boxes_or_mask):
            return inpt

        output = inpt[params["valid"]]

        if is_label:
            return output
        else:
            return tv_tensors.wrap(output, like=inpt)
