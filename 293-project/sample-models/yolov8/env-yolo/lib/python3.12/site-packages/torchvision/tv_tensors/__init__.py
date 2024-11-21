import torch

from ._bounding_boxes import BoundingBoxes, BoundingBoxFormat
from ._image import Image
from ._mask import Mask
from ._torch_function_helpers import set_return_type
from ._tv_tensor import TVTensor
from ._video import Video


# TODO: Fix this. We skip this method as it leads to
# RecursionError: maximum recursion depth exceeded while calling a Python object
# Until `disable` is removed, there will be graph breaks after all calls to functional transforms
@torch.compiler.disable
def wrap(wrappee, *, like, **kwargs):
    """Convert a :class:`torch.Tensor` (``wrappee``) into the same :class:`~torchvision.tv_tensors.TVTensor` subclass as ``like``.

    If ``like`` is a :class:`~torchvision.tv_tensors.BoundingBoxes`, the ``format`` and ``canvas_size`` of
    ``like`` are assigned to ``wrappee``, unless they are passed as ``kwargs``.

    Args:
        wrappee (Tensor): The tensor to convert.
        like (:class:`~torchvision.tv_tensors.TVTensor`): The reference.
            ``wrappee`` will be converted into the same subclass as ``like``.
        kwargs: Can contain "format" and "canvas_size" if ``like`` is a :class:`~torchvision.tv_tensor.BoundingBoxes`.
            Ignored otherwise.
    """
    if isinstance(like, BoundingBoxes):
        return BoundingBoxes._wrap(
            wrappee,
            format=kwargs.get("format", like.format),
            canvas_size=kwargs.get("canvas_size", like.canvas_size),
        )
    else:
        return wrappee.as_subclass(type(like))
