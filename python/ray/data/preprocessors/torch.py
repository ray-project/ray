from typing import TYPE_CHECKING, Callable, Dict, List, Union

import numpy as np

from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch


@PublicAPI(stability="alpha")
class TorchVisionPreprocessor(Preprocessor):
    """Apply a `TorchVision transform <https://pytorch.org/vision/stable/transforms.html>`_
    to image columns.

    Examples:
        >>> import ray
        >>> dataset = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
        >>> dataset  # doctest: +ellipsis
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=(..., 3), dtype=float)})

        :class:`TorchVisionPreprocessor` passes ndarrays to your transform. To convert
        ndarrays to Torch tensors, add ``ToTensor`` to your pipeline.

        >>> from torchvision import transforms
        >>> from ray.data.preprocessors import TorchVisionPreprocessor
        >>> transform = transforms.Compose([
        ...     transforms.ToTensor(),
        ...     transforms.Resize((224, 224)),
        ... ])
        >>> preprocessor = TorchVisionPreprocessor(["image"], transform=transform)
        >>> preprocessor.transform(dataset)  # doctest: +ellipsis
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=(3, 224, 224), dtype=float)})

        For better performance, set ``batched`` to ``True`` and replace ``ToTensor``
        with a batch-supporting ``Lambda``.

        >>> def to_tensor(batch: np.ndarray) -> torch.Tensor:
        ...     tensor = torch.as_tensor(batch, dtype=torch.float)
        ...     # (B, H, W, C) -> (B, C, H, W)
        ...     tensor = tensor.permute(0, 3, 1, 2)
        ...     # [0., 255.] -> [0., 1.]
        ...     tensor = tensor.div(255)
        ...     return tensor
        >>> transform = transforms.Compose([
        ...     transforms.Lambda(to_tensor),
        ...     transforms.Resize((224, 224))
        ... ])
        >>> preprocessor = TorchVisionPreprocessor(
        ...     ["image"], transform=transform, batched=True
        ... )
        >>> preprocessor.transform(dataset)  # doctest: +ellipsis
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=(3, 224, 224), dtype=float)})

    Args:
        columns: The columns to apply the TorchVision transform to.
        transform: The TorchVision transform you want to apply. This transform should
            accept an ``np.ndarray`` as input and return a ``torch.Tensor`` as output.
        batched: If ``True``, apply ``transform`` to batches of shape
            :math:`(B, H, W, C)`. Otherwise, apply ``transform`` to individual images.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        transform: Callable[["np.ndarray"], "torch.Tensor"],
        batched: bool = False,
    ):
        self._columns = columns
        self._fn = transform
        self._batched = batched

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns}, "
            f"transform={self._fn!r})"
        )

    def _transform_numpy(
        self, np_data: Union["np.ndarray", Dict[str, "np.ndarray"]]
    ) -> Union["np.ndarray", Dict[str, "np.ndarray"]]:
        def transform(batch: np.ndarray) -> np.ndarray:
            if self._batched:
                return self._fn(batch).numpy()
            return np.array([self._fn(array).numpy() for array in batch])

        if isinstance(np_data, dict):
            outputs = {}
            for column, batch in np_data.items():
                if column in self._columns:
                    outputs[column] = transform(batch)
                else:
                    outputs[column] = batch
        else:
            outputs = transform(np_data)

        return outputs
