from typing import TYPE_CHECKING, Callable, Dict, List, Union

import numpy as np

from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch


@PublicAPI(stability="alpha")
class TorchVisionPreprocessor(Preprocessor):
    """Apply a TorchVision transform to an image column.

    Examples:
        >>> import ray
        >>> dataset = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
        >>> dataset  # doctest: +ellipsis
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=(..., 3), dtype=float)})

        >>> from torchvision import transforms
        >>> from ray.data.preprocessors import TorchVisionPreprocessor
        >>> transform = transforms.Compose([
        ...     transforms.ToTensor(),
        ...     transforms.Resize((224, 224)),
        ... ])
        >>> preprocessor = TorchVisionPreprocessor(["image"], transform=transform)
        >>> preprocessor.transform(dataset)  # doctest: +ellipsis
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=(3, 224, 224), dtype=float)})

    Args:
        columns: The columns to apply the TorchVision transform to.
        transform: The TorchVision transform you want to apply. This transform should
            accept an ``np.ndarray`` as input and return a ``torch.Tensor`` as output.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        transform: Callable[["np.ndarray"], "torch.Tensor"],
    ):
        self._columns = columns
        self._fn = transform

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns}, "
            f"transform={self._fn!r})"
        )

    def _transform_numpy(
        self, np_data: Union["np.ndarray", Dict[str, "np.ndarray"]]
    ) -> Union["np.ndarray", Dict[str, "np.ndarray"]]:
        def transform(batch: np.ndarray) -> np.ndarray:
            return np.array([self._fn(array).numpy() for array in batch])

        if isinstance(np_data, dict):
            outputs = {
                column: transform(batch)
                for column, batch in np_data.items()
                if column in self._columns
            }
        else:
            outputs = transform(np_data)

        return outputs
