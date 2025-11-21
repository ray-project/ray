from typing import TYPE_CHECKING, Callable, Dict, List, Mapping, Optional, Union

import numpy as np

from ray.air.util.data_batch_conversion import BatchFormat
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch


@PublicAPI(stability="alpha")
class TorchVisionPreprocessor(Preprocessor):
    """Apply a `TorchVision transform <https://pytorch.org/vision/stable/transforms.html>`_
    to image columns.

    Examples:

        Torch models expect inputs of shape :math:`(B, C, H, W)` in the range
        :math:`[0.0, 1.0]`. To convert images to this format, add ``ToTensor`` to your
        preprocessing pipeline.

        .. testcode::

            from torchvision import transforms

            import ray
            from ray.data.preprocessors import TorchVisionPreprocessor

            transform = transforms.Compose([
                transforms.ToTensor(),
                transforms.Resize((224, 224)),
            ])
            preprocessor = TorchVisionPreprocessor(["image"], transform=transform)

            dataset = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
            dataset = preprocessor.transform(dataset)


        For better performance, set ``batched`` to ``True`` and replace ``ToTensor``
        with a batch-supporting ``Lambda``.

        .. testcode::

            import numpy as np
            import torch

            def to_tensor(batch: np.ndarray) -> torch.Tensor:
                tensor = torch.as_tensor(batch, dtype=torch.float)
                # (B, H, W, C) -> (B, C, H, W)
                tensor = tensor.permute(0, 3, 1, 2).contiguous()
                # [0., 255.] -> [0., 1.]
                tensor = tensor.div(255)
                return tensor

            transform = transforms.Compose([
                transforms.Lambda(to_tensor),
                transforms.Resize((224, 224))
            ])
            preprocessor = TorchVisionPreprocessor(["image"], transform=transform, batched=True)

            dataset = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
            dataset = preprocessor.transform(dataset)

    Args:
        columns: The columns to apply the TorchVision transform to.
        transform: The TorchVision transform you want to apply. This transform should
            accept a ``np.ndarray`` or ``torch.Tensor`` as input and return a
            ``torch.Tensor`` as output.
        output_columns: The output name for each input column. If not specified, this
            defaults to the same set of columns as the columns.
        batched: If ``True``, apply ``transform`` to batches of shape
            :math:`(B, H, W, C)`. Otherwise, apply ``transform`` to individual images.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        transform: Callable[[Union["np.ndarray", "torch.Tensor"]], "torch.Tensor"],
        output_columns: Optional[List[str]] = None,
        batched: bool = False,
    ):
        super().__init__()
        if not output_columns:
            output_columns = columns
        if len(columns) != len(output_columns):
            raise ValueError(
                "The length of columns should match the "
                f"length of output_columns: {columns} vs {output_columns}."
            )
        self._columns = columns
        self._output_columns = output_columns
        self._torchvision_transform = transform
        self._batched = batched

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"columns={self._columns}, "
            f"output_columns={self._output_columns}, "
            f"transform={self._torchvision_transform!r})"
        )

    def _transform_numpy(
        self, data_batch: Dict[str, "np.ndarray"]
    ) -> Dict[str, "np.ndarray"]:
        import torch

        from ray.air._internal.torch_utils import convert_ndarray_to_torch_tensor

        def apply_torchvision_transform(array: np.ndarray) -> np.ndarray:
            try:
                tensor = convert_ndarray_to_torch_tensor(array)
                output = self._torchvision_transform(tensor)
            except TypeError:
                # Transforms like `ToTensor` expect a `np.ndarray` as input.
                output = self._torchvision_transform(array)
            if isinstance(output, torch.Tensor):
                output = output.numpy()
            if not isinstance(output, np.ndarray):
                raise ValueError(
                    "`TorchVisionPreprocessor` expected your transform to return a "
                    "`torch.Tensor` or `np.ndarray`, but your transform returned a "
                    f"`{type(output).__name__}` instead."
                )
            return output

        def transform_batch(batch: np.ndarray) -> np.ndarray:
            if self._batched:
                return apply_torchvision_transform(batch)
            return _create_possibly_ragged_ndarray(
                [apply_torchvision_transform(array) for array in batch]
            )

        if isinstance(data_batch, Mapping):
            for input_col, output_col in zip(self._columns, self._output_columns):
                data_batch[output_col] = transform_batch(data_batch[input_col])
        else:
            # TODO(ekl) deprecate this code path. Unfortunately, predictors are still
            # sending schemaless arrays to preprocessors.
            data_batch = transform_batch(data_batch)

        return data_batch

    def get_input_columns(self) -> List[str]:
        return self._columns

    def get_output_columns(self) -> List[str]:
        return self._output_columns

    def preferred_batch_format(cls) -> BatchFormat:
        return BatchFormat.NUMPY
