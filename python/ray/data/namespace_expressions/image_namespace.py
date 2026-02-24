from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


@dataclass
class _ImageNamespace:
    """Namespace for image operations on expression columns."""

    _expr: "Expr"

    def compose(self, transforms: List[Callable]) -> "UDFExpr":
        """Apply a pipeline of torchvision transforms to images.

        Transforms are applied individually to each image.

        Args:
            transforms: List of torchvision transform objects. Must be non-empty.

        Returns:
            UDFExpr that applies the transform pipeline, returning numpy arrays

        Note:
            The transform pipeline must return torch.Tensor or np.ndarray.

        Example:
            >>> from torchvision.transforms import v2
            >>> augmentation = [
            ...     v2.ToTensor(),
            ...     v2.RandomResizedCrop(224),
            ...     v2.RandomHorizontalFlip(p=0.5),
            ... ]
            >>> ds.with_column("augmented", col("image").image.compose(augmentation))  # doctest: +SKIP
        """
        if not transforms:
            raise ValueError("transforms list must not be empty")

        from torchvision.transforms import v2

        composed = v2.Compose(transforms)

        @pyarrow_udf(return_dtype=DataType(object))
        def _apply_transforms(images):
            import numpy as np
            import pyarrow as pa
            import torch

            from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
            from ray.data._internal.tensor_extensions.utils import (
                _create_possibly_ragged_ndarray,
            )

            # Convert PyArrow ChunkedArray/Array to numpy
            if isinstance(images, pa.ChunkedArray):
                images = images.combine_chunks()

            if isinstance(images, (pa.Array, ArrowTensorArray)):
                np_images = images.to_numpy(zero_copy_only=False)
            else:
                np_images = images

            # Apply transforms to each image individually.
            # Pass numpy arrays directly — v2 transforms handle numpy natively.
            def apply_single(image):
                output = composed(image)
                if isinstance(output, torch.Tensor):
                    output = output.numpy()
                if not isinstance(output, np.ndarray):
                    raise ValueError(
                        "Expected your transform to return a `torch.Tensor` or "
                        "`np.ndarray`, but your transform returned a "
                        f"`{type(output).__name__}` instead."
                    )
                return output

            results = _create_possibly_ragged_ndarray(
                [apply_single(img) for img in np_images]
            )

            return ArrowTensorArray.from_numpy(results)

        return _apply_transforms(self._expr)
