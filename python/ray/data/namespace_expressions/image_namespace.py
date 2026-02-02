from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List

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

        Reuses the transform logic from TorchVisionPreprocessor with batched=False,
        meaning transforms are applied individually to each image.

        Args:
            transforms: List of torchvision transform objects. Must be non-empty.

        Returns:
            UDFExpr that applies the transform pipeline, returning numpy arrays

        Note:
            The transform pipeline must return torch.Tensor or np.ndarray.
            For better performance with batch-supporting transforms, consider
            using TorchVisionPreprocessor directly with batched=True.

        Example:
            >>> from torchvision import transforms
            >>> augmentation = [
            ...     transforms.ToTensor(),
            ...     transforms.RandomResizedCrop(224),
            ...     transforms.RandomHorizontalFlip(p=0.5),
            ... ]
            >>> ds.with_column("augmented", col("image").image.compose(augmentation))
        """
        if not transforms:
            raise ValueError("transforms list must not be empty")

        try:
            from torchvision import transforms as T
        except ImportError:
            raise ImportError(
                "torchvision is required for image.compose(). "
                "Install it with: pip install torchvision"
            )

        from ray.data.preprocessors import TorchVisionPreprocessor

        # Create composed transform
        composed = T.Compose(transforms)

        # Reuse TorchVisionPreprocessor's transform logic
        preprocessor = TorchVisionPreprocessor(
            columns=["__image__"],
            transform=composed,
            batched=False,
        )

        @pyarrow_udf(return_dtype=DataType(object))
        def _apply_transforms(images):
            import pyarrow as pa

            from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray

            # Convert PyArrow ChunkedArray/Array to numpy for TorchVisionPreprocessor
            if isinstance(images, pa.ChunkedArray):
                # Combine all chunks to avoid data loss with multi-chunk arrays
                images = images.combine_chunks()

            if isinstance(images, (pa.Array, ArrowTensorArray)):
                np_images = images.to_numpy(zero_copy_only=False)
            else:
                np_images = images

            # Use preprocessor's internal transform method
            batch = {"__image__": np_images}
            result = preprocessor._transform_numpy(batch)

            # Convert result back to ArrowTensorArray for proper storage
            return ArrowTensorArray.from_numpy(result["__image__"])

        return _apply_transforms(self._expr)
