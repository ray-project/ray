from __future__ import annotations

import logging
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable

import pyarrow
from PIL import Image, ImageFilter

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


logger = logging.getLogger(__name__)


# --- Optimized Helper Functions ---


def _to_pil(obj: Any) -> Image.Image | None:
    """Converts bytes, bytearray, or existing Image objects to PIL Image."""
    if obj is None:
        return None
    if isinstance(obj, Image.Image):
        return obj
    # Convert bytes to PIL
    return Image.open(BytesIO(obj)).convert("RGBA")


def _pil_to_bytes(img: Image.Image | None, format: str = "PNG") -> bytes | None:
    """Converts a PIL Image back to bytes."""
    if img is None:
        return None
    buf = BytesIO()
    img.save(buf, format=format)
    return buf.getvalue()


def _apply_image_op(
    arr: pyarrow.Array, op: Callable[[Image.Image], Image.Image]
) -> pyarrow.Array:
    """
    Generic handler handles decoding, None-checks, operation application, and encoding.
    """
    # Optimization: to_pylist() avoids high overhead of iterating PyArrow scalars
    raw_values = arr.to_pylist()
    out_list = []

    for raw_val in raw_values:
        if raw_val is None:
            out_list.append(None)
            continue

        try:
            img = _to_pil(raw_val)
            processed_img = op(img)
            out_list.append(_pil_to_bytes(processed_img))
        except Exception as e:
            logger.exception(f"Failed to process image, Error: {e}")
            out_list.append(None)

    return pyarrow.array(out_list)


# --- Namespace with Examples ---


@dataclass
class _ImageNamespace:
    """Namespace for expression operations on image columns.

    This namespace lets you perform simple image processing in Ray Data
    expressions. Each method converts input bytes or ``PIL.Image`` objects
    into images, applies a transformation, and returns PNG-encoded bytes.

    Example:
        >>> from ray.data.expressions import col
        >>> # Resize images to 128x128
        >>> expr = col("image_bytes").image.resize(128, 128)
        >>> # Apply Gaussian Blur with a radius of 2.0
        >>> expr = col("image_bytes").image.gaussian_blur(2.0)
    """

    _expr: "Expr"

    def resize(self, width: int, height: int) -> "UDFExpr":
        return_dtype = DataType.binary()

        @pyarrow_udf(return_dtype=return_dtype)
        def _resize(arr: pyarrow.Array) -> pyarrow.Array:
            return _apply_image_op(arr, lambda img: img.resize((width, height)))

        return _resize(self._expr)

    def gaussian_blur(self, radius: float) -> "UDFExpr":
        return_dtype = DataType.binary()

        @pyarrow_udf(return_dtype=return_dtype)
        def _blur(arr: pyarrow.Array) -> pyarrow.Array:
            return _apply_image_op(
                arr, lambda img: img.filter(ImageFilter.GaussianBlur(radius))
            )

        return _blur(self._expr)
