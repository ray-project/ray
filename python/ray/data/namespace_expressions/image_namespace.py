"""
Image namespace for expression operations on image columns.

This namespace lets you perform simple image processing in Ray Data
expressions.  Each method converts input bytes or ``PIL.Image`` objects
into images, applies a transformation and returns PNG‑encoded bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pyarrow
from PIL import Image, ImageFilter

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr

# Helper functions to convert between PIL images and bytes


def _to_pil(obj: Any) -> Image.Image:
    if obj is None:
        return None
    if isinstance(obj, Image.Image):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        return Image.open(BytesIO(obj)).convert("RGB")
    raise ValueError(f"Unsupported image type: {type(obj)!r}")


def _pil_to_bytes(img: Image.Image) -> bytes:
    if img is None:
        return None
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@dataclass
class _ImageNamespace:
    """Namespace for image operations on expression columns."""

    _expr: "Expr"

    def resize(self, width: int, height: int) -> "UDFExpr":
        return_dtype = DataType.binary()

        @pyarrow_udf(return_dtype=return_dtype)
        def _resize(arr: pyarrow.Array) -> pyarrow.Array:
            out_list = []
            for item in arr:
                if item is None:
                    out_list.append(None)
                    continue
                img = _to_pil(item.as_py() if hasattr(item, "as_py") else item)
                if img is None:
                    out_list.append(None)
                    continue
                resized = img.resize((width, height))
                out_list.append(_pil_to_bytes(resized))
            return pyarrow.array(out_list)

        return _resize(self._expr)

    def gaussian_blur(self, radius: float) -> "UDFExpr":
        return_dtype = DataType.binary()

        @pyarrow_udf(return_dtype=return_dtype)
        def _blur(arr: pyarrow.Array) -> pyarrow.Array:
            out_list = []
            for item in arr:
                if item is None:
                    out_list.append(None)
                    continue
                img = _to_pil(item.as_py() if hasattr(item, "as_py") else item)
                if img is None:
                    out_list.append(None)
                    continue
                blurred = img.filter(ImageFilter.GaussianBlur(radius))
                out_list.append(_pil_to_bytes(blurred))
            return pyarrow.array(out_list)

        return _blur(self._expr)
