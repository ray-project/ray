from io import BytesIO
from typing import List, Optional, Union

from fastapi import File
from pydantic import BaseModel, Field
import numpy as np

from ray.serve.utils import require_packages
from ray.ml.predictor import DataBatchType


_1DArray = List[float]
_2DArray = List[List[float]]
_3DArray = List[List[List[float]]]
_4DArray = List[List[List[List[float]]]]


class NdArray(BaseModel):
    """Schema for numeric array input."""

    array: Union[_1DArray, _2DArray, _3DArray, _4DArray] = Field(
        ...,
        description=(
            "The array content as a nested list. "
            "You can pass in 1D to 4D array as nested list, or flatten them. "
            "When you flatten the array, you can use the `shape` parameter to perform "
            "reshaping."
        ),
    )
    shape: Optional[List[int]] = Field(
        default=None,
        description=("The shape of the array. If present, the array will be reshaped."),
    )
    dtype: Optional[str] = Field(
        default=None,
        description=(
            "The numpy dtype of the array. If present, the array will be cast "
            "by `astype`."
        ),
    )


def array_to_databatch(payload: NdArray) -> DataBatchType:
    """Accepts an NdArray from an HTTP body and converts it to a DataBatchType."""
    arr = np.array(payload.array)
    if payload.shape:
        arr = arr.reshape(*payload.shape)
    if payload.dtype:
        arr = arr.astype(payload.dtype)
    return arr


@require_packages(["PIL"])
def image_to_databatch(img: bytes = File(...)) -> DataBatchType:
    """Accepts a PIL-readable file from an HTTP form and converts
    it to a DataBatchType.
    """
    from PIL import Image

    image = Image.open(BytesIO(img))
    return np.array(image)
