from io import BytesIO
from typing import List, Optional, Union

from fastapi import File
from pydantic import BaseModel, Field

from ray.serve.model_wrappers import DataBatchType
from ray.serve.utils import require_packages


_1DArray = List[float]
_2DArray = List[List[float]]
_3DArray = List[List[List[float]]]
_4DArray = List[List[List[List[float]]]]


class NdArray(BaseModel):
    """Schema for numeric array input."""

    array: Union[_1DArray, _2DArray, _3DArray, _4DArray] = Field(
        ...,
        description=(
            "The array content as a list or nested list. "
            "You can pass in 1D to 4D array in its original form or the flatten verion."
        ),
    )
    shape: Optional[List[int]] = Field(
        default=None,
        description=("The shape of the array. If present, the array will be reshaped."),
    )
    dtype: Optional[str] = Field(
        default=None,
        description=(
            "The dtype of the array. If present, the array will be cast by `astype`."
        ),
    )


@require_packages(["numpy"])
def array_to_databatch(payload: NdArray) -> DataBatchType:
    """Accept a NdArray from HTTP body, returns array."""
    import numpy as np

    arr = np.array(payload.array)
    if payload.shape:
        arr = arr.reshape(*payload.shape)
    if payload.dtype:
        arr = arr.astype(payload.dtype)
    return arr


@require_packages(["PIL", "numpy"])
def image_to_databatch(img: bytes = File(...)) -> DataBatchType:
    """Accept a PIL readable file from HTTP Form, returns array."""
    from PIL import Image
    import numpy as np

    image = Image.open(BytesIO(img))
    return np.array(image)
