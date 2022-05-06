from io import BytesIO
from typing import Any, Dict, List, Optional, Union

from fastapi import File
from pydantic import BaseModel, Field
import numpy as np

from ray.serve.utils import require_packages
import starlette.requests


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


def json_to_ndarray(payload: NdArray) -> np.ndarray:
    """Accepts an NdArray JSON from an HTTP body and converts it to a numpy array."""
    arr = np.array(payload.array)
    if payload.shape:
        arr = arr.reshape(*payload.shape)
    if payload.dtype:
        arr = arr.astype(payload.dtype)
    return arr


def starlette_request(
    request: starlette.requests.Request,
) -> starlette.requests.Request:
    """Returns the raw request object."""
    # NOTE(simon): This adapter is used for ease of getting started.
    return request


async def json_request(request: starlette.requests.Request) -> Dict[str, Any]:
    """Return the JSON object from request body."""
    return await request.json()


@require_packages(["PIL"])
def image_to_ndarray(img: bytes = File(...)) -> np.ndarray:
    """Accepts a PIL-readable file from an HTTP form and converts
    it to a numpy array.
    """
    from PIL import Image

    image = Image.open(BytesIO(img))
    return np.array(image)
