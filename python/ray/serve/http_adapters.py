from io import BytesIO
from typing import Any, Dict, List, Optional, Union

from fastapi import File, Request
from pydantic import BaseModel, Field
import numpy as np
import starlette.requests

from ray.util.annotations import PublicAPI
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


@PublicAPI(stability="beta")
def json_to_ndarray(payload: NdArray) -> np.ndarray:
    """Accepts an NdArray JSON from an HTTP body and converts it to a numpy array."""
    arr = np.array(payload.array)
    if payload.shape:
        arr = arr.reshape(*payload.shape)
    if payload.dtype:
        arr = arr.astype(payload.dtype)
    return arr


@PublicAPI(stability="beta")
def json_to_multi_ndarray(payload: Dict[str, NdArray]) -> Dict[str, np.ndarray]:
    """Accepts a JSON of shape {str_key: NdArray} and converts it to dict of arrays."""
    return {key: json_to_ndarray(arr_obj) for key, arr_obj in payload.items()}


@PublicAPI(stability="beta")
def starlette_request(
    request: starlette.requests.Request,
) -> starlette.requests.Request:
    """Returns the raw request object."""
    # NOTE(simon): This adapter is used for ease of getting started.
    return request


@PublicAPI(stability="beta")
async def json_request(request: starlette.requests.Request) -> Dict[str, Any]:
    """Return the JSON object from request body."""
    return await request.json()


@require_packages(["PIL"])
@PublicAPI(stability="beta")
def image_to_ndarray(img: bytes = File(...)) -> np.ndarray:
    """Accepts a PIL-readable file from an HTTP form and convert it to a numpy array."""
    from PIL import Image

    image = Image.open(BytesIO(img))
    return np.array(image)


@require_packages(["pandas"])
@PublicAPI(stability="beta")
async def pandas_read_json(raw_request: Request):
    """Accept JSON body and converts into pandas DataFrame.

    This function simply uses `pandas.read_json(body, **query_params)` under the hood.
    """
    import pandas as pd

    raw_json = await raw_request.body()
    return pd.read_json(raw_json, **raw_request.query_params)
