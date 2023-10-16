from io import BytesIO
from typing import Any, Dict, List, Optional, Union

import numpy as np
import starlette.requests
from fastapi import File, Request

from ray._private.pydantic_compat import BaseModel, Field
from ray.serve._private.constants import DAG_DEPRECATION_MESSAGE
from ray.serve._private.http_util import make_buffered_asgi_receive
from ray.serve._private.utils import require_packages
from ray.util.annotations import Deprecated

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


@Deprecated(DAG_DEPRECATION_MESSAGE)
def json_to_ndarray(payload: NdArray) -> np.ndarray:
    """Accepts an NdArray JSON from an HTTP body and converts it to a numpy array.

    .. autopydantic_model:: ray.serve.http_adapters.NdArray
    """
    arr = np.array(payload.array)
    if payload.shape:
        arr = arr.reshape(*payload.shape)
    if payload.dtype:
        arr = arr.astype(payload.dtype)
    return arr


@Deprecated(DAG_DEPRECATION_MESSAGE)
def json_to_multi_ndarray(payload: Dict[str, NdArray]) -> Dict[str, np.ndarray]:
    """Accepts a JSON of shape {str_key: NdArray} and converts it to dict of arrays."""
    return {key: json_to_ndarray(arr_obj) for key, arr_obj in payload.items()}


@Deprecated(DAG_DEPRECATION_MESSAGE)
async def starlette_request(
    request: starlette.requests.Request,
) -> starlette.requests.Request:
    """Returns a buffered (serializable) version of the Starlette Request."""

    async def empty_send():
        pass

    request._send = empty_send
    request._receive = make_buffered_asgi_receive(await request.body())
    return request


@Deprecated(DAG_DEPRECATION_MESSAGE)
async def json_request(request: starlette.requests.Request) -> Dict[str, Any]:
    """Return the JSON object from request body."""
    return await request.json()


@require_packages(["PIL"])
@Deprecated(DAG_DEPRECATION_MESSAGE)
def image_to_ndarray(img: bytes = File(...)) -> np.ndarray:
    """Accepts a PIL-readable file from an HTTP form and convert it to a numpy array."""
    from PIL import Image

    image = Image.open(BytesIO(img))
    return np.array(image)


@require_packages(["pandas"])
@Deprecated(DAG_DEPRECATION_MESSAGE)
async def pandas_read_json(raw_request: Request):
    """Accept JSON body and converts into pandas DataFrame.

    This function simply uses `pandas.read_json(body, **query_params)` under the hood.
    """
    import pandas as pd

    raw_json = await raw_request.body()
    if isinstance(raw_json, bytes):
        raw_json = raw_json.decode("utf-8")
    return pd.read_json(raw_json, **raw_request.query_params)
