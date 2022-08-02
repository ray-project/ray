import io
from dataclasses import dataclass

import numpy as np
import pytest
from PIL import Image
import pandas as pd

from ray.serve.http_adapters import (
    NdArray,
    json_to_multi_ndarray,
    json_to_ndarray,
    image_to_ndarray,
    pandas_read_json,
)
from ray.serve._private.utils import require_packages


@pytest.mark.asyncio
async def test_require_packages():
    @require_packages(["missing_package"])
    def func():
        pass

    @require_packages(["missing_package"])
    async def async_func():
        pass

    with pytest.raises(ImportError, match="missing_package"):
        func()

    with pytest.raises(ImportError, match="missing_package"):
        await async_func()


def test_json_to_ndarray():
    np.testing.assert_equal(
        json_to_ndarray(NdArray(array=[1, 2], shape=None, dtype=None)),
        np.array([1, 2]),
    )
    np.testing.assert_equal(
        json_to_ndarray(NdArray(array=[[1], [2]], shape=None, dtype=None)),
        np.array([[1], [2]]),
    )
    np.testing.assert_equal(
        json_to_ndarray(NdArray(array=[[1], [2]], shape=[1, 2], dtype=None)),
        np.array([[1, 2]]),
    )
    np.testing.assert_equal(
        json_to_ndarray(NdArray(array=[[1.9], [2.1]], shape=[1, 2], dtype="int")),
        np.array([[1.9, 2.1]]).astype("int"),
    )


def test_json_to_multi_ndarray():
    assert json_to_multi_ndarray(
        {"a": NdArray(array=[1]), "b": NdArray(array=[3])}
    ) == {"a": np.array(1), "b": np.array(3)}


def test_image_to_ndarray():
    buffer = io.BytesIO()
    arr = (np.random.rand(100, 100, 3) * 255).astype("uint8")
    image = Image.fromarray(arr).convert("RGB")
    image.save(buffer, format="png")
    np.testing.assert_almost_equal(image_to_ndarray(buffer.getvalue()), arr)


@dataclass
class MockRequest:
    _body: str
    query_params: dict

    async def body(self):
        return self._body


@pytest.mark.asyncio
async def test_pandas_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    raw_json = df.to_json()
    parsed_df = await pandas_read_json(MockRequest(_body=raw_json, query_params={}))
    assert parsed_df.equals(df)

    raw_json = df.to_json(orient="index")
    parsed_df = await pandas_read_json(
        MockRequest(_body=raw_json, query_params={"orient": "index"})
    )
    assert parsed_df.equals(df)

    raw_json = df.to_json(orient="records")
    parsed_df = await pandas_read_json(
        MockRequest(_body=raw_json, query_params={"orient": "records"})
    )
    assert parsed_df.equals(df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
