import io

import numpy as np
import pytest
from PIL import Image
from ray.serve.http_adapters import NdArray, json_to_ndarray, image_to_ndarray
from ray.serve.utils import require_packages


def test_require_packages():
    @require_packages(["missing_package"])
    def func():
        pass

    with pytest.raises(ImportError, match="missing_package"):
        func()


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


def test_image_to_ndarray():
    buffer = io.BytesIO()
    arr = (np.random.rand(100, 100, 3) * 255).astype("uint8")
    image = Image.fromarray(arr).convert("RGB")
    image.save(buffer, format="png")
    np.testing.assert_almost_equal(image_to_ndarray(buffer.getvalue()), arr)
