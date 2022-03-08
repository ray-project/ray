import io

import numpy as np
import pytest
from PIL import Image
from ray.serve.http_adapters import NdArray, array_to_databatch, image_to_databatch
from ray.serve.utils import require_packages


def test_require_packages():
    @require_packages(["missing_package"])
    def func():
        pass

    with pytest.raises(ImportError, match="missing_package"):
        func()


def test_array_to_databatch():
    np.testing.assert_equal(
        array_to_databatch(NdArray(array=[1, 2], shape=None, dtype=None)),
        np.array([1, 2]),
    )
    np.testing.assert_equal(
        array_to_databatch(NdArray(array=[[1], [2]], shape=None, dtype=None)),
        np.array([[1], [2]]),
    )
    np.testing.assert_equal(
        array_to_databatch(NdArray(array=[[1], [2]], shape=[1, 2], dtype=None)),
        np.array([[1, 2]]),
    )
    np.testing.assert_equal(
        array_to_databatch(NdArray(array=[[1.9], [2.1]], shape=[1, 2], dtype="int")),
        np.array([[1.9, 2.1]]).astype("int"),
    )


def test_image_to_databatch():
    buffer = io.BytesIO()
    arr = (np.random.rand(100, 100, 3) * 255).astype("uint8")
    image = Image.fromarray(arr).convert("RGB")
    image.save(buffer, format="png")
    np.testing.assert_almost_equal(image_to_databatch(buffer.getvalue()), arr)
