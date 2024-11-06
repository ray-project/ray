import pyarrow as pa

from ray.air.util.tensor_extensions.arrow import _infer_pyarrow_type
from ray.air.util.tensor_extensions.utils import create_ragged_ndarray


def test_pa_infer_type():
    class UserObj:
        pass

    # Represent a single column that will be using `ArrowPythonObjectExtension` type
    # to ser/de native Python objects into bytes
    column_vals = create_ragged_ndarray(["hi", 1, None, [[[[]]]], {"a": [[{"b": 2, "c": UserObj()}]]}, UserObj()])

    inferred_dtype = _infer_pyarrow_type(column_vals)

    # Arrow (17.0) seem to fallback to assume the dtype of the first element
    assert pa.string().equals(inferred_dtype)
