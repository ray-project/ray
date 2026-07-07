"""Tests for the three bug fixes:
1. _concatenate_chunked_arrays error message prints actual type, not None
2. UnionOperator._add_input_inner rejects input_index == len(input_dependencies)
3. ZipOperator._add_input_inner rejects input_index == len(input_dependencies)
"""

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.arrow_ops.transform_pyarrow import _concatenate_chunked_arrays
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data.context import DataContext
from ray.data.extensions import ArrowTensorArray


def test_concatenate_chunked_arrays_tensor_error_message_contains_type():
    """Bug 1: The assertion error in _concatenate_chunked_arrays used {type_}
    which was None. It should print {arr.type} (the actual tensor type)."""
    # Create a chunked array with a tensor extension type.
    tensor_arr = ArrowTensorArray.from_numpy(np.zeros((2, 3)))
    chunked = pa.chunked_array([tensor_arr])

    # The function should raise an AssertionError whose message contains the
    # actual tensor type, not "None".
    with pytest.raises(AssertionError) as exc_info:
        _concatenate_chunked_arrays([chunked])
    msg = str(exc_info.value)
    # The bug: the old code printed {type_} which was None at that point.
    assert "None" not in msg, f"Error message should not contain 'None', got: {msg}"
    # The fix: should print {arr.type} which is the actual tensor type.
    assert (
        "TensorType" in msg or "tensor" in msg.lower()
    ), f"Error message should contain the tensor type, got: {msg}"


def test_union_operator_rejects_out_of_bounds_input_index():
    """Bug 2: UnionOperator._add_input_inner used <= instead of <, allowing
    input_index == len(input_dependencies) which is out of bounds."""
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    op = UnionOperator(DataContext.get_current(), input1, input2)

    # Valid indices: 0 and 1. Index 2 == len(input_dependencies) should fail.
    assert len(op._input_dependencies) == 2

    # input_index == len(input_dependencies) must raise AssertionError.
    with pytest.raises(AssertionError):
        # refs is None here, but the assert fires before refs is used.
        op._add_input_inner(None, input_index=2)


def test_zip_operator_rejects_out_of_bounds_input_index():
    """Bug 3: ZipOperator._add_input_inner used <= instead of <, allowing
    input_index == len(input_dependencies) which is out of bounds."""
    input1 = PhysicalOperator("op1", [], DataContext.get_current())
    input2 = PhysicalOperator("op2", [], DataContext.get_current())
    op = ZipOperator(DataContext.get_current(), input1, input2)

    # Valid indices: 0 and 1. Index 2 == len(input_dependencies) should fail.
    assert len(op._input_dependencies) == 2

    # input_index == len(input_dependencies) must raise AssertionError.
    with pytest.raises(AssertionError):
        # refs is None here, but the assert fires before refs is used.
        op._add_input_inner(None, input_index=2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
