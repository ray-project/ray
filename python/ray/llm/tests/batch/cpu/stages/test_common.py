import sys

import numpy as np
import pytest

from ray.llm._internal.batch.stages.common import (
    maybe_convert_ndarray_to_list,
    to_token_id_list,
)


def test_maybe_convert_ndarray_to_list():
    assert maybe_convert_ndarray_to_list(np.array([1, 2, 3])) == [1, 2, 3]
    assert maybe_convert_ndarray_to_list([np.array([1]), np.array([2])]) == [[1], [2]]
    assert maybe_convert_ndarray_to_list({"a": np.array([1, 2])}) == {"a": [1, 2]}
    assert maybe_convert_ndarray_to_list("scalar") == "scalar"


@pytest.mark.parametrize(
    "value",
    [[1, 2], np.array([1, 2]), [np.int64(1), 2], np.array([1, 2], dtype=np.int32)],
)
def test_to_token_id_list(value):
    result = to_token_id_list(value, "ids")
    assert result == [1, 2]
    assert all(type(token_id) is int for token_id in result)


@pytest.mark.parametrize("value", ["12", [1.0, 2], [True], None, [[1]]])
def test_to_token_id_list_rejects(value):
    with pytest.raises(ValueError, match="'ids' must be a list of integer token IDs"):
        to_token_id_list(value, "ids")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
