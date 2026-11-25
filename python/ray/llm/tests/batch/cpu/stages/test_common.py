import sys

import numpy as np
import pytest

from ray.llm._internal.batch.stages.common import to_token_id_list


@pytest.mark.parametrize(
    "value",
    [[1, 2], np.array([1, 2]), [np.int64(1), 2], np.array([1, 2], dtype=np.int32)],
)
def test_to_token_id_list(value):
    result = to_token_id_list(value, "ids")
    assert result == [1, 2]
    assert all(type(token_id) is int for token_id in result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
