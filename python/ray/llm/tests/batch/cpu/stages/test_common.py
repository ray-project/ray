import sys

import numpy as np
import pytest

from ray.llm._internal.batch.stages.common import (
    convert_to_embed_override_tensors,
    maybe_convert_ndarray_to_list,
    to_token_id_list,
)

torch = pytest.importorskip("torch")


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


def _nested_object_array(entries):
    array = np.empty(len(entries), dtype=object)
    for i, entry in enumerate(entries):
        array[i] = entry
    return array


@pytest.mark.parametrize(
    "embeds,expected_count,dtype",
    [
        (np.ones((2, 3), dtype=np.float32), 2, torch.float32),
        (np.ones(3, dtype=np.float32), 1, torch.float32),
        ([[0.1, 0.2, 0.3]], 1, torch.float64),
        (_nested_object_array([np.ones(3), np.zeros(3)]), 2, torch.float64),
        (torch.ones(2, 3, dtype=torch.bfloat16), 2, torch.bfloat16),
        ([torch.ones(3, dtype=torch.bfloat16)] * 3, 3, torch.bfloat16),
        (
            _nested_object_array([torch.ones(3, dtype=torch.bfloat16)] * 2),
            2,
            torch.bfloat16,
        ),
    ],
    ids=[
        "ndarray_2d",
        "ndarray_1d",
        "list",
        "object_ndarray",
        "bf16_tensor",
        "bf16_tensor_list",
        "bf16_object_ndarray",
    ],
)
def test_convert_to_embed_override_tensors(embeds, expected_count, dtype):
    result = convert_to_embed_override_tensors(embeds)
    assert len(result) == expected_count
    assert all(t.shape == (1, 3) and t.dtype == dtype for t in result)


@pytest.mark.parametrize("embeds", [None, [], np.zeros((0, 3))])
def test_convert_to_embed_override_tensors_empty(embeds):
    assert convert_to_embed_override_tensors(embeds) is None


@pytest.mark.parametrize(
    "embeds,match",
    [
        (np.zeros((2, 2, 2)), "shaped"),
        (_nested_object_array([np.ones(2), np.ones(3)]), "same shape"),
        ([torch.ones(2), torch.ones(3)], "same shape"),
    ],
)
def test_convert_to_embed_override_tensors_errors(embeds, match):
    with pytest.raises(ValueError, match=match):
        convert_to_embed_override_tensors(embeds)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
