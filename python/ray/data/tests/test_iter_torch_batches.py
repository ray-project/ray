import sys
from typing import Dict

import numpy as np
import pytest
import torch

import ray


@pytest.fixture
def fake_pin_memory(monkeypatch):
    """Fake `torch.Tensor.pin_memory` so pinning logic is testable without an
    accelerator (actual pinning requires CUDA)."""

    def fake_pin(tensor):
        pinned = tensor.clone()
        pinned._fake_pinned = True
        return pinned

    monkeypatch.setattr(torch.Tensor, "pin_memory", fake_pin)
    monkeypatch.setattr(
        torch.Tensor, "is_pinned", lambda self: getattr(self, "_fake_pinned", False)
    )


def _check_pinned_structure(original, result):
    """Recursively assert that `result` mirrors `original`'s structure with all
    tensors pinned."""
    if isinstance(original, torch.Tensor):
        assert result.is_pinned()
        assert torch.equal(result, original)
    elif isinstance(original, dict):
        assert isinstance(result, dict)
        assert set(result.keys()) == set(original.keys())
        for key in original:
            _check_pinned_structure(original[key], result[key])
    else:
        assert type(result) is type(original)
        assert len(result) == len(original)
        for orig_item, result_item in zip(original, result):
            _check_pinned_structure(orig_item, result_item)


@pytest.mark.parametrize(
    "batch_factory",
    [
        pytest.param(lambda: torch.ones(2), id="tensor"),
        pytest.param(lambda: [torch.ones(2), torch.zeros(2)], id="list"),
        pytest.param(lambda: (torch.ones(2), torch.zeros(2)), id="tuple"),
        pytest.param(
            lambda: [[torch.ones(2)], [torch.zeros(2), torch.ones(2)]],
            id="nested_list",
        ),
        pytest.param(lambda: {"a": torch.ones(2), "b": torch.zeros(2)}, id="dict"),
        pytest.param(
            lambda: {"a": [torch.ones(2), torch.zeros(2)]}, id="dict_of_lists"
        ),
    ],
)
def test_pin_memory_tensor_batch_variants(fake_pin_memory, batch_factory):
    """Test that pin_memory pins every tensor and preserves the batch structure."""
    from ray.data.util.torch_utils import pin_tensors_to_memory

    batch = batch_factory()
    pinned = pin_tensors_to_memory(batch)
    _check_pinned_structure(batch, pinned)


def test_pin_memory_already_pinned_no_op(fake_pin_memory):
    """Test that already-pinned tensors are returned as-is without copying."""
    from ray.data.util.torch_utils import pin_tensors_to_memory

    pinned = pin_tensors_to_memory(torch.ones(2))
    assert pin_tensors_to_memory(pinned) is pinned


def test_pin_memory_non_tensor_passthrough(fake_pin_memory):
    """Test that non-tensor leaves pass through unchanged."""
    from ray.data.util.torch_utils import pin_tensors_to_memory

    assert pin_tensors_to_memory(5) == 5
    batch = {"a": np.ones(2)}
    assert pin_tensors_to_memory(batch)["a"] is batch["a"]


def test_pinning_collate_fn_wrapper(fake_pin_memory):
    """Test that the wrapper pins TensorBatchType collate outputs."""
    from ray.data.collate_fn import _PinMemoryCollateFnWrapper

    wrapper = _PinMemoryCollateFnWrapper(
        lambda batch: {k: torch.as_tensor(v) for k, v in batch.items()}
    )
    out = wrapper({"a": np.arange(3)})
    assert out["a"].is_pinned()
    assert torch.equal(out["a"], torch.arange(3))


def test_pinning_collate_fn_wrapper_passthrough():
    """Test that non-TensorBatchType collate outputs pass through untouched."""
    from ray.data.collate_fn import _PinMemoryCollateFnWrapper

    sentinel = object()
    wrapper = _PinMemoryCollateFnWrapper(lambda batch: sentinel)
    assert wrapper({"ignored": 1}) is sentinel


def test_iter_torch_batches_pin_memory_with_custom_collate_fn(
    ray_start_regular_shared, fake_pin_memory
):
    """Test that pin_memory=True composes with a custom collate fn."""
    from ray.data.collate_fn import NumpyBatchCollateFn

    class _TensorDictCollateFn(NumpyBatchCollateFn):
        def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
            return {k: torch.as_tensor(v) for k, v in batch.items()}

    ds = ray.data.range(8)
    batches = list(
        ds.iterator().iter_torch_batches(
            collate_fn=_TensorDictCollateFn(), batch_size=4, pin_memory=True
        )
    )
    assert len(batches) == 2
    assert all(batch["id"].is_pinned() for batch in batches)
    assert sorted(t for batch in batches for t in batch["id"].tolist()) == list(
        range(8)
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
