import numpy as np
import pytest
import torch

from ray.train.torch.train_loop_utils import _WrappedDataLoader


@pytest.mark.parametrize(
    ("device_choice", "auto_transfer"),
    [
        ("cpu", True),
        ("cpu", False),
        ("cuda", True),
        ("cuda", False),
    ],
)
def test_auto_transfer_data_from_host_to_device(
    ray_start_1_cpu_1_gpu, device_choice, auto_transfer
):
    def compute_average_runtime(func):
        device = torch.device(device_choice)
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        runtime = []
        for _ in range(10):
            torch.cuda.synchronize()
            start.record()
            func(device)
            end.record()
            torch.cuda.synchronize()
            runtime.append(start.elapsed_time(end))
        return np.mean(runtime)

    small_dataloader = [
        (torch.randn((1024 * 4, 1024 * 4), device="cpu"),) for _ in range(10)
    ]

    def host_to_device(device):
        for (x,) in small_dataloader:
            x = x.to(device)
            torch.matmul(x, x)

    def host_to_device_auto_pipeline(device):
        wrapped_dataloader = _WrappedDataLoader(small_dataloader, device, auto_transfer)
        for (x,) in wrapped_dataloader:
            torch.matmul(x, x)

    # test if all four configurations are okay
    with_auto_transfer = compute_average_runtime(host_to_device_auto_pipeline)

    if device_choice == "cuda" and auto_transfer:
        # check if auto transfer is faster than manual transfer
        without_auto_transfer = compute_average_runtime(host_to_device)
        assert with_auto_transfer <= without_auto_transfer


def _nested_batch_loader(num_batches, num_indices, table_rows):
    """A loader whose batches nest a tensor inside a list, as PyG's
    ``NeighborSampler`` does with ``(batch_size, n_id, adjs)``. Each batch
    carries the same index tensor twice to require that `record_stream` is
    called on both.
    """
    batches = []
    for _ in range(num_batches):
        idx = torch.randint(0, table_rows, (num_indices,), dtype=torch.long)
        batches.append((idx.numel(), idx.clone(), [(idx.clone(), None, (0, 0))]))
    return batches


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires one CUDA device")
def test_record_stream_called_on_nested_tensors():
    """Regression test for the nested-tensor ``record_stream`` gap.

    Deterministic: it asserts on the traversal, using real CUDA streams, and
    does not depend on winning a race.
    """
    device = torch.device("cuda")
    batches = _nested_batch_loader(num_batches=1, num_indices=16, table_rows=32)
    wrapped = _WrappedDataLoader(batches, device, auto_transfer=True)

    recorded = set()
    original = torch.Tensor.record_stream

    def spy(self, stream):
        recorded.add(self.data_ptr())
        return original(self, stream)

    torch.Tensor.record_stream = spy
    try:
        _, top_level, nested = next(iter(wrapped))
    finally:
        torch.Tensor.record_stream = original

    assert top_level.data_ptr() in recorded, "top-level tensor lost its protection"
    assert nested[0][0].data_ptr() in recorded, (
        "the tensor nested inside a list never got record_stream, so the caching "
        "allocator may hand its block to a later prefetch while the training "
        "stream is still reading it"
    )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires one CUDA device")
def test_nested_batch_survives_prefetch():
    """End-to-end: a nested tensor must still hold its values when the GPU reads it.

    The loop queues heavy work on the compute stream and never synchronizes, so
    the host runs far ahead and the prefetch for later batches overlaps the
    kernels reading earlier ones -- the condition under which the missing
    ``record_stream`` corrupts data.

    Before the fix this is expected to fail, though not necessarily on every
    run: it still needs the allocator to reuse the freed block. After the fix
    it is deterministically clean, so it cannot produce false failures. To make
    a pre-fix failure near-certain, shrink the allocator pool first with
    ``torch.cuda.set_per_process_memory_fraction(0.05)``.
    """
    device = torch.device("cuda")
    table_rows, num_indices, num_batches = 4096, 8192, 200

    batches = _nested_batch_loader(num_batches, num_indices, table_rows)
    wrapped = _WrappedDataLoader(batches, device, auto_transfer=True)

    a = torch.randn(2048, 2048, device=device)
    mismatches = []
    for _, top_level, nested in wrapped:
        nested_idx = nested[0][0]
        # Keep the compute stream busy so the host runs ahead. Do not
        # synchronize in here -- that closes the very window under test.
        for _ in range(30):
            a = torch.mm(a, a) * 1e-3
        # Queued behind that work, so it reads ``nested_idx`` at execution
        # time, after Python has already dropped the previous batch.
        mismatches.append((nested_idx != top_level).sum())

    torch.cuda.synchronize()
    corrupted = sum(int(m) for m in mismatches)
    assert corrupted == 0, (
        f"{corrupted} elements of the nested index tensor were overwritten "
        "while the training stream was still using them"
    )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
