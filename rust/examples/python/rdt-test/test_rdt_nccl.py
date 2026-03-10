#!/usr/bin/env python3
"""
RDT (Rapid Data Transport) NCCL test for the Rust Ray backend.

Demonstrates direct GPU-to-GPU tensor transfer via NCCL, bypassing the
object store entirely.  Adapted from
python/ray/tests/rdt/test_rdt_nccl.py to run against the all-Rust backend.

Requirements:
    - 2+ NVIDIA GPUs on the same machine
    - PyTorch with CUDA support
    - Rust Ray backend built (cargo + maturin)
    - PYTHONPATH pointing at rust/ray and the _raylet.so location

Usage:
    pytest test_rdt_nccl.py -sv
    # or simply:
    python test_rdt_nccl.py
"""

import sys
import time

import torch
import ray
from ray.experimental.collective import create_collective_group


# ── Actor ──────────────────────────────────────────────────────────────


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    """GPU actor that echoes tensors via NCCL and computes sums."""

    @ray.method(tensor_transport="nccl")
    def echo(self, data):
        """Move *data* to CUDA and return it (will be intercepted by RDT)."""
        return data.to("cuda")

    def sum(self, data):
        """Return the sum of *data* as a Python scalar."""
        return data.sum().item()


# ── Tests ──────────────────────────────────────────────────────────────


def test_p2p(actors):
    """Point-to-point: actor 0 echoes a tensor, actor 1 sums it via NCCL."""
    print("\n" + "=" * 70)
    print("  Test 1: P2P NCCL tensor transfer (small)")
    print("=" * 70)

    src_actor, dst_actor = actors

    tensor = torch.tensor([1.0, 2.0, 3.0])
    expected_sum = tensor.sum().item()  # 6.0
    print(f"Input tensor: {tensor.tolist()}, expected sum: {expected_sum}")

    t0 = time.time()
    rdt_ref = src_actor.echo.remote(tensor)
    remote_sum = ray.get(dst_actor.sum.remote(rdt_ref), timeout=60.0)
    elapsed = time.time() - t0

    print(f"Remote sum: {remote_sum} (expected {expected_sum}), "
          f"elapsed: {elapsed:.3f}s")

    assert expected_sum == remote_sum, (
        f"Mismatch: expected {expected_sum}, got {remote_sum}"
    )
    print("PASS: tensor.sum() == remote_sum")


def test_larger_tensor(actors):
    """Transfer a larger tensor (1M floats = 4 MB) via NCCL."""
    print("\n" + "=" * 70)
    print("  Test 2: Larger tensor NCCL transfer (4 MB)")
    print("=" * 70)

    src_actor, dst_actor = actors
    tensor = torch.ones(1024 * 1024, dtype=torch.float32)  # 4 MB
    expected_sum = float(1024 * 1024)

    t0 = time.time()
    rdt_ref = src_actor.echo.remote(tensor)
    remote_sum = ray.get(dst_actor.sum.remote(rdt_ref), timeout=60.0)
    elapsed = time.time() - t0

    nbytes = 1024 * 1024 * 4
    print(f"Transferred {nbytes / 1e6:.1f} MB in {elapsed:.3f}s")
    print(f"Remote sum: {remote_sum} (expected {expected_sum})")

    assert remote_sum is not None and abs(expected_sum - remote_sum) < 1.0, (
        f"Mismatch: expected {expected_sum}, got {remote_sum}"
    )
    print("PASS")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    t_start = time.time()
    passed = 0
    failed = 0

    ray.init(num_gpus=2)

    try:
        actors = [GPUTestActor.remote() for _ in range(2)]
        print(f"Created {len(actors)} GPU actors")

        create_collective_group(actors, backend="nccl")
        print("NCCL collective group created")

        for test_fn in [test_p2p, test_larger_tensor]:
            try:
                test_fn(actors)
                passed += 1
            except Exception as e:
                failed += 1
                print(f"FAIL: {test_fn.__name__}: {e}")
                import traceback
                traceback.print_exc()
    finally:
        ray.shutdown()

    total = time.time() - t_start
    print("\n" + "=" * 70)
    print(f"  Results: {passed} passed, {failed} failed  ({total:.1f}s)")
    print("=" * 70)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
