"""
RDT NIXL Profiling: actor ray.put + borrow-via-list to another actor.

Follows the pattern from PR #60732 (test_weights_transfer.py):
  - Sender actor does ray.put(_tensor_transport="nixl")
  - Sender passes [ref] to Receiver actor method
  - Receiver calls set_target_for_ref + ray.get to pull via NIXL

Uses register_nixl_memory on both sides to eliminate memory registration cost.

Instrumentation is added directly in:
  - python/ray/_private/worker.py (put_object, deserialize_objects)
  - python/ray/_private/serialization.py (object_ref_reducer)
  - python/ray/experimental/rdt/nixl_tensor_transport.py (extract_tensor_transport_metadata, recv_multiple_tensors)
  - python/ray/experimental/rdt/rdt_manager.py (_fetch_object)
"""

import time

import torch

import ray
from ray.experimental import register_nixl_memory, set_target_for_ref


@ray.remote(num_gpus=1, enable_tensor_transport=True)
class Sender:
    def __init__(self, shape, num_views):
        self.weight = torch.randn(shape, device="cuda")
        # Pre-register the underlying storage once; all views share it.
        register_nixl_memory(self.weight)
        self.num_views = num_views
        self.views = [self.weight[i] for i in range(num_views)]

    def put_and_send(self, receiver):
        import ray._private.worker as _w

        _w._rdt_profile_timings = {}

        ref = ray.put(self.views, _tensor_transport="nixl")
        recv_timings = ray.get(receiver.recv.remote([ref]))

        sender_timings = _w._rdt_profile_timings.copy()
        return sender_timings, recv_timings

    def warmup(self, receiver):
        ref = ray.put(self.views, _tensor_transport="nixl")
        ray.get(receiver.recv.remote([ref]))

    def get_bytes(self):
        return sum(v.numel() * v.element_size() for v in self.views)


@ray.remote(num_gpus=1, enable_tensor_transport=True)
class Receiver:
    def __init__(self, shape, num_views):
        self.weight = torch.empty(shape, device="cuda")
        # Pre-register the underlying storage once; all views share it.
        register_nixl_memory(self.weight)
        self.views = [self.weight[i] for i in range(num_views)]

    def recv(self, refs):
        import ray._private.worker as _w

        _w._rdt_profile_timings = {}

        set_target_for_ref(refs[0], self.views)
        tensors = ray.get(refs[0])
        assert tensors[0].data_ptr() == self.views[0].data_ptr(), "zero-copy failed"

        return _w._rdt_profile_timings.copy()

    def warmup(self):
        return True


def print_timings(label, timings):
    if not timings:
        print(f"  [{label}] No timings collected")
        return
    max_key_len = max(len(k) for k in timings)
    for k, v in sorted(timings.items()):
        if "iterations" in k or "count" in k:
            print(f"  [{label}] {k:<{max_key_len}}  {v:8.0f}")
        else:
            print(f"  [{label}] {k:<{max_key_len}}  {v*1000:8.3f} ms")


if __name__ == "__main__":
    TOTAL_SIZE_BYTES = 200 * 1024 * 1024
    NUM_ROWS = 1_000
    NUM_VIEWS = NUM_ROWS
    shape = (NUM_ROWS, TOTAL_SIZE_BYTES // NUM_ROWS // 4)
    ray.init()

    sender = Sender.remote(shape, NUM_VIEWS)
    receiver = Receiver.remote(shape, NUM_VIEWS)
    ray.get(receiver.warmup.remote())
    total_bytes = ray.get(sender.get_bytes.remote())

    # Warmup
    ray.get(sender.warmup.remote(receiver))

    # Profiled run
    overall_start = time.perf_counter()
    sender_timings, recv_timings = ray.get(sender.put_and_send.remote(receiver))
    overall_end = time.perf_counter()

    # Compute overhead totals
    sender_overhead = sum(v for k, v in sender_timings.items() if "iterations" not in k)

    transfer_time = recv_timings.get("D3_poll_wait", 0) + recv_timings.get(
        "D1_transfer_call", 0
    )
    # Receiver overhead = everything except transfer/poll and the summary totals
    recv_overhead_exclude = (
        "D_transfer_and_poll",
        "D1_transfer_call",
        "D2_poll_iterations",
        "D3_poll_wait",
        "F_fetch_object_total",
        "G_deserialize_objects_total",
    )
    recv_overhead = sum(
        v
        for k, v in recv_timings.items()
        if k not in recv_overhead_exclude and "iterations" not in k
    )

    e2e = overall_end - overall_start

    print(f"\n{'='*60}")
    print(
        f"RDT NIXL Profile: {total_bytes/1e9:.2f} GB as {NUM_VIEWS} views of shape {shape[1:]}"
    )
    print(f"{'='*60}")
    print(f"\nEnd-to-end (driver):      {e2e*1000:8.3f} ms")
    print(
        f"  Sender overhead:        {sender_overhead*1000:8.3f} ms  ({sender_overhead/e2e*100:4.1f}%)"
    )
    print(
        f"  Receiver overhead:      {recv_overhead*1000:8.3f} ms  ({recv_overhead/e2e*100:4.1f}%)"
    )
    print(
        f"  NIXL transfer:          {transfer_time*1000:8.3f} ms  ({transfer_time/e2e*100:4.1f}%)"
    )
    print(
        f"  Ray scheduling/network: {(e2e - sender_overhead - recv_overhead - transfer_time)*1000:8.3f} ms"
    )

    print("\nSender-side sub-timings (ray.put + submission):")
    print_timings("sender", sender_timings)

    print("\nReceiver-side sub-timings (ray.get + NIXL pull):")
    print_timings("receiver", recv_timings)
    print()
