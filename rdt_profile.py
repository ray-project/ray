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

        body_start = time.perf_counter()

        put_start = time.perf_counter()
        ref = ray.put(self.views, _tensor_transport="nixl")
        put_total = time.perf_counter() - put_start

        submit_start = time.perf_counter()
        recv_future = receiver.recv.remote([ref])
        submit_total = time.perf_counter() - submit_start

        wait_start = time.perf_counter()
        recv_timings = ray.get(recv_future)
        wait_total = time.perf_counter() - wait_start

        body_total = time.perf_counter() - body_start

        sender_timings = _w._rdt_profile_timings.copy()
        sender_timings["S1_ray_put_total"] = put_total
        sender_timings["S2_recv_remote_submit"] = submit_total
        sender_timings["S3_recv_remote_wait"] = wait_total
        sender_timings["S0_put_and_send_body"] = body_total
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

        body_start = time.perf_counter()

        set_target_start = time.perf_counter()
        set_target_for_ref(refs[0], self.views)
        set_target_total = time.perf_counter() - set_target_start

        get_start = time.perf_counter()
        tensors = ray.get(refs[0])
        get_total = time.perf_counter() - get_start
        assert tensors[0].data_ptr() == self.views[0].data_ptr(), "zero-copy failed"

        body_total = time.perf_counter() - body_start

        timings = _w._rdt_profile_timings.copy()
        timings["R1_set_target_for_ref"] = set_target_total
        timings["R2_ray_get_total"] = get_total
        timings["R0_recv_body"] = body_total
        return timings

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

    e2e = overall_end - overall_start

    # Wall-clock segments measured at each layer. By construction these
    # are nested:
    #   e2e (driver wall-clock)
    #     >= S0_put_and_send_body (sender actor wall-clock)
    #         = S1_ray_put_total (= A1 + A2 + A3 + sender python glue)
    #           + S2_recv_remote_submit (= B + arg pickling + plasma put + RPC submit)
    #           + S3_recv_remote_wait (>= R0_recv_body via RPC)
    #           + small sender python glue
    #         where S3_recv_remote_wait >= R0_recv_body
    #     and R0_recv_body
    #         = R1_set_target_for_ref + R2_ray_get_total + small receiver glue
    #       R2_ray_get_total >= G_deserialize_objects_total
    #         G_deserialize_objects_total >= F_fetch_object_total >= D_transfer_and_poll
    #
    # Each line below is exclusive (parent minus its measured children),
    # so the four numbers sum to e2e exactly with no double counting.

    s0_body = sender_timings.get("S0_put_and_send_body", 0)
    s1_put = sender_timings.get("S1_ray_put_total", 0)
    s2_submit = sender_timings.get("S2_recv_remote_submit", 0)
    s3_wait = sender_timings.get("S3_recv_remote_wait", 0)
    a1 = sender_timings.get("A1_serialize_rdt_objects", 0)
    a2 = sender_timings.get("A2_core_worker_put", 0)
    a3 = sender_timings.get("A3_rdt_manager_put_object", 0)
    b = sender_timings.get("B_object_ref_reducer", 0)

    r0_body = recv_timings.get("R0_recv_body", 0)
    r1_set_target = recv_timings.get("R1_set_target_for_ref", 0)
    r2_get = recv_timings.get("R2_ray_get_total", 0)
    g = recv_timings.get("G_deserialize_objects_total", 0)
    f = recv_timings.get("F_fetch_object_total", 0)
    d = recv_timings.get("D_transfer_and_poll", 0)

    # Driver-side overhead = e2e minus sender actor body wall-clock.
    # Covers driver->sender RPC submit, return-value transit, driver glue.
    driver_rpc = e2e - s0_body

    # Sender python glue inside put_and_send body (dict copies, list, etc).
    sender_glue = s0_body - s1_put - s2_submit - s3_wait

    # ray.put internals (A1 + A2 + A3) + any python glue inside ray.put wrapper.
    ray_put_glue = s1_put - a1 - a2 - a3

    # Submission of receiver.recv.remote([ref]):
    #   B = object_ref_reducer (in-band RDTMeta pickling)
    #   the rest = arg pickling + plasma put for spilled arg + RPC submit
    submit_other = s2_submit - b

    # S3_recv_remote_wait is dominated by the receiver actor + return RPC.
    # The portion not explained by R0_recv_body is sender->receiver scheduling
    # delay, plasma-get of the arg on receiver, and return-value transit.
    sender_to_receiver_rpc = s3_wait - r0_body

    # Receiver glue inside recv body (set_target_for_ref + ray.get + tiny glue).
    receiver_glue = r0_body - r1_set_target - r2_get

    # ray.get internals on receiver: G is the rdt-collection part of
    # deserialize_objects, R2 - G is the get_objects() + final pickle.loads
    # of the CPU metadata (which contains the embedded RDTMeta).
    ray_get_other = r2_get - g

    # NIXL transfer wall-clock (D includes D1 transfer call + D3 poll wait).
    transfer = d

    # Receiver side of the NIXL pull excluding the transfer:
    # F_fetch_object_total - D = C1..C5 + E + receiver-side add_object.
    nixl_recv_setup = f - d

    # Anything in G but not in F (small).
    g_minus_f = g - f

    print(f"\n{'='*70}")
    print(
        f"RDT NIXL Profile: {total_bytes/1e9:.2f} GB as {NUM_VIEWS} views of shape {shape[1:]}"
    )
    print(f"{'='*70}")
    print(f"\nEnd-to-end (driver):                 {e2e*1000:9.3f} ms (100.0%)")
    print("\nDecomposition (exclusive, sums to e2e):")

    def line(label, val, indent=2):
        pct = val / e2e * 100 if e2e > 0 else 0
        print(f"{' '*indent}{label:<46} {val*1000:9.3f} ms ({pct:5.1f}%)")

    line("Driver <-> sender RPC + glue", driver_rpc)
    line("Sender body: ray.put A1 (serialize_rdt_objects)", a1)
    line("Sender body: ray.put A2 (core_worker.put_object)", a2)
    line("Sender body: ray.put A3 (rdt_manager.put_object)", a3)
    line("Sender body: ray.put python glue", ray_put_glue)
    line("Sender body: B (object_ref_reducer)", b)
    line("Sender body: recv.remote() submit (arg+plasma+RPC)", submit_other)
    line("Sender body: python glue", sender_glue)
    line("Sender <-> receiver RPC (incl. plasma get of arg)", sender_to_receiver_rpc)
    line("Receiver body: set_target_for_ref", r1_set_target)
    line("Receiver body: ray.get other (plasma+pickle.loads)", ray_get_other)
    line("Receiver body: G - F (deserialize_objects glue)", g_minus_f)
    line("Receiver body: F - D (NIXL setup C+E+add_object)", nixl_recv_setup)
    line("Receiver body: D (NIXL transfer+poll)", transfer)
    line("Receiver body: python glue", receiver_glue)

    # Sanity check: the components above should sum to e2e.
    total = (
        driver_rpc
        + a1
        + a2
        + a3
        + ray_put_glue
        + b
        + submit_other
        + sender_glue
        + sender_to_receiver_rpc
        + r1_set_target
        + ray_get_other
        + g_minus_f
        + nixl_recv_setup
        + transfer
        + receiver_glue
    )
    print(f"\n  {'Sum of components (sanity check)':<46} {total*1000:9.3f} ms")
    print(f"  {'Difference from e2e':<46} {(total - e2e)*1000:9.3f} ms")

    print("\nSender-side raw sub-timings:")
    print_timings("sender", sender_timings)

    print("\nReceiver-side raw sub-timings:")
    print_timings("receiver", recv_timings)
    print()
