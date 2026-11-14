"""Integration tests for RDT store residency in native stats and ``ray memory``.

CPU-only GLOO tests. They verify that the process-local RDT residency published
from :class:`RDTStore` (via ``CoreWorker.set_rdt_object_memory_info``) surfaces
in ``GetCoreWorkerStats`` / the actual ``ray memory`` CLI:

* the producer actor reports its RDT row with exact logical bytes, the driver
  (owner) holds the serialized payload in the object-reference table but no RDT
  row, and releasing the ref removes the producer's row;
* a blocked receiver keeps a queued receive resident until it consumes the ref;
* shutdown with a held RDT ref across two init/shutdown cycles discards the
  manager without hanging.
"""
import os
import subprocess
import sys

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.internal_api import node_stats
from ray.dashboard.memory_utils import decode_object_ref_if_needed
from ray.dashboard.utils import node_stats_to_dict
from ray.experimental.collective import create_collective_group


@ray.remote
class RdtMemorySender:
    @ray.method(tensor_transport="gloo")
    def produce(self, x):
        return x

    def pid(self):
        return os.getpid()

    def num_rdt_objects(self):
        return ray._private.worker.global_worker.rdt_manager.rdt_store.get_num_objects()


@ray.remote(enable_tensor_transport=True)
class RdtMemoryReceiver:
    def block(self, signal):
        ray.get(signal.wait.remote())

    def consume(self, data):
        return data

    def pid(self):
        return os.getpid()

    def num_rdt_objects(self):
        return ray._private.worker.global_worker.rdt_manager.rdt_store.get_num_objects()


def _core_workers_stats():
    stats_list = []
    for node in ray.nodes():
        if not node.get("Alive", False):
            continue
        stats = node_stats_to_dict(
            node_stats(node["NodeManagerAddress"], node["NodeManagerPort"])
        )
        stats_list.extend(stats["coreWorkersStats"])
    return stats_list


def _worker_stats_by_pid(pid):
    for cw in _core_workers_stats():
        if cw.get("pid") == pid:
            return cw
    return None


def _rdt_object(cw, obj_id):
    for entry in cw.get("rdtObjects", []) or []:
        if decode_object_ref_if_needed(entry["objectId"]).hex() == obj_id:
            # protojson may render int64 scalars as strings.
            return {
                "tensorBytes": {
                    device: int(num_bytes or 0)
                    for device, num_bytes in (
                        entry.get("tensorBytes", {}) or {}
                    ).items()
                },
                "numCopies": int(entry.get("numCopies", 0) or 0),
                "numErrors": int(entry.get("numErrors", 0) or 0),
            }
    return None


def _stats_present_rdt_gone(pid, obj_id):
    """True only when the worker's stats are present and its RDT row is gone."""
    cw = _worker_stats_by_pid(pid)
    if cw is None:
        return False
    return _rdt_object(cw, obj_id) is None


def test_rdt_memory_lifecycle(ray_start_regular, tmp_path):
    sender = RdtMemorySender.remote()
    create_collective_group([sender], backend="gloo")

    tensor = torch.ones(8, dtype=torch.float32)  # 8 * 4 = 32 logical bytes
    payload = b"x" * 64  # serialized, non-tensor part of the returned object
    ref = sender.produce.remote([tensor, payload])
    obj_id = ref.hex()
    sender_pid = ray.get(sender.pid.remote())

    wait_for_condition(
        lambda: ray.get(sender.num_rdt_objects.remote(), timeout=5) == 1,
        timeout=10,
        retry_interval_ms=100,
    )

    def _producer_row_ok():
        cw = _worker_stats_by_pid(sender_pid)
        if cw is None:
            return False
        entry = _rdt_object(cw, obj_id)
        return entry is not None and entry.get("tensorBytes") == {"cpu": 32}

    wait_for_condition(_producer_row_ok, timeout=10, retry_interval_ms=100)
    entry = _rdt_object(_worker_stats_by_pid(sender_pid), obj_id)
    assert entry["numCopies"] == 1 and entry["numErrors"] == 0

    # The driver owns the ref but holds no tensors. Its stats row must exist.
    driver_cw = _worker_stats_by_pid(os.getpid())
    assert driver_cw is not None
    assert _rdt_object(driver_cw, obj_id) is None
    object_ref = None
    for oref in driver_cw.get("objectRefs", []) or []:
        if decode_object_ref_if_needed(oref["objectId"]).hex() == obj_id:
            object_ref = oref
            break
    assert object_ref is not None
    assert int(object_ref.get("objectSize", 0) or 0) > 0

    # Invoke the actual `ray memory` CLI via subprocess and check the RDT table.
    cli = subprocess.run(
        [
            sys.executable,
            "-m",
            "ray.scripts.scripts",
            "memory",
            "--address",
            ray_start_regular["address"],
            "--units",
            "B",
            "--no-format",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert cli.returncode == 0, cli.stderr
    assert "RDT store objects" in cli.stdout
    assert obj_id in cli.stdout
    (tmp_path / "CLI-EVIDENCE.txt").write_text(cli.stdout)

    # Releasing the ref frees the producer's primary copy and removes the row.
    del ref
    wait_for_condition(
        lambda: ray.get(sender.num_rdt_objects.remote(), timeout=5) == 0,
        timeout=10,
        retry_interval_ms=100,
    )
    wait_for_condition(
        lambda: _stats_present_rdt_gone(sender_pid, obj_id),
        timeout=10,
        retry_interval_ms=100,
    )


def test_rdt_memory_receiver_residency(ray_start_regular):
    sender = RdtMemorySender.remote()
    receiver = RdtMemoryReceiver.remote()
    create_collective_group([sender, receiver], backend="gloo")
    signal = SignalActor.options(num_cpus=0).remote()

    tensor = torch.ones(8, dtype=torch.float32)
    ref = sender.produce.remote(tensor)
    obj_id = ref.hex()
    receiver_pid = ray.get(receiver.pid.remote())

    wait_for_condition(
        lambda: ray.get(sender.num_rdt_objects.remote(), timeout=5) == 1,
        timeout=10,
        retry_interval_ms=100,
    )

    # Occupy the receiver's default concurrency group so the consuming task
    # cannot pop the received tensors, while `_ray_system` can still receive.
    receiver.block.remote(signal)
    try:
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote(), timeout=5) == 1,
            timeout=10,
            retry_interval_ms=100,
        )
        consume_ref = receiver.consume.remote(ref)

        def _receiver_row_ok():
            cw = _worker_stats_by_pid(receiver_pid)
            if cw is None:
                return False
            entry = _rdt_object(cw, obj_id)
            return entry is not None and entry.get("tensorBytes") == {"cpu": 32}

        wait_for_condition(_receiver_row_ok, timeout=10, retry_interval_ms=100)
    finally:
        ray.get(signal.send.remote(), timeout=10)

    result = ray.get(consume_ref, timeout=10)
    assert torch.equal(result, tensor)

    wait_for_condition(
        lambda: _stats_present_rdt_gone(receiver_pid, obj_id),
        timeout=10,
        retry_interval_ms=100,
    )


def test_rdt_memory_rejects_invalid_object_id(ray_start_regular):
    core_worker = ray._private.worker.global_worker.core_worker
    with pytest.raises(ValueError, match=f"{ray.ObjectRef.size()} bytes"):
        core_worker.set_rdt_object_memory_info(b"\x00" * 20, [("cpu", 32)], 1, 0)


def test_rdt_memory_shutdown_reinit_cycles():
    # Two full init/shutdown cycles with a held RDT ref at shutdown: the manager
    # must be discarded after CoreWorker deletion, and no subprocess may hang.
    for _ in range(2):
        ray.init("local", num_cpus=2, object_store_memory=150 * 1024 * 1024)
        try:
            sender = RdtMemorySender.remote()
            create_collective_group([sender], backend="gloo")
            ref = sender.produce.remote(torch.ones(8, dtype=torch.float32))
            wait_for_condition(
                lambda: ray.get(sender.num_rdt_objects.remote(), timeout=5) == 1,
                timeout=10,
                retry_interval_ms=100,
            )
            # Shutdown while `ref` is still held.
        finally:
            ray.shutdown()
        assert ray._private.worker.global_worker._rdt_manager is None
        del ref


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
