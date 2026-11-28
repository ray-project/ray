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
    def __init__(self):
        self._injected_erase_failure_seen = False

    @ray.method(tensor_transport="gloo")
    def produce(self, x):
        return x

    def pid(self):
        return os.getpid()

    def num_rdt_objects(self):
        return ray._private.worker.global_worker.rdt_manager.rdt_store.get_num_objects()

    def arm_transient_erase_failure(self):
        """Wrap this worker's real publish callback to fail the next erase once.

        The wrapper delegates every non-failing call to the actual
        ``Worker._rdt_memory_info_callback`` (which reaches the native
        ``CoreWorker.set_rdt_object_memory_info``), so native stats are never
        fabricated — only the erase publication is made to fail once.
        """
        store = ray._private.worker.global_worker.rdt_manager.rdt_store
        original = store._memory_info_callback
        armed = {"value": True}

        def wrapped(object_id_bytes, device_bytes, num_copies, num_errors):
            if armed["value"] and num_copies == 0 and num_errors == 0:
                armed["value"] = False
                self._injected_erase_failure_seen = True
                raise RuntimeError("injected native erase publication failure")
            return original(object_id_bytes, device_bytes, num_copies, num_errors)

        store._memory_info_callback = wrapped

    def injected_erase_failure_seen(self):
        return self._injected_erase_failure_seen


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


def test_rdt_memory_transient_erase_failure_recovers_idle(ray_start_regular):
    # A transient native erase-publication failure must not leave a ghost row:
    # the producer's RDTStore retains a zero-state tombstone and a lazily
    # scheduled timer republishes the erase while the worker is otherwise idle.
    sender = RdtMemorySender.remote()
    create_collective_group([sender], backend="gloo")

    tensor = torch.ones(8, dtype=torch.float32)  # 8 * 4 = 32 logical bytes
    ref = sender.produce.remote(tensor)
    obj_id = ref.hex()
    sender_pid = ray.get(sender.pid.remote())

    # A successful nonzero row is published before the fault is armed.
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

    # Inject a one-shot failure on the real worker erase callback.
    ray.get(sender.arm_transient_erase_failure.remote(), timeout=10)

    # Drop the ref: the native GC pops the primary copy and the erase publish
    # fails once. No further add/pop is needed to recover.
    del ref
    wait_for_condition(
        lambda: ray.get(sender.num_rdt_objects.remote(), timeout=5) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # The native row must disappear while the worker is otherwise idle.
    wait_for_condition(
        lambda: _stats_present_rdt_gone(sender_pid, obj_id),
        timeout=15,
        retry_interval_ms=100,
    )

    # The fault genuinely fired (the erase callback failed once), and the worker
    # remains alive afterward.
    assert ray.get(sender.injected_erase_failure_seen.remote(), timeout=5)
    assert ray.get(sender.num_rdt_objects.remote(), timeout=5) == 0


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


def test_rdt_memory_retained_store_does_not_erase_new_runtime():
    # Shutting down with a retained RDT store (but not the native core worker
    # wrapper) must detach its publisher so popping that old store cannot write
    # into — or erase — the freshly initialized runtime's row. The Worker object
    # is reused across sessions, so a stale callback would resolve to the new
    # core worker and clobber it.
    obj_id = ray.ObjectRef.from_random().hex()

    def _driver_tensor_bytes():
        cw = _worker_stats_by_pid(os.getpid())
        if cw is None:
            return None
        entry = _rdt_object(cw, obj_id)
        return None if entry is None else entry["tensorBytes"]

    # Session 1: publish a row, then shutdown retaining only the store object.
    ray.init(
        "local",
        num_cpus=1,
        object_store_memory=150 * 1024 * 1024,
        include_dashboard=False,
    )
    try:
        store1 = ray._private.worker.global_worker.rdt_manager.rdt_store
        store1.add_object(obj_id, [torch.ones(8, dtype=torch.float32)])
        wait_for_condition(
            lambda: _driver_tensor_bytes() == {"cpu": 32},
            timeout=10,
            retry_interval_ms=100,
        )
    finally:
        ray.shutdown()

    # Session 2: the reused Worker's fresh store publishes a different size for
    # the same object id.
    ray.init(
        "local",
        num_cpus=1,
        object_store_memory=150 * 1024 * 1024,
        include_dashboard=False,
    )
    try:
        store2 = ray._private.worker.global_worker.rdt_manager.rdt_store
        store2.add_object(obj_id, [torch.ones(16, dtype=torch.float32)])
        wait_for_condition(
            lambda: _driver_tensor_bytes() == {"cpu": 64},
            timeout=10,
            retry_interval_ms=100,
        )

        # Popping the old retained store must leave the live new row untouched.
        store1.pop_object(obj_id)
        assert _driver_tensor_bytes() == {"cpu": 64}

        store2.pop_object(obj_id)
    finally:
        ray.shutdown()


def test_rdt_memory_held_core_worker_after_shutdown_is_safe(tmp_path):
    # The native setter must no-op (not QuickExit) when invoked on a CoreWorker
    # wrapper held across ray.shutdown(). Run in a subprocess because the old
    # path exits the process before printing the sentinel.
    script = tmp_path / "held_core_worker.py"
    script.write_text(
        "import ray\n"
        "\n"
        "ray.init('local', num_cpus=1, object_store_memory=150 * 1024 * 1024, "
        "include_dashboard=False)\n"
        "obj_id = bytes(ray.ObjectRef.size())\n"
        "core_worker = ray._private.worker.global_worker.core_worker\n"
        "ray.shutdown()\n"
        "# Held wrapper across shutdown: the old path QuickExits before the sentinel.\n"
        "core_worker.set_rdt_object_memory_info(obj_id, [('cpu', 8)], 1, 0)\n"
        "print('SENTINEL_OK', flush=True)\n"
    )
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "SENTINEL_OK" in result.stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
