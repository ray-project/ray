import os
import json
import grpc
import pytest
import requests
import time
import numpy as np

import ray
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.dashboard.memory import (ReferenceType, decode_object_ref_if_needed,
                                  MemoryTableEntry, MemoryTable, SortingType)
from ray.test_utils import (RayTestTimeoutException,
                            wait_until_succeeded_without_exception,
                            wait_until_server_available, wait_for_condition)

import psutil  # We must import psutil after ray because we bundle it with ray.


def test_worker_stats(shutdown_only):
    addresses = ray.init(num_cpus=1, include_dashboard=True)
    raylet = ray.nodes()[0]
    num_cpus = raylet["Resources"]["CPU"]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    ray.nodes()[0]["NodeManagerPort"])

    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)

    def try_get_node_stats(num_retry=5, timeout=2):
        reply = None
        for _ in range(num_retry):
            try:
                reply = stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(), timeout=timeout)
                break
            except grpc.RpcError:
                continue
        assert reply is not None
        return reply

    reply = try_get_node_stats()
    # Check that there is one connected driver.
    drivers = [worker for worker in reply.workers_stats if worker.is_driver]
    assert len(drivers) == 1
    assert os.getpid() == drivers[0].pid

    @ray.remote
    def f():
        ray.show_in_webui("test")
        return os.getpid()

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.show_in_webui("test")
            return os.getpid()

    # Test show_in_webui for remote functions.
    worker_pid = ray.get(f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for worker in reply.workers_stats:
        stats = worker.core_worker_stats
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    # Test show_in_webui for remote actors.
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())
    reply = try_get_node_stats()
    target_worker_present = False
    for worker in reply.workers_stats:
        stats = worker.core_worker_stats
        if stats.webui_display[""] == '{"message": "test", "dtype": "text"}':
            target_worker_present = True
            assert worker.pid == worker_pid
        else:
            assert stats.webui_display[""] == ""  # Empty proto
    assert target_worker_present

    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for worker processes")

        # Wait for the workers to start.
        if len(reply.workers_stats) < num_cpus + 1:
            time.sleep(1)
            reply = try_get_node_stats()
            continue

        # Check that the rest of the processes are workers, 1 for each CPU.
        assert len(reply.workers_stats) == num_cpus + 1
        views = [view.view_name for view in reply.view_data]
        assert "local_available_resource" in views
        # Check that all processes are Python.
        pids = [worker.pid for worker in reply.workers_stats]
        processes = [
            p.info["name"] for p in psutil.process_iter(attrs=["pid", "name"])
            if p.info["pid"] in pids
        ]
        for process in processes:
            # TODO(ekl) why does travis/mi end up in the process list
            assert ("python" in process or "mini" in process
                    or "conda" in process or "travis" in process
                    or "runner" in process or "ray" in process)
        break

    # Test kill_actor.
    def actor_killed(PID):
        """Check For the existence of a unix pid."""
        try:
            os.kill(PID, 0)
        except OSError:
            return True
        else:
            return False

    assert (wait_until_server_available(addresses["webui_url"]) is True)

    webui_url = addresses["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")
    for worker in reply.workers_stats:
        if worker.is_driver:
            continue
        requests.get(
            webui_url + "/api/kill_actor",
            params={
                "actor_id": ray.utils.binary_to_hex(
                    worker.core_worker_stats.actor_id),
                "ip_address": worker.core_worker_stats.ip_address,
                "port": worker.core_worker_stats.port
            })
    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException("Timed out while killing actors")
        if all(
                actor_killed(worker.pid) for worker in reply.workers_stats
                if not worker.is_driver):
            break


def test_raylet_info_endpoint(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    @ray.remote
    def f():
        return "test"

    @ray.remote(num_cpus=1)
    class ActorA:
        def __init__(self):
            pass

    @ray.remote(resources={"CustomResource": 1})
    class ActorB:
        def __init__(self):
            pass

    @ray.remote(num_cpus=2)
    class ActorC:
        def __init__(self):
            self.children = [ActorA.remote(), ActorB.remote()]

        def local_store(self):
            self.local_storage = [f.remote() for _ in range(10)]

        def remote_store(self):
            self.remote_storage = ray.put(np.zeros(200 * 1024, dtype=np.uint8))

        def getpid(self):
            return os.getpid()

    c = ActorC.remote()
    actor_pid = ray.get(c.getpid.remote())
    c.local_store.remote()
    c.remote_store.remote()

    assert (wait_until_server_available(addresses["webui_url"]) is True)

    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            webui_url = addresses["webui_url"]
            webui_url = webui_url.replace("localhost", "http://127.0.0.1")
            response = requests.get(webui_url + "/api/raylet_info")
            response.raise_for_status()
            try:
                raylet_info = response.json()
            except Exception as ex:
                print("failed response: {}".format(response.text))
                raise ex
            actor_info = raylet_info["result"]["actors"]
            try:
                assert len(actor_info) == 1
                _, parent_actor_info = actor_info.popitem()
                assert parent_actor_info["numObjectRefsInScope"] == 13
                assert parent_actor_info["numLocalObjects"] == 10
                children = parent_actor_info["children"]
                assert len(children) == 2
                break
            except AssertionError:
                if time.time() > start_time + 30:
                    raise Exception("Timed out while waiting for actor info \
                        or object store info update.")
        except requests.exceptions.ConnectionError:
            if time.time() > start_time + 30:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")

    def cpu_resources(actor_info):
        cpu_resources = 0
        for slot in actor_info["usedResources"]["CPU"]["resourceSlots"]:
            cpu_resources += slot["allocation"]
        return cpu_resources

    assert cpu_resources(parent_actor_info) == 2
    assert parent_actor_info["numExecutedTasks"] == 4
    for _, child_actor_info in children.items():
        if child_actor_info["state"] == -1:
            assert child_actor_info["requiredResources"]["CustomResource"] == 1
        else:
            assert child_actor_info[
                "state"] == ray.gcs_utils.ActorTableData.ALIVE
            assert len(child_actor_info["children"]) == 0
            assert cpu_resources(child_actor_info) == 1

    profiling_id = requests.get(
        webui_url + "/api/launch_profiling",
        params={
            "node_id": ray.nodes()[0]["NodeID"],
            "pid": actor_pid,
            "duration": 5
        }).json()["result"]
    start_time = time.time()
    while True:
        # Sometimes some startup time is required
        if time.time() - start_time > 30:
            raise RayTestTimeoutException(
                "Timed out while collecting profiling stats.")
        profiling_info = requests.get(
            webui_url + "/api/check_profiling_status",
            params={
                "profiling_id": profiling_id,
            }).json()
        status = profiling_info["result"]["status"]
        assert status in ("finished", "pending", "error")
        if status in ("finished", "error"):
            break
        time.sleep(1)


def test_raylet_infeasible_tasks(shutdown_only):
    """
    This test creates an actor that requires 5 GPUs
    but a ray cluster only has 3 GPUs. As a result,
    the new actor should be an infeasible actor.
    """
    addresses = ray.init(num_gpus=3)

    @ray.remote(num_gpus=5)
    class ActorRequiringGPU:
        def __init__(self):
            pass

    ActorRequiringGPU.remote()

    def test_infeasible_actor(ray_addresses):
        assert (wait_until_server_available(addresses["webui_url"]) is True)
        webui_url = ray_addresses["webui_url"].replace("localhost",
                                                       "http://127.0.0.1")
        raylet_info = requests.get(webui_url + "/api/raylet_info").json()
        actor_info = raylet_info["result"]["actors"]
        assert len(actor_info) == 1

        _, infeasible_actor_info = actor_info.popitem()
        assert infeasible_actor_info["state"] == -1
        assert infeasible_actor_info["invalidStateType"] == "infeasibleActor"

    assert (wait_until_succeeded_without_exception(
        test_infeasible_actor,
        (AssertionError, requests.exceptions.ConnectionError),
        addresses,
        timeout_ms=30000,
        retry_interval_ms=1000) is True)


def test_raylet_pending_tasks(shutdown_only):
    # Make sure to specify num_cpus. Otherwise, the test can be broken
    # when the number of cores is less than the number of spawned actors.
    addresses = ray.init(num_gpus=3, num_cpus=4)

    @ray.remote(num_gpus=1)
    class ActorRequiringGPU:
        def __init__(self):
            pass

    @ray.remote
    class ParentActor:
        def __init__(self):
            self.a = [ActorRequiringGPU.remote() for i in range(4)]

    # If we do not get ParentActor actor handler, reference counter will
    # terminate ParentActor.
    parent_actor = ParentActor.remote()
    assert parent_actor is not None

    def test_pending_actor(ray_addresses):
        assert (wait_until_server_available(addresses["webui_url"]) is True)
        webui_url = ray_addresses["webui_url"].replace("localhost",
                                                       "http://127.0.0.1")
        raylet_info = requests.get(webui_url + "/api/raylet_info").json()
        actor_info = raylet_info["result"]["actors"]
        assert len(actor_info) == 1
        _, infeasible_actor_info = actor_info.popitem()

        # Verify there are 4 spawned actors.
        children = infeasible_actor_info["children"]
        assert len(children) == 4

        pending_actor_detected = 0
        for child_id, child in children.items():
            if ("invalidStateType" in child
                    and child["invalidStateType"] == "pendingActor"):
                pending_actor_detected += 1
        # 4 GPUActors are spawned although there are only 3 GPUs.
        # One actor should be in the pending state.
        assert pending_actor_detected == 1

    assert (wait_until_succeeded_without_exception(
        test_pending_actor,
        (AssertionError, requests.exceptions.ConnectionError),
        addresses,
        timeout_ms=30000,
        retry_interval_ms=1000) is True)


@pytest.mark.skipif(
    os.environ.get("TRAVIS") is None,
    reason="This test requires password-less sudo due to py-spy requirement.")
def test_profiling_info_endpoint(shutdown_only):
    ray.init(num_cpus=1)

    redis_client = ray.worker.global_worker.redis_client

    node_ip = ray.nodes()[0]["NodeManagerAddress"]

    while True:
        reporter_port = redis_client.get("REPORTER_PORT:{}".format(node_ip))
        if reporter_port:
            break

    reporter_channel = grpc.insecure_channel("{}:{}".format(
        node_ip, int(reporter_port)))
    reporter_stub = reporter_pb2_grpc.ReporterServiceStub(reporter_channel)

    @ray.remote(num_cpus=1)
    class ActorA:
        def __init__(self):
            pass

        def getpid(self):
            return os.getpid()

    a = ActorA.remote()
    actor_pid = ray.get(a.getpid.remote())

    reply = reporter_stub.GetProfilingStats(
        reporter_pb2.GetProfilingStatsRequest(pid=actor_pid, duration=10))
    profiling_stats = json.loads(reply.profiling_stats)
    assert profiling_stats is not None


# This variable is used inside test_memory_dashboard.
# It is defined as a global variable to be used across all nested test
# functions. We use it because memory table is updated every one second,
# and we need to have a way to verify if the test is running with a fresh
# new memory table.
prev_memory_table = MemoryTable([]).__dict__()["group"]


def test_memory_dashboard(shutdown_only):
    """Test Memory table.

    These tests verify examples in this document.
    https://docs.ray.io/en/latest/memory-management.html#debugging-using-ray-memory
    """
    addresses = ray.init(num_cpus=2)
    webui_url = addresses["webui_url"].replace("localhost", "http://127.0.0.1")
    assert (wait_until_server_available(addresses["webui_url"]) is True)

    def get_memory_table():
        memory_table = requests.get(webui_url + "/api/memory_table").json()
        return memory_table["result"]

    def memory_table_ready():
        """Wait until the new fresh memory table is ready."""
        global prev_memory_table
        memory_table = get_memory_table()
        is_ready = memory_table["group"] != prev_memory_table
        prev_memory_table = memory_table["group"]
        return is_ready

    def stop_memory_table():
        requests.get(webui_url + "/api/stop_memory_table").json()

    def test_local_reference():
        @ray.remote
        def f(arg):
            return arg

        # a and b are local references.
        a = ray.put(None)  # Noqa F841
        b = f.remote(None)  # Noqa F841

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        group = memory_table["group"]
        assert summary["total_captured_in_objects"] == 0
        assert summary["total_pinned_in_memory"] == 0
        assert summary["total_used_by_pending_task"] == 0
        assert summary["total_local_ref_count"] == 2
        for table in group.values():
            for entry in table["entries"]:
                assert (
                    entry["reference_type"] == ReferenceType.LOCAL_REFERENCE)
        stop_memory_table()
        return True

    def test_object_pinned_in_memory():

        a = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
        b = ray.get(a)  # Noqa F841
        del a

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        group = memory_table["group"]
        assert summary["total_captured_in_objects"] == 0
        assert summary["total_pinned_in_memory"] == 1
        assert summary["total_used_by_pending_task"] == 0
        assert summary["total_local_ref_count"] == 0
        for table in group.values():
            for entry in table["entries"]:
                assert (
                    entry["reference_type"] == ReferenceType.PINNED_IN_MEMORY)
        stop_memory_table()
        return True

    def test_pending_task_references():
        @ray.remote
        def f(arg):
            time.sleep(1)

        a = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
        b = f.remote(a)

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        assert summary["total_captured_in_objects"] == 0
        assert summary["total_pinned_in_memory"] == 1
        assert summary["total_used_by_pending_task"] == 1
        assert summary["total_local_ref_count"] == 1
        # Make sure the function f is done before going to the next test.
        # Otherwise, the memory table will be corrupted because the
        # task f won't be done when the next test is running.
        ray.get(b)
        stop_memory_table()
        return True

    def test_serialized_object_ref_reference():
        @ray.remote
        def f(arg):
            time.sleep(1)

        a = ray.put(None)
        b = f.remote([a])  # Noqa F841

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        assert summary["total_captured_in_objects"] == 0
        assert summary["total_pinned_in_memory"] == 0
        assert summary["total_used_by_pending_task"] == 1
        assert summary["total_local_ref_count"] == 2
        # Make sure the function f is done before going to the next test.
        # Otherwise, the memory table will be corrupted because the
        # task f won't be done when the next test is running.
        ray.get(b)
        stop_memory_table()
        return True

    def test_captured_object_ref_reference():
        a = ray.put(None)
        b = ray.put([a])  # Noqa F841
        del a

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        assert summary["total_captured_in_objects"] == 1
        assert summary["total_pinned_in_memory"] == 0
        assert summary["total_used_by_pending_task"] == 0
        assert summary["total_local_ref_count"] == 1
        stop_memory_table()
        return True

    def test_actor_handle_reference():
        @ray.remote
        class Actor:
            pass

        a = Actor.remote()  # Noqa F841
        b = Actor.remote()  # Noqa F841
        c = Actor.remote()  # Noqa F841

        wait_for_condition(memory_table_ready)
        memory_table = get_memory_table()
        summary = memory_table["summary"]
        group = memory_table["group"]
        assert summary["total_captured_in_objects"] == 0
        assert summary["total_pinned_in_memory"] == 0
        assert summary["total_used_by_pending_task"] == 0
        assert summary["total_local_ref_count"] == 0
        assert summary["total_actor_handles"] == 3
        for table in group.values():
            for entry in table["entries"]:
                assert (entry["reference_type"] == ReferenceType.ACTOR_HANDLE)
        stop_memory_table()
        return True

    # These tests should be retried because it takes at least one second
    # to get the fresh new memory table. It is because memory table is updated
    # Whenever raylet and node info is renewed which takes 1 second.
    wait_for_condition(
        test_local_reference, timeout=30000, retry_interval_ms=1000)

    wait_for_condition(
        test_object_pinned_in_memory, timeout=30000, retry_interval_ms=1000)

    wait_for_condition(
        test_pending_task_references, timeout=30000, retry_interval_ms=1000)

    wait_for_condition(
        test_serialized_object_ref_reference,
        timeout=30000,
        retry_interval_ms=1000)

    wait_for_condition(
        test_captured_object_ref_reference,
        timeout=30000,
        retry_interval_ms=1000)

    wait_for_condition(
        test_actor_handle_reference, timeout=30000, retry_interval_ms=1000)


"""Memory Table Unit Test"""

NODE_ADDRESS = "127.0.0.1"
IS_DRIVER = True
PID = 1
OBJECT_ID = "7wpsIhgZiBz/////AQAAyAEAAAA="
ACTOR_ID = "fffffffffffffffff66d17ba010000c801000000"
DECODED_ID = decode_object_ref_if_needed(OBJECT_ID)
OBJECT_SIZE = 100


def build_memory_entry(*,
                       local_ref_count,
                       pinned_in_memory,
                       submitted_task_reference_count,
                       contained_in_owned,
                       object_size,
                       pid,
                       object_id=OBJECT_ID,
                       node_address=NODE_ADDRESS):
    object_ref = {
        "objectId": object_id,
        "callSite": "(task call) /Users:458",
        "objectSize": object_size,
        "localRefCount": local_ref_count,
        "pinnedInMemory": pinned_in_memory,
        "submittedTaskRefCount": submitted_task_reference_count,
        "containedInOwned": contained_in_owned
    }
    return MemoryTableEntry(
        object_ref=object_ref,
        node_address=node_address,
        is_driver=IS_DRIVER,
        pid=pid)


def build_local_reference_entry(object_size=OBJECT_SIZE,
                                pid=PID,
                                node_address=NODE_ADDRESS):
    return build_memory_entry(
        local_ref_count=1,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address)


def build_used_by_pending_task_entry(object_size=OBJECT_SIZE,
                                     pid=PID,
                                     node_address=NODE_ADDRESS):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=2,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address)


def build_captured_in_object_entry(object_size=OBJECT_SIZE,
                                   pid=PID,
                                   node_address=NODE_ADDRESS):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[OBJECT_ID],
        object_size=object_size,
        pid=pid,
        node_address=node_address)


def build_actor_handle_entry(object_size=OBJECT_SIZE,
                             pid=PID,
                             node_address=NODE_ADDRESS):
    return build_memory_entry(
        local_ref_count=1,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
        object_id=ACTOR_ID)


def build_pinned_in_memory_entry(object_size=OBJECT_SIZE,
                                 pid=PID,
                                 node_address=NODE_ADDRESS):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=True,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address)


def build_entry(object_size=OBJECT_SIZE,
                pid=PID,
                node_address=NODE_ADDRESS,
                reference_type=ReferenceType.PINNED_IN_MEMORY):
    if reference_type == ReferenceType.USED_BY_PENDING_TASK:
        return build_used_by_pending_task_entry(
            pid=pid, object_size=object_size, node_address=node_address)
    elif reference_type == ReferenceType.LOCAL_REFERENCE:
        return build_local_reference_entry(
            pid=pid, object_size=object_size, node_address=node_address)
    elif reference_type == ReferenceType.PINNED_IN_MEMORY:
        return build_pinned_in_memory_entry(
            pid=pid, object_size=object_size, node_address=node_address)
    elif reference_type == ReferenceType.ACTOR_HANDLE:
        return build_actor_handle_entry(
            pid=pid, object_size=object_size, node_address=node_address)
    elif reference_type == ReferenceType.CAPTURED_IN_OBJECT:
        return build_captured_in_object_entry(
            pid=pid, object_size=object_size, node_address=node_address)


def test_invalid_memory_entry():
    memory_entry = build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=OBJECT_SIZE,
        pid=PID)
    assert memory_entry.is_valid() is False
    memory_entry = build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=-1,
        pid=PID)
    assert memory_entry.is_valid() is False


def test_valid_reference_memory_entry():
    memory_entry = build_local_reference_entry()
    assert memory_entry.reference_type == ReferenceType.LOCAL_REFERENCE
    assert memory_entry.object_ref == ray.ObjectRef(
        decode_object_ref_if_needed(OBJECT_ID))
    assert memory_entry.is_valid() is True


def test_reference_type():
    # pinned in memory
    memory_entry = build_pinned_in_memory_entry()
    assert memory_entry.reference_type == ReferenceType.PINNED_IN_MEMORY

    # used by pending task
    memory_entry = build_used_by_pending_task_entry()
    assert memory_entry.reference_type == ReferenceType.USED_BY_PENDING_TASK

    # captued in object
    memory_entry = build_captured_in_object_entry()
    assert memory_entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT

    # actor handle
    memory_entry = build_actor_handle_entry()
    assert memory_entry.reference_type == ReferenceType.ACTOR_HANDLE


def test_memory_table_summary():
    entries = [
        build_pinned_in_memory_entry(),
        build_used_by_pending_task_entry(),
        build_captured_in_object_entry(),
        build_actor_handle_entry(),
        build_local_reference_entry(),
        build_local_reference_entry()
    ]
    memory_table = MemoryTable(entries)
    assert len(memory_table.group) == 1
    assert memory_table.summary["total_actor_handles"] == 1
    assert memory_table.summary["total_captured_in_objects"] == 1
    assert memory_table.summary["total_local_ref_count"] == 2
    assert memory_table.summary[
        "total_object_size"] == len(entries) * OBJECT_SIZE
    assert memory_table.summary["total_pinned_in_memory"] == 1
    assert memory_table.summary["total_used_by_pending_task"] == 1


def test_memory_table_sort_by_pid():
    unsort = [1, 3, 2]
    entries = [build_entry(pid=pid) for pid in unsort]
    memory_table = MemoryTable(entries, sort_by_type=SortingType.PID)
    sort = sorted(unsort)
    for pid, entry in zip(sort, memory_table.table):
        assert pid == entry.pid


def test_memory_table_sort_by_reference_type():
    unsort = [
        ReferenceType.USED_BY_PENDING_TASK, ReferenceType.LOCAL_REFERENCE,
        ReferenceType.LOCAL_REFERENCE, ReferenceType.PINNED_IN_MEMORY
    ]
    entries = [
        build_entry(reference_type=reference_type) for reference_type in unsort
    ]
    memory_table = MemoryTable(
        entries, sort_by_type=SortingType.REFERENCE_TYPE)
    sort = sorted(unsort)
    for reference_type, entry in zip(sort, memory_table.table):
        assert reference_type == entry.reference_type


def test_memory_table_sort_by_object_size():
    unsort = [312, 214, -1, 1244, 642]
    entries = [build_entry(object_size=object_size) for object_size in unsort]
    memory_table = MemoryTable(entries, sort_by_type=SortingType.OBJECT_SIZE)
    sort = sorted(unsort)
    for object_size, entry in zip(sort, memory_table.table):
        assert object_size == entry.object_size


def test_group_by():
    node_second = "127.0.0.2"
    node_first = "127.0.0.1"
    entries = [
        build_entry(node_address=node_second, pid=2),
        build_entry(node_address=node_second, pid=1),
        build_entry(node_address=node_first, pid=2),
        build_entry(node_address=node_first, pid=1)
    ]
    memory_table = MemoryTable(entries)

    # Make sure it is correctly grouped
    assert node_first in memory_table.group
    assert node_second in memory_table.group

    # make sure pid is sorted in the right order.
    for group_key, group_memory_table in memory_table.group.items():
        pid = 1
        for entry in group_memory_table.table:
            assert pid == entry.pid
            pid += 1


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
