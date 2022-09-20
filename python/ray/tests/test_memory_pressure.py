from math import ceil
import sys
import time

import psutil
import pytest

import ray
from ray._private import test_utils
from ray._private.test_utils import get_node_stats, wait_for_condition


memory_usage_threshold_fraction = 0.7
memory_monitor_interval_ms = 100
expected_worker_eviction_message = "System memory low at node with IP"
task_failure_entry_gc_period_ms = 1 * 1000
task_failure_entry_ttl_ms = 2 * 60 * 1000


@pytest.fixture
def ray_with_memory_monitor(shutdown_only):
    metrics_report_interval_ms = 100

    with ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
            "metrics_report_interval_ms": metrics_report_interval_ms,
            "task_failure_entry_gc_period_ms": task_failure_entry_gc_period_ms,
            "task_failure_entry_ttl_ms": task_failure_entry_ttl_ms,
        },
    ):
        yield


@ray.remote(max_retries=-1)
def inf_retry(
    allocate_bytes: int, num_chunks: int = 10, allocate_interval_s: float = 0
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start


@ray.remote(max_retries=0)
def no_retry(
    allocate_bytes: int,
    num_chunks: int = 10,
    allocate_interval_s: float = 0,
    post_allocate_sleep_s: float = 0,
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    time.sleep(post_allocate_sleep_s)
    return end - start


@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)

    def get_worker_id(self):
        return ray._private.worker.global_worker.core_worker.get_worker_id().hex()

    def get_actor_id(self):
        return ray._private.worker.global_worker.core_worker.get_actor_id().hex()


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = node_mem.total * pct - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return bytes_needed


def has_metric_tagged_with_value(tag, value) -> bool:
    raylet = ray.nodes()[0]
    reply = get_node_stats(raylet)
    for view in reply.view_data:
        for measure in view.measures:
            if tag in measure.tags:
                if hasattr(measure, "int_value"):
                    if measure.int_value == value:
                        return True
                if hasattr(measure, "double_value"):
                    if measure.double_value == value:
                        return True
    return False


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_actor(ray_with_memory_monitor):
    leaker = Leaker.remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.90)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.ActorEviction.Total",
        value=1.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.95)
    try:
        ray.get(no_retry.remote(bytes_to_alloc))
    except ray.exceptions.OutOfMemoryError as error:
        message = str(error)
        assert "no_retry()" in message
        assert "threshold 0.7" in message

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.TaskEviction.Total",
        value=1.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task_t(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    no_retry.remote(allocate_bytes=bytes_to_alloc, allocate_interval_s=0, post_allocate_sleep_s=1000)
    
    from ray._private.internal_api import memory_summary

    # node_table = ray._private.state.GlobalState().node_table()
    # print(node_table)
    # print(node_table[0])
    
    raylet_port = ray._private.worker._global_node._ray_params.node_manager_port
    raylet_address = ray._private.worker._global_node.raylet_ip_address

    import grpc
    from ray.core.generated import node_manager_pb2_grpc, node_manager_pb2

    # Kill a raylet gracefully.
    def kill_raylet(ip, port, graceful=True):
        raylet_address = f"{ip}:{port}"
        channel = grpc.insecure_channel(raylet_address)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        print(f"Sending a shutdown request to {ip}:{port}")
        stub.ShutdownRaylet(
            node_manager_pb2.ShutdownRayletRequest(graceful=graceful)
        )
        
    kill_raylet(raylet_address, raylet_port, graceful=False)

    time.sleep(1000)

    try:
        ray.get()
    except ray.exceptions.OutOfMemoryError as error:
        message = str(error)
        assert "no_retry()" in message
        assert "threshold 0.7" in message


    

@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_newest_worker(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )

    actor_ref = Leaker.options(name="actor").remote()
    ray.get(actor_ref.allocate.remote(bytes_to_alloc))

    try:
        ray.get(no_retry.remote(allocate_bytes=bytes_to_alloc))
    except ray.exceptions.OutOfMemoryError as error:
        message = str(error)
        assert "no_retry()" in message
        assert "Leaker" not in message
        assert "threshold 0.7" in message

    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "actor" in actors


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task_if_actor_submitted_task_first(
    ray_with_memory_monitor,
):
    actor_ref = Leaker.options(name="leaker1").remote()
    ray.get(actor_ref.allocate.remote(10))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )
    task_ref = no_retry.remote(
        allocate_bytes=bytes_to_alloc, allocate_interval_s=0, post_allocate_sleep_s=1000
    )

    ray.get(actor_ref.allocate.remote(bytes_to_alloc))
    try:
        ray.get(task_ref)
    except ray.exceptions.OutOfMemoryError as error:
        message = str(error)
        assert "no_retry()" in message
        assert "Leaker" not in message
        assert "threshold 0.7" in message

    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "leaker1" in actors


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
async def test_actor_oom_logs_error(ray_with_memory_monitor):
    first_actor = Leaker.options(name="first_random_actor").remote()
    ray.get(first_actor.get_worker_id.remote())

    oom_actor = Leaker.options(name="the_real_oom_actor").remote()
    worker_id = ray.get(oom_actor.get_worker_id.remote())
    actor_id = ray.get(oom_actor.get_actor_id.remote())

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(
            oom_actor.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3)
        )

    state_api_client = test_utils.get_local_state_client()
    result = await state_api_client.get_all_worker_info(timeout=5, limit=10)
    verified = False
    for worker in result.worker_table_data:
        if worker.worker_address.worker_id.hex() == worker_id:
            assert expected_worker_eviction_message in worker.exit_detail
            verified = True
    assert verified

    result = await state_api_client.get_all_actor_info(timeout=5, limit=10)
    verified = False
    for actor in result.actor_table_data:
        if actor.actor_id.hex() == actor_id:
            assert actor.death_cause
            assert actor.death_cause.actor_died_error_context
            assert (
                expected_worker_eviction_message
                in actor.death_cause.actor_died_error_context.error_message
            )
            verified = True
    assert verified

    # TODO(clarng): verify log info once state api can dump log info


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
async def test_task_oom_logs_error(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    try:
        ray.get(
            no_retry.remote(
                allocate_bytes=bytes_to_alloc,
                allocate_interval_s=0,
                post_allocate_sleep_s=1000,
            )
        )
    except ray.exceptions.OutOfMemoryError as error:
        message = str(error)
        assert "no_retry()" in message
        assert "threshold 0.7" in message

    state_api_client = test_utils.get_local_state_client()
    result = await state_api_client.get_all_worker_info(timeout=5, limit=10)
    verified = False
    for worker in result.worker_table_data:
        if worker.exit_detail:
            assert expected_worker_eviction_message in worker.exit_detail
        verified = True
    assert verified

    # TODO(clarng): verify task info once state_api_client.get_task_info
    # returns the crashed task.
    # TODO(clarng): verify log info once state api can dump log info


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
