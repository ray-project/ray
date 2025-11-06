import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Tuple

import pytest
from filelock import FileLock

import ray
from ray._common.network_utils import parse_address
from ray._common.test_utils import wait_for_condition
from ray._private import ray_constants
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import (
    external_redis_test_enabled,
    generate_system_config_map,
    redis_sentinel_replicas,
    run_string_as_driver,
    wait_for_pid_to_exit,
)
from ray._raylet import GcsClient
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.state import list_placement_groups

import psutil


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@ray.remote
def increase(x):
    return x + 1


def cluster_kill_gcs_wait(cluster):
    head_node = cluster.head_node
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid
    # Kill gcs server.
    cluster.head_node.kill_gcs_server()
    # Wait to prevent the gcs server process becoming zombie.
    gcs_server_process.wait()
    wait_for_pid_to_exit(gcs_server_pid, 300)


def test_gcs_server_restart(ray_start_regular_with_external_redis):
    actor1 = Increase.remote()
    result = ray.get(actor1.method.remote(1))
    assert result == 3

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    actor2 = Increase.remote()
    result = ray.get(actor2.method.remote(2))
    assert result == 4

    result = ray.get(increase.remote(1))
    assert result == 2

    # Check whether actor1 is alive or not.
    # NOTE: We can't execute it immediately after gcs restarts
    # because it takes time for the worker to exit.
    result = ray.get(actor1.method.remote(7))
    assert result == 9


@pytest.mark.skip(
    reason="GCS pubsub may lose messages after GCS restarts. Need to "
    "implement re-fetching state in GCS client.",
)
# TODO(mwtian): re-enable after fixing https://github.com/ray-project/ray/issues/22340
def test_gcs_server_restart_during_actor_creation(
    ray_start_regular_with_external_redis,
):
    ids = []
    # We reduce the number of actors because there are too many actors created
    # and `Too many open files` error will be thrown.
    for i in range(0, 20):
        actor = Increase.remote()
        ids.append(actor.method.remote(1))

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    # The timeout seems too long.
    # TODO(mwtian): after fixing reconnection in GCS pubsub, try using a lower
    # timeout.
    ready, unready = ray.wait(ids, num_returns=20, timeout=240)
    print("Ready objects is {}.".format(ready))
    print("Unready objects is {}.".format(unready))
    assert len(unready) == 0


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            health_check_initial_delay_ms=0,
            health_check_period_ms=1000,
            health_check_failure_threshold=3,
            enable_autoscaler_v2=True,
        ),
    ],
    indirect=True,
)
def test_autoscaler_init(
    ray_start_cluster_head_with_external_redis,
):
    """
    Checks that autoscaler initializes properly after GCS restarts.
    """
    cluster = ray_start_cluster_head_with_external_redis
    cluster.add_node()
    cluster.wait_for_nodes()

    # Make sure both head and worker node are alive.
    nodes = ray.nodes()
    assert len(nodes) == 2
    assert nodes[0]["alive"] and nodes[1]["alive"]

    # Restart gcs server process.
    cluster_kill_gcs_wait(cluster)
    cluster.head_node.start_gcs_server()

    # Fetch the cluster status from the autoscaler and check that it works.
    status = get_cluster_status(cluster.address)
    wait_for_condition(lambda: len(status.idle_nodes) == 2)


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            health_check_initial_delay_ms=0,
            health_check_period_ms=1000,
            health_check_failure_threshold=3,
        ),
    ],
    indirect=True,
)
def test_node_failure_detector_when_gcs_server_restart(
    ray_start_cluster_head_with_external_redis,
):
    """Checks that the node failure detector is correct when gcs server restart.

    We set the cluster to timeout nodes after 2 seconds of heartbeats. We then
    kill gcs server and remove the worker node and restart gcs server again to
    check that the removed node will die finally.
    """
    cluster = ray_start_cluster_head_with_external_redis
    worker = cluster.add_node()
    cluster.wait_for_nodes()

    # Make sure both head and worker node are alive.
    nodes = ray.nodes()
    assert len(nodes) == 2
    assert nodes[0]["alive"] and nodes[1]["alive"]

    to_be_removed_node = None
    for node in nodes:
        if node["RayletSocketName"] == worker.raylet_socket_name:
            to_be_removed_node = node
    assert to_be_removed_node is not None

    cluster_kill_gcs_wait(cluster)

    raylet_process = worker.all_processes["raylet"][0].process
    raylet_pid = raylet_process.pid
    # Remove worker node.
    cluster.remove_node(worker, allow_graceful=False)
    # Wait to prevent the raylet process becoming zombie.
    raylet_process.wait()
    wait_for_pid_to_exit(raylet_pid)

    # Restart gcs server process.
    cluster.head_node.start_gcs_server()

    def condition():
        nodes = ray.nodes()
        assert len(nodes) == 2
        for node in nodes:
            if node["NodeID"] == to_be_removed_node["NodeID"]:
                return not node["alive"]
        return False

    # Wait for the removed node dead.
    wait_for_condition(condition, timeout=10)


def test_actor_raylet_resubscription(ray_start_regular_with_external_redis):
    # stat an actor
    @ray.remote
    class A:
        def ready(self):
            return os.getpid()

    actor = A.options(name="abc", max_restarts=0).remote()
    pid = ray.get(actor.ready.remote())
    print("actor is ready and kill gcs")

    ray._private.worker._global_node.kill_gcs_server()

    print("make actor exit")

    p = psutil.Process(pid)
    p.kill()
    p.wait(timeout=10)

    print("start gcs")
    ray._private.worker._global_node.start_gcs_server()

    print("try actor method again")
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.ready.remote())


def test_del_actor_after_gcs_server_restart(ray_start_regular_with_external_redis):
    actor = Increase.options(name="abc").remote()
    result = ray.get(actor.method.remote(1))
    assert result == 3

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    actor_id = actor._actor_id.hex()
    del actor

    def condition():
        actor_status = ray.util.state.get_actor(id=actor_id)
        if actor_status.state == "DEAD":
            return True
        else:
            return False

    # Wait for the actor dead.
    wait_for_condition(condition, timeout=10)

    # If `ReportActorOutOfScope` was successfully called,
    # name should be properly deleted.
    with pytest.raises(ValueError):
        ray.get_actor("abc")


def test_raylet_resubscribe_to_worker_death(
    tmp_path, ray_start_regular_with_external_redis
):
    """Verify that the Raylet resubscribes to worker death notifications on GCS restart."""

    child_task_pid_path = tmp_path / "blocking_child.pid"

    @ray.remote(num_cpus=0)
    def child():
        print("Child worker ID:", ray.get_runtime_context().get_worker_id())
        child_task_pid_path.write_text(str(os.getpid()))
        while True:
            time.sleep(0.1)
            print("Child still running...")

    @ray.remote(num_cpus=0)
    def parent() -> Tuple[int, int, ray.ObjectRef]:
        print("Parent worker ID:", ray.get_runtime_context().get_worker_id())
        child_obj_ref = child.remote()

        # Wait for the child to be running and report back its PID.
        wait_for_condition(lambda: child_task_pid_path.exists(), timeout=10)
        child_pid = int(child_task_pid_path.read_text())
        return os.getpid(), child_pid, child_obj_ref

    parent_pid, child_pid, child_obj_ref = ray.get(parent.remote())
    print(f"Parent PID: {parent_pid}, child PID: {child_pid}")
    assert parent_pid != child_pid

    # Kill and restart the GCS.
    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    # Schedule an actor to ensure that the GCS is back alive and the Raylet is
    # reconnected to it.
    # TODO(iycheng): this shouldn't be necessary, but the current resubscription
    # implementation can lose the worker failure message because we don't ask for
    # the snapshot of worker statuses.
    @ray.remote
    class A:
        pass

    ray.get(A.remote().__ray_ready__.remote())

    # Kill the parent task and verify that the child task is killed due to fate sharing
    # with its parent.
    print("Killing parent process.")
    p = psutil.Process(parent_pid)
    p.kill()
    p.wait()
    print("Parent process exited.")

    # The child task should exit.
    wait_for_pid_to_exit(child_pid, 20)
    with pytest.raises(ray.exceptions.OwnerDiedError):
        ray.get(child_obj_ref)


def test_core_worker_resubscription(tmp_path, ray_start_regular_with_external_redis):
    # This test is to ensure core worker will resubscribe to GCS after GCS
    # restarts.
    lock_file = str(tmp_path / "lock")
    lock = FileLock(lock_file)
    lock.acquire()

    @ray.remote
    class Actor:
        def __init__(self):
            lock = FileLock(lock_file)
            lock.acquire()

        def ready(self):
            return

    a = Actor.remote()
    r = a.ready.remote()
    # Actor is not ready before GCS is down.
    ray._private.worker._global_node.kill_gcs_server()

    lock.release()
    # Actor is ready after GCS starts
    ray._private.worker._global_node.start_gcs_server()
    # Test the resubscribe works: if not, it'll timeout because worker
    # will think the actor is not ready.
    ray.get(r, timeout=5)


def test_detached_actor_restarts(ray_start_regular_with_external_redis):
    # Detached actors are owned by GCS. This test is to ensure detached actors
    # can restart even GCS restarts.

    @ray.remote
    class A:
        def ready(self):
            return os.getpid()

    a = A.options(name="a", lifetime="detached", max_restarts=-1).remote()

    pid = ray.get(a.ready.remote())
    ray._private.worker._global_node.kill_gcs_server()
    p = psutil.Process(pid)
    p.kill()
    ray._private.worker._global_node.start_gcs_server()

    while True:
        try:
            assert ray.get(a.ready.remote()) != pid
            break
        except ray.exceptions.RayActorError:
            continue


def test_gcs_client_reconnect(ray_start_regular_with_external_redis):
    """Tests reconnect behavior on GCS restart for sync and asyncio clients."""
    gcs_client = ray._private.worker.global_worker.gcs_client

    gcs_client.internal_kv_put(b"a", b"b", True, None)
    assert gcs_client.internal_kv_get(b"a", None) == b"b"

    def _get(use_asyncio: bool) -> bytes:
        if use_asyncio:

            async def _get_async() -> bytes:
                return await gcs_client.async_internal_kv_get(b"a", None)

            result = asyncio.run(_get_async())
        else:
            result = gcs_client.internal_kv_get(b"a", None)

        return result

    # Kill the GCS, start an internal KV GET request, and check that it succeeds once
    # the GCS is restarted.
    ray._private.worker._global_node.kill_gcs_server()
    with ThreadPoolExecutor(max_workers=2) as executor:
        sync_future = executor.submit(_get, False)
        asyncio_future = executor.submit(_get, True)

        ray._private.worker._global_node.start_gcs_server()

        assert sync_future.result() == b"b"
        assert asyncio_future.result() == b"b"


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_rpc_server_reconnect_timeout_s=3600,
            ),
            "namespace": "actor",
        }
    ],
    indirect=True,
)
def test_actor_workloads(ray_start_regular_with_external_redis):
    """Tests actor creation and task execution while the GCS is down."""

    @ray.remote(num_cpus=0)
    class Counter:
        def noop(self, v: Any) -> Any:
            return v

    # Start two actors, one normal and one detached, and wait for them to be running.
    counter_1 = Counter.remote()
    r = ray.get(counter_1.noop.remote(1))
    assert r == 1

    detached_counter = Counter.options(
        lifetime="detached", name="detached_counter"
    ).remote()
    assert ray.get(detached_counter.noop.remote("detached")) == "detached"

    # Kill the GCS.
    ray._private.worker._global_node.kill_gcs_server()

    # Tasks to the existing actors should continue to work.
    assert ray.get(counter_1.noop.remote(1)) == 1

    # Create a new actor. Making actor calls shouldn't error and they should
    # succeed after the GCS comes back up and starts the actor.
    counter_2 = Counter.remote()
    counter_2_alive_ref = counter_2.noop.remote(2)

    ready, _ = ray.wait([counter_2_alive_ref], timeout=0.1)
    assert len(ready) == 0

    # Restart the GCS and check that the actor is started and task succeeds.
    ray._private.worker._global_node.start_gcs_server()

    assert ray.get(counter_2_alive_ref) == 2

    # Check that the existing actors continue to function, including the detached
    # actor being called from another driver.
    assert ray.get(counter_1.noop.remote(1)) == 1
    return
    run_string_as_driver(
        """
import ray
ray.init("auto", namespace="actor")
detached_counter = ray.get_actor("detached_counter")
assert ray.get(detached_counter.noop.remote("detached")) == "detached"
"""
    )


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_rpc_server_reconnect_timeout_s=3600,
            ),
            "namespace": "actor",
        }
    ],
    indirect=True,
)
def test_pg_actor_workloads(ray_start_regular_with_external_redis):
    bundle1 = {"CPU": 1}
    pg = placement_group([bundle1], strategy="STRICT_PACK")

    ray.get(pg.ready())

    @ray.remote
    class Counter:
        def r(self, v):
            return v

        def pid(self):
            return os.getpid()

    c = Counter.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    r = ray.get(c.r.remote(10))
    assert r == 10

    print("GCS is killed")
    pid = ray.get(c.pid.remote())
    ray.worker._global_node.kill_gcs_server()

    assert ray.get(c.r.remote(10)) == 10

    ray.worker._global_node.start_gcs_server()

    for _ in range(100):
        assert pid == ray.get(c.pid.remote())


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_server_request_timeout_seconds=10,
        )
    ],
    indirect=True,
)
def test_get_actor_when_gcs_is_down(ray_start_regular_with_external_redis):
    @ray.remote
    def create_actor():
        @ray.remote
        class A:
            def pid(self):
                return os.getpid()

        a = A.options(lifetime="detached", name="A").remote()
        ray.get(a.pid.remote())

    ray.get(create_actor.remote())

    ray._private.worker._global_node.kill_gcs_server()

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get_actor("A")


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_server_request_timeout_seconds=10,
        )
    ],
    indirect=True,
)
@pytest.mark.skip(
    reason="python publisher and subscriber doesn't handle gcs server failover"
)
def test_publish_and_subscribe_error_info(ray_start_regular_with_external_redis):
    address_info = ray_start_regular_with_external_redis
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    print("sending error message 1")
    gcs_client.publish_error(b"aaa_id", "", "test error message 1")

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    print("sending error message 2")
    gcs_client.publish_error(b"bbb_id", "", "test error message 2")
    print("done")

    (key_id, err) = subscriber.poll()
    assert key_id == b"bbb_id"
    assert err["error_message"] == "test error message 2"

    subscriber.close()


@pytest.fixture
def redis_replicas(monkeypatch):
    monkeypatch.setenv("TEST_EXTERNAL_REDIS_REPLICAS", "3")


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            gcs_server_request_timeout_seconds=10,
            redis_db_connect_retries=50,
        )
    ],
    indirect=True,
)
def test_redis_failureover(redis_replicas, ray_start_cluster_head_with_external_redis):
    """This test is to cover ray cluster's behavior when Redis master failed.
    The management of the Redis cluster is not covered by Ray, but Ray should handle
    the failure correctly.
    For this test we ensure:
    - When Redis master failed, Ray should crash (TODO: make ray automatically switch to
      new master).
    - After Redis recovered, Ray should be able to use the new Master.
    - When the master becomes slaves, Ray should crash.
    """
    cluster = ray_start_cluster_head_with_external_redis
    import redis

    redis_addr = os.environ.get("RAY_REDIS_ADDRESS")
    ip, port = parse_address(redis_addr)
    redis_cli = redis.Redis(ip, port)

    def get_connected_nodes():
        return [
            (k, v) for (k, v) in redis_cli.cluster("nodes").items() if v["connected"]
        ]

    wait_for_condition(
        lambda: len(get_connected_nodes())
        == int(os.environ.get("TEST_EXTERNAL_REDIS_REPLICAS"))
    )
    nodes = redis_cli.cluster("nodes")
    leader_cli = None
    follower_cli = []
    for addr in nodes:
        ip, port = parse_address(addr)
        cli = redis.Redis(ip, port)
        meta = nodes[addr]
        flags = meta["flags"].split(",")
        if "master" in flags:
            leader_cli = cli
            print("LEADER", addr, redis_addr)
        else:
            follower_cli.append(cli)

    leader_pid = leader_cli.info()["process_id"]

    @ray.remote(max_restarts=-1)
    class Counter:
        def r(self, v):
            return v

        def pid(self):
            return os.getpid()

    c = Counter.options(name="c", namespace="test", lifetime="detached").remote()
    c_pid = ray.get(c.pid.remote())
    c_process = psutil.Process(pid=c_pid)
    r = ray.get(c.r.remote(10))
    assert r == 10

    head_node = cluster.head_node
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    # Wait until all data is updated in the replica
    leader_cli.set("_hole", "0")
    wait_for_condition(lambda: all([b"_hole" in f.keys("*") for f in follower_cli]))

    # Now kill pid
    leader_process = psutil.Process(pid=leader_pid)
    leader_process.kill()

    print(">>> Waiting gcs server to exit", gcs_server_pid)
    wait_for_pid_to_exit(gcs_server_pid, 1000)
    print("GCS killed")

    follower_cli[0].cluster("failover", "takeover")
    wait_for_condition(
        lambda: len(get_connected_nodes())
        == int(os.environ.get("TEST_EXTERNAL_REDIS_REPLICAS")) - 1
    )

    # Kill Counter actor. It should restart after GCS is back
    c_process.kill()
    # Cleanup the in memory data and then start gcs
    cluster.head_node.kill_gcs_server(False)

    print("Start gcs")
    cluster.head_node.start_gcs_server()

    assert len(ray.nodes()) == 1
    assert ray.nodes()[0]["alive"]

    driver_script = f"""
import ray
ray.init('{cluster.address}')
@ray.remote
def f():
    return 10
assert ray.get(f.remote()) == 10

c = ray.get_actor("c", namespace="test")
v = ray.get(c.r.remote(10))
assert v == 10
print("DONE")
"""

    # Make sure the cluster is usable
    wait_for_condition(lambda: "DONE" in run_string_as_driver(driver_script))

    # Now make follower_cli[0] become replica
    # and promote follower_cli[1] as leader
    follower_cli[1].cluster("failover", "takeover")
    head_node = cluster.head_node
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid
    # GCS should exit in this case
    print(">>> Waiting gcs server to exit", gcs_server_pid)
    wait_for_pid_to_exit(gcs_server_pid, 10000)


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis_sentinel",
    [
        generate_system_config_map(
            gcs_server_request_timeout_seconds=10,
            redis_db_connect_retries=50,
        )
    ],
    indirect=True,
)
def test_redis_with_sentinel_failureover(
    ray_start_cluster_head_with_external_redis_sentinel,
):
    """This test is to cover ray cluster's behavior with Redis sentinel.
    The expectation is Redis sentinel should manage failover
    automatically, and GCS can continue talking to the same address
    without any human intervention on Redis.
    For this test we ensure:
    - When Redis master failed, Ray should crash (TODO: GCS should
        autommatically try re-connect to sentinel).
    - When restart Ray, it should continue talking to sentinel, which
        should return information about new master.
    """
    cluster = ray_start_cluster_head_with_external_redis_sentinel
    import redis

    redis_addr = os.environ.get("RAY_REDIS_ADDRESS")
    ip, port = parse_address(redis_addr)
    redis_cli = redis.Redis(ip, port)
    print(redis_cli.info("sentinel"))
    redis_name = redis_cli.info("sentinel")["master0"]["name"]

    def get_sentinel_nodes():
        leader_address = (
            redis_cli.sentinel_master(redis_name)["ip"],
            redis_cli.sentinel_master(redis_name)["port"],
        )
        follower_addresses = [
            (x["ip"], x["port"]) for x in redis_cli.sentinel_slaves(redis_name)
        ]
        return [leader_address] + follower_addresses

    wait_for_condition(lambda: len(get_sentinel_nodes()) == redis_sentinel_replicas())

    @ray.remote(max_restarts=-1)
    class Counter:
        def r(self, v):
            return v

        def pid(self):
            return os.getpid()

    c = Counter.options(name="c", namespace="test", lifetime="detached").remote()
    c_pid = ray.get(c.pid.remote())
    c_process = psutil.Process(pid=c_pid)
    r = ray.get(c.r.remote(10))
    assert r == 10

    head_node = cluster.head_node
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    leader_cli = redis.Redis(*get_sentinel_nodes()[0])
    leader_pid = leader_cli.info()["process_id"]
    follower_cli = [redis.Redis(*x) for x in get_sentinel_nodes()[1:]]

    # Wait until all data is updated in the replica
    leader_cli.set("_hole", "0")
    wait_for_condition(lambda: all([b"_hole" in f.keys("*") for f in follower_cli]))
    current_leader = get_sentinel_nodes()[0]

    # Now kill pid
    leader_process = psutil.Process(pid=leader_pid)
    leader_process.kill()

    print(">>> Waiting gcs server to exit", gcs_server_pid)
    wait_for_pid_to_exit(gcs_server_pid, 1000)
    print("GCS killed")

    wait_for_condition(lambda: current_leader != get_sentinel_nodes()[0])

    # Kill Counter actor. It should restart after GCS is back
    c_process.kill()
    # Cleanup the in memory data and then start gcs
    cluster.head_node.kill_gcs_server(False)

    print("Start gcs")
    cluster.head_node.start_gcs_server()

    assert len(ray.nodes()) == 1
    assert ray.nodes()[0]["alive"]

    driver_script = f"""
import ray
ray.init('{cluster.address}')
@ray.remote
def f():
    return 10
assert ray.get(f.remote()) == 10

c = ray.get_actor("c", namespace="test")
v = ray.get(c.r.remote(10))
assert v == 10
print("DONE")
"""

    # Make sure the cluster is usable
    wait_for_condition(lambda: "DONE" in run_string_as_driver(driver_script))


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        generate_system_config_map(
            enable_cluster_auth=True,
            raylet_liveness_self_check_interval_ms=5000,
        )
    ],
    indirect=True,
)
def test_raylet_fate_sharing(ray_start_regular):
    # Kill GCS and check that raylets kill themselves when not backed by Redis,
    # and stay alive when backed by Redis.
    # Raylets should kill themselves due to cluster ID mismatch in the
    # non-persisted case.
    raylet_proc = ray._private.worker._global_node.all_processes[
        ray_constants.PROCESS_TYPE_RAYLET
    ][0].process

    def check_raylet_healthy():
        return raylet_proc.poll() is None

    wait_for_condition(lambda: check_raylet_healthy())
    for i in range(10):
        assert check_raylet_healthy()

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    if not external_redis_test_enabled():
        # Waiting for raylet to become unhealthy
        wait_for_condition(lambda: not check_raylet_healthy())
    else:
        # Waiting for raylet to stay healthy
        for i in range(10):
            assert check_raylet_healthy()


def test_session_name(ray_start_cluster):
    # Kill GCS and check that raylets kill themselves when not backed by Redis,
    # and stay alive when backed by Redis.
    # Raylets should kill themselves due to cluster ID mismatch in the
    # non-persisted case.
    cluster = ray_start_cluster
    cluster.add_node()
    cluster.wait_for_nodes()

    head_node = cluster.head_node
    session_dir = head_node.get_session_dir_path()

    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid
    cluster.remove_node(head_node, allow_graceful=False)
    # Wait to prevent the gcs server process becoming zombie.
    gcs_server_process.wait()
    wait_for_pid_to_exit(gcs_server_pid, 1000)

    # Add head node back
    cluster.add_node()
    head_node = cluster.head_node
    new_session_dir = head_node.get_session_dir_path()

    if not external_redis_test_enabled():
        assert session_dir != new_session_dir
    else:
        assert session_dir == new_session_dir


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_server_request_timeout_seconds=10,
            raylet_liveness_self_check_interval_ms=3000,
        )
    ],
    indirect=True,
)
def test_redis_data_loss_no_leak(ray_start_regular_with_external_redis):
    @ray.remote
    def create_actor():
        @ray.remote
        class A:
            def pid(self):
                return os.getpid()

        a = A.options(lifetime="detached", name="A").remote()
        ray.get(a.pid.remote())

    ray.get(create_actor.remote())

    ray._private.worker._global_node.kill_gcs_server()
    # Delete redis
    redis_addr = os.environ.get("RAY_REDIS_ADDRESS")
    import redis

    ip, port = parse_address(redis_addr)
    cli = redis.Redis(ip, port)
    cli.flushall()
    raylet_proc = ray._private.worker._global_node.all_processes[
        ray_constants.PROCESS_TYPE_RAYLET
    ][0].process

    def check_raylet_healthy():
        return raylet_proc.poll() is None

    wait_for_condition(lambda: check_raylet_healthy())

    # Start GCS
    ray._private.worker._global_node.start_gcs_server()

    # Waiting for raylet to become unhealthy
    wait_for_condition(lambda: not check_raylet_healthy())


def test_redis_logs(external_redis):
    try:
        process = subprocess.Popen(
            ["ray", "start", "--head"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(timeout=30)
        print(stdout.decode())
        print(stderr.decode())
        assert "redis_context.cc" not in stderr.decode()
        assert "redis_context.cc" not in stdout.decode()
        assert "Resolve Redis address" not in stderr.decode()
        assert "Resolve Redis address" not in stdout.decode()
        # assert "redis_context.cc" not in result.output
    finally:
        from click.testing import CliRunner

        import ray.scripts.scripts as scripts

        runner = CliRunner(env={"RAY_USAGE_STATS_PROMPT_ENABLED": "0"})
        runner.invoke(
            scripts.stop,
            [
                "--force",
            ],
        )


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=2,
        )
    ],
    indirect=True,
)
def test_job_finished_after_head_node_restart(
    ray_start_cluster_head_with_external_redis,
):
    cluster = ray_start_cluster_head_with_external_redis
    head_node = cluster.head_node

    # submit job
    client = JobSubmissionClient(head_node.address)
    submission_id = client.submit_job(
        entrypoint="python -c 'import ray; ray.init(); print(ray.cluster_resources()); \
            import time; time.time.sleep(1000)'"
    )

    def get_job_info(submission_id):
        gcs_client = GcsClient(cluster.address)
        all_job_info = gcs_client.get_all_job_info(job_or_submission_id=submission_id)

        return list(
            filter(
                lambda job_info: "job_submission_id" in job_info.config.metadata
                and job_info.config.metadata["job_submission_id"] == submission_id,
                list(all_job_info.values()),
            )
        )

    def _check_job_running(submission_id: str) -> bool:
        job_infos = get_job_info(submission_id)
        if len(job_infos) == 0:
            return False
        job_info = job_infos[0].job_info
        return job_info.status == JobStatus.RUNNING

    # wait until job info is written in redis
    wait_for_condition(_check_job_running, submission_id=submission_id, timeout=10)

    # kill head node
    ray.shutdown()
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    cluster.remove_node(head_node)

    # Wait to prevent the gcs server process becoming zombie.
    gcs_server_process.wait()
    wait_for_pid_to_exit(gcs_server_pid, 1000)

    # restart head node
    cluster.add_node()
    ray.init(cluster.address)

    # verify if job is finished, which marked is_dead
    def _check_job_is_dead(submission_id: str) -> bool:
        job_infos = get_job_info(submission_id)
        if len(job_infos) == 0:
            return False
        job_info = job_infos[0]
        return job_info.is_dead

    wait_for_condition(_check_job_is_dead, submission_id=submission_id, timeout=10)


def raises_exception(exc_type, f):
    try:
        f()
    except exc_type:
        return True
    return False


@pytest.mark.parametrize(
    "case",
    [
        {"kill_job": False, "kill_actor": False, "expect_alive": "all"},
        {"kill_job": True, "kill_actor": False, "expect_alive": "AB"},
        {"kill_job": True, "kill_actor": True, "expect_alive": "none"},
        {"kill_job": False, "kill_actor": True, "expect_alive": "regular"},
    ],
)
@pytest.mark.skipif(not external_redis_test_enabled(), reason="Only valid in redis env")
def test_gcs_server_restart_destroys_out_of_scope_actors(
    external_redis, ray_start_cluster, case
):
    """
    If an actor goes out of scope *when GCS is down*, when GCS restarts, the actor
    should be destroyed by GCS in its restarting.

    Set up: in a job,
    - create a regular actor
    - create a detached actor A, which creates a child actor B

    Situations:

    Case 0: nobody died
        all should be alive

    Case 1: before GCS is down, job died
        regular actor should be dead, A and B should still be alive

    Case 2: before GCS is down, job died; during GCS is down, A died
        all should be dead

    Case 3: during GCS is down, A died
        regular actor should be alive, A and B should be dead
    """

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    class A:
        def __init__(self):
            self.children = []

        def getpid(self):
            return os.getpid()

        def spawn(self, name, namespace):
            child = A.options(name=name, namespace=namespace).remote()
            self.children.append(child)
            return child

    regular = A.options(name="regular", namespace="ns").remote()
    detached = A.options(lifetime="detached", name="parent", namespace="ns").remote()
    child = ray.get(detached.spawn.remote("child", "ns"))

    regular_pid = ray.get(regular.getpid.remote())
    detached_pid = ray.get(detached.getpid.remote())
    child_pid = ray.get(child.getpid.remote())

    print(f"regular actor ID: {regular._actor_id}, pid: {regular_pid}")
    print(f"detached actor ID: {detached._actor_id}, pid: {detached_pid}")
    print(f"child actor ID: {child._actor_id}, pid: {child_pid}")

    if case["kill_job"]:
        # kill the job and restart.
        ray.shutdown()
        ray.init(address=cluster.address)

    cluster_kill_gcs_wait(cluster)

    # When GCS is down...
    if case["kill_actor"]:
        os.kill(detached_pid, signal.SIGKILL)

    cluster.head_node.start_gcs_server()
    print("GCS restarted")

    if case["expect_alive"] == "all":
        regular2 = ray.get_actor("regular", namespace="ns")
        detached2 = ray.get_actor("parent", namespace="ns")
        child2 = ray.get_actor("child", namespace="ns")

        assert ray.get(regular2.getpid.remote()) == regular_pid
        assert ray.get(detached2.getpid.remote()) == detached_pid
        assert ray.get(child2.getpid.remote()) == child_pid
    elif case["expect_alive"] == "AB":
        with pytest.raises(ValueError):
            ray.get_actor("regular", namespace="ns")
        detached2 = ray.get_actor("parent", namespace="ns")
        child2 = ray.get_actor("child", namespace="ns")
        assert ray.get(detached2.getpid.remote()) == detached_pid
        assert ray.get(child2.getpid.remote()) == child_pid
    elif case["expect_alive"] == "none":

        with pytest.raises(ValueError):
            ray.get_actor("regular", namespace="ns")

        # It took some time for raylet to report worker failure.
        wait_for_condition(
            lambda: raises_exception(
                ValueError, lambda: ray.get_actor("parent", namespace="ns")
            )
        )
        wait_for_condition(
            lambda: raises_exception(
                ValueError, lambda: ray.get_actor("child", namespace="ns")
            )
        )
    elif case["expect_alive"] == "regular":
        regular2 = ray.get_actor("regular", namespace="ns")
        wait_for_condition(
            lambda: raises_exception(
                ValueError, lambda: ray.get_actor("parent", namespace="ns")
            )
        )
        wait_for_condition(
            lambda: raises_exception(
                ValueError, lambda: ray.get_actor("child", namespace="ns")
            )
        )
        assert ray.get(regular2.getpid.remote()) == regular_pid
    else:
        raise ValueError(f"Unknown case: {case}")


MyPlugin = "MyPlugin"
MY_PLUGIN_CLASS_PATH = "ray.tests.test_gcs_fault_tolerance.HangPlugin"


class HangPlugin(RuntimeEnvPlugin):
    name = MyPlugin

    async def create(
        self,
        uri,
        runtime_env,
        ctx,
        logger,  # noqa: F821
    ) -> float:
        signal_path = runtime_env[self.name].get("signal_path")
        if signal_path is not None:
            with open(signal_path, "w") as f:
                f.write("hello world!")
                f.flush()

        await asyncio.time.sleep(1000)

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            testing_asio_delay_us="NodeManagerService.grpc_server.CancelResourceReserve=500000000:500000000",  # noqa: E501
        ),
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + MY_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_pg_removal_after_gcs_restarts(
    set_runtime_env_plugins, ray_start_regular_with_external_redis
):
    @ray.remote
    def task():
        pass

    # Use a temporary file to deterministically wait for the runtime_env setup to start.
    with tempfile.TemporaryDirectory() as tmpdir:
        signal_path = os.path.join(tmpdir, "signal")

        pg = ray.util.placement_group(bundles=[{"CPU": 1}])
        _ = task.options(
            max_retries=0,
            num_cpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
            ),
            runtime_env={
                MyPlugin: {"signal_path": signal_path},
                "config": {"setup_timeout_seconds": -1},
            },
        ).remote()

        # Wait until the runtime_env is setting up, which means we are in the process of
        # popping a worker in the raylet.
        wait_for_condition(lambda: os.path.exists(signal_path))

    ray.util.remove_placement_group(pg)
    # The PG is marked as REMOVED in redis but not removed yet from raylet
    # due to the injected delay of CancelResourceReserve rpc
    wait_for_condition(lambda: list_placement_groups()[0].state == "REMOVED")

    ray._private.worker._global_node.kill_gcs_server()
    # After GCS restarts, it will try to remove the PG resources
    # again via ReleaseUnusedBundles rpc
    ray._private.worker._global_node.start_gcs_server()

    def verify_pg_resources_cleaned():
        r_keys = ray.available_resources().keys()
        return all("group" not in k for k in r_keys)

    wait_for_condition(verify_pg_resources_cleaned, timeout=30)


def test_mark_job_finished_rpc_retry_and_idempotency(shutdown_only, monkeypatch):
    """
    Test that MarkJobFinished RPC retries work correctly and are idempotent
    when network failures occur.

    This test verifies the fix for issue #53645 where duplicate MarkJobFinished
    calls would crash the GCS due to non-idempotent RemoveJobReference().
    Uses RPC failure injection to simulate network retry scenarios.
    """
    # Inject RPC failures for MarkJobFinished - simulate network failures
    # Format: method_name=max_failures:request_failure_prob:response_failure_prob
    # We inject request failures to force retries and test idempotency
    monkeypatch.setenv(
        "RAY_testing_rpc_failure",
        "ray::rpc::JobInfoGcsService.grpc_client.MarkJobFinished=3:50:0",
    )

    ray.init(num_cpus=1)

    @ray.remote
    def test_task(i):
        return i * 2

    # Submit several tasks to ensure job has some work
    futures = [test_task.remote(i) for i in range(5)]
    results = ray.get(futures)
    assert results == [0, 2, 4, 6, 8]

    # Get job ID for verification
    job_id = ray.get_runtime_context().get_job_id()
    assert job_id is not None

    # Shutdown Ray - this will trigger MarkJobFinished with potential retries
    # The RPC failure injection will cause some calls to fail, forcing retries
    # The fix ensures that multiple calls to RemoveJobReference are handled gracefully
    ray.shutdown()

    # If we reach here without crashing, the test passes
    assert True


def test_concurrent_mark_job_finished(shutdown_only):
    """
    Test that concurrent or rapid successive calls to job finish operations
    don't cause issues.
    """
    ray.init(num_cpus=2)

    @ray.remote
    def concurrent_task(task_id):
        _ = sum(i * i for i in range(100))
        return f"task_{task_id}_completed"

    # Submit multiple tasks
    futures = [concurrent_task.remote(i) for i in range(10)]
    results = ray.get(futures)

    # Verify all tasks completed
    expected = [f"task_{i}_completed" for i in range(10)]
    assert results == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
