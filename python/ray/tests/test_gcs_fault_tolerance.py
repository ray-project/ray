import sys
import asyncio
import os
import threading
from time import sleep
import signal

import pytest

import ray
from ray._private.utils import get_or_create_event_loop
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
import ray._private.gcs_utils as gcs_utils
from ray._private import ray_constants
from ray._private.test_utils import (
    convert_actor_state,
    enable_external_redis,
    generate_system_config_map,
    wait_for_condition,
    wait_for_pid_to_exit,
    run_string_as_driver,
    redis_sentinel_replicas,
)
from ray.job_submission import JobSubmissionClient, JobStatus
from ray._raylet import GcsClient
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
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


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
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


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
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
            gcs_rpc_server_reconnect_timeout_s=60,
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

    cluster_kill_gcs_wait(cluster)

    # Restart gcs server process.
    cluster.head_node.start_gcs_server()

    from ray.autoscaler.v2.sdk import get_cluster_status

    status = get_cluster_status(ray.get_runtime_context().gcs_address)
    assert len(status.idle_nodes) == 2


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
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


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
def test_actor_raylet_resubscription(ray_start_regular_with_external_redis):
    # stat an actor
    @ray.remote
    class A:
        def ready(self):
            import os

            return os.getpid()

    actor = A.options(name="abc", max_restarts=0).remote()
    pid = ray.get(actor.ready.remote())
    print("actor is ready and kill gcs")

    ray._private.worker._global_node.kill_gcs_server()

    print("make actor exit")
    import psutil

    p = psutil.Process(pid)
    p.kill()
    from time import sleep

    sleep(1)
    print("start gcs")
    ray._private.worker._global_node.start_gcs_server()

    print("try actor method again")
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.ready.remote())


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
def test_del_actor_after_gcs_server_restart(ray_start_regular_with_external_redis):
    actor = Increase.options(name="abc").remote()
    result = ray.get(actor.method.remote(1))
    assert result == 3

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    actor_id = actor._actor_id.hex()
    del actor

    def condition():
        actor_status = ray._private.state.actors(actor_id=actor_id)
        if actor_status["State"] == convert_actor_state(gcs_utils.ActorTableData.DEAD):
            return True
        else:
            return False

    # Wait for the actor dead.
    wait_for_condition(condition, timeout=10)

    # If `ReportActorOutOfScope` was successfully called,
    # name should be properly deleted.
    with pytest.raises(ValueError):
        ray.get_actor("abc")


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
def test_worker_raylet_resubscription(tmp_path, ray_start_regular_with_external_redis):
    # This test is to make sure resubscription in raylet is working.
    # When subscription failed, raylet will not get worker failure error
    # and thus, it won't kill the worker which is fate sharing with the failed
    # one.

    @ray.remote
    def long_run():
        from time import sleep

        print("LONG_RUN")
        import os

        (tmp_path / "long_run.pid").write_text(str(os.getpid()))
        sleep(10000)

    @ray.remote
    def bar():
        import os

        return (
            os.getpid(),
            # Use runtime env to make sure task is running in a different
            # ray worker
            long_run.options(runtime_env={"env_vars": {"P": ""}}).remote(),
        )

    (pid, obj_ref) = ray.get(bar.remote())

    long_run_pid = None

    def condition():
        nonlocal long_run_pid
        long_run_pid = int((tmp_path / "long_run.pid").read_text())
        return True

    wait_for_condition(condition, timeout=5)

    # kill the gcs
    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()
    # make sure resubscription is done
    # TODO(iycheng): The current way of resubscription potentially will lose
    # worker failure message because we don't ask for the snapshot of worker
    # status for now. We need to fix it.
    sleep(4)

    # then kill the owner
    p = psutil.Process(pid)
    p.kill()

    # The long_run_pid should exit
    wait_for_pid_to_exit(long_run_pid, 5)


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
def test_core_worker_resubscription(tmp_path, ray_start_regular_with_external_redis):
    # This test is to ensure core worker will resubscribe to GCS after GCS
    # restarts.
    from filelock import FileLock

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


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
        )
    ],
    indirect=True,
)
def test_detached_actor_restarts(ray_start_regular_with_external_redis):
    # Detached actors are owned by GCS. This test is to ensure detached actors
    # can restart even GCS restarts.

    @ray.remote
    class A:
        def ready(self):
            import os

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
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = ray._raylet.GcsClient(address=gcs_address)

    gcs_client.internal_kv_put(b"a", b"b", True, None)
    assert gcs_client.internal_kv_get(b"a", None) == b"b"

    passed = [False]

    def kv_get():
        assert gcs_client.internal_kv_get(b"a", None) == b"b"
        passed[0] = True

    ray._private.worker._global_node.kill_gcs_server()
    t = threading.Thread(target=kv_get)
    t.start()
    sleep(5)
    ray._private.worker._global_node.start_gcs_server()
    t.join()
    assert passed[0]


def test_gcs_aio_client_reconnect(ray_start_regular_with_external_redis):
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = ray._raylet.GcsClient(address=gcs_address)

    gcs_client.internal_kv_put(b"a", b"b", True, None)
    assert gcs_client.internal_kv_get(b"a", None) == b"b"

    passed = [False]

    async def async_kv_get():
        gcs_aio_client = gcs_utils.GcsAioClient(address=gcs_address)
        assert await gcs_aio_client.internal_kv_get(b"a", None) == b"b"
        return True

    def kv_get():
        import asyncio

        asyncio.set_event_loop(asyncio.new_event_loop())
        passed[0] = get_or_create_event_loop().run_until_complete(async_kv_get())

    ray._private.worker._global_node.kill_gcs_server()
    t = threading.Thread(target=kv_get)
    t.start()
    sleep(5)
    ray._private.worker._global_node.start_gcs_server()
    t.join()
    assert passed[0]


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
    """This test cover the case to create actor while gcs is down
    and also make sure existing actor continue to work even when
    GCS is down.
    """

    @ray.remote
    class Counter:
        def r(self, v):
            return v

    c = Counter.remote()
    r = ray.get(c.r.remote(10))
    assert r == 10

    print("GCS is killed")
    ray._private.worker._global_node.kill_gcs_server()

    print("Start to create a new actor")
    cc = Counter.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(cc.r.remote(10), timeout=5)

    assert ray.get(c.r.remote(10)) == 10
    ray._private.worker._global_node.start_gcs_server()

    def f():
        assert ray.get(cc.r.remote(10)) == 10

    t = threading.Thread(target=f)
    t.start()
    t.join()

    c = Counter.options(lifetime="detached", name="C").remote()

    assert ray.get(c.r.remote(10)) == 10

    ray._private.worker._global_node.kill_gcs_server()

    sleep(2)

    assert ray.get(c.r.remote(10)) == 10

    ray._private.worker._global_node.start_gcs_server()

    from ray._private.test_utils import run_string_as_driver

    run_string_as_driver(
        """
import ray
ray.init('auto', namespace='actor')
a = ray.get_actor("C")
assert ray.get(a.r.remote(10)) == 10
"""
    )


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_rpc_server_reconnect_timeout_s=3600,
                gcs_server_request_timeout_seconds=10,
            ),
            "namespace": "actor",
        }
    ],
    indirect=True,
)
def test_named_actor_workloads(ray_start_regular_with_external_redis):
    """This test cover the case to create actor while gcs is down
    and also make sure existing actor continue to work even when
    GCS is down.
    """

    @ray.remote
    class Counter:
        def r(self, v):
            return v

    c = Counter.options(name="c", lifetime="detached").remote()
    r = ray.get(c.r.remote(10))
    assert r == 10

    print("GCS is killed")
    ray.worker._global_node.kill_gcs_server()

    # detached actor should keep working
    assert ray.get(c.r.remote(10)) == 10

    print("Start to create a new actor")
    with pytest.raises(ray.exceptions.GetTimeoutError):
        cc = Counter.options(name="cc", lifetime="detached").remote()

    ray.worker._global_node.start_gcs_server()
    cc = Counter.options(name="cc", lifetime="detached").remote()
    assert ray.get(cc.r.remote(10)) == 10


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
            import os

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
            gcs_rpc_server_reconnect_timeout_s=60,
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
            gcs_rpc_server_reconnect_timeout_s=60,
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

    publisher = ray._raylet.GcsPublisher(address=gcs_server_addr)
    print("sending error message 1")
    publisher.publish_error(b"aaa_id", "", "test error message 1")

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    print("sending error message 2")
    publisher.publish_error(b"bbb_id", "", "test error message 2")
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
            gcs_rpc_server_reconnect_timeout_s=60,
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
    ip, port = redis_addr.split(":")
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
        ip, port = addr.split(":")
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
            import os

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
    sleep(2)
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
            gcs_rpc_server_reconnect_timeout_s=60,
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
    ip, port = redis_addr.split(":")
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
            import os

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
    sleep(2)
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
        )
    ],
    indirect=True,
)
def test_cluster_id(ray_start_regular):
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
        sleep(1)

    ray._private.worker._global_node.kill_gcs_server()
    ray._private.worker._global_node.start_gcs_server()

    if not enable_external_redis():
        # Waiting for raylet to become unhealthy
        wait_for_condition(lambda: not check_raylet_healthy())
    else:
        # Waiting for raylet to stay healthy
        for i in range(10):
            assert check_raylet_healthy()
            sleep(1)


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

    if not enable_external_redis():
        assert session_dir != new_session_dir
    else:
        assert session_dir == new_session_dir


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
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

    ip, port = redis_addr.split(":")
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
        import subprocess

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
            import time; time.sleep(1000)'"
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
@pytest.mark.skipif(not enable_external_redis(), reason="Only valid in redis env")
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
        while True:
            await asyncio.sleep(1)

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_rpc_server_reconnect_timeout_s=60,
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
def test_placement_group_removal_after_gcs_restarts(
    set_runtime_env_plugins, ray_start_regular_with_external_redis
):
    @ray.remote
    def task():
        pass

    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    _ = task.options(
        max_retries=0,
        num_cpus=1,
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        ),
        runtime_env={
            MyPlugin: {"name": "f2"},
            "config": {"setup_timeout_seconds": -1},
        },
    ).remote()

    # The task should be popping worker
    # TODO(jjyao) Use a more determinstic way to
    # decide whether the task is popping worker
    sleep(5)

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


if __name__ == "__main__":

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
