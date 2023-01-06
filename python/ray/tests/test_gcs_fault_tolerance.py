import sys
import os
import threading
from time import sleep

import pytest

import ray
from ray._private.utils import get_or_create_event_loop
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    convert_actor_state,
    generate_system_config_map,
    wait_for_condition,
    wait_for_pid_to_exit,
)

import psutil


@ray.remote
class Increase:
    def method(self, x):
        return x + 2


@ray.remote
def increase(x):
    return x + 1


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_failover_worker_reconnect_timeout=20,
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
            gcs_failover_worker_reconnect_timeout=20,
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
            gcs_failover_worker_reconnect_timeout=2,
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

    head_node = cluster.head_node
    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid
    # Kill gcs server.
    cluster.head_node.kill_gcs_server()
    # Wait to prevent the gcs server process becoming zombie.
    gcs_server_process.wait()
    wait_for_pid_to_exit(gcs_server_pid, 1000)

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
            gcs_failover_worker_reconnect_timeout=20,
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
            gcs_failover_worker_reconnect_timeout=20,
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

    # If `PollOwnerForActorOutOfScope` was successfully called,
    # name should be properly deleted.
    with pytest.raises(ValueError):
        ray.get_actor("abc")


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        generate_system_config_map(
            gcs_failover_worker_reconnect_timeout=20,
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
            gcs_failover_worker_reconnect_timeout=20,
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
            gcs_failover_worker_reconnect_timeout=20,
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


@pytest.mark.parametrize("auto_reconnect", [True, False])
def test_gcs_client_reconnect(ray_start_regular_with_external_redis, auto_reconnect):
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = gcs_utils.GcsClient(
        address=gcs_address, nums_reconnect_retry=20 if auto_reconnect else 0
    )

    gcs_client.internal_kv_put(b"a", b"b", True, None)
    assert gcs_client.internal_kv_get(b"a", None) == b"b"

    passed = [False]

    def kv_get():
        if not auto_reconnect:
            with pytest.raises(Exception):
                gcs_client.internal_kv_get(b"a", None)
        else:
            assert gcs_client.internal_kv_get(b"a", None) == b"b"
        passed[0] = True

    ray._private.worker._global_node.kill_gcs_server()
    t = threading.Thread(target=kv_get)
    t.start()
    sleep(5)
    ray._private.worker._global_node.start_gcs_server()
    t.join()
    assert passed[0]


@pytest.mark.parametrize("auto_reconnect", [True, False])
def test_gcs_aio_client_reconnect(
    ray_start_regular_with_external_redis, auto_reconnect
):
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = gcs_utils.GcsClient(address=gcs_address)

    gcs_client.internal_kv_put(b"a", b"b", True, None)
    assert gcs_client.internal_kv_get(b"a", None) == b"b"

    passed = [False]

    async def async_kv_get():
        gcs_aio_client = gcs_utils.GcsAioClient(
            address=gcs_address, nums_reconnect_retry=20 if auto_reconnect else 0
        )
        if not auto_reconnect:
            with pytest.raises(Exception):
                await gcs_aio_client.internal_kv_get(b"a", None)
        else:
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
                gcs_failover_worker_reconnect_timeout=20,
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
                gcs_failover_worker_reconnect_timeout=20,
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

    print("Start to create a new actor")
    with pytest.raises(ray.exceptions.GetTimeoutError):
        cc = Counter.options(name="cc", lifetime="detached").remote()

    assert ray.get(c.r.remote(10)) == 10
    ray.worker._global_node.start_gcs_server()
    cc = Counter.options(name="cc", lifetime="detached").remote()
    assert ray.get(cc.r.remote(10)) == 10


@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_failover_worker_reconnect_timeout=20,
                gcs_rpc_server_reconnect_timeout_s=3600,
            ),
            "namespace": "actor",
        }
    ],
    indirect=True,
)
def test_pg_actor_workloads(ray_start_regular_with_external_redis):
    from ray.util.placement_group import placement_group

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
            gcs_failover_worker_reconnect_timeout=20,
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


if __name__ == "__main__":

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
