import os
import subprocess
import sys

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._common.network_utils import parse_address
from ray._common.test_utils import Semaphore, wait_for_condition
from ray._private.test_utils import (
    client_test_enabled,
    external_redis_test_enabled,
    get_gcs_memory_used,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
)
from ray._raylet import GCS_PID_KEY, GcsClient
from ray.experimental.internal_kv import _internal_kv_list
from ray.tests.conftest import call_ray_start

import psutil


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_back_pressure(shutdown_only_with_initialization_check):
    ray.init()

    signal_actor = Semaphore.options(max_pending_calls=10).remote(value=0)

    try:
        for i in range(10):
            signal_actor.acquire.remote()
    except ray.exceptions.PendingCallsLimitExceeded:
        assert False

    with pytest.raises(ray.exceptions.PendingCallsLimitExceeded):
        signal_actor.acquire.remote()

    @ray.remote
    def release(signal_actor):
        ray.get(signal_actor.release.remote())
        return 1

    # Release signal actor through common task,
    # because actor tasks will be back pressured
    for i in range(10):
        ray.get(release.remote(signal_actor))

    # Check whether we can call remote actor normally after
    # back presssure released.
    try:
        signal_actor.acquire.remote()
    except ray.exceptions.PendingCallsLimitExceeded:
        assert False

    ray.shutdown()


def test_local_mode_deadlock(shutdown_only_with_initialization_check):
    ray.init(local_mode=True)

    @ray.remote
    class Foo:
        def __init__(self):
            pass

        def ping_actor(self, actor):
            actor.ping.remote()
            return 3

    @ray.remote
    class Bar:
        def __init__(self):
            pass

        def ping(self):
            return 1

    foo = Foo.remote()
    bar = Bar.remote()
    # Expect ping_actor call returns normally without deadlock.
    assert ray.get(foo.ping_actor.remote(bar)) == 3


def function_entry_num(job_id):
    from ray._private.ray_constants import KV_NAMESPACE_FUNCTION_TABLE

    return (
        len(
            _internal_kv_list(
                b"RemoteFunction:" + job_id, namespace=KV_NAMESPACE_FUNCTION_TABLE
            )
        )
        + len(
            _internal_kv_list(
                b"ActorClass:" + job_id, namespace=KV_NAMESPACE_FUNCTION_TABLE
            )
        )
        + len(
            _internal_kv_list(
                b"FunctionsToRun:" + job_id, namespace=KV_NAMESPACE_FUNCTION_TABLE
            )
        )
    )


@pytest.mark.skipif(
    client_test_enabled(), reason="client api doesn't support namespace right now."
)
def test_function_table_gc(call_ray_start):
    """This test tries to verify that function table is cleaned up
    after job exits.
    """

    def f():
        data = "0" * 1024 * 1024  # 1MB

        @ray.remote
        def r():
            nonlocal data

            @ray.remote
            class Actor:
                pass

        return r.remote()

    ray.init(address="auto", namespace="b")

    # It should use > 500MB data
    ray.get([f() for _ in range(500)])

    # It's not working on win32.
    if sys.platform != "win32":
        assert get_gcs_memory_used() > 500 * 1024 * 1024
    job_id = ray._private.worker.global_worker.current_job_id.hex().encode()
    assert function_entry_num(job_id) > 0
    ray.shutdown()

    # now check the function table is cleaned up after job finished
    ray.init(address="auto", namespace="a")
    wait_for_condition(lambda: function_entry_num(job_id) == 0, timeout=30)


@pytest.mark.skipif(
    client_test_enabled(), reason="client api doesn't support namespace right now."
)
def test_function_table_gc_actor(call_ray_start):
    """If there is a detached actor, the table won't be cleaned up."""
    ray.init(address="auto", namespace="a")

    @ray.remote
    class Actor:
        def ready(self):
            return

    # If there is a detached actor, the function won't be deleted.
    a = Actor.options(lifetime="detached", name="a").remote()
    ray.get(a.ready.remote())
    job_id = ray._private.worker.global_worker.current_job_id.hex().encode()
    ray.shutdown()

    ray.init(address="auto", namespace="b")
    with pytest.raises(Exception):
        wait_for_condition(lambda: function_entry_num(job_id) == 0)
    a = ray.get_actor("a", namespace="a")
    ray.kill(a)
    wait_for_condition(lambda: function_entry_num(job_id) == 0)

    # If there is not a detached actor, it'll be deleted when the job finishes.
    a = Actor.remote()
    ray.get(a.ready.remote())
    job_id = ray._private.worker.global_worker.current_job_id.hex().encode()
    ray.shutdown()
    ray.init(address="auto", namespace="c")
    wait_for_condition(lambda: function_entry_num(job_id) == 0)


def test_node_liveness_after_restart(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(cluster.address)
    worker = cluster.add_node(node_manager_port=9037)
    wait_for_condition(lambda: len([n for n in ray.nodes() if n["Alive"]]) == 2)

    cluster.remove_node(worker)
    wait_for_condition(lambda: len([n for n in ray.nodes() if n["Alive"]]) == 1)
    worker = cluster.add_node(node_manager_port=9037)
    wait_for_condition(lambda: len([n for n in ray.nodes() if n["Alive"]]) == 2)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="This test is only run on linux machines.",
)
def test_worker_oom_score(shutdown_only):
    @ray.remote
    def get_oom_score():
        pid = os.getpid()
        with open(f"/proc/{pid}/oom_score", "r") as f:
            oom_score = f.read()
            return int(oom_score)

    assert ray.get(get_oom_score.remote()) >= 1000


call_ray_start_2 = call_ray_start


@pytest.mark.skipif(not external_redis_test_enabled(), reason="Only valid in redis env")
@pytest.mark.parametrize(
    "call_ray_start,call_ray_start_2",
    [
        (
            {"env": {"RAY_external_storage_namespace": "A1"}},
            {"env": {"RAY_external_storage_namespace": "A2"}},
        )
    ],
    indirect=True,
)
def test_storage_isolation(external_redis, call_ray_start, call_ray_start_2):
    script = """
import ray
ray.init("{address}", namespace="a")
@ray.remote
class A:
    def ready(self):
        return {val}
    pass

a = A.options(lifetime="detached", name="A").remote()
assert ray.get(a.ready.remote()) == {val}
assert ray.get_runtime_context().get_job_id() == '01000000'
    """
    run_string_as_driver(script.format(address=call_ray_start, val=1))
    run_string_as_driver(script.format(address=call_ray_start_2, val=2))

    script = """
import ray
ray.init("{address}", namespace="a")
a = ray.get_actor(name="A")
assert ray.get(a.ready.remote()) == {val}
assert ray.get_runtime_context().get_job_id() == '02000000'
"""
    run_string_as_driver(script.format(address=call_ray_start, val=1))
    run_string_as_driver(script.format(address=call_ray_start_2, val=2))


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_gcs_connection_no_leak(ray_start_cluster):
    cluster = ray_start_cluster
    head_node = cluster.add_node()

    gcs_server_process = head_node.all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    def get_gcs_num_of_connections():
        p = psutil.Process(gcs_server_pid)
        num_connections = len(p.connections())
        print(">>", num_connections)
        return num_connections

    @ray.remote
    class GcsKVActor:
        def __init__(self, address):
            self.gcs_client = GcsClient(address=address)
            self.gcs_client.internal_kv_get(
                GCS_PID_KEY.encode(),
            )

        def ready(self):
            return "WORLD"

    @ray.remote
    class A:
        def ready(self):
            print("HELLO")
            return "WORLD"

    gcs_kv_actor = None

    with ray.init(cluster.address):
        # Wait for workers  to be ready.
        gcs_kv_actor = GcsKVActor.remote(cluster.address)
        _ = ray.get(gcs_kv_actor.ready.remote())
        # Note: `fds_with_some_workers` need to be recorded *after* `ray.init`, because
        # a prestarted worker is started on the first driver init. This worker keeps 1
        # connection to the GCS, and it stays alive even after the driver exits. If
        # we move this line before `ray.init`, we will find 1 extra connection after
        # the driver exits.
        fds_with_some_workers = get_gcs_num_of_connections()
        num_of_actors = 10
        actors = [A.remote() for _ in range(num_of_actors)]
        print(ray.get([t.ready.remote() for t in actors]))

        # Kill the actors
        del actors

    # Make sure the # of fds opened by the GCS dropped.
    # This assumes worker processes are not created after the actor worker
    # processes die.
    wait_for_condition(lambda: get_gcs_num_of_connections() < fds_with_some_workers)
    num_fds_after_workers_die = get_gcs_num_of_connections()

    n = cluster.add_node(wait=True)

    # Make sure the # of fds opened by the GCS increased.
    wait_for_condition(lambda: get_gcs_num_of_connections() > num_fds_after_workers_die)

    cluster.remove_node(n)

    # Make sure the # of fds opened by the GCS dropped.
    wait_for_condition(lambda: get_gcs_num_of_connections() < fds_with_some_workers)


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --num-cpus=2"],
    indirect=True,
)
def test_demands_when_driver_exits(call_ray_start):
    script = f"""
import ray
ray.init(address='{call_ray_start}')

import os
import time
@ray.remote(num_cpus=3)
def use_gpu():
    pass

@ray.remote(num_gpus=10)
class A:
    pass

A.options(name="a", lifetime="detached").remote()

print(ray.get([use_gpu.remote(), use_gpu.remote()]))
"""

    proc = run_string_as_driver_nonblocking(script)
    gcs_cli = ray._raylet.GcsClient(address=f"{call_ray_start}")

    def check_demands(n):
        status = gcs_cli.internal_kv_get(
            ray._private.ray_constants.DEBUG_AUTOSCALING_STATUS.encode(), namespace=None
        )
        import json

        status = json.loads(status.decode())
        return len(status["load_metrics_report"]["resource_demand"]) == n

    wait_for_condition(lambda: check_demands(2))
    proc.terminate()
    wait_for_condition(lambda: check_demands(1))


@pytest.mark.skipif(external_redis_test_enabled(), reason="Only valid in non redis env")
def test_redis_not_available(monkeypatch, call_ray_stop_only):
    monkeypatch.setenv("RAY_redis_db_connect_retries", "5")
    monkeypatch.setenv("RAY_REDIS_ADDRESS", "localhost:12345")

    p = subprocess.run(
        "ray start --head",
        shell=True,
        capture_output=True,
    )
    assert "Could not establish connection to Redis" in p.stderr.decode()
    assert "Please check " in p.stderr.decode()
    assert "redis storage is alive or not." in p.stderr.decode()


@pytest.mark.skipif(not external_redis_test_enabled(), reason="Only valid in redis env")
def test_redis_wrong_password(monkeypatch, external_redis, call_ray_stop_only):
    monkeypatch.setenv("RAY_redis_db_connect_retries", "5")
    p = subprocess.run(
        "ray start --head  --redis-password=1234",
        shell=True,
        capture_output=True,
    )

    assert "RedisError: ERR AUTH <password> called" in p.stderr.decode()


@pytest.mark.skipif(not external_redis_test_enabled(), reason="Only valid in redis env")
def test_redis_full(ray_start_cluster_head):
    import redis

    gcs_address = ray_start_cluster_head.gcs_address
    redis_addr = os.environ["RAY_REDIS_ADDRESS"]
    host, port = parse_address(redis_addr)
    if os.environ.get("TEST_EXTERNAL_REDIS_REPLICAS", "1") != "1":
        cli = redis.RedisCluster(host, int(port))
    else:
        cli = redis.Redis(host, int(port))
    # Set the max memory to 10MB
    cli.config_set("maxmemory", 5 * 1024 * 1024)

    gcs_cli = ray._raylet.GcsClient(address=gcs_address)
    # GCS should fail
    # GcsClient assumes GCS is HA so it keeps retrying, although GCS is down. We must
    # set timeout for this.
    with pytest.raises(ray.exceptions.RpcError):
        gcs_cli.internal_kv_put(b"A", b"A" * 6 * 1024 * 1024, True, timeout=5)
    logs_dir = ray_start_cluster_head.head_node._logs_dir

    with open(os.path.join(logs_dir, "gcs_server.err")) as err:
        assert "OOM command not allowed when used" in err.read()


def test_omp_threads_set_third_party(ray_start_cluster, monkeypatch):
    ###########################
    # Test the OMP_NUM_THREADS are picked up by 3rd party libraries
    # when running tasks if no OMP_NUM_THREADS is set by user.
    # e.g. numpy, numexpr
    ###########################
    with monkeypatch.context() as m:
        m.delenv("OMP_NUM_THREADS", raising=False)

        cluster = ray_start_cluster
        cluster.add_node(num_cpus=4)
        ray.init(address=cluster.address)

        @ray.remote(num_cpus=2)
        def f():
            # Assert numpy using 2 threads for it's parallelism backend.
            import numpy  # noqa: F401
            from threadpoolctl import threadpool_info

            for pool_info in threadpool_info():
                assert pool_info["num_threads"] == 2

            import numexpr

            assert numexpr.nthreads == 2
            return True

        assert ray.get(f.remote())


def test_gcs_fd_usage(shutdown_only):
    ray.init(
        _system_config={
            "prestart_worker_first_driver": False,
            "enable_worker_prestart": False,
        },
    )
    gcs_process = ray._private.worker._global_node.all_processes["gcs_server"][0]
    gcs_process = psutil.Process(gcs_process.process.pid)
    print("GCS connections", len(gcs_process.connections()))

    @ray.remote(runtime_env={"env_vars": {"Hello": "World"}})
    class A:
        def f(self):
            return os.environ.get("Hello")

    # In case there are still some pre-start workers, consume all of them
    aa = [A.remote() for _ in range(32)]
    for a in aa:
        assert ray.get(a.f.remote()) == "World"
    base_fd_num = len(gcs_process.connections())
    print("GCS connections", base_fd_num)

    bb = [A.remote() for _ in range(16)]
    for b in bb:
        assert ray.get(b.f.remote()) == "World"
    new_fd_num = len(gcs_process.connections())
    print("GCS connections", new_fd_num)
    # each worker has two connections:
    #   GCS -> CoreWorker
    #   CoreWorker -> GCS
    # Sometimes, there is one more sockets opened. The reason
    # is still unknown.
    assert (new_fd_num - base_fd_num) <= len(bb) * 2 + 1


@pytest.mark.skipif(
    sys.platform != "linux", reason="jemalloc is only prebuilt on linux"
)
def test_jemalloc_ray_start(monkeypatch, ray_start_cluster):
    def check_jemalloc_enabled(pid=None):
        if pid is None:
            pid = os.getpid()
        pmap = subprocess.run(
            ["pmap", str(pid)], check=True, text=True, stdout=subprocess.PIPE
        )
        return "libjemalloc.so" in pmap.stdout

    # Firstly, remove the LD_PRELOAD and make sure
    # jemalloc is loaded.
    monkeypatch.delenv("LD_PRELOAD", False)
    cluster = ray_start_cluster
    node = cluster.add_node(num_cpus=1)

    # Make sure raylet/gcs/worker all have jemalloc
    assert check_jemalloc_enabled(
        node.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.pid
    )
    assert check_jemalloc_enabled(
        node.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.pid
    )
    assert not ray.get(ray.remote(check_jemalloc_enabled).remote())

    ray.shutdown()
    cluster.shutdown()

    monkeypatch.setenv("LD_PRELOAD", "")
    node = cluster.add_node(num_cpus=1)
    # Make sure raylet/gcs/worker all have jemalloc
    assert not check_jemalloc_enabled(
        node.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0].process.pid
    )
    assert not check_jemalloc_enabled(
        node.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.pid
    )
    assert not ray.get(ray.remote(check_jemalloc_enabled).remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
