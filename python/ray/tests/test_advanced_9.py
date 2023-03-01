import sys
import time

import pytest

import ray
from ray._private.test_utils import (
    Semaphore,
    enable_external_redis,
    client_test_enabled,
    run_string_as_driver,
    wait_for_condition,
    get_gcs_memory_used,
)
from ray.experimental.internal_kv import _internal_kv_list
from ray.tests.conftest import call_ray_start


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
                b"IsolatedExports:" + job_id, namespace=KV_NAMESPACE_FUNCTION_TABLE
            )
        )
        + len(
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
    worker = cluster.add_node(node_manager_port=9037)
    for _ in range(10):
        wait_for_condition(lambda: len([n for n in ray.nodes() if n["Alive"]]) == 2)
        time.sleep(1)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="This test is only run on linux machines.",
)
def test_worker_oom_score(shutdown_only):
    @ray.remote
    def get_oom_score():
        import os

        pid = os.getpid()
        with open(f"/proc/{pid}/oom_score", "r") as f:
            oom_score = f.read()
            return int(oom_score)

    assert ray.get(get_oom_score.remote()) >= 1000


call_ray_start_2 = call_ray_start


@pytest.mark.skipif(not enable_external_redis(), reason="Only valid in redis env")
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


if __name__ == "__main__":
    import pytest
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
