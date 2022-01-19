import pytest
import ray
import subprocess
import sys

from ray._private.test_utils import (Semaphore, client_test_enabled,
                                     wait_for_condition)
from ray.experimental.internal_kv import _internal_kv_list


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_initialized(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0)
    assert ray.is_initialized()


def test_initialized_local_mode(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0, local_mode=True)
    assert ray.is_initialized()


def test_ray_start_and_stop():
    for i in range(10):
        subprocess.check_call(["ray", "start", "--head"])
        subprocess.check_call(["ray", "stop"])


def test_ray_memory(shutdown_only):
    ray.init(num_cpus=1)
    subprocess.check_call(["ray", "memory"])


def test_jemalloc_env_var_propagate():
    """Test `propagate_jemalloc_env_var`"""
    gcs_ptype = ray.ray_constants.PROCESS_TYPE_GCS_SERVER
    """
    If the shared library path is not specified,
    it should return an empty dict.
    """
    expected = {}
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path="",
        jemalloc_conf="",
        jemalloc_comps=[],
        process_type=gcs_ptype)
    assert actual == expected
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=None,
        jemalloc_conf="a,b,c",
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_GCS_SERVER],
        process_type=gcs_ptype)
    assert actual == expected
    """
    When the shared library is specified
    """
    library_path = "/abc"
    expected = {"LD_PRELOAD": library_path}
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=library_path,
        jemalloc_conf="",
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_GCS_SERVER],
        process_type=gcs_ptype)
    assert actual == expected

    # comps should be a list type.
    with pytest.raises(AssertionError):
        ray._private.services.propagate_jemalloc_env_var(
            jemalloc_path=library_path,
            jemalloc_conf="",
            jemalloc_comps="ray.ray_constants.PROCESS_TYPE_GCS_SERVER,",
            process_type=gcs_ptype)

    # When comps don't match the process_type, it should return an empty dict.
    expected = {}
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=library_path,
        jemalloc_conf="",
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_RAYLET],
        process_type=gcs_ptype)
    """
    When the malloc config is specified
    """
    library_path = "/abc"
    malloc_conf = "a,b,c"
    expected = {"LD_PRELOAD": library_path, "MALLOC_CONF": malloc_conf}
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=library_path,
        jemalloc_conf=malloc_conf,
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_GCS_SERVER],
        process_type=gcs_ptype)
    assert actual == expected


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


def get_gcs_memory_used():
    import psutil
    m = sum([
        process.memory_info().rss for process in psutil.process_iter()
        if process.name() in ("gcs_server", "redis-server")
    ])
    return m


def function_entry_num(job_id):
    from ray.ray_constants import KV_NAMESPACE_FUNCTION_TABLE
    return len(_internal_kv_list(b"IsolatedExports:" + job_id,
                                 namespace=KV_NAMESPACE_FUNCTION_TABLE)) + \
        len(_internal_kv_list(b"RemoteFunction:" + job_id,
                              namespace=KV_NAMESPACE_FUNCTION_TABLE)) + \
        len(_internal_kv_list(b"ActorClass:" + job_id,
                              namespace=KV_NAMESPACE_FUNCTION_TABLE))


@pytest.mark.skipif(
    client_test_enabled(),
    reason="client api doesn't support namespace right now.")
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
    job_id = ray.worker.global_worker.current_job_id.hex().encode()
    assert function_entry_num(job_id) > 0
    ray.shutdown()

    # now check the function table is cleaned up after job finished
    ray.init(address="auto", namespace="a")
    wait_for_condition(lambda: function_entry_num(job_id) == 0, timeout=30)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="client api doesn't support namespace right now.")
def test_function_table_gc_actor(call_ray_start):
    """If there is a detached actor, the table won't be cleaned up.
    """
    ray.init(address="auto", namespace="a")

    @ray.remote
    class Actor:
        def ready(self):
            return

    # If there is a detached actor, the function won't be deleted.
    a = Actor.options(lifetime="detached", name="a").remote()
    ray.get(a.ready.remote())
    job_id = ray.worker.global_worker.current_job_id.hex().encode()
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
    job_id = ray.worker.global_worker.current_job_id.hex().encode()
    ray.shutdown()
    ray.init(address="auto", namespace="c")
    wait_for_condition(lambda: function_entry_num(job_id) == 0)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
