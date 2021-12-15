import pytest
import ray
import subprocess
import sys
from ray._private.test_utils import Semaphore


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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
