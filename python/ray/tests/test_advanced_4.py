import pytest
import ray
import subprocess
import sys


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
        jemalloc_path="", jemalloc_conf="", jemalloc_comps=[], process_type=gcs_ptype
    )
    assert actual == expected
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=None,
        jemalloc_conf="a,b,c",
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_GCS_SERVER],
        process_type=gcs_ptype,
    )
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
        process_type=gcs_ptype,
    )
    assert actual == expected

    # comps should be a list type.
    with pytest.raises(AssertionError):
        ray._private.services.propagate_jemalloc_env_var(
            jemalloc_path=library_path,
            jemalloc_conf="",
            jemalloc_comps="ray.ray_constants.PROCESS_TYPE_GCS_SERVER,",
            process_type=gcs_ptype,
        )

    # When comps don't match the process_type, it should return an empty dict.
    expected = {}
    actual = ray._private.services.propagate_jemalloc_env_var(
        jemalloc_path=library_path,
        jemalloc_conf="",
        jemalloc_comps=[ray.ray_constants.PROCESS_TYPE_RAYLET],
        process_type=gcs_ptype,
    )
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
        process_type=gcs_ptype,
    )
    assert actual == expected


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
