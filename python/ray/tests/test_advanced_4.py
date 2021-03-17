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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
