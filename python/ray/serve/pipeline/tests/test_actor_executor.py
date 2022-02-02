import os

from ray.serve import pipeline


def test_num_replicas(shared_ray_instance):
    @pipeline.step(execution_mode="ACTORS", num_replicas=3)
    def get_pid(arg):
        return os.getpid()

    get_pid = get_pid(pipeline.INPUT).deploy()
    assert len({get_pid.call("") for _ in range(100)}) == 3


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
