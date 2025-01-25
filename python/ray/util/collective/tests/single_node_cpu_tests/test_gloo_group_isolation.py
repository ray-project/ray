from python.ray.util.collective.types import Backend
from python.ray.util.collective.collective_group.gloo_collective_group import GLOOGroup
import ray
import ray.util.collective as col
import time


@ray.remote
class Worker:
    def __init__(self):
        pass

    def init_gloo_group(
        self, world_size: int, rank: int, group_name: str, gloo_timeout: int = 30000
    ):
        col.init_collective_group(
            world_size, rank, Backend.GLOO, group_name, gloo_timeout
        )
        return True

    def get_gloo_timeout(self, group_name: str) -> int:
        g = col.get_group_handle(group_name)
        # Check if the group is initialized correctly
        assert isinstance(g, GLOOGroup)
        return g._gloo_context.getTimeout()


def test_two_groups_in_one_cluster(ray_start_regular_shared):
    name1 = "name_1"
    name2 = "name_2"
    time1 = 40000
    time2 = 60000
    w1 = Worker.remote()
    ret1 = w1.init_gloo_group.remote(1, 0, name1, time1)
    w2 = Worker.remote()
    ret2 = w2.init_gloo_group.remote(1, 0, name2, time2)
    assert ray.get(ret1)
    assert ray.get(ret2)
    assert ray.get(w1.get_gloo_timeout.remote(name1)) == time1
    assert ray.get(w2.get_gloo_timeout.remote(name2)) == time2


def test_failure_when_initializing(shutdown_only):
    # job1
    ray.init()
    w1 = Worker.remote()
    ret1 = w1.init_gloo_group.remote(2, 0, "name_1")
    ray.wait([ret1], timeout=1)
    time.sleep(5)
    ray.shutdown()

    # job2
    ray.init()
    w2 = Worker.remote()
    ret2 = w2.init_gloo_group.remote(1, 0, "name_1")
    assert ray.get(ret2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
